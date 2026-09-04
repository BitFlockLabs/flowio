//! Process-isolated runtime longevity and resource-reclamation oracle.

// Live io_uring, sockets, `/proc`, and child processes are not available to
// Miri. Allocator and list coverage that needs none of them still runs there.
#![cfg(not(miri))]

mod common;
#[path = "common/runtime_longevity.rs"]
mod runtime_longevity_support;

use flowio::net::tcp::TcpStream;
use flowio::net::unix::UnixStream;
use flowio::runtime::buffer::IoBuffMut;
use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;
use flowio::runtime::executor::Executor;
use flowio::runtime::timer::{TimeoutError, timeout};
use runtime_longevity_support::{
    SlabPlateau, assert_fd_count_instrument_discriminates, assert_quiescent, process_fd_count,
};
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::time::Duration;

const CHILD_ENV: &str = "FLOWIO_RUNTIME_LONGEVITY_CHILD";
const TEST_NAME: &str = "runtime_tcp_unix_longevity_reclaims_each_batch";
const CHILD_DEADLINE: Duration = Duration::from_secs(60);
const WARMUP_CYCLES: usize = 1_000;
const BATCHES: usize = 10;
const CYCLES_PER_BATCH: usize = 1_000;
const CYCLE_WINDOW: usize = 16;
const CANCEL_AFTER: Duration = Duration::from_millis(1);
const READ_IOVECS: usize = 17;

enum SocketCycle {
    Unix {
        reader: UnixStream,
        peer: UnixStream,
    },
    Tcp {
        reader: TcpStream,
        peer: TcpStream,
    },
}

impl SocketCycle {
    async fn cancel_vectored_read(self) {
        match self {
            Self::Unix { mut reader, peer } => {
                let result = timeout(CANCEL_AFTER, reader.readv(read_chain())).await;
                assert!(
                    matches!(result, Err(TimeoutError::Elapsed)),
                    "Unix read completed before its cancellation deadline"
                );
                drop(peer);
                drop(reader);
            }
            Self::Tcp { mut reader, peer } => {
                let result = timeout(CANCEL_AFTER, reader.readv(read_chain())).await;
                assert!(
                    matches!(result, Err(TimeoutError::Elapsed)),
                    "TCP read completed before its cancellation deadline"
                );
                drop(peer);
                drop(reader);
            }
        }
    }
}

fn read_chain() -> IoBuffVecMut<READ_IOVECS> {
    let mut chain = IoBuffVecMut::new();
    for _ in 0..READ_IOVECS {
        chain
            .push(IoBuffMut::new(0, 1, 0).expect("allocate longevity read segment"))
            .expect("fixed read chain capacity is exact");
    }
    chain
}

fn tcp_cycle(listener: &StdTcpListener, address: SocketAddr) -> SocketCycle {
    let client = StdTcpStream::connect_timeout(&address, Duration::from_secs(1))
        .expect("connect longevity TCP pair");
    let (server, _peer) = listener.accept().expect("accept longevity TCP pair");
    client
        .set_nonblocking(true)
        .expect("set longevity TCP client nonblocking");
    server
        .set_nonblocking(true)
        .expect("set longevity TCP server nonblocking");
    SocketCycle::Tcp {
        reader: TcpStream::from_owned_fd(client.into()),
        peer: TcpStream::from_owned_fd(server.into()),
    }
}

async fn exercise_cycles(cycles: usize) {
    let listener = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("bind longevity TCP listener");
    let address = listener.local_addr().expect("read longevity TCP address");

    let mut completed = 0usize;
    while completed < cycles {
        let window = (cycles - completed).min(CYCLE_WINDOW);
        let mut handles = Vec::with_capacity(window);
        for offset in 0..window {
            let cycle_index = completed + offset;
            let cycle = if cycle_index.is_multiple_of(2) {
                let (reader, peer) = UnixStream::pair().expect("create longevity Unix pair");
                SocketCycle::Unix { reader, peer }
            } else {
                tcp_cycle(&listener, address)
            };
            handles.push(
                Executor::spawn(cycle.cancel_vectored_read()).expect("spawn longevity task cycle"),
            );
        }
        for handle in handles {
            handle.await.expect("longevity task cycle was cancelled");
        }
        completed += window;
    }
}

fn run_child_oracle() {
    assert_fd_count_instrument_discriminates();
    let pre_construction_fds = process_fd_count();
    let mut executor = Executor::new().expect("construct longevity executor");
    let runtime_fd_baseline = process_fd_count();

    executor
        .run(async { exercise_cycles(WARMUP_CYCLES).await })
        .expect("run longevity warmup");
    let warm = executor.test_quiescence();
    assert_quiescent(warm, "warmup");
    assert_eq!(
        process_fd_count(),
        runtime_fd_baseline,
        "warmup descriptor drift"
    );
    assert!(
        warm.retained_pooled_allocs > 0,
        "warmup missed retained pool"
    );
    assert!(warm.scratch_pooled_allocs > 0, "warmup missed scratch pool");
    let plateau = SlabPlateau::from(warm);

    for batch in 1..=BATCHES {
        executor
            .run(async { exercise_cycles(CYCLES_PER_BATCH).await })
            .unwrap_or_else(|err| panic!("run longevity batch {batch}: {err}"));
        let snapshot = executor.test_quiescence();
        assert_quiescent(snapshot, &format!("batch {batch}"));
        assert_eq!(
            process_fd_count(),
            runtime_fd_baseline,
            "batch {batch}: descriptor drift"
        );
        assert_eq!(
            SlabPlateau::from(snapshot),
            plateau,
            "batch {batch}: slab count grew after warmup"
        );
    }

    drop(executor);
    assert_eq!(
        process_fd_count(),
        pre_construction_fds,
        "executor teardown descriptor drift"
    );
}

#[test]
fn runtime_quiescence_contract_rejects_nonquiescent_state() {
    use flowio::runtime::executor::RuntimeQuiescence;

    let clean = RuntimeQuiescence {
        ready_queue_empty: true,
        task_registry_empty: true,
        executor_owner_refs: 1,
        scratch_owner_refs: 1,
        ..RuntimeQuiescence::default()
    };
    assert_quiescent(clean, "synthetic clean control");

    let mutations: [fn(&mut RuntimeQuiescence); 16] = [
        |snapshot| snapshot.live_tasks = 1,
        |snapshot| snapshot.inflight_ops = 1,
        |snapshot| snapshot.ready_queue_empty = false,
        |snapshot| snapshot.task_registry_empty = false,
        |snapshot| snapshot.timers_pending = true,
        |snapshot| snapshot.live_ops = 1,
        |snapshot| snapshot.pending_cancels = 1,
        |snapshot| snapshot.queued_sqes = 1,
        |snapshot| snapshot.pending_reactor_closes = 1,
        |snapshot| snapshot.deferred_reactor_closes = 1,
        |snapshot| snapshot.executor_owner_refs = 2,
        |snapshot| snapshot.scratch_owner_refs = 2,
        |snapshot| snapshot.retained_pooled_allocs = 1,
        |snapshot| snapshot.retained_heap_allocs = 1,
        |snapshot| snapshot.scratch_pooled_allocs = 1,
        |snapshot| snapshot.storage_abandoned = true,
    ];
    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut changed = clean;
        mutate(&mut changed);
        assert!(
            std::panic::catch_unwind(|| assert_quiescent(changed, "negative control")).is_err(),
            "quiescence mutation {index} was not rejected"
        );
    }

    let plateau = SlabPlateau::from(clean);
    let mut grown = clean;
    grown.task_slab_pages = 1;
    assert_ne!(
        SlabPlateau::from(grown),
        plateau,
        "slab-growth control was not observable"
    );
}

#[test]
fn runtime_tcp_unix_longevity_reclaims_each_batch() {
    if std::env::var_os(CHILD_ENV).is_some() {
        run_child_oracle();
        return;
    }
    common::run_exact_test_child_with_watchdog(TEST_NAME, CHILD_ENV, CHILD_DEADLINE);
}
