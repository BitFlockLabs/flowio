mod common;

use common::{
    BoundedTcpListener, BoundedTcpPeer, BoundedTcpStream, DYNAMIC_PROJECTED_PIECES,
    DropTrackedProjected17, EmptyProjected, ProjectedSourceWitness, TestIoBuffMut as IoBuffMut,
    TestProjected, TryCountMismatchedProjected, TryMismatchedProjected, TryOversizedProjected,
    connect_bounded_tcp_peer, fill_try_send_buffer, ipv6_loopback_capability_unavailable,
    make_payload_chain, make_read_chain, make_read_only_chain, poll_once_pending, run_test,
    run_test_output, set_positive_linger, spawn_bounded_tcp_peer,
};
use flowio::net::tcp::{TcpConnector, TcpListener, TcpStream};
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::executor::{Executor, ExecutorConfig};
use flowio::runtime::reactor::ReactorConfig;
use flowio::runtime::timer::{sleep, timeout};
use flowio::test_support::net::tcp::test_accept_slot_drop_cached_state_preserves_unrelated_fd;
use flowio::test_support::runtime::test_hooks;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

const TCP_SHUTDOWN_FALLBACK_CHILD_ENV: &str = "FLOWIO_TCP_SHUTDOWN_FALLBACK_CHILD";
const TCP_SHUTDOWN_FALLBACK_TEST: &str =
    "runtime_tcp_shutdown_fallback_abandons_unretired_readiness_state_with_watchdog";
const TCP_UNSUBMITTED_READINESS_CHILD_ENV: &str = "FLOWIO_TCP_UNSUBMITTED_READINESS_CHILD";
const TCP_UNSUBMITTED_READINESS_TEST: &str =
    "runtime_tcp_unsubmitted_readiness_retains_listener_fd_until_ring_safe";
const TCP_BOUNDED_PEER_STALL_CHILD_ENV: &str = "FLOWIO_TCP_BOUNDED_PEER_STALL_CHILD";
const TCP_BOUNDED_PEER_STALL_TEST: &str = "bounded_tcp_peer_forced_stalls_fail_with_context";
const TCP_PROJECTED_TLS_DESTRUCTOR_CHILD_ENV: &str = "FLOWIO_TCP_PROJECTED_TLS_DESTRUCTOR_CHILD";
const TCP_PROJECTED_TLS_DESTRUCTOR_TEST: &str =
    "runtime_tcp_try_writev_projected_survives_tls_destructor_order";
const TCP_IPV6_FLOWIO_TIMEOUT: Duration = Duration::from_secs(2);

fn bind_std_ipv6_tcp_listener_or_skip(test_name: &str) -> Option<BoundedTcpListener> {
    match BoundedTcpListener::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0))) {
        Ok(listener) => Some(listener),
        Err(err) if ipv6_loopback_capability_unavailable(&err) => {
            eprintln!("skipping {test_name}: IPv6 loopback unavailable ({err})");
            None
        }
        Err(err) => panic!("trusted std IPv6 TCP probe failed for {test_name}: {err}"),
    }
}

/// Spawns a std TCP peer that connects, verifies the payload it receives, and
/// writes a fixed response.
fn spawn_std_tcp_peer(
    addr: SocketAddr,
    expected_recv: Vec<u8>,
    response: Vec<u8>,
) -> BoundedTcpPeer<()> {
    spawn_bounded_tcp_peer("standard TCP request-response peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        let mut buf = vec![0u8; expected_recv.len()];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(buf, expected_recv, "std peer received unexpected payload");
        stream.write_all(&response).expect("std write failed");
    })
}

/// Returns a FlowIO TcpStream wrapping an accepted nonblocking std socket plus
/// its connected std peer for try_* tests that do not need a reactor.
fn connected_try_tcp_stream() -> (TcpStream, BoundedTcpStream) {
    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let deadline = common::TcpPeerDeadline::new("connected try-operation TCP peer");
    let peer = deadline.connect(addr).expect("std connect failed");
    let (mut stream, _) = deadline.accept(&listener).expect("std accept failed");
    stream
        .set_nonblocking(true)
        .expect("set_nonblocking failed");
    (TcpStream::from_owned_fd(stream.into_inner().into()), peer)
}

struct ProjectedTlsOrderProbe {
    drops: RefCell<Option<Arc<AtomicUsize>>>,
}

impl ProjectedTlsOrderProbe {
    const fn new() -> Self {
        Self {
            drops: RefCell::new(None),
        }
    }

    fn arm(&self, drops: Arc<AtomicUsize>) -> bool {
        let Ok(mut slot) = self.drops.try_borrow_mut() else {
            return false;
        };
        if slot.is_some() {
            return false;
        }
        *slot = Some(drops);
        true
    }
}

impl Drop for ProjectedTlsOrderProbe {
    fn drop(&mut self) {
        if let Some(drops) = self.drops.get_mut().take() {
            drops.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
struct ProjectedTlsDestructorOutcome {
    probe_unavailable: bool,
    probe_drops_before_write: usize,
    write_result: Result<usize, io::ErrorKind>,
    source_identity_preserved: bool,
    projection_calls: usize,
    drops_before_explicit_drop: usize,
    drops_after_explicit_drop: usize,
    shutdown_result: Result<(), io::ErrorKind>,
    read_result: Result<(), io::ErrorKind>,
    received: [u8; DYNAMIC_PROJECTED_PIECES],
    eof_result: Result<usize, io::ErrorKind>,
}

struct ProjectedTlsDestructorState {
    stream: TcpStream,
    peer: BoundedTcpStream,
    source_witness: ProjectedSourceWitness,
    probe_drops: Arc<AtomicUsize>,
    outcome: Arc<Mutex<Option<ProjectedTlsDestructorOutcome>>>,
}

impl Drop for ProjectedTlsDestructorState {
    fn drop(&mut self) {
        let probe_unavailable = PROJECTED_TLS_DESTRUCTOR_ORDER_PROBE
            .try_with(|_| ())
            .is_err();
        let probe_drops_before_write = self.probe_drops.load(Ordering::Relaxed);

        let source = DropTrackedProjected17::from_witness(b'D', &self.source_witness);
        let (write_result, source) = self.stream.try_writev_projected(source);
        let source_identity_preserved = source.has_identity(&self.source_witness);
        let projection_calls = self.source_witness.projection_calls();
        let drops_before_explicit_drop = self.source_witness.drops();
        drop(source);
        let drops_after_explicit_drop = self.source_witness.drops();

        let shutdown_result = self
            .stream
            .shutdown(Shutdown::Write)
            .map_err(|err| err.kind());
        let mut received = [0; DYNAMIC_PROJECTED_PIECES];
        let read_result = self
            .peer
            .read_exact(&mut received)
            .map_err(|err| err.kind());
        let mut trailing = [0; 1];
        let eof_result = self.peer.read(&mut trailing).map_err(|err| err.kind());

        let outcome = ProjectedTlsDestructorOutcome {
            probe_unavailable,
            probe_drops_before_write,
            write_result: write_result.map_err(|err| err.kind()),
            source_identity_preserved,
            projection_calls,
            drops_before_explicit_drop,
            drops_after_explicit_drop,
            shutdown_result,
            read_result,
            received,
            eof_result,
        };
        if let Ok(mut slot) = self.outcome.lock()
            && slot.is_none()
        {
            *slot = Some(outcome);
        }
    }
}

thread_local! {
    static PROJECTED_TLS_DESTRUCTOR_STATE: RefCell<Option<ProjectedTlsDestructorState>> =
        const { RefCell::new(None) };
    static PROJECTED_TLS_DESTRUCTOR_ORDER_PROBE: ProjectedTlsOrderProbe =
        const { ProjectedTlsOrderProbe::new() };
}

#[test]
fn runtime_tcp_initial_submissions_extract_poll_context_once() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 4).expect("bind failed");
    let addr = listener.local_addr();
    let mut connector = TcpConnector::new();
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            poll_once_pending(listener.accept()).await;
            poll_once_pending(connector.connect(addr).expect("connect init failed")).await;
            poll_once_pending(TcpStream::connect(addr).expect("owned connect init failed")).await;
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    assert_eq!(
        executor.last_stats().poll_context_extractions,
        3,
        "each TCP accept/connect submission should derive the validated context once"
    );
}

#[test]
fn ipv6_loopback_capability_policy_is_narrow() {
    for errno in [libc::EAFNOSUPPORT, libc::EPFNOSUPPORT, libc::EADDRNOTAVAIL] {
        let err = io::Error::from_raw_os_error(errno);
        assert!(
            ipv6_loopback_capability_unavailable(&err),
            "accepted IPv6 loopback errno {errno} was not classified unavailable"
        );
    }

    for errno in [
        libc::EPERM,
        libc::EACCES,
        libc::EINVAL,
        libc::ENETUNREACH,
        libc::ECONNREFUSED,
    ] {
        let err = io::Error::from_raw_os_error(errno);
        assert!(
            !ipv6_loopback_capability_unavailable(&err),
            "IPv6 loopback errno {errno} should remain a failure"
        );
    }

    assert!(
        !ipv6_loopback_capability_unavailable(&io::Error::other("probe failed without an errno")),
        "an IPv6 loopback failure without an errno should remain visible"
    );
}

#[test]
fn bounded_tcp_peer_forced_stalls_fail_with_context() {
    if std::env::var_os(TCP_BOUNDED_PEER_STALL_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            TCP_BOUNDED_PEER_STALL_TEST,
            TCP_BOUNDED_PEER_STALL_CHILD_ENV,
            Duration::from_secs(5),
        );
        return;
    }

    fn assert_timed_out_with_context(err: io::Error, operation: &str) {
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        let message = err.to_string();
        assert!(
            message.contains("forced") && message.contains(operation),
            "missing bounded-peer {operation} context: {message}"
        );
    }

    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 1)
        .expect("forced connect-stall listener bind failed");
    let addr = listener.local_addr();
    const MAX_QUEUED_CONNECTS: usize = 8;
    let mut queued = Vec::with_capacity(MAX_QUEUED_CONNECTS);
    let mut connect_err = None;
    for _ in 0..MAX_QUEUED_CONNECTS {
        let deadline = common::TcpPeerDeadline::with_timeout(
            "forced connect stall",
            Duration::from_millis(100),
        );
        let started = std::time::Instant::now();
        match deadline.connect(addr) {
            Ok(stream) => queued.push(stream),
            Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                assert!(
                    started.elapsed() >= Duration::from_millis(20),
                    "forced connect stall returned before entering the bounded syscall"
                );
                connect_err = Some(err);
                break;
            }
            Err(err) => panic!("forced connect stall returned an unexpected error: {err}"),
        }
    }
    let connect_err = connect_err.expect("loopback listen queue did not force a connect stall");
    assert_timed_out_with_context(connect_err, "connect");
    drop(queued);
    drop(listener);

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("forced accept-expiry listener bind failed");
    let expired =
        common::TcpPeerDeadline::with_timeout("forced accept expiry", Duration::from_millis(1));
    std::thread::sleep(Duration::from_millis(5));
    let accept_err = match expired.accept(&listener) {
        Ok(_) => panic!("expired accept deadline should fail"),
        Err(err) => err,
    };
    assert_timed_out_with_context(accept_err, "accept");
    let listener_flags = unsafe { libc::fcntl(listener.as_raw_fd(), libc::F_GETFL) };
    assert!(
        listener_flags >= 0,
        "F_GETFL failed after expired bounded accept"
    );
    assert_eq!(
        listener_flags & libc::O_NONBLOCK,
        0,
        "expired bounded accept left its reusable listener nonblocking"
    );

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("forced read-stall listener bind failed");
    let addr = listener.local_addr().expect("forced read-stall address");
    let read_deadline =
        common::TcpPeerDeadline::with_timeout("forced read stall", Duration::from_millis(100));
    let mut reader = read_deadline
        .connect(addr)
        .expect("forced read-stall connect failed");
    let (_silent_writer, _) = read_deadline
        .accept(&listener)
        .expect("forced read-stall accept failed");
    assert_timed_out_with_context(
        reader
            .read(&mut [0u8; 1])
            .expect_err("silent peer read should time out"),
        "read",
    );

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("forced write-stall listener bind failed");
    let addr = listener.local_addr().expect("forced write-stall address");
    let write_deadline =
        common::TcpPeerDeadline::with_timeout("forced write stall", Duration::from_millis(100));
    let mut writer = write_deadline
        .connect(addr)
        .expect("forced write-stall connect failed");
    let (_silent_reader, _) = write_deadline
        .accept(&listener)
        .expect("forced write-stall accept failed");
    assert_timed_out_with_context(
        writer
            .write_all(&vec![0xA5; 16 * 1024 * 1024])
            .expect_err("undrained peer write should time out"),
        "write",
    );

    let release = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let peer_release = std::sync::Arc::clone(&release);
    let peer_exited = std::sync::Arc::clone(&exited);
    let peer = common::spawn_bounded_tcp_peer_with_timeout(
        "forced join stall",
        Duration::from_millis(50),
        move |_deadline| {
            while !peer_release.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            peer_exited.store(true, std::sync::atomic::Ordering::Release);
        },
    );
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| peer.finish()))
        .expect_err("stalled peer finish should panic");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&'static str>().copied())
        .expect("bounded peer panic should carry text");
    assert!(
        message.contains("forced join stall") && message.contains("deadline"),
        "missing bounded-peer join context: {message}"
    );
    release.store(true, std::sync::atomic::Ordering::Release);
    let release_deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !exited.load(std::sync::atomic::Ordering::Acquire)
        && std::time::Instant::now() < release_deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        exited.load(std::sync::atomic::Ordering::Acquire),
        "released join-stall peer did not exit"
    );
}

#[test]
fn runtime_tcp_fresh_listener_drop_skips_linger_query() {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 16)
        .expect("runtime TCP bind failed");
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(listener);
        })
        .expect("fresh TCP listener close failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 0);
        assert_eq!(stats.close_ring_submissions, 1);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
    }
}

#[test]
fn runtime_tcp_saved_public_fd_positive_linger_routes_to_worker() {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 16)
        .expect("runtime TCP bind failed");
    let saved_raw = listener.as_raw_fd();
    set_positive_linger(saved_raw);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(listener);
        })
        .expect("positive-linger TCP listener close failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_worker_admissions, 1);
        assert_eq!(stats.close_ring_submissions, 0);
        assert_eq!(stats.close_direct_closes, 0);
    }
    drop(executor);
}

#[test]
fn runtime_tcp_accept_inherits_known_listener_provenance() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 16).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        deadline.connect(addr).expect("std connect failed")
    });
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (stream, _) = listener.accept().await.expect("accept failed");
            drop(stream);
            drop(listener);
        })
        .expect("known-provenance accept run failed");
    drop(peer.finish());
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 0);
        assert_eq!(stats.close_ring_submissions, 2);
        assert_eq!(stats.close_worker_admissions, 0);
    }
}

#[test]
fn runtime_tcp_accept_inherits_exposed_positive_listener_provenance() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 16).expect("bind failed");
    let addr = listener.local_addr();
    set_positive_linger(listener.as_raw_fd());
    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        deadline.connect(addr).expect("std connect failed")
    });
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (stream, _) = listener.accept().await.expect("accept failed");
            drop(stream);
            drop(listener);
        })
        .expect("uncertain-provenance accept run failed");
    drop(peer.finish());
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 2);
        assert_eq!(stats.close_worker_admissions, 2);
        assert_eq!(stats.close_ring_submissions, 0);
    }
    drop(executor);
}

#[test]
fn runtime_tcp_forgotten_accept_observes_late_listener_exposure() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 16).expect("bind failed");
    let addr = listener.local_addr();
    let (start_tx, start_rx) = std::sync::mpsc::sync_channel(1);
    let (connected_tx, connected_rx) = std::sync::mpsc::sync_channel(1);
    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        deadline
            .recv(&start_rx)
            .expect("late-exposure start signal");
        let mut stream = deadline.connect(addr).expect("std connect failed");
        connected_tx
            .send(())
            .expect("late-exposure connected signal");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set peer read timeout");
        let mut byte = [0u8; 1];
        match stream.read(&mut byte) {
            Ok(0) => {}
            Err(err) if err.kind() == io::ErrorKind::ConnectionReset => {}
            result => panic!("late-exposure listener backlog stayed open: {result:?}"),
        }
    });
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let mut accept = Box::pin(listener.accept());
            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("accept completed before client start"),
            })
            .await;
            std::mem::forget(accept);

            set_positive_linger(listener.as_raw_fd());
            start_tx.send(()).expect("start late-exposure peer");
            loop {
                match connected_rx.try_recv() {
                    Ok(()) => break,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        sleep(Duration::from_millis(1))
                            .await
                            .expect("connected wait sleep failed");
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        panic!("late-exposure peer disconnected before connect")
                    }
                }
            }

            // Drive one timed wait after connect so the readiness CQE can
            // enter the forgotten slot before listener teardown.
            sleep(Duration::from_millis(10))
                .await
                .expect("accept completion wait failed");
            drop(listener);
        })
        .expect("late-exposure accept run failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_worker_admissions, 1);
        assert_eq!(stats.close_ring_submissions, 0);
    }
    drop(executor);
    peer.finish();
}

#[test]
fn runtime_tcp_owned_fd_adoption_preserves_nonblocking_and_close_ownership() {
    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let deadline = common::TcpPeerDeadline::new("owned-fd adoption TCP peer");
    let mut peer = deadline.connect(addr).expect("std connect failed");
    let (mut standard, _) = deadline.accept(&listener).expect("std accept failed");
    standard
        .set_nonblocking(true)
        .expect("set_nonblocking failed");

    let raw = standard.as_raw_fd();
    let owned: OwnedFd = standard.into_inner().into();
    let stream = TcpStream::from_owned_fd(owned);
    let status = unsafe { libc::fcntl(raw, libc::F_GETFL) };
    assert!(status >= 0, "F_GETFL failed for adopted TCP fd");
    assert_ne!(
        status & libc::O_NONBLOCK,
        0,
        "adopted TCP fd became blocking"
    );

    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            drop(stream);
        })
        .expect("runtime-owned TCP close run failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_ring_submissions, 1);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
    }
    drop(executor);

    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set_read_timeout failed");
    let mut byte = [0u8; 1];
    match peer.read(&mut byte) {
        Ok(0) => {}
        Err(err) if err.kind() == io::ErrorKind::ConnectionReset => {}
        result => panic!("adopted TCP descriptor was not closed exactly once: {result:?}"),
    }
}

#[test]
fn runtime_tcp_try_read_immediate_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();
    peer.write_all(b"pong").expect("std write failed");

    let (res, buf) = stream.try_read(vec![0u8; 4], 4);
    assert_eq!(res.expect("try_read failed"), 4);
    assert_eq!(&buf[..], b"pong");
}

#[test]
fn runtime_tcp_try_read_would_block() {
    let (mut stream, _peer) = connected_try_tcp_stream();

    let (res, buf) = stream.try_read(vec![0u8; 4], 4);
    let err = res.expect_err("try_read should report WouldBlock");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(
        buf.len(),
        4,
        "buffer ownership and length should be preserved"
    );
}

#[test]
fn runtime_tcp_try_read_partial_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();
    peer.write_all(b"hi").expect("std write failed");

    let mut recv = IoBuffMut::new(0, 8, 0);
    recv.payload_append(b"HEAD").unwrap();
    let (res, buf) = stream.try_read(recv, 4);
    assert_eq!(res.expect("try_read failed"), 2);
    assert_eq!(buf.payload_bytes(), b"HEADhi");
}

#[test]
fn runtime_tcp_try_read_eof() {
    let (mut stream, peer) = connected_try_tcp_stream();
    peer.shutdown(Shutdown::Write)
        .expect("std shutdown write failed");

    let mut buf = b"HEAD".to_vec();
    buf.reserve(4);
    for _ in 0..100 {
        let (res, returned) = stream.try_read(buf, 4);
        buf = returned;
        match res {
            Ok(0) => {
                assert_eq!(buf, b"HEAD", "EOF must preserve existing contents");
                return;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
            other => panic!("unexpected try_read EOF result: {other:?}"),
        }
    }

    panic!("try_read did not observe EOF");
}

#[test]
fn runtime_tcp_try_read_rejects_invalid_len() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    let mut recv = IoBuffMut::new(0, 4, 0);
    recv.payload_append(b"ab").unwrap();

    let (res, recv) = stream.try_read(recv, 3);
    let err = res.expect_err("oversize try_read should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(recv.payload_bytes(), b"ab");
}

#[test]
fn runtime_tcp_try_read_append_success_and_partial() {
    let (mut stream, mut peer) = connected_try_tcp_stream();
    peer.write_all(b"body").expect("std write failed");

    let mut recv = IoBuffMut::new(0, 12, 0);
    recv.payload_append(b"HEAD").unwrap();
    let (res, recv) = stream.try_read_append(recv, 4);
    assert_eq!(res.expect("try_read_append failed"), 4);
    assert_eq!(recv.payload_bytes(), b"HEADbody");

    peer.write_all(b"!!").expect("std write failed");
    let (res, recv) = stream.try_read_append(recv, 4);
    assert_eq!(res.expect("partial try_read_append failed"), 2);
    assert_eq!(recv.payload_bytes(), b"HEADbody!!");
}

#[test]
fn runtime_tcp_try_read_append_would_block_and_invalid_len() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    let mut recv = IoBuffMut::new(0, 6, 0);
    recv.payload_append(b"HEAD").unwrap();

    let (res, recv) = stream.try_read_append(recv, 2);
    let err = res.expect_err("try_read_append should report WouldBlock");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(recv.payload_bytes(), b"HEAD");

    let (res, recv) = stream.try_read_append(recv, 3);
    let err = res.expect_err("oversize try_read_append should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(recv.payload_bytes(), b"HEAD");
}

#[test]
fn runtime_tcp_try_read_append_eof_preserves_payload() {
    let (mut stream, peer) = connected_try_tcp_stream();
    peer.shutdown(Shutdown::Write)
        .expect("std shutdown write failed");

    let mut recv = IoBuffMut::new(0, 8, 0);
    recv.payload_append(b"HEAD").unwrap();

    for _ in 0..100 {
        let (res, returned) = stream.try_read_append(recv, 4);
        recv = returned;
        match res {
            Ok(0) => {
                assert_eq!(recv.payload_bytes(), b"HEAD");
                return;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
            other => panic!("unexpected try_read_append EOF result: {other:?}"),
        }
    }

    panic!("try_read_append did not observe EOF");
}

#[test]
fn runtime_tcp_try_write_immediate_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();

    let (res, buf) = stream.try_write(b"ping".to_vec());
    assert_eq!(res.expect("try_write failed"), 4);
    assert_eq!(buf, b"ping".to_vec());

    let mut got = [0u8; 4];
    peer.read_exact(&mut got).expect("std read failed");
    assert_eq!(&got, b"ping");
}

#[test]
fn runtime_tcp_try_write_partial_and_would_block() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    stream
        .set_send_buffer_size(4096)
        .expect("set send buffer size failed");

    let (saw_partial, payload) = fill_try_send_buffer(|payload| stream.try_write(payload));
    assert!(
        saw_partial,
        "bounded nonblocking fill should observe at least one partial write"
    );

    let source = TestProjected::new([&b"x"[..]]);
    let (res, source) = stream.try_writev_projected(source);
    let err = res.expect_err("full socket should reject try_writev_projected");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(source.expected(), b"x".to_vec());

    let (res, payload) = stream.try_write(payload);
    let err = res.expect_err("full socket should reject try_write");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(payload.len(), 1024 * 1024);
}

#[test]
fn runtime_tcp_try_writev_projected_immediate_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();

    let source = TestProjected::new([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
    let expected = source.expected();
    let (res, source) = stream.try_writev_projected(source);
    assert_eq!(res.expect("try_writev_projected failed"), expected.len());
    assert_eq!(source.expected(), expected);

    let mut got = vec![0u8; expected.len()];
    peer.read_exact(&mut got).expect("std read failed");
    assert_eq!(got, expected);
}

#[test]
fn runtime_tcp_try_writev_projected_large_piece_count_immediate_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();

    let source = TestProjected::new([&b"x"[..]; 17]);
    let expected = source.expected();
    let (res, source) = stream.try_writev_projected(source);
    assert_eq!(
        res.expect("17-piece try_writev_projected failed"),
        expected.len()
    );
    assert_eq!(source.expected(), expected);

    let mut got = vec![0u8; expected.len()];
    peer.read_exact(&mut got).expect("std read failed");
    assert_eq!(got, expected);
}

#[test]
fn runtime_tcp_try_writev_projected_reentrant_large_projection_returns_both_sources() {
    let (mut outer_stream, mut outer_peer) = connected_try_tcp_stream();
    let (inner_stream, mut inner_peer) = connected_try_tcp_stream();
    common::assert_reentrant_projected_try_success(
        &mut outer_stream,
        &mut outer_peer,
        inner_stream,
        &mut inner_peer,
    );
}

#[test]
fn runtime_tcp_try_writev_projected_survives_tls_destructor_order() {
    if std::env::var_os(TCP_PROJECTED_TLS_DESTRUCTOR_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            TCP_PROJECTED_TLS_DESTRUCTOR_TEST,
            TCP_PROJECTED_TLS_DESTRUCTOR_CHILD_ENV,
            Duration::from_secs(5),
        );
        return;
    }

    let outcome = Arc::new(Mutex::new(None));
    let child_outcome = Arc::clone(&outcome);
    let worker = common::spawn_bounded_tcp_peer_with_timeout(
        "projected-write TLS destructor worker",
        Duration::from_secs(2),
        move |_deadline| {
            let (stream, mut peer) = connected_try_tcp_stream();
            peer.set_read_timeout(Some(Duration::from_millis(500)))
                .expect("set destructor peer read timeout");
            let source_witness = ProjectedSourceWitness::new();
            let probe_drops = Arc::new(AtomicUsize::new(0));
            let state = ProjectedTlsDestructorState {
                stream,
                peer,
                source_witness,
                probe_drops: Arc::clone(&probe_drops),
                outcome: child_outcome,
            };

            // Initialize the user destructor first and the order probe second.
            // Warming FlowIO's dynamic scratch last makes its teardown precede the
            // probe, which in turn must precede the user destructor.
            PROJECTED_TLS_DESTRUCTOR_STATE.with(|slot| {
                let mut slot = slot.borrow_mut();
                assert!(slot.is_none(), "destructor state initialized twice");
                *slot = Some(state);
            });
            PROJECTED_TLS_DESTRUCTOR_ORDER_PROBE.with(|probe| {
                assert!(
                    probe.arm(probe_drops),
                    "destructor order probe initialized twice"
                );
            });

            let (mut warm_stream, mut warm_peer) = connected_try_tcp_stream();
            let (warm_source, warm_witness) = DropTrackedProjected17::new(b'W');
            let (warm_result, warm_source) = warm_stream.try_writev_projected(warm_source);
            assert_eq!(
                warm_result.expect("warm dynamic projected write failed"),
                DYNAMIC_PROJECTED_PIECES
            );
            assert!(warm_source.has_identity(&warm_witness));
            assert_eq!(warm_witness.projection_calls(), 1);
            assert_eq!(warm_witness.drops(), 0);
            let mut warm_bytes = [0; DYNAMIC_PROJECTED_PIECES];
            warm_peer
                .read_exact(&mut warm_bytes)
                .expect("read warm projected bytes");
            assert_eq!(warm_bytes, [b'W'; DYNAMIC_PROJECTED_PIECES]);
            drop(warm_source);
            assert_eq!(warm_witness.drops(), 1);
        },
    );
    worker.finish();

    let outcome = outcome
        .lock()
        .expect("destructor outcome lock poisoned")
        .take()
        .expect("TLS destructor did not publish its projected-write outcome");
    assert!(
        outcome.probe_unavailable,
        "order probe remained accessible during the user TLS destructor"
    );
    assert_eq!(outcome.probe_drops_before_write, 1);
    assert_eq!(outcome.write_result, Ok(DYNAMIC_PROJECTED_PIECES));
    assert!(outcome.source_identity_preserved);
    assert_eq!(outcome.projection_calls, 1);
    assert_eq!(outcome.drops_before_explicit_drop, 0);
    assert_eq!(outcome.drops_after_explicit_drop, 1);
    assert_eq!(outcome.shutdown_result, Ok(()));
    assert_eq!(outcome.read_result, Ok(()));
    assert_eq!(outcome.received, [b'D'; DYNAMIC_PROJECTED_PIECES]);
    assert_eq!(outcome.eof_result, Ok(0));
}

#[test]
fn runtime_tcp_try_writev_projected_invalid_projection_returns_source() {
    let (mut stream, mut peer) = connected_try_tcp_stream();

    common::assert_empty_projected_try_cases!(stream);
    common::assert_reported_projected_try_cases!(stream);

    let (res, source) = stream.try_writev_projected(TryMismatchedProjected);
    let err = res.expect_err("mismatched projection should fail");
    common::assert_message_free_invalid_input(err);
    let _source = source;

    let (res, source) = stream.try_writev_projected(TryCountMismatchedProjected);
    let err = res.expect_err("piece-count mismatch should fail");
    common::assert_message_free_invalid_input(err);
    let _source = source;

    let (res, source) = stream.try_writev_projected(TryOversizedProjected);
    let err = res.expect_err("oversized projection should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    let _source = source;

    peer.set_nonblocking(true)
        .expect("peer set_nonblocking failed");
    let mut byte = [0u8; 1];
    let err = peer
        .read(&mut byte)
        .expect_err("rejected projected writes should send no bytes");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn runtime_tcp_async_empty_projected_validation_uses_no_submission_or_retained_scratch() {
    let (mut stream, mut peer) = connected_try_tcp_stream();
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    let returned_stream = Rc::new(Cell::new(None));
    let return_slot = Rc::clone(&returned_stream);

    executor
        .run(async move {
            common::assert_empty_projected_async_cases!(stream, writev_projected);
            common::assert_empty_projected_async_cases!(stream, writev_all_projected);
            common::assert_reported_projected_async_cases!(stream, writev_projected);
            common::assert_reported_projected_async_cases!(stream, writev_all_projected);

            return_slot.set(Some(stream));
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, 0);
        assert_eq!(stats.retained_pooled_allocs, 0);
        assert_eq!(stats.retained_heap_fallbacks, 0);
        assert_eq!(stats.writev_scratch_inline_allocs, 0);
        assert_eq!(stats.writev_scratch_pooled_allocs, 0);
    }

    let stream = returned_stream
        .take()
        .expect("empty projected test did not return the stream");

    peer.set_nonblocking(true)
        .expect("peer set_nonblocking failed");
    let mut byte = [0u8; 1];
    let err = peer
        .read(&mut byte)
        .expect_err("empty projected validation should send no bytes");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    drop(stream);
}

#[test]
fn runtime_tcp_empty_read_write_complete_without_submission() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    let returned_stream = Rc::new(Cell::new(None));
    let return_slot = Rc::clone(&returned_stream);

    executor
        .run(async move {
            common::assert_empty_stream_io_cases!(stream);
            return_slot.set(Some(stream));
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, 0);
        assert_eq!(stats.cqe_completions, 0);
        assert_eq!(stats.retained_pooled_allocs, 0);
        assert_eq!(stats.retained_heap_fallbacks, 0);
        assert_eq!(
            stats.poll_context_extractions, 2,
            "each local completion must still validate its FlowIO context"
        );
    }

    drop(
        returned_stream
            .take()
            .expect("empty stream I/O test did not return its TCP stream"),
    );
}

#[test]
fn runtime_tcp_async_projected_shape_mismatches_return_source() {
    let (mut stream, _peer) = connected_try_tcp_stream();

    run_test(async move {
        common::assert_projected_async_mismatches!(stream, writev_projected);
        common::assert_projected_async_mismatches!(stream, writev_all_projected);
    });
}

#[test]
fn runtime_tcp_empty_projected_rejects_outside_run_before_projection() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    let mut cx = Context::from_waker(Waker::noop());

    let mut future = Box::pin(stream.writev_projected(EmptyProjected::valid()));
    match future.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), source)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(source.projection_calls(), 0);
        }
        _ => panic!("empty partial projection should reject an inactive poll context"),
    }
    drop(future);

    let mut future = Box::pin(stream.writev_all_projected(EmptyProjected::valid()));
    match future.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), source)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(source.projection_calls(), 0);
        }
        _ => panic!("empty all projection should reject an inactive poll context"),
    }
    drop(future);

    let mut future = Box::pin(
        stream.writev_projected(common::MalformedReportedProjected::bytes_without_pieces()),
    );
    match future.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), source)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(source.reported_shape(), (0, 1));
            assert_eq!(source.projection_calls(), 0);
        }
        _ => panic!("malformed partial projection should prefer an inactive poll context"),
    }
    drop(future);

    let mut future = Box::pin(
        stream.writev_all_projected(common::MalformedReportedProjected::pieces_without_bytes()),
    );
    match future.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), source)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(source.reported_shape(), (1, 0));
            assert_eq!(source.projection_calls(), 0);
        }
        _ => panic!("malformed all projection should prefer an inactive poll context"),
    }
}

#[test]
fn runtime_tcp_try_clone_for_split_duplicates_connected_stream_descriptor() {
    let (mut read_owner, mut peer) = connected_try_tcp_stream();
    let mut write_owner = read_owner
        .try_clone_for_split()
        .expect("try_clone_for_split failed");

    let (res, sent) = write_owner.try_write(b"ping".to_vec());
    assert_eq!(res.expect("split write failed"), 4);
    assert_eq!(sent, b"ping".to_vec());

    let mut got = [0u8; 4];
    peer.read_exact(&mut got).expect("std peer read failed");
    assert_eq!(&got, b"ping");

    drop(write_owner);

    peer.write_all(b"pong").expect("std peer write failed");
    let (res, recv) = read_owner.try_read(vec![0u8; 4], 4);
    assert_eq!(res.expect("read after dropping split owner failed"), 4);
    assert_eq!(&recv[..], b"pong");
}

#[test]
fn runtime_tcp_successful_split_clone_taints_both_fresh_handles() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 16).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        deadline.connect(addr).expect("std connect failed")
    });
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (stream, _) = listener.accept().await.expect("accept failed");
            let clone = stream
                .try_clone_for_split()
                .expect("fresh split clone failed");
            drop(clone);
            drop(stream);
            drop(listener);
        })
        .expect("fresh split-clone run failed");
    drop(peer.finish());
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(
            stats.close_linger_queries, 2,
            "both aliased stream owners must become uncertain"
        );
        assert_eq!(stats.close_ring_submissions, 3);
        assert_eq!(stats.close_worker_admissions, 0);
    }
}

#[test]
fn runtime_tcp_split_clone_supports_concurrent_async_read_and_write() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        stream.write_all(b"pong").expect("std write failed");

        let mut got = [0u8; 4];
        stream.read_exact(&mut got).expect("std read failed");
        assert_eq!(&got, b"ping");
    });

    run_test(async move {
        let (mut read_owner, _addr) = listener.accept().await.expect("accept failed");
        let mut write_owner = read_owner
            .try_clone_for_split()
            .expect("try_clone_for_split failed");

        let writer = Executor::spawn(async move {
            let (res, sent) = write_owner.write_all(b"ping".to_vec()).await;
            assert_eq!(res.expect("split async write failed"), 4);
            sent
        })
        .expect("spawn split writer failed");

        let reader = Executor::spawn(async move {
            let (res, recv) = read_owner.read_exact(vec![0u8; 4], 4).await;
            assert_eq!(res.expect("split async read failed"), 4);
            recv
        })
        .expect("spawn split reader failed");

        assert_eq!(
            writer.await.expect("writer task cancelled"),
            b"ping".to_vec()
        );
        assert_eq!(&reader.await.expect("reader task cancelled")[..], b"pong");
    });

    peer.finish();
}

#[test]
fn runtime_tcp_ping_pong() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"ping".to_vec(), b"pong".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let (res, _buf) = stream.write(b"ping".to_vec()).await;
        assert_eq!(res.expect("write failed"), 4);

        let recv = vec![0u8; 4];
        let (res, buf) = stream.read(recv, 4).await;
        assert_eq!(res.expect("read failed"), 4);
        assert_eq!(&buf[..4], b"pong");
    });

    peer.finish();
}

#[test]
fn runtime_tcp_ipv6_listener_accepts_bounded_std_peer() {
    const TEST_NAME: &str = "runtime_tcp_ipv6_listener_accepts_bounded_std_peer";

    assert!(
        TCP_IPV6_FLOWIO_TIMEOUT < common::TCP_PEER_TIMEOUT,
        "the FlowIO IPv6 TCP timeout must expire before the peer deadline"
    );
    let Some(probe) = bind_std_ipv6_tcp_listener_or_skip(TEST_NAME) else {
        return;
    };
    drop(probe);

    let mut listener = TcpListener::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0)), 128)
        .expect("FlowIO IPv6 TCP bind failed after the trusted std probe succeeded");
    let listener_addr = listener.local_addr();
    assert_ne!(listener_addr.port(), 0);
    assert_eq!(
        listener_addr,
        SocketAddr::from((Ipv6Addr::LOCALHOST, listener_addr.port()))
    );

    let peer = spawn_bounded_tcp_peer("IPv6 TCP connector peer", move |deadline| {
        let mut stream = deadline
            .connect(listener_addr)
            .expect("bounded std IPv6 TCP connect failed");
        let local_addr = stream
            .local_addr()
            .expect("bounded std IPv6 TCP local_addr failed");
        let peer_addr = stream
            .peer_addr()
            .expect("bounded std IPv6 TCP peer_addr failed");
        assert_eq!(
            local_addr,
            SocketAddr::from((Ipv6Addr::LOCALHOST, local_addr.port()))
        );
        assert_eq!(peer_addr, listener_addr);

        stream
            .write_all(b"ping")
            .expect("bounded std IPv6 TCP write failed");
        let mut response = [0u8; 4];
        stream
            .read_exact(&mut response)
            .expect("bounded std IPv6 TCP read failed");
        assert_eq!(&response, b"pong");
        (local_addr, peer_addr)
    });

    let mut executor =
        Executor::new().expect("failed to construct FlowIO IPv6 TCP listener executor");
    let (flowio_local_addr, flowio_peer_addr, accepted_addr) =
        run_test_output(&mut executor, async move {
            timeout(TCP_IPV6_FLOWIO_TIMEOUT, async move {
                let (mut stream, accepted_addr) = listener
                    .accept()
                    .await
                    .expect("FlowIO IPv6 TCP accept failed");
                let local_addr = stream
                    .local_addr()
                    .expect("FlowIO IPv6 TCP local_addr failed");
                let peer_addr = stream
                    .peer_addr()
                    .expect("FlowIO IPv6 TCP peer_addr failed");
                assert_eq!(local_addr, listener_addr);
                assert_eq!(peer_addr, accepted_addr);
                assert_eq!(
                    accepted_addr,
                    SocketAddr::from((Ipv6Addr::LOCALHOST, accepted_addr.port()))
                );

                let (read_result, received) = stream.read_exact(vec![0u8; 4], 4).await;
                assert_eq!(
                    read_result.expect("FlowIO IPv6 TCP read failed"),
                    b"ping".len()
                );
                assert_eq!(received.as_slice(), b"ping");

                let (write_result, sent) = stream.write_all(b"pong".to_vec()).await;
                assert_eq!(
                    write_result.expect("FlowIO IPv6 TCP write failed"),
                    b"pong".len()
                );
                assert_eq!(sent.as_slice(), b"pong");
                (local_addr, peer_addr, accepted_addr)
            })
            .await
            .expect("FlowIO IPv6 TCP listener exchange timed out")
        });

    let (std_local_addr, std_peer_addr) = peer.finish();
    assert_eq!(std_peer_addr, flowio_local_addr);
    assert_eq!(std_local_addr, flowio_peer_addr);
    assert_eq!(std_local_addr, accepted_addr);
}

#[test]
fn runtime_tcp_ipv6_connector_connect_timeout_to_bounded_std_peer() {
    const TEST_NAME: &str = "runtime_tcp_ipv6_connector_connect_timeout_to_bounded_std_peer";

    assert!(
        TCP_IPV6_FLOWIO_TIMEOUT < common::TCP_PEER_TIMEOUT,
        "the FlowIO IPv6 TCP timeout must expire before the peer deadline"
    );
    let Some(listener) = bind_std_ipv6_tcp_listener_or_skip(TEST_NAME) else {
        return;
    };
    let listener_addr = listener
        .local_addr()
        .expect("trusted std IPv6 TCP listener local_addr failed");
    assert_ne!(listener_addr.port(), 0);
    assert_eq!(
        listener_addr,
        SocketAddr::from((Ipv6Addr::LOCALHOST, listener_addr.port()))
    );

    let peer = spawn_bounded_tcp_peer("IPv6 TCP listener peer", move |deadline| {
        let (mut stream, accepted_addr) = deadline
            .accept(&listener)
            .expect("bounded std IPv6 TCP accept failed");
        let local_addr = stream
            .local_addr()
            .expect("bounded std IPv6 TCP local_addr failed");
        let peer_addr = stream
            .peer_addr()
            .expect("bounded std IPv6 TCP peer_addr failed");
        assert_eq!(local_addr, listener_addr);
        assert_eq!(peer_addr, accepted_addr);
        assert_eq!(
            accepted_addr,
            SocketAddr::from((Ipv6Addr::LOCALHOST, accepted_addr.port()))
        );

        let mut request = [0u8; 4];
        stream
            .read_exact(&mut request)
            .expect("bounded std IPv6 TCP read failed");
        assert_eq!(&request, b"ping");
        stream
            .write_all(b"pong")
            .expect("bounded std IPv6 TCP write failed");
        (accepted_addr, local_addr, peer_addr)
    });

    let mut connector = TcpConnector::new();
    let mut executor =
        Executor::new().expect("failed to construct FlowIO IPv6 TCP connector executor");
    let (flowio_local_addr, flowio_peer_addr) = run_test_output(&mut executor, async move {
        timeout(TCP_IPV6_FLOWIO_TIMEOUT, async move {
            let mut stream = connector
                .connect_timeout(listener_addr, Duration::from_secs(1))
                .expect("FlowIO IPv6 TCP connect_timeout initialization failed")
                .await
                .expect(
                    "FlowIO IPv6 TCP connect_timeout failed after the trusted std probe succeeded",
                );
            let local_addr = stream
                .local_addr()
                .expect("FlowIO IPv6 TCP local_addr failed");
            let peer_addr = stream
                .peer_addr()
                .expect("FlowIO IPv6 TCP peer_addr failed");
            assert_eq!(
                local_addr,
                SocketAddr::from((Ipv6Addr::LOCALHOST, local_addr.port()))
            );
            assert_eq!(peer_addr, listener_addr);

            let (write_result, sent) = stream.write_all(b"ping".to_vec()).await;
            assert_eq!(
                write_result.expect("FlowIO IPv6 TCP write failed"),
                b"ping".len()
            );
            assert_eq!(sent.as_slice(), b"ping");

            let (read_result, received) = stream.read_exact(vec![0u8; 4], 4).await;
            assert_eq!(
                read_result.expect("FlowIO IPv6 TCP read failed"),
                b"pong".len()
            );
            assert_eq!(received.as_slice(), b"pong");
            (local_addr, peer_addr)
        })
        .await
        .expect("FlowIO IPv6 TCP connector exchange timed out")
    });

    let (accepted_addr, std_local_addr, std_peer_addr) = peer.finish();
    assert_eq!(flowio_peer_addr, std_local_addr);
    assert_eq!(flowio_local_addr, std_peer_addr);
    assert_eq!(flowio_local_addr, accepted_addr);
}

/// Cancelling readiness does not consume a queued connection; the next accept
/// receives that same peer and the reusable slot remains usable afterward.
#[test]
fn runtime_tcp_cancelled_accept_preserves_backlog_and_reaccepts() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    run_test(async move {
        let mut accept = Box::pin(listener.accept());
        std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("accept completed before test client connected"),
        })
        .await;

        let queued_client = connect_bounded_tcp_peer("queued cancelled-accept client", addr)
            .expect("queued client connect");
        let queued_addr = queued_client
            .local_addr()
            .expect("queued client local_addr failed");
        drop(accept);

        let (queued_stream, remote_addr) = listener.accept().await.expect("queued accept failed");
        assert_eq!(remote_addr, queued_addr);
        drop(queued_stream);
        drop(queued_client);

        let second_client = connect_bounded_tcp_peer("second cancelled-accept client", addr)
            .expect("second client connect");
        let second_addr = second_client
            .local_addr()
            .expect("second client local_addr failed");
        let (_stream, remote_addr) = listener.accept().await.expect("second accept failed");
        assert_eq!(remote_addr, second_addr);
    });
}

#[test]
fn runtime_tcp_accept_rearms_after_stale_readiness() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let listener_fd = listener.as_raw_fd();
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let mut accept = Box::pin(listener.accept());
            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("accept completed before first client connected"),
            })
            .await;

            let first_client = connect_bounded_tcp_peer("first stale-readiness client", addr)
                .expect("first client connect");
            sleep(Duration::from_millis(10))
                .await
                .expect("readiness wait failed");

            // This raw accept is an intentional test-only violation of the
            // documented no-concurrent-accept contract. It makes the completed
            // readiness stale before FlowIO performs its owner-thread accept4.
            let stolen = unsafe {
                libc::accept4(
                    listener_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                )
            };
            assert!(
                stolen >= 0,
                "external accept should steal first readiness: {}",
                io::Error::last_os_error()
            );
            // SAFETY: the successful test accept4 returned one sole-owned fd.
            let stolen = unsafe { OwnedFd::from_raw_fd(stolen) };

            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("stale readiness was not rearmed"),
            })
            .await;

            let second_client = connect_bounded_tcp_peer("second stale-readiness client", addr)
                .expect("second client connect");
            let second_addr = second_client
                .local_addr()
                .expect("second client local_addr failed");
            let (_stream, remote_addr) = accept.await.expect("rearmed accept failed");
            assert_eq!(remote_addr, second_addr);

            drop(stolen);
            drop(first_client);
            drop(second_client);
        })
        .expect("stale-readiness run failed");
    #[cfg(debug_assertions)]
    {
        assert_eq!(executor.last_stats().accept_readiness_rearms, 1);
        assert_eq!(
            executor.last_stats().accept_descriptor_exhaustions,
            0,
            "ordinary stale readiness must not count descriptor exhaustion"
        );
    }
}

#[test]
fn runtime_tcp_completed_readiness_context_rejection_preserves_backlog() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    run_test(async move {
        let mut accept = Box::pin(listener.accept());
        std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("accept completed before client connected"),
        })
        .await;

        let client = connect_bounded_tcp_peer("context-rejection queued client", addr)
            .expect("queued client connect");
        let client_addr = client.local_addr().expect("client local_addr failed");
        sleep(Duration::from_millis(10))
            .await
            .expect("readiness wait failed");

        let mut invalid_cx = Context::from_waker(Waker::noop());
        let err = match Future::poll(accept.as_mut(), &mut invalid_cx) {
            Poll::Ready(Err(err)) => err,
            Poll::Ready(Ok(_)) => panic!("invalid-context accept unexpectedly succeeded"),
            Poll::Pending => panic!("completed readiness remained pending"),
        };
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        drop(accept);

        let (_stream, remote_addr) = listener
            .accept()
            .await
            .expect("origin-context reaccept failed");
        assert_eq!(remote_addr, client_addr);
    });
}

#[test]
fn runtime_tcp_unsubmitted_readiness_retains_listener_fd_until_ring_safe() {
    if std::env::var_os(TCP_UNSUBMITTED_READINESS_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            TCP_UNSUBMITTED_READINESS_TEST,
            TCP_UNSUBMITTED_READINESS_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 16).expect("bind failed");
    let listener_fd = listener.as_raw_fd();
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    executor
        .run(async move {
            let mut accept = Box::pin(listener.accept());
            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("empty listener unexpectedly accepted"),
            })
            .await;
            std::mem::forget(accept);

            // Keep the full one-entry SQ userspace-only when listener teardown
            // tries to queue cancellation.
            test_hooks::fail_next_ring_submit_errno(libc::EBUSY);
            test_hooks::fail_next_ring_submit_errno(libc::EBUSY);
            drop(listener);
            assert_eq!(
                test_hooks::ring_submit_failures_remaining(),
                0,
                "listener teardown did not consume both injected submit failures"
            );

            let flags = unsafe { libc::fcntl(listener_fd, libc::F_GETFD) };
            assert!(
                flags >= 0,
                "readiness payload released listener fd before ring safety: {}",
                io::Error::last_os_error()
            );
            let replacement = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .expect("replacement bind failed");
            assert_ne!(
                replacement.as_raw_fd(),
                listener_fd,
                "open retained listener descriptor was numerically reused"
            );
        })
        .expect("unsubmitted readiness ownership run failed");
    drop(executor);
    let flags = unsafe { libc::fcntl(listener_fd, libc::F_GETFD) };
    assert_eq!(
        flags, -1,
        "listener fd remained open after reactor teardown"
    );
    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
}

#[test]
fn runtime_tcp_shutdown_fallback_abandons_unretired_readiness_state_with_watchdog() {
    if std::env::var_os(TCP_SHUTDOWN_FALLBACK_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            TCP_SHUTDOWN_FALLBACK_TEST,
            TCP_SHUTDOWN_FALLBACK_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 16).expect("bind failed");
    let addr = listener.local_addr();
    let listener_fd = listener.as_raw_fd();
    set_positive_linger(listener.as_raw_fd());
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");
    let staged = Rc::new(RefCell::new(Some(Box::pin(async move {
        listener.accept().await
    }))));
    let staged_for_run = Rc::clone(&staged);

    let err = executor
        .run(async move {
            std::future::poll_fn(|cx| {
                let mut slot = staged_for_run.borrow_mut();
                let accept = slot.as_mut().expect("staged TCP accept missing");
                match Future::poll(accept.as_mut(), cx) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("accept completed before shutdown peer"),
                }
            })
            .await;
            test_hooks::fail_next_ring_wait_errno(libc::EIO);
            std::future::pending::<()>().await;
        })
        .expect_err("injected wait failure should stop the executor");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));

    // The readiness CQE may remain unread. Force the bounded fallback after a
    // peer makes the listener ready so the retained readiness payload must be
    // abandoned instead of reclaimed without its target CQE.
    let peer = connect_bounded_tcp_peer("shutdown-fallback observer", addr)
        .expect("shutdown peer connect failed");
    test_hooks::force_next_reactor_shutdown_fallback();
    drop(executor);
    assert_eq!(
        test_hooks::reactor_shutdown_fallbacks_remaining(),
        0,
        "forced TCP shutdown fallback was not consumed"
    );

    let mut accept = staged
        .borrow_mut()
        .take()
        .expect("staged TCP accept disappeared");
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        Future::poll(accept.as_mut(), &mut cx),
        Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
    ));
    drop(accept);

    let flags = unsafe { libc::fcntl(listener_fd, libc::F_GETFD) };
    assert!(
        flags >= 0,
        "ring-abandoned TCP readiness owner was released without its target CQE: {}",
        io::Error::last_os_error()
    );
    drop(peer);
}

/// Teardown probe: a completed readiness CQE must never be interpreted as an
/// accepted descriptor.
#[test]
fn tcp_accept_slot_drop_cached_state_preserves_unrelated_fd() {
    test_accept_slot_drop_cached_state_preserves_unrelated_fd().unwrap();
}

#[test]
fn tcp_accept_forgotten_future_reports_busy_slot_would_block() {
    let mut listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128)
        .expect("listener bind failed");
    let first_accept = listener.accept();
    std::mem::forget(first_accept);

    run_test(async move {
        let err = match listener.accept().await {
            Ok(_) => panic!("forgotten accept future should keep listener slot busy"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    });
}

#[test]
fn tcp_accept_busy_error_outside_run_prefers_context_error() {
    let mut listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128)
        .expect("listener bind failed");
    let first_accept = listener.accept();
    std::mem::forget(first_accept);

    let mut second_accept = listener.accept();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        Pin::new(&mut second_accept).poll(&mut cx),
        Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
    ));
}

#[test]
fn runtime_tcp_connect_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let (mut stream, _) = deadline.accept(&listener).expect("std accept failed");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").expect("std write failed");
    });

    let mut connector = TcpConnector::new();

    executor
        .run(async move {
            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let (res, _buf) = stream.write(b"ping".to_vec()).await;
            assert_eq!(res.expect("write failed"), 4);

            let recv = vec![0u8; 4];
            let (res, buf) = stream.read(recv, 4).await;
            assert_eq!(res.expect("read failed"), 4);
            assert_eq!(&buf[..4], b"pong");
        })
        .expect("executor run failed");

    peer.finish();
}

#[test]
fn runtime_tcp_write_all_read_exact() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"ping".to_vec(), b"pong".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let send = b"ping".to_vec();
        let (res, _buf) = stream.write_all(send).await;
        assert_eq!(res.expect("write_all failed"), 4);

        let recv = vec![0u8; 4];
        let (res, buf) = stream.read_exact(recv, 4).await;
        assert_eq!(res.expect("read_exact failed"), 4);
        assert_eq!(&buf[..], b"pong");
    });

    peer.finish();
}

#[test]
fn runtime_tcp_write_all_read_exact_large_payload() {
    let msg_size = 256 * 1024; // 256KB
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, vec![0xABu8; msg_size], vec![0xABu8; msg_size]);

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let send = vec![0xABu8; msg_size];
        let (res, _buf) = stream.write_all(send).await;
        assert_eq!(res.expect("write_all failed"), msg_size);

        let recv = vec![0u8; msg_size];
        let (res, buf) = stream.read_exact(recv, msg_size).await;
        assert_eq!(res.expect("read_exact failed"), msg_size);
        assert!(buf.iter().all(|&b| b == 0xAB), "data mismatch on read");
    });

    peer.finish();
}

#[test]
fn runtime_tcp_read_exact_eof() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        stream.write_all(b"hi").expect("std write failed");
        drop(stream); // close before sending 4 bytes
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut recv = IoBuffMut::new(0, 8, 0);
            recv.payload_append(b"HEAD").unwrap();
            let (res, buf) = stream.read_exact(recv, 4).await;
            let err = res.expect_err("should fail with UnexpectedEof");
            assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
            assert_eq!(buf.payload_bytes(), b"HEADhi");
        })
        .expect("executor run failed");

    peer.finish();
}

/// TcpStream::connect() convenience creates a connection without a TcpConnector.
#[test]
fn runtime_tcp_stream_connect() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let (mut stream, _) = deadline.accept(&listener).expect("std accept failed");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").expect("std write failed");
    });

    executor
        .run(async move {
            let mut stream = TcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let send = b"ping".to_vec();
            let (res, _buf) = stream.write_all(send).await;
            assert_eq!(res.expect("write_all failed"), 4);

            let recv = vec![0u8; 4];
            let (res, buf) = stream.read_exact(recv, 4).await;
            assert_eq!(res.expect("read_exact failed"), 4);
            assert_eq!(&buf[..], b"pong");
        })
        .expect("executor run failed");

    peer.finish();
}

#[test]
fn runtime_tcp_stream_connect_timeout_success() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let (_stream, _) = deadline.accept(&listener).expect("std accept failed");
    });

    executor
        .run(async move {
            let stream = TcpStream::connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await
                .expect("connect_timeout failed");
            assert_eq!(stream.peer_addr().expect("peer_addr failed"), addr);
        })
        .expect("executor run failed");

    peer.finish();
}

#[test]
fn runtime_tcp_connector_connect_timeout_success() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let (_stream, _) = deadline.accept(&listener).expect("std accept failed");
    });

    let mut connector = TcpConnector::new();
    executor
        .run(async move {
            let stream = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await
                .expect("connect_timeout failed");
            assert_eq!(stream.peer_addr().expect("peer_addr failed"), addr);
        })
        .expect("executor run failed");

    peer.finish();
}

#[test]
fn runtime_tcp_connector_reuses_slot_for_plain_then_timed_success() {
    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = spawn_bounded_tcp_peer("two-connection standard TCP peer", move |deadline| {
        let (first, first_remote) = deadline.accept(&listener).expect("first std accept failed");
        assert_eq!(
            first.local_addr().expect("first std local_addr failed"),
            addr
        );
        assert_eq!(
            first.peer_addr().expect("first std peer_addr failed"),
            first_remote
        );

        let (second, second_remote) = deadline
            .accept(&listener)
            .expect("second std accept failed");
        assert_eq!(
            second.local_addr().expect("second std local_addr failed"),
            addr
        );
        assert_eq!(
            second.peer_addr().expect("second std peer_addr failed"),
            second_remote
        );
        assert_ne!(
            first_remote, second_remote,
            "two live clients reused one local endpoint"
        );
        (first_remote, second_remote)
    });

    let mut connector = TcpConnector::new();
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    let (first, second, first_local, second_local) = run_test_output(&mut executor, async move {
        let first = connector
            .connect(addr)
            .expect("first connect init failed")
            .await
            .expect("first connect failed");
        let first_local = first.local_addr().expect("first local_addr failed");
        assert_eq!(first.peer_addr().expect("first peer_addr failed"), addr);

        let second = connector
            .connect_timeout(addr, Duration::from_secs(1))
            .expect("second connect_timeout init failed")
            .await
            .expect("second connect_timeout failed");
        let second_local = second.local_addr().expect("second local_addr failed");
        assert_eq!(second.peer_addr().expect("second peer_addr failed"), addr);
        assert_ne!(
            first.as_raw_fd(),
            second.as_raw_fd(),
            "two live connector results reused one descriptor"
        );
        (first, second, first_local, second_local)
    });

    let (first_remote, second_remote) = peer.finish();
    assert_eq!(first_local, first_remote);
    assert_eq!(second_local, second_remote);
    assert_ne!(first_local, second_local);
    assert_eq!(
        first.peer_addr().expect("retained first peer_addr failed"),
        addr
    );
    assert_eq!(
        second
            .peer_addr()
            .expect("retained second peer_addr failed"),
        addr
    );
}

#[test]
fn runtime_tcp_stream_connect_timeout_propagates_connect_error() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    drop(listener);

    executor
        .run(async move {
            let result = TcpStream::connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await;
            let err = match result {
                Ok(_) => panic!("connect_timeout should propagate connect failure"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_tcp_connector_connect_timeout_propagates_connect_error() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    drop(listener);

    let mut connector = TcpConnector::new();
    executor
        .run(async move {
            let result = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await;
            let err = match result {
                Ok(_) => panic!("connect_timeout should propagate connect failure"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_tcp_connect_timeout_preserves_timer_runtime_error() {
    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            test_hooks::fail_next_timer_alloc();
            let result = TcpStream::connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await;
            let err = match result {
                Ok(_) => panic!("timer allocation failure should abort connect_timeout"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
        })
        .expect("executor run failed");
}

/// TcpStream address queries and socket options.
#[test]
fn runtime_tcp_socket_options() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        let mut buf = [0u8; 1];
        let _ = stream.read(&mut buf);
    });

    executor
        .run(async move {
            let (stream, peer_addr) = listener.accept().await.expect("accept failed");

            let local = stream.local_addr().expect("local_addr failed");
            assert_eq!(local, addr);
            let remote = stream.peer_addr().expect("peer_addr failed");
            assert_eq!(remote, peer_addr);

            stream.set_nodelay(true).expect("set_nodelay failed");
            assert!(stream.nodelay().expect("nodelay failed"));
            stream.set_nodelay(false).expect("set_nodelay false failed");
            assert!(!stream.nodelay().expect("nodelay false failed"));

            stream.set_keepalive(true).expect("set_keepalive failed");

            stream
                .set_send_buffer_size(65536)
                .expect("set_send_buffer_size failed");
            assert!(stream.send_buffer_size().expect("send_buffer_size failed") > 0);

            stream
                .set_recv_buffer_size(65536)
                .expect("set_recv_buffer_size failed");
            assert!(stream.recv_buffer_size().expect("recv_buffer_size failed") > 0);

            stream
                .shutdown(std::net::Shutdown::Write)
                .expect("shutdown failed");
        })
        .expect("executor run failed");

    peer.finish();
}

/// TcpListener::bind_reuse_port sets SO_REUSEPORT.
#[test]
fn runtime_tcp_listener_reuse_port() {
    let listener = TcpListener::bind_reuse_port(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128)
        .expect("bind_reuse_port failed");

    let addr = listener.local_addr();
    assert_ne!(addr.port(), 0);
}

/// `TcpConnector::default()` produces a working connector identical to `TcpConnector::new()`.
#[test]
fn runtime_tcp_connector_default_trait() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = BoundedTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let (_stream, _) = deadline.accept(&listener).expect("std accept failed");
    });

    let mut connector = TcpConnector::default();
    executor
        .run(async move {
            let stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            assert_eq!(stream.peer_addr().expect("peer_addr failed"), addr);
        })
        .expect("executor run failed");

    peer.finish();
}

// ============================================================================
// IoBuffMut / IoBuff transport integration tests
// ============================================================================

/// Ping-pong using IoBuffMut for receive and IoBuff (frozen) for send.
#[test]
fn runtime_tcp_ping_pong_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut send_buf = IoBuffMut::new(0, 4, 0);
            send_buf.payload_append(b"ping").unwrap();
            let (res, _buf) = stream.write(send_buf).await;
            assert_eq!(res.expect("write failed"), 4);

            let mut recv_buf = IoBuffMut::new(0, 8, 0);
            recv_buf.payload_append(b"HEAD").unwrap();
            let (res, buf) = stream.read(recv_buf, 4).await;
            assert_eq!(res.expect("read failed"), 4);
            assert_eq!(buf.payload_bytes(), b"HEADpong");
        })
        .expect("executor run failed");

    peer.finish();
}

/// write_all with frozen IoBuff, read_exact with IoBuffMut.
#[test]
fn runtime_tcp_write_all_read_exact_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut send_buf = IoBuffMut::new(0, 4, 0);
            send_buf.payload_append(b"ping").unwrap();
            let frozen = send_buf.freeze();
            let (res, _buf) = stream.write_all(frozen).await;
            assert_eq!(res.expect("write_all failed"), 4);

            let mut recv_buf = IoBuffMut::new(0, 8, 0);
            recv_buf.payload_append(b"HEAD").unwrap();
            let (res, buf) = stream.read_exact(recv_buf, 4).await;
            assert_eq!(res.expect("read_exact failed"), 4);
            assert_eq!(buf.payload_bytes(), b"HEADpong");
        })
        .expect("executor run failed");

    peer.finish();
}

/// Staged IoBuffMut append reads preserve previously-read payload bytes.
#[test]
fn runtime_tcp_read_exact_append_iobuff_staged() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        stream.write_all(b"HEADbody").expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let recv_buf = IoBuffMut::new(0, 8, 0);
            let (res, buf) = stream.read_exact_append(recv_buf, 4).await;
            assert_eq!(res.expect("header append read failed"), 4);
            assert_eq!(buf.payload_bytes(), b"HEAD");

            let (res, buf) = stream.read_exact_append(buf, 4).await;
            assert_eq!(res.expect("body append read failed"), 4);
            assert_eq!(buf.payload_len(), 8);
            assert_eq!(buf.payload_bytes(), b"HEADbody");
        })
        .expect("executor run failed");

    peer.finish();
}

/// IoBuffMut with headroom — prepend a protocol header before sending.
#[test]
fn runtime_tcp_iobuff_headroom() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        let mut buf = [0u8; 9];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"HDR:world");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut buf = IoBuffMut::new(4, 16, 0);
            buf.payload_append(b"world").unwrap();
            buf.headroom_prepend(b"HDR:").unwrap();
            assert_eq!(buf.bytes(), b"HDR:world");

            let (res, _buf) = stream.write_all(buf).await;
            assert_eq!(res.expect("write_all failed"), 9);
        })
        .expect("executor run failed");

    peer.finish();
}

/// Pool-allocated buffers through TCP transport.
#[test]
fn runtime_tcp_pool_buffers() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"hello");
        stream.write_all(b"world").expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 64,
                tailroom: 0,
                objs_per_slab: 16,
            })
            .expect("pool config invalid");
            pool.init();

            let mut send_buf = pool.alloc().expect("pool alloc failed");
            send_buf.payload_append(b"hello").unwrap();
            let (res, _buf) = stream.write_all(send_buf).await;
            assert_eq!(res.expect("write_all failed"), 5);

            let recv_buf = pool.alloc().expect("pool alloc failed");
            let (res, buf) = stream.read_exact(recv_buf, 5).await;
            assert_eq!(res.expect("read_exact failed"), 5);
            assert_eq!(buf.payload_bytes(), b"world");
        })
        .expect("executor run failed");

    peer.finish();
}

/// Large payload with IoBuffMut — forces partial kernel transfers.
#[test]
fn runtime_tcp_write_all_read_exact_large_iobuff() {
    let msg_size = 256 * 1024;
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        let mut buf = vec![0u8; msg_size];
        stream.read_exact(&mut buf).expect("std read failed");
        assert!(buf.iter().all(|&b| b == 0xAB), "data mismatch");
        stream.write_all(&buf).expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut send_buf = IoBuffMut::new(0, msg_size, 0);
            send_buf.payload_append(&vec![0xABu8; msg_size]).unwrap();
            let (res, _buf) = stream.write_all(send_buf).await;
            assert_eq!(res.expect("write_all failed"), msg_size);

            let recv_buf = IoBuffMut::new(0, msg_size, 0);
            let (res, buf) = stream.read_exact(recv_buf, msg_size).await;
            assert_eq!(res.expect("read_exact failed"), msg_size);
            assert!(
                buf.payload_bytes().iter().all(|&b| b == 0xAB),
                "data mismatch on read"
            );
        })
        .expect("executor run failed");

    peer.finish();
}

// ============================================================================
// Vectored I/O (readv / writev) tests
// ============================================================================

/// writev 3 segments to a std peer, readv the echo back.
#[test]
fn runtime_tcp_writev_readv() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = spawn_bounded_tcp_peer("bounded standard TCP peer", move |deadline| {
        let mut stream = deadline.connect(addr).expect("std connect failed");
        let mut buf = [0u8; 11];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"hello world");
        stream.write_all(&buf).expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let frozen = make_payload_chain([&b"hello"[..], &b" "[..], &b"world"[..]]);

            let (res, _chain) = stream.writev(frozen).await;
            assert_eq!(res.expect("writev failed"), 11);

            let read_chain = make_read_chain([6, 5]);

            let (res, chain) = stream.readv(read_chain).await;
            assert_eq!(res.expect("readv failed"), 11);
            assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hello ");
            assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b"world");
        })
        .expect("executor run failed");

    peer.finish();
}

#[test]
fn runtime_tcp_vectored_empty_chain_semantics() {
    let (mut stream, _peer) = connected_try_tcp_stream();

    run_test(async move {
        let (res, chain) = stream.writev(make_payload_chain::<0>([])).await;
        assert_eq!(res.expect("writev empty failed"), 0);
        assert!(chain.is_empty());

        let (res, chain) = stream.writev_all(make_payload_chain::<0>([])).await;
        assert_eq!(res.expect("writev_all empty failed"), 0);
        assert!(chain.is_empty());

        let (res, chain) = stream.writev(make_read_only_chain::<0>([])).await;
        assert_eq!(res.expect("empty read-only chain writev failed"), 0);
        assert!(chain.is_empty());

        let (res, source) = stream.writev_projected(TestProjected::<0>::new([])).await;
        assert_eq!(res.expect("writev_projected empty failed"), 0);
        assert!(source.expected().is_empty());

        let (res, source) = stream
            .writev_all_projected(TestProjected::<0>::new([]))
            .await;
        assert_eq!(res.expect("writev_all_projected empty failed"), 0);
        assert!(source.expected().is_empty());

        let (res, chain) = stream.readv(make_read_chain::<0>([])).await;
        let err = res.expect_err("readv empty should reject ambiguous EOF result");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(chain.is_empty());

        let (res, chain) = stream.readv_exact(make_read_chain::<0>([]), 0).await;
        assert_eq!(res.expect("readv_exact zero failed"), 0);
        assert!(chain.is_empty());
    });
}

#[test]
fn runtime_tcp_writev_all_readonly_chain_to_std_peer() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"hello world".to_vec(), b"ack".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let chain = make_read_only_chain([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
        let (res, chain) = stream.writev_all(chain).await;
        assert_eq!(res.expect("read-only chain writev_all failed"), 11);
        assert_eq!(chain.segments(), 4);

        let (res, buf) = stream.read_exact(vec![0u8; 3], 3).await;
        assert_eq!(res.expect("read_exact failed"), 3);
        assert_eq!(&buf[..], b"ack");
    });

    peer.finish();
}

#[test]
fn runtime_tcp_writev_all_projected_to_std_peer() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"hello world".to_vec(), b"ack".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let source = TestProjected::new([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
        let expected = source.expected();
        let (res, source) = stream.writev_all_projected(source).await;
        assert_eq!(res.expect("writev_all_projected failed"), expected.len());
        assert_eq!(source.expected(), expected);

        let (res, buf) = stream.read_exact(vec![0u8; 3], 3).await;
        assert_eq!(res.expect("read_exact failed"), 3);
        assert_eq!(&buf[..], b"ack");
    });

    peer.finish();
}
