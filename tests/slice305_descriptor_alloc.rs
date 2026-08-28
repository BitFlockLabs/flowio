//! Allocation regressions for the owner-thread descriptor core.

// These process/fd/io_uring allocation oracles exercise facilities that Miri
// intentionally does not emulate. The public trait and layout guards remain in
// `slice305_public_compat` and do run under Miri.
#![cfg(not(miri))]

mod common;

#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{
    CountingAllocator, ThreadLocalAllocationSnapshot, assert_allocation_failure_consumed,
    fail_next_allocation,
};
use flowio::net::sctp::SctpStream;
use flowio::net::tcp::{TcpConnector, TcpListener, TcpStream};
use flowio::net::tls::{TlsClientOptions, TlsClientStream};
use flowio::net::udp::UdpSocket;
use flowio::net::unix::UnixStream;
use flowio::runtime::executor::Executor;
use flowio::test_support::child::capture_child_with_watchdog;
use flowio::test_support::net::sctp::{
    test_construct_sctp_accept_result, test_construct_sctp_connect_result,
};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use std::fs::File;
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::os::fd::OwnedFd;
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const CORE_OOM_CHILD_ENV: &str = "FLOWIO_SLICE305_CORE_OOM_CHILD";
const CORE_OOM_TEST: &str = "descriptor_core_oom_uses_global_allocation_handler";

fn null_owned_fd() -> OwnedFd {
    File::open("/dev/null")
        .expect("open descriptor-allocation fixture")
        .into()
}

fn assert_one_core_allocation<T>(construct: impl FnOnce() -> T) {
    let before = ThreadLocalAllocationSnapshot::current();
    let value = construct();
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 0);
    drop(value);
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 1);
}

#[test]
fn adopted_descriptor_cores_allocate_and_deallocate_exactly_once() {
    let tcp_fd = null_owned_fd();
    assert_one_core_allocation(|| TcpStream::from_owned_fd(tcp_fd));

    let unix_fd = null_owned_fd();
    assert_one_core_allocation(|| UnixStream::from_owned_fd(unix_fd));

    let sctp_fd = null_owned_fd();
    assert_one_core_allocation(|| {
        SctpStream::from_owned_fd(sctp_fd, SocketAddr::from((Ipv4Addr::LOCALHOST, 9)))
    });
}

#[test]
fn descriptor_construction_paths_have_exact_core_allocation_counts() {
    let before_pair = ThreadLocalAllocationSnapshot::current();
    let pair = UnixStream::pair().expect("Unix pair construction failed");
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before_pair, 2, 0);
    drop(pair);
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before_pair, 2, 2);

    assert_one_core_allocation(|| {
        UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("UDP bind failed")
    });

    // The listener used one `Rc` allocation before Slice 305. Its descriptor
    // core replaces that owner; it must not add a second allocation.
    assert_one_core_allocation(|| {
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 8)
            .expect("TCP listener bind failed")
    });

    let source = TcpStream::from_owned_fd(null_owned_fd());
    let before_clone = ThreadLocalAllocationSnapshot::current();
    let split = source
        .try_clone_for_split()
        .expect("TCP split descriptor clone failed");
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before_clone, 1, 0);
    drop(split);
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before_clone, 1, 1);
    drop(source);
}

#[test]
fn accepted_and_connected_tcp_results_allocate_and_deallocate_one_core() {
    let mut accept_listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 8)
        .expect("TCP accept listener bind failed");
    let accept_addr = accept_listener.local_addr();
    let warm_accept_peer =
        StdTcpStream::connect(accept_addr).expect("warm TCP accept peer connect failed");
    let measured_accept_peer =
        StdTcpStream::connect(accept_addr).expect("measured TCP accept peer connect failed");
    let mut accept_executor = Executor::new().expect("TCP accept executor construction failed");

    accept_executor
        .run(async move {
            let (warm_accepted, _peer_addr) = accept_listener
                .accept()
                .await
                .expect("warm FlowIO TCP accept failed");
            drop(warm_accepted);

            let before = ThreadLocalAllocationSnapshot::current();
            let (accepted, _peer_addr) = accept_listener
                .accept()
                .await
                .expect("FlowIO TCP accept failed");
            ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 0);
            drop(accepted);
            ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 1);
            std::hint::black_box((&warm_accept_peer, &measured_accept_peer));
        })
        .expect("TCP accept executor run failed");

    let connect_listener = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("TCP connect listener bind failed");
    let connect_addr = connect_listener
        .local_addr()
        .expect("TCP connect listener address failed");
    let mut connector = TcpConnector::new();
    let mut connect_executor = Executor::new().expect("TCP connect executor construction failed");

    connect_executor
        .run(async move {
            let warm_future = connector
                .connect(connect_addr)
                .expect("warm FlowIO TCP connect setup failed");
            let warm_connected = warm_future.await.expect("warm FlowIO TCP connect failed");
            drop(warm_connected);

            let future = connector
                .connect(connect_addr)
                .expect("FlowIO TCP connect setup failed");
            let before = ThreadLocalAllocationSnapshot::current();
            let connected = future.await.expect("FlowIO TCP connect failed");
            ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 0);
            drop(connected);
            ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 1);
            std::hint::black_box(&connect_listener);
        })
        .expect("TCP connect executor run failed");
}

#[test]
fn sctp_accept_and_connect_result_seams_allocate_and_deallocate_one_core() {
    let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 3868));
    assert_one_core_allocation(|| {
        test_construct_sctp_accept_result(null_owned_fd(), peer)
            .expect("synthetic SCTP accept-result construction failed")
    });
    assert_one_core_allocation(|| test_construct_sctp_connect_result(null_owned_fd(), peer));
}

#[test]
fn tls_wrapper_reuses_the_existing_tcp_descriptor_core() {
    let listener = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("TLS allocation listener bind failed");
    let client = StdTcpStream::connect(
        listener
            .local_addr()
            .expect("TLS allocation listener address failed"),
    )
    .expect("TLS allocation client connect failed");
    let (peer, _peer_addr) = listener
        .accept()
        .expect("TLS allocation peer accept failed");
    let stream = TcpStream::from_owned_fd(client.into());
    let config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let server_name =
        ServerName::try_from("localhost").expect("TLS allocation server name invalid");
    let options = TlsClientOptions {
        rustls_buffer_limit: None,
        transport_read_buffer_size: 1_024,
        transport_write_buffer_size: 1_024,
    };

    // The exact constructor delta is frozen against Slice 304. Moving the TCP
    // stream into TLS must reuse its existing core rather than allocating a
    // second descriptor owner; TLS protocol and scratch allocations are the
    // unchanged baseline work represented by this count.
    let before = ThreadLocalAllocationSnapshot::current();
    let tls = TlsClientStream::new(stream, config, server_name, options)
        .expect("TLS allocation wrapper construction failed");
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 27, 18);

    std::hint::black_box((&tls, &peer));
    drop(tls);
}

#[test]
fn warmed_data_operation_lease_clone_and_nonfinal_release_do_not_allocate() {
    let (mut writer, peer) = UnixStream::pair().expect("Unix pair construction failed");
    let mut executor = Executor::new().expect("executor construction failed");

    executor
        .run(async move {
            let (warm_result, buffer) = writer.write(vec![0x5a; 64]).await;
            warm_result.expect("warm Unix write failed");

            let before = ThreadLocalAllocationSnapshot::current();
            let (result, buffer) = writer.write(buffer).await;
            result.expect("measured Unix write failed");
            let after = ThreadLocalAllocationSnapshot::current();

            std::hint::black_box((&writer, &peer, &buffer));
            after.assert_unchanged_since(before);
        })
        .expect("executor run failed");
}

#[test]
fn descriptor_core_oom_uses_global_allocation_handler() {
    if std::env::var_os(CORE_OOM_CHILD_ENV).is_some() {
        let fd = null_owned_fd();
        fail_next_allocation();
        let stream = TcpStream::from_owned_fd(fd);

        // Reaching here means construction did not request the declared core
        // allocation. This helper disarms the fault before panicking, so the
        // parent cannot mistake a later formatting allocation for the oracle.
        drop(stream);
        assert_allocation_failure_consumed();
        panic!("descriptor construction returned after an armed core OOM");
    }

    let current_exe = std::env::current_exe().expect("current allocation-test executable");
    let child = Command::new(current_exe)
        .args(["--exact", CORE_OOM_TEST, "--nocapture"])
        .env(CORE_OOM_CHILD_ENV, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn descriptor-core OOM child");
    let output = capture_child_with_watchdog(child, Duration::from_secs(5))
        .unwrap_or_else(|err| panic!("descriptor-core OOM child capture failed: {err}"));
    assert_eq!(
        output.status.signal(),
        Some(libc::SIGABRT),
        "descriptor-core OOM did not terminate through the global allocation handler: status={:?}, stdout={}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
