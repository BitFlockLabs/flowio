mod common;

use common::{
    DropTrackedReadOnly, DropTrackedReadWrite, TestIoBuffMut as IoBuffMut,
    assert_poll_after_ready_parks, enable_socket_timestampns, ipv6_loopback_capability_unavailable,
    poll_once_pending, run_test_output, set_positive_linger, wait_for_drop_count,
};
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
use common::{SparseOversizedReadOnly, assert_oversized_send_rejected};
use flowio::net::udp::UdpSocket;
use flowio::runtime::executor::Executor;
use flowio::runtime::timer::{TimeoutError, timeout};
#[cfg(any(debug_assertions, feature = "test-support"))]
use flowio::test_support::runtime::test_hooks;
use std::cell::Cell;
use std::fs::File;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

const UDP_PUBLIC_RAW_CLOSE_CHILD_ENV: &str = "FLOWIO_UDP_PUBLIC_RAW_CLOSE_CHILD";
const UDP_PUBLIC_RAW_CLOSE_TEST: &str =
    "runtime_udp_public_raw_exposure_classifies_then_uses_ring_close";
const UDP_SILENT_PEER_CHILD_ENV: &str = "FLOWIO_UDP_SILENT_PEER_CHILD";
const UDP_SILENT_PEER_TEST: &str = "runtime_udp_silent_peer_times_out_under_watchdog";
const UDP_MISSING_REPLY_CHILD_ENV: &str = "FLOWIO_UDP_MISSING_REPLY_CHILD";
const UDP_MISSING_REPLY_TEST: &str = "runtime_udp_missing_peer_reply_times_out_under_watchdog";
const UDP_TEST_TIMEOUT: Duration = Duration::from_secs(2);

fn prefilled_udp_buffer(writable: usize) -> flowio::runtime::buffer::IoBuffMut {
    let mut buffer = IoBuffMut::new(0, 4 + writable, 0);
    buffer.payload_append(b"HEAD").unwrap();
    buffer
}

fn connected_udp_pair() -> (UdpSocket, StdUdpSocket, SocketAddr) {
    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    socket.connect(peer_addr).expect("runtime connect failed");
    peer.connect(local_addr).expect("std peer connect failed");
    (socket, peer, peer_addr)
}

fn configure_std_udp_peer_timeouts(peer: &StdUdpSocket) {
    peer.set_read_timeout(Some(UDP_TEST_TIMEOUT))
        .expect("failed to set standard UDP peer read timeout");
    peer.set_write_timeout(Some(UDP_TEST_TIMEOUT))
        .expect("failed to set standard UDP peer write timeout");
}

fn assert_udp_timeout_elapsed<T>(result: Result<T, TimeoutError>, context: &str) {
    match result {
        Err(TimeoutError::Elapsed) => {}
        Err(TimeoutError::Runtime(err)) => {
            panic!("{context} timer failed instead of expiring: {err}")
        }
        Ok(_) => panic!("{context} unexpectedly completed before its timeout"),
    }
}

#[test]
fn runtime_udp_initial_submissions_extract_poll_context_once() {
    let mut connected = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("connected runtime bind failed");
    let mut unconnected = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("unconnected runtime bind failed");
    let peer =
        StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("peer bind failed");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    connected
        .connect(peer_addr)
        .expect("runtime connect failed");
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            poll_once_pending(connected.recv(vec![0u8; 1], 1)).await;
            poll_once_pending(connected.recv_msg(vec![0u8; 1], 1)).await;
            poll_once_pending(connected.send(b"s".to_vec())).await;
            poll_once_pending(unconnected.recv_from(vec![0u8; 1], 1)).await;
            poll_once_pending(unconnected.send_to(b"t".to_vec(), peer_addr)).await;
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    assert_eq!(
        executor.last_stats().poll_context_extractions,
        5,
        "each UDP submission should derive the validated context once"
    );
}

fn raw_fd_is_closed(fd: RawFd) -> bool {
    // SAFETY: F_GETFD accepts any integer descriptor and reads no pointed-to
    // memory; EBADF is the expected closed-descriptor result.
    let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    rc == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
}

fn raw_udp_local_addr(fd: RawFd) -> io::Result<SocketAddr> {
    // SAFETY: `fd` is a live socket descriptor for the duration of this
    // helper. F_DUPFD_CLOEXEC creates a distinct descriptor whose sole owner
    // is transferred immediately to `StdUdpSocket`.
    let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `duplicate` was created successfully above and ownership has
    // not been transferred elsewhere.
    let socket = unsafe { StdUdpSocket::from_raw_fd(duplicate) };
    socket.local_addr()
}

#[test]
fn runtime_udp_wildcard_ipv4_local_addr_reports_kernel_assigned_port() {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
        .expect("wildcard IPv4 UDP bind failed");

    let local_addr = socket
        .local_addr()
        .expect("wildcard IPv4 UDP local_addr failed");

    assert_eq!(local_addr.ip(), Ipv4Addr::UNSPECIFIED);
    assert_ne!(local_addr.port(), 0);
    assert_eq!(
        local_addr,
        raw_udp_local_addr(socket.as_raw_fd()).expect("raw IPv4 getsockname failed")
    );
}

#[test]
fn runtime_udp_local_addr_tracks_connect_and_reconnect() {
    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
        .expect("wildcard IPv4 UDP bind failed");
    let first_peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("first UDP peer bind failed");
    let second_peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("second UDP peer bind failed");

    let first_peer_addr = first_peer
        .local_addr()
        .expect("first peer local_addr failed");
    socket
        .connect(first_peer_addr)
        .expect("first UDP connect failed");
    let first_local = socket
        .local_addr()
        .expect("local_addr after first UDP connect failed");
    assert_eq!(
        first_local,
        raw_udp_local_addr(socket.as_raw_fd())
            .expect("raw getsockname after first UDP connect failed")
    );
    assert_eq!(socket.peer_addr(), Some(first_peer_addr));

    let second_peer_addr = second_peer
        .local_addr()
        .expect("second peer local_addr failed");
    socket
        .connect(second_peer_addr)
        .expect("second UDP connect failed");
    let second_local = socket
        .local_addr()
        .expect("local_addr after UDP reconnect failed");
    assert_eq!(
        second_local,
        raw_udp_local_addr(socket.as_raw_fd()).expect("raw getsockname after UDP reconnect failed")
    );
    assert_eq!(socket.peer_addr(), Some(second_peer_addr));
}

#[test]
fn runtime_udp_ipv6_local_addr_matches_raw_getsockname() {
    const TEST_NAME: &str = "runtime_udp_ipv6_local_addr_matches_raw_getsockname";

    let peer = match StdUdpSocket::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0))) {
        Ok(peer) => peer,
        Err(err) if ipv6_loopback_capability_unavailable(&err) => {
            eprintln!("skipping {TEST_NAME}: IPv6 loopback unavailable ({err})");
            return;
        }
        Err(err) => panic!("IPv6 UDP capability probe failed for {TEST_NAME}: {err}"),
    };

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))
        .expect("FlowIO IPv6 wildcard bind failed after capability probe");
    let bound_addr = socket
        .local_addr()
        .expect("FlowIO IPv6 wildcard local_addr failed");
    assert_eq!(bound_addr.ip(), Ipv6Addr::UNSPECIFIED);
    assert_ne!(bound_addr.port(), 0);
    assert_eq!(
        bound_addr,
        raw_udp_local_addr(socket.as_raw_fd()).expect("raw IPv6 wildcard getsockname failed")
    );

    let peer_addr = peer.local_addr().expect("IPv6 peer local_addr failed");
    socket
        .connect(peer_addr)
        .expect("FlowIO IPv6 connect failed after capability probe");
    assert_eq!(
        socket
            .local_addr()
            .expect("FlowIO IPv6 connected local_addr failed"),
        raw_udp_local_addr(socket.as_raw_fd()).expect("raw connected IPv6 getsockname failed")
    );
}

#[test]
fn runtime_udp_local_addr_propagates_getsockname_error() {
    let socket =
        UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("UDP bind failed");
    let raw = socket.as_raw_fd();
    let replacement = File::open("/dev/null").expect("failed to open descriptor replacement");

    // SAFETY: both descriptors are live. dup2 atomically replaces `raw` while
    // preserving FlowIO's sole ownership of that descriptor number.
    let replaced = unsafe { libc::dup2(replacement.as_raw_fd(), raw) };
    assert_eq!(replaced, raw, "failed to replace UDP descriptor");

    let err = socket
        .local_addr()
        .expect_err("getsockname on a non-socket descriptor should fail");
    assert_eq!(err.raw_os_error(), Some(libc::ENOTSOCK));
}

#[test]
fn runtime_udp_fresh_drop_skips_linger_query() {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime UDP socket");
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(socket);
        })
        .expect("fresh UDP close run failed");
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
fn runtime_udp_saved_public_fd_positive_linger_routes_to_worker() {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime UDP socket");
    let saved_raw = socket.as_raw_fd();
    set_positive_linger(saved_raw);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(socket);
        })
        .expect("positive-linger UDP close failed");
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
fn runtime_udp_public_raw_exposure_classifies_then_uses_ring_close() {
    if std::env::var_os(UDP_PUBLIC_RAW_CLOSE_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            UDP_PUBLIC_RAW_CLOSE_TEST,
            UDP_PUBLIC_RAW_CLOSE_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let raw = socket.as_raw_fd();
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(socket);
        })
        .expect("runtime-owned UDP close run failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_ring_submissions, 1);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
    }
    assert!(
        raw_fd_is_closed(raw),
        "publicly exposed UDP descriptor was not closed"
    );
    drop(executor);
}

#[test]
fn runtime_udp_silent_peer_times_out_under_watchdog() {
    if std::env::var_os(UDP_SILENT_PEER_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            UDP_SILENT_PEER_TEST,
            UDP_SILENT_PEER_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind silent-peer FlowIO socket");
    let local_addr = socket
        .local_addr()
        .expect("failed to read silent-peer FlowIO address");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind silent standard UDP peer");
    configure_std_udp_peer_timeouts(&peer);
    let mut executor = Executor::new().expect("failed to construct silent-peer executor");

    executor
        .run(async move {
            let drops = Rc::new(Cell::new(0));
            let buffer = DropTrackedReadWrite::zeroed(1, &drops);
            let result = timeout(UDP_TEST_TIMEOUT, async {
                let (result, _buffer) = socket.recv_from(buffer, 1).await;
                result
            })
            .await;
            assert_udp_timeout_elapsed(result, "silent UDP peer receive");
            assert_eq!(
                drops.get(),
                0,
                "timed-out UDP receive buffer dropped before its CQE"
            );

            assert_eq!(
                peer.send_to(b"x", local_addr)
                    .expect("failed to retire silent-peer receive"),
                1
            );
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("silent-peer executor run failed");
}

#[test]
fn runtime_udp_missing_peer_reply_times_out_under_watchdog() {
    if std::env::var_os(UDP_MISSING_REPLY_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            UDP_MISSING_REPLY_TEST,
            UDP_MISSING_REPLY_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    let (mut socket, peer, _peer_addr) = connected_udp_pair();
    configure_std_udp_peer_timeouts(&peer);
    let mut executor = Executor::new().expect("failed to construct missing-reply executor");

    executor
        .run(async move {
            let (result, request) = socket.send(b"request".to_vec()).await;
            assert_eq!(result.expect("FlowIO request send failed"), request.len());

            let drops = Rc::new(Cell::new(0));
            let buffer = DropTrackedReadWrite::zeroed(1, &drops);
            let result = timeout(UDP_TEST_TIMEOUT, async {
                let (result, _buffer) = socket.recv(buffer, 1).await;
                result
            })
            .await;
            assert_udp_timeout_elapsed(result, "missing UDP peer reply");
            assert_eq!(
                drops.get(),
                0,
                "timed-out UDP reply buffer dropped before its CQE"
            );

            assert_eq!(
                peer.send(b"x")
                    .expect("failed to retire missing-reply receive"),
                1
            );
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("missing-reply executor run failed");
}

#[test]
fn runtime_udp_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");

    socket.connect(peer_addr).expect("runtime connect failed");
    configure_std_udp_peer_timeouts(&peer);

    let peer_thread = std::thread::spawn(move || {
        peer.connect(local_addr).expect("std peer connect failed");

        let mut recv_buf = [0u8; 4];
        let recv_len = peer.recv(&mut recv_buf).expect("std recv failed");
        assert_eq!(recv_len, 4);
        assert_eq!(&recv_buf, b"ping");

        let send_len = peer.send(b"pong").expect("std send failed");
        assert_eq!(send_len, 4);
    });

    executor
        .run(async move {
            timeout(UDP_TEST_TIMEOUT, async move {
                let (res, _buf) = socket.send(b"ping".to_vec()).await;
                assert_eq!(res.expect("send failed"), 4);

                let recv = vec![0u8; 4];
                let (res, buf) = socket.recv(recv, 4).await;
                assert_eq!(res.expect("recv failed"), 4);
                assert_eq!(&buf[..4], b"pong");
            })
            .await
            .expect("connected UDP Vec exchange timed out");
        })
        .expect("executor run failed");

    peer_thread.join().expect("peer thread panicked");
}

#[test]
fn runtime_udp_ipv6_connected_bidirectional_ping_pong() {
    const TEST_NAME: &str = "runtime_udp_ipv6_connected_bidirectional_ping_pong";
    const FORWARD: &[u8] = b"flowio-udp-ipv6-forward";
    const REVERSE: &[u8] = b"flowio-udp-ipv6-reverse";

    let probe = match StdUdpSocket::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0))) {
        Ok(probe) => probe,
        Err(err) if ipv6_loopback_capability_unavailable(&err) => {
            eprintln!("skipping {TEST_NAME}: IPv6 loopback unavailable ({err})");
            return;
        }
        Err(err) => panic!("IPv6 UDP capability probe failed for {TEST_NAME}: {err}"),
    };
    drop(probe);

    let mut left = UdpSocket::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0)))
        .expect("left FlowIO IPv6 UDP bind failed after the capability probe succeeded");
    let mut right = UdpSocket::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0)))
        .expect("right FlowIO IPv6 UDP bind failed after the capability probe succeeded");
    let left_addr = left.local_addr().expect("left FlowIO local_addr failed");
    let right_addr = right.local_addr().expect("right FlowIO local_addr failed");

    assert_ne!(left_addr.port(), 0);
    assert_ne!(right_addr.port(), 0);
    assert_ne!(left_addr, right_addr);
    assert_eq!(
        left_addr,
        SocketAddr::from((Ipv6Addr::LOCALHOST, left_addr.port()))
    );
    assert_eq!(
        right_addr,
        SocketAddr::from((Ipv6Addr::LOCALHOST, right_addr.port()))
    );

    left.connect(right_addr)
        .expect("left FlowIO IPv6 UDP connect failed");
    right
        .connect(left_addr)
        .expect("right FlowIO IPv6 UDP connect failed");
    assert_eq!(left.peer_addr(), Some(right_addr));
    assert_eq!(right.peer_addr(), Some(left_addr));

    let mut executor = Executor::new().expect("failed to construct IPv6 UDP executor");
    executor
        .run(async move {
            timeout(UDP_TEST_TIMEOUT, async move {
                let (send_result, sent) = left.send(FORWARD.to_vec()).await;
                assert_eq!(
                    send_result.expect("left IPv6 UDP send failed"),
                    FORWARD.len()
                );
                assert_eq!(sent.as_slice(), FORWARD);

                let (recv_result, received) =
                    right.recv(vec![0u8; FORWARD.len()], FORWARD.len()).await;
                assert_eq!(
                    recv_result.expect("right IPv6 UDP recv failed"),
                    FORWARD.len()
                );
                assert_eq!(received.as_slice(), FORWARD);

                let (send_result, sent) = right.send(REVERSE.to_vec()).await;
                assert_eq!(
                    send_result.expect("right IPv6 UDP send failed"),
                    REVERSE.len()
                );
                assert_eq!(sent.as_slice(), REVERSE);

                let (recv_result, received) =
                    left.recv(vec![0u8; REVERSE.len()], REVERSE.len()).await;
                assert_eq!(
                    recv_result.expect("left IPv6 UDP recv failed"),
                    REVERSE.len()
                );
                assert_eq!(received.as_slice(), REVERSE);
            })
            .await
            .expect("bidirectional FlowIO IPv6 UDP exchange timed out");
        })
        .expect("FlowIO IPv6 UDP executor run failed");
}

#[test]
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
fn runtime_udp_send_paths_reject_oversize_iobuff_before_submission() {
    let (mut socket, _peer, peer_addr) = connected_udp_pair();
    let oversized =
        SparseOversizedReadOnly::new().expect("failed to reserve sparse oversized mapping");
    let mapping_base_addr = oversized.mapping_base_addr();
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let (oversized, mut socket) = run_test_output(&mut executor, async move {
        let (res, oversized) = socket.send(oversized).await;
        assert_oversized_send_rejected(res, &oversized);
        assert_eq!(oversized.mapping_base_addr(), mapping_base_addr);

        let (res, oversized) = socket.send_to(oversized, peer_addr).await;
        assert_oversized_send_rejected(res, &oversized);
        assert_eq!(oversized.mapping_base_addr(), mapping_base_addr);

        (oversized, socket)
    });

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, 0, "oversized UDP sends submitted an SQE");
        assert_eq!(
            stats.cqe_completions, 0,
            "oversized UDP sends observed a CQE"
        );
        assert_eq!(
            stats.retained_pooled_allocs, 0,
            "oversized UDP sends allocated retained payloads"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "oversized UDP sends used retained heap fallback"
        );
        assert_eq!(
            stats.poll_context_extractions, 2,
            "each local validation must still inspect its FlowIO context"
        );
    }

    let mut future = Box::pin(socket.send_to(oversized, peer_addr));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready((Err(error), oversized)) => {
            assert_eq!(error.kind(), io::ErrorKind::NotConnected);
            assert_eq!(oversized.mapping_base_addr(), mapping_base_addr);
            assert_eq!(
                oversized.as_ptr_calls(),
                0,
                "context rejection consulted the oversized buffer pointer"
            );
        }
        Poll::Ready((Ok(_), _)) => panic!("oversized send_to unexpectedly succeeded outside run"),
        Poll::Pending => panic!("oversized send_to remained pending outside run"),
    }
}

#[test]
fn runtime_udp_send_to_zero_datagram_submits_and_delivers() {
    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime UDP socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind peer UDP socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    peer.set_read_timeout(Some(UDP_TEST_TIMEOUT))
        .expect("failed to bound peer receive");
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    let empty = Vec::with_capacity(1);
    let empty_ptr = empty.as_ptr();
    let empty_capacity = empty.capacity();

    let (empty, socket) = run_test_output(&mut executor, async move {
        let (result, empty) = socket.send_to(empty, peer_addr).await;
        assert_eq!(result.expect("zero-length send_to failed"), 0);
        (empty, socket)
    });

    assert!(empty.is_empty());
    assert_eq!(empty.as_ptr(), empty_ptr);
    assert_eq!(empty.capacity(), empty_capacity);
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, 1);
        assert_eq!(stats.cqe_completions, 1);
        assert_eq!(
            stats.poll_context_extractions, 2,
            "submission and completion must each validate the FlowIO context"
        );
    }

    let mut received = [0u8; 1];
    let (received_len, from) = peer
        .recv_from(&mut received)
        .expect("peer did not receive zero-length datagram");
    assert_eq!(received_len, 0);
    assert_eq!(from, local_addr);
    drop(socket);
}

#[test]
fn runtime_udp_send_to_recv_from_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    configure_std_udp_peer_timeouts(&peer);

    let peer_thread = std::thread::spawn(move || {
        let send_len = peer
            .send_to(b"ping", local_addr)
            .expect("std send_to failed");
        assert_eq!(send_len, 4);

        let mut recv_buf = [0u8; 4];
        let (recv_len, from) = peer.recv_from(&mut recv_buf).expect("std recv_from failed");
        assert_eq!(recv_len, 4);
        assert_eq!(&recv_buf, b"pong");
        assert_eq!(from, local_addr);
    });

    executor
        .run(async move {
            timeout(UDP_TEST_TIMEOUT, async move {
                let recv = vec![0u8; 4];
                let (res, buf) = socket.recv_from(recv, 4).await;
                let (recv_len, from) = res.expect("recv_from failed");
                assert_eq!(recv_len, 4);
                assert_eq!(from, peer_addr);
                assert_eq!(&buf[..4], b"ping");

                let (res, _buf) = socket.send_to(b"pong".to_vec(), peer_addr).await;
                assert_eq!(res.expect("send_to failed"), 4);
            })
            .await
            .expect("unconnected UDP Vec exchange timed out");
        })
        .expect("executor run failed");

    peer_thread.join().expect("peer thread panicked");
}

#[test]
fn runtime_udp_rental_send_futures_poll_after_ready_parks() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    socket.connect(peer_addr).expect("runtime connect failed");

    executor
        .run(async move {
            assert_poll_after_ready_parks(socket.send(Vec::<u8>::new())).await;
            assert_poll_after_ready_parks(socket.send_to(Vec::<u8>::new(), peer_addr)).await;
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_recv_from_rejects_truncated_datagram() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    enable_socket_timestampns(socket.as_raw_fd());

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    peer.send_to(b"oversized", local_addr)
        .expect("std send_to failed");

    executor
        .run(async move {
            let recv = vec![0u8; 4];
            let (res, buf) = socket.recv_from(recv, 4).await;
            let err = res.expect_err("truncated datagram should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "UDP recv_from message was truncated");
            assert_eq!(&buf[..], b"over");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_recv_from_accepts_complete_payload_when_ancillary_metadata_is_truncated() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    enable_socket_timestampns(socket.as_raw_fd());

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    assert_eq!(
        peer.send_to(b"exact", local_addr)
            .expect("std send_to failed"),
        5
    );

    executor
        .run(async move {
            let recv = vec![0u8; 5];
            let (result, buffer) = timeout(UDP_TEST_TIMEOUT, socket.recv_from(recv, 5))
                .await
                .expect("timestamped recv_from timed out");
            let (received, from) = result.expect("complete recv_from payload was rejected");
            assert_eq!(received, 5);
            assert_eq!(from, peer_addr);
            assert_eq!(&buffer[..received], b"exact");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_recv_msg_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    socket.connect(peer_addr).expect("runtime connect failed");
    peer.connect(local_addr).expect("std peer connect failed");
    peer.send(b"pong").expect("std send failed");

    executor
        .run(async move {
            let recv = vec![0u8; 4];
            let (res, buf) = socket.recv_msg(recv, 4).await;
            assert_eq!(res.expect("recv_msg failed"), 4);
            assert_eq!(&buf[..4], b"pong");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_recv_msg_accepts_complete_payload_when_ancillary_metadata_is_truncated() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    enable_socket_timestampns(socket.as_raw_fd());

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    socket.connect(peer_addr).expect("runtime connect failed");
    peer.connect(local_addr).expect("std peer connect failed");
    assert_eq!(peer.send(b"exact").expect("std send failed"), 5);

    executor
        .run(async move {
            let recv = vec![0u8; 5];
            let (result, buffer) = timeout(UDP_TEST_TIMEOUT, socket.recv_msg(recv, 5))
                .await
                .expect("timestamped recv_msg timed out");
            let received = result.expect("complete recv_msg payload was rejected");
            assert_eq!(received, 5);
            assert_eq!(&buffer[..received], b"exact");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_recv_msg_rejects_truncated_datagram() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    enable_socket_timestampns(socket.as_raw_fd());

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    socket.connect(peer_addr).expect("runtime connect failed");
    peer.connect(local_addr).expect("std peer connect failed");
    peer.send(b"oversized").expect("std send failed");

    executor
        .run(async move {
            let recv = vec![0u8; 4];
            let (res, buf) = socket.recv_msg(recv, 4).await;
            let err = res.expect_err("truncated datagram should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "UDP recv_msg message was truncated");
            assert_eq!(&buf[..], b"over");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_recv_rejects_oversize_iobuff() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    socket.connect(peer_addr).expect("runtime connect failed");

    executor
        .run(async move {
            let recv = IoBuffMut::new(0, 4, 0);
            let (res, buf) = socket.recv(recv, 5).await;
            let err = res.expect_err("oversize recv should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(buf.payload_len(), 0);
            assert_eq!(buf.payload_remaining(), 4);
        })
        .expect("executor run failed");
}

/// Retain-until-CQE: a timed-out recv keeps its buffer alive because the
/// cancel CQE frees nothing; sending a datagram retires the original CQE.
#[test]
fn runtime_udp_cancelled_recv_retains_buffer_until_cqe() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    socket
        .connect(peer.local_addr().expect("peer local_addr failed"))
        .expect("runtime connect failed");

    executor
        .run(async move {
            let drops = Rc::new(Cell::new(0));
            let recv = DropTrackedReadWrite::zeroed(64, &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _buf) = socket.recv(recv, 64).await;
                res
            })
            .await;
            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "recv should time out without a datagram: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "UDP recv buffer dropped while original SQE was live"
            );
            peer.send_to(b"x", local_addr)
                .expect("std peer send_to failed");
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// Same retain-until-CQE rule as recv, but through recv_from with source
/// address storage retained beside the buffer.
#[test]
fn runtime_udp_cancelled_recv_from_retains_buffer_until_cqe() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");

    executor
        .run(async move {
            let drops = Rc::new(Cell::new(0));
            let recv = DropTrackedReadWrite::zeroed(64, &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _buf) = socket.recv_from(recv, 64).await;
                res
            })
            .await;
            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "recv_from should time out without a datagram: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "UDP recv_from buffer dropped while original SQE was live"
            );
            peer.send_to(b"x", local_addr)
                .expect("std peer send_to failed");
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// Same retain-until-CQE rule as recv, but through recv_msg with retained
/// msghdr/iovec metadata.
#[test]
fn runtime_udp_cancelled_recv_msg_retains_buffer_until_cqe() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");
    socket
        .connect(peer.local_addr().expect("peer local_addr failed"))
        .expect("runtime connect failed");

    executor
        .run(async move {
            let drops = Rc::new(Cell::new(0));
            let recv = DropTrackedReadWrite::zeroed(64, &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _buf) = socket.recv_msg(recv, 64).await;
                res
            })
            .await;
            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "recv_msg should time out without a datagram: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "UDP recv_msg buffer dropped while original SQE was live"
            );
            peer.send_to(b"x", local_addr)
                .expect("std peer send_to failed");
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

/// Dropping an in-flight UDP send keeps the payload alive until the original
/// CQE retires; synchronous completion returns the buffer to the caller.
#[test]
fn runtime_udp_cancelled_send_retains_buffer_until_cqe() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    socket
        .connect(peer.local_addr().expect("peer local_addr failed"))
        .expect("runtime connect failed");

    executor
        .run(async move {
            let drops = Rc::new(Cell::new(0));
            let send = DropTrackedReadOnly::new(b"x".to_vec(), &drops);
            let mut send = Box::pin(socket.send(send));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(send.as_mut(), cx))).await;

            match first_poll {
                Poll::Pending => {
                    drop(send);
                    assert_eq!(
                        drops.get(),
                        0,
                        "UDP send buffer dropped while original SQE was live"
                    );
                    wait_for_drop_count(&drops, 1).await;
                }
                Poll::Ready((_res, returned)) => {
                    assert_eq!(drops.get(), 0, "UDP send returned buffer before drop");
                    drop(returned);
                    assert_eq!(drops.get(), 1, "UDP send returned buffer dropped once");
                }
            }
        })
        .expect("executor run failed");
}

/// Same send retain-until-CQE rule for explicit-destination send_to.
#[test]
fn runtime_udp_cancelled_send_to_retains_buffer_until_cqe() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");

    executor
        .run(async move {
            let drops = Rc::new(Cell::new(0));
            let send = DropTrackedReadOnly::new(b"x".to_vec(), &drops);
            let mut send = Box::pin(socket.send_to(send, peer_addr));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(send.as_mut(), cx))).await;

            match first_poll {
                Poll::Pending => {
                    drop(send);
                    assert_eq!(
                        drops.get(),
                        0,
                        "UDP send_to buffer dropped while original SQE was live"
                    );
                    wait_for_drop_count(&drops, 1).await;
                }
                Poll::Ready((_res, returned)) => {
                    assert_eq!(drops.get(), 0, "UDP send_to returned buffer before drop");
                    drop(returned);
                    assert_eq!(drops.get(), 1, "UDP send_to returned buffer dropped once");
                }
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_kernel_error_send_returns_payload_once() {
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut socket, _peer, _peer_addr) = connected_udp_pair();

    executor
        .run(async move {
            const OVERSIZED_IPV4_DATAGRAM_LEN: usize = 65_508;
            let drops = Rc::new(Cell::new(0));
            let bytes = (0..OVERSIZED_IPV4_DATAGRAM_LEN)
                .map(|index| (index as u8).wrapping_mul(31).wrapping_add(7))
                .collect();
            let payload = DropTrackedReadOnly::new(bytes, &drops);
            let original_ptr = payload.bytes().as_ptr();

            let (res, returned) = socket.send(payload).await;
            let err = res.expect_err("oversized IPv4 UDP send should fail");
            assert_eq!(err.raw_os_error(), Some(libc::EMSGSIZE));
            assert_eq!(drops.get(), 0, "udp payload dropped before return");
            assert_eq!(returned.bytes().as_ptr(), original_ptr);
            assert_eq!(returned.bytes().len(), OVERSIZED_IPV4_DATAGRAM_LEN);
            assert!(
                returned
                    .bytes()
                    .iter()
                    .copied()
                    .enumerate()
                    .all(|(index, byte)| byte == (index as u8).wrapping_mul(31).wrapping_add(7)),
                "udp payload contents changed while retained by the failed send"
            );

            drop(returned);
            assert_eq!(drops.get(), 1, "udp payload dropped exactly once");
        })
        .expect("executor run failed");
}

/// UdpSocket peer_addr and socket options.
#[test]
fn runtime_udp_socket_options() {
    let mut socket =
        UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("failed to bind");

    assert!(socket.peer_addr().is_none());

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind peer");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");

    socket.connect(peer_addr).expect("connect failed");
    assert_eq!(socket.peer_addr(), Some(peer_addr));

    socket
        .set_send_buffer_size(65536)
        .expect("set_send_buffer_size failed");
    assert!(socket.send_buffer_size().expect("send_buffer_size failed") > 0);

    socket
        .set_recv_buffer_size(65536)
        .expect("set_recv_buffer_size failed");
    assert!(socket.recv_buffer_size().expect("recv_buffer_size failed") > 0);

    socket.set_broadcast(true).expect("set_broadcast failed");
    assert!(socket.broadcast().expect("broadcast failed"));
    socket
        .set_broadcast(false)
        .expect("set_broadcast false failed");
    assert!(!socket.broadcast().expect("broadcast false failed"));
}

// ============================================================================
// IoBuffMut / IoBuff transport integration tests
// ============================================================================

/// Connected UDP ping-pong using IoBuffMut for receive and IoBuffMut for send.
#[test]
fn runtime_udp_ping_pong_iobuff() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");

    socket.connect(peer_addr).expect("runtime connect failed");
    configure_std_udp_peer_timeouts(&peer);

    let peer_thread = std::thread::spawn(move || {
        peer.connect(local_addr).expect("std peer connect failed");

        let mut recv_buf = [0u8; 4];
        let recv_len = peer.recv(&mut recv_buf).expect("std recv failed");
        assert_eq!(recv_len, 4);
        assert_eq!(&recv_buf, b"ping");

        let send_len = peer.send(b"pong").expect("std send failed");
        assert_eq!(send_len, 4);
    });

    executor
        .run(async move {
            timeout(UDP_TEST_TIMEOUT, async move {
                let mut send_buf = IoBuffMut::new(0, 4, 0);
                send_buf.payload_append(b"ping").unwrap();
                let (res, _buf) = socket.send(send_buf).await;
                assert_eq!(res.expect("send failed"), 4);

                let recv_buf = IoBuffMut::new(0, 64, 0);
                let (res, buf) = socket.recv(recv_buf, 4).await;
                assert_eq!(res.expect("recv failed"), 4);
                assert_eq!(buf.payload_bytes(), b"pong");
            })
            .await
            .expect("connected UDP IoBuff exchange timed out");
        })
        .expect("executor run failed");

    peer_thread.join().expect("peer thread panicked");
}

/// Unconnected send_to / recv_from using IoBuffMut and IoBuff.
#[test]
fn runtime_udp_send_to_recv_from_iobuff() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr().expect("runtime local_addr failed");

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    configure_std_udp_peer_timeouts(&peer);

    let peer_thread = std::thread::spawn(move || {
        let send_len = peer
            .send_to(b"ping", local_addr)
            .expect("std send_to failed");
        assert_eq!(send_len, 4);

        let mut recv_buf = [0u8; 4];
        let (recv_len, from) = peer.recv_from(&mut recv_buf).expect("std recv_from failed");
        assert_eq!(recv_len, 4);
        assert_eq!(&recv_buf, b"pong");
        assert_eq!(from, local_addr);
    });

    executor
        .run(async move {
            timeout(UDP_TEST_TIMEOUT, async move {
                let recv_buf = IoBuffMut::new(0, 64, 0);
                let (res, buf) = socket.recv_from(recv_buf, 4).await;
                let (recv_len, from) = res.expect("recv_from failed");
                assert_eq!(recv_len, 4);
                assert_eq!(from, peer_addr);
                assert_eq!(buf.payload_bytes(), b"ping");

                let mut send_buf = IoBuffMut::new(0, 4, 0);
                send_buf.payload_append(b"pong").unwrap();
                let frozen = send_buf.freeze();
                let (res, _buf) = socket.send_to(frozen, peer_addr).await;
                assert_eq!(res.expect("send_to failed"), 4);
            })
            .await
            .expect("unconnected UDP IoBuff exchange timed out");
        })
        .expect("executor run failed");

    peer_thread.join().expect("peer thread panicked");
}

#[test]
fn runtime_udp_prefilled_iobuff_receives_append_for_all_apis() {
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut socket, peer, peer_addr) = connected_udp_pair();

    assert_eq!(peer.send(b"one").expect("std send one failed"), 3);
    assert_eq!(peer.send(b"two").expect("std send two failed"), 3);
    assert_eq!(peer.send(b"tri").expect("std send tri failed"), 3);

    executor
        .run(async move {
            let (result, buffer) = socket.recv(prefilled_udp_buffer(3), 3).await;
            assert_eq!(result.expect("recv failed"), 3);
            assert_eq!(buffer.payload_bytes(), b"HEADone");

            let (result, buffer) = socket.recv_msg(prefilled_udp_buffer(3), 3).await;
            assert_eq!(result.expect("recv_msg failed"), 3);
            assert_eq!(buffer.payload_bytes(), b"HEADtwo");

            let (result, buffer) = socket.recv_from(prefilled_udp_buffer(3), 3).await;
            let (received, from) = result.expect("recv_from failed");
            assert_eq!(received, 3);
            assert_eq!(from, peer_addr);
            assert_eq!(buffer.payload_bytes(), b"HEADtri");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_zero_datagrams_preserve_prefilled_iobuff_for_all_apis() {
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut socket, peer, peer_addr) = connected_udp_pair();

    assert_eq!(peer.send(&[]).expect("std send empty recv failed"), 0);
    assert_eq!(peer.send(&[]).expect("std send empty recv_msg failed"), 0);
    assert_eq!(peer.send(&[]).expect("std send empty recv_from failed"), 0);

    executor
        .run(async move {
            let (result, buffer) = socket.recv(prefilled_udp_buffer(4), 4).await;
            assert_eq!(result.expect("zero recv failed"), 0);
            assert_eq!(buffer.payload_bytes(), b"HEAD");

            let (result, buffer) = socket.recv_msg(prefilled_udp_buffer(4), 4).await;
            assert_eq!(result.expect("zero recv_msg failed"), 0);
            assert_eq!(buffer.payload_bytes(), b"HEAD");

            let (result, buffer) = socket.recv_from(prefilled_udp_buffer(4), 4).await;
            let (received, from) = result.expect("zero recv_from failed");
            assert_eq!(received, 0);
            assert_eq!(from, peer_addr);
            assert_eq!(buffer.payload_bytes(), b"HEAD");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_truncation_appends_copied_bytes_to_prefilled_iobuff() {
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut socket, peer, _peer_addr) = connected_udp_pair();

    assert_eq!(
        peer.send(b"oversized")
            .expect("std send recv_msg truncation failed"),
        9
    );
    assert_eq!(
        peer.send(b"oversized")
            .expect("std send recv_from truncation failed"),
        9
    );

    executor
        .run(async move {
            let (result, buffer) = socket.recv_msg(prefilled_udp_buffer(4), 4).await;
            let error = result.expect_err("truncated recv_msg should fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(buffer.payload_bytes(), b"HEADover");

            let (result, buffer) = socket.recv_from(prefilled_udp_buffer(4), 4).await;
            let error = result.expect_err("truncated recv_from should fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(buffer.payload_bytes(), b"HEADover");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_no_progress_boundaries_preserve_prefilled_iobuff() {
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut socket, peer, _peer_addr) = connected_udp_pair();

    assert_eq!(peer.send(&[]).expect("std send empty boundary failed"), 0);

    executor
        .run(async move {
            let (result, buffer) = socket.recv(prefilled_udp_buffer(2), 3).await;
            let error = result.expect_err("oversize recv should fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(buffer.payload_bytes(), b"HEAD");

            let (result, buffer) = socket.recv_msg(prefilled_udp_buffer(2), 3).await;
            let error = result.expect_err("oversize recv_msg should fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(buffer.payload_bytes(), b"HEAD");

            let (result, buffer) = socket.recv_from(prefilled_udp_buffer(2), 3).await;
            let error = result.expect_err("oversize recv_from should fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(buffer.payload_bytes(), b"HEAD");

            let mut sealed = IoBuffMut::new(0, 8, 4);
            sealed.payload_append(b"HEAD").unwrap();
            sealed.tailroom_append(b"TAIL").unwrap();
            let (result, buffer) = socket.recv(sealed, 0).await;
            assert_eq!(result.expect("zero-length sealed recv failed"), 0);
            assert_eq!(buffer.payload_bytes(), b"HEAD");
            assert_eq!(buffer.bytes(), b"HEADTAIL");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_context_errors_preserve_prefilled_iobuff_for_all_apis() {
    let (mut socket, _peer, _peer_addr) = connected_udp_pair();
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());

    {
        let mut future = Box::pin(socket.recv(prefilled_udp_buffer(4), 4));
        match future.as_mut().poll(&mut context) {
            Poll::Ready((Err(error), buffer)) => {
                assert_eq!(error.kind(), io::ErrorKind::NotConnected);
                assert_eq!(buffer.payload_bytes(), b"HEAD");
            }
            Poll::Ready((Ok(_), _)) => panic!("recv unexpectedly succeeded outside run"),
            Poll::Pending => panic!("recv remained pending outside run"),
        }
    }

    {
        let mut future = Box::pin(socket.recv_msg(prefilled_udp_buffer(4), 4));
        match future.as_mut().poll(&mut context) {
            Poll::Ready((Err(error), buffer)) => {
                assert_eq!(error.kind(), io::ErrorKind::NotConnected);
                assert_eq!(buffer.payload_bytes(), b"HEAD");
            }
            Poll::Ready((Ok(_), _)) => panic!("recv_msg unexpectedly succeeded outside run"),
            Poll::Pending => panic!("recv_msg remained pending outside run"),
        }
    }

    {
        let mut future = Box::pin(socket.recv_from(prefilled_udp_buffer(4), 4));
        match future.as_mut().poll(&mut context) {
            Poll::Ready((Err(error), buffer)) => {
                assert_eq!(error.kind(), io::ErrorKind::NotConnected);
                assert_eq!(buffer.payload_bytes(), b"HEAD");
            }
            Poll::Ready((Ok(_), _)) => panic!("recv_from unexpectedly succeeded outside run"),
            Poll::Pending => panic!("recv_from remained pending outside run"),
        }
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_udp_submission_errors_preserve_prefilled_iobuff_for_all_apis() {
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut socket, _peer, peer_addr) = connected_udp_pair();

    executor
        .run(async move {
            test_hooks::fail_next_sqe_submit();
            let (result, buffer) = socket.recv(prefilled_udp_buffer(4), 4).await;
            let error = result.expect_err("forced recv submission should fail");
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(buffer.payload_bytes(), b"HEAD");

            test_hooks::fail_next_sqe_submit();
            let (result, buffer) = socket.recv_msg(prefilled_udp_buffer(4), 4).await;
            let error = result.expect_err("forced recv_msg submission should fail");
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(buffer.payload_bytes(), b"HEAD");

            test_hooks::fail_next_sqe_submit();
            let (result, buffer) = socket.recv_from(prefilled_udp_buffer(4), 4).await;
            let error = result.expect_err("forced recv_from submission should fail");
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(buffer.payload_bytes(), b"HEAD");

            test_hooks::fail_next_sqe_submit();
            let (result, buffer) = socket.send_to(prefilled_udp_buffer(4), peer_addr).await;
            let error = result.expect_err("forced send_to submission should fail");
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(buffer.payload_bytes(), b"HEAD");
        })
        .expect("executor run failed");
}
