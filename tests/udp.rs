mod common;

use common::{
    DropTrackedReadOnly, DropTrackedReadWrite, HugeReadOnly, TestIoBuffMut as IoBuffMut,
    assert_poll_after_ready_parks, wait_for_drop_count,
};
use flowio::net::udp::UdpSocket;
use flowio::runtime::executor::Executor;
use flowio::runtime::timer::{sleep, timeout};
#[cfg(debug_assertions)]
use flowio::test_support::runtime::test_hooks;
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

fn is_connection_refused(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::ECONNREFUSED) || err.kind() == io::ErrorKind::ConnectionRefused
}

fn prefilled_udp_buffer(writable: usize) -> flowio::runtime::buffer::IoBuffMut {
    let mut buffer = IoBuffMut::new(0, 4 + writable, 0);
    buffer.payload_append(b"HEAD").unwrap();
    buffer
}

fn connected_udp_pair() -> (UdpSocket, StdUdpSocket, SocketAddr) {
    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr();
    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");
    socket.connect(peer_addr).expect("runtime connect failed");
    peer.connect(local_addr).expect("std peer connect failed");
    (socket, peer, peer_addr)
}

#[test]
fn runtime_udp_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr();

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");

    socket.connect(peer_addr).expect("runtime connect failed");

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
            let (res, _buf) = socket.send(b"ping".to_vec()).await;
            assert_eq!(res.expect("send failed"), 4);

            let recv = vec![0u8; 4];
            let (res, buf) = socket.recv(recv, 4).await;
            assert_eq!(res.expect("recv failed"), 4);
            assert_eq!(&buf[..4], b"pong");
        })
        .expect("executor run failed");

    peer_thread.join().expect("peer thread panicked");
}

#[test]
fn runtime_udp_send_rejects_oversize_iobuff() {
    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind udp socket");
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (res, _buf) = socket.send(HugeReadOnly).await;
            let err = res.expect_err("oversize udp send should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_send_to_recv_from_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr();

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");

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
            let recv = vec![0u8; 4];
            let (res, buf) = socket.recv_from(recv, 4).await;
            let (recv_len, from) = res.expect("recv_from failed");
            assert_eq!(recv_len, 4);
            assert_eq!(from, peer_addr);
            assert_eq!(&buf[..4], b"ping");

            let (res, _buf) = socket.send_to(b"pong".to_vec(), peer_addr).await;
            assert_eq!(res.expect("send_to failed"), 4);
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
    let local_addr = socket.local_addr();

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
            assert_eq!(&buf[..], b"over");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_udp_recv_msg_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr();

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
fn runtime_udp_recv_msg_rejects_truncated_datagram() {
    let mut executor = Executor::new().expect("failed to construct executor");

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let local_addr = socket.local_addr();

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
    let local_addr = socket.local_addr();
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
            assert!(result.is_err(), "recv should time out without a datagram");
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
    let local_addr = socket.local_addr();
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
                result.is_err(),
                "recv_from should time out without a datagram"
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
    let local_addr = socket.local_addr();
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
                result.is_err(),
                "recv_msg should time out without a datagram"
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

    let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind runtime udp socket");
    let closed_peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind closed udp peer");
    let closed_addr = closed_peer
        .local_addr()
        .expect("closed peer local_addr failed");
    drop(closed_peer);
    socket.connect(closed_addr).expect("runtime connect failed");

    executor
        .run(async move {
            let (probe_res, _probe) = socket.send(b"probe".to_vec()).await;
            if let Err(err) = probe_res
                && !is_connection_refused(&err)
            {
                panic!("initial udp probe failed unexpectedly: {err}");
            }

            sleep(Duration::from_millis(20))
                .await
                .expect("udp refused wait sleep failed");

            let drops = Rc::new(Cell::new(0));
            let mut payload = DropTrackedReadOnly::new(b"kernel-error".to_vec(), &drops);
            for _ in 0..16 {
                let (res, returned) = socket.send(payload).await;
                match res {
                    Err(err) if is_connection_refused(&err) => {
                        assert_eq!(drops.get(), 0, "udp payload dropped before return");
                        drop(returned);
                        assert_eq!(drops.get(), 1, "udp payload dropped exactly once");
                        return;
                    }
                    Err(err) => panic!("udp send failed with unexpected error: {err}"),
                    Ok(_) => {
                        assert_eq!(
                            drops.get(),
                            0,
                            "udp payload dropped after successful return"
                        );
                        payload = returned;
                        sleep(Duration::from_millis(10))
                            .await
                            .expect("udp retry wait sleep failed");
                    }
                }
            }

            panic!("connected udp send did not observe ECONNREFUSED from closed peer");
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
    let local_addr = socket.local_addr();

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");

    socket.connect(peer_addr).expect("runtime connect failed");

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
            let mut send_buf = IoBuffMut::new(0, 4, 0);
            send_buf.payload_append(b"ping").unwrap();
            let (res, _buf) = socket.send(send_buf).await;
            assert_eq!(res.expect("send failed"), 4);

            let recv_buf = IoBuffMut::new(0, 64, 0);
            let (res, buf) = socket.recv(recv_buf, 4).await;
            assert_eq!(res.expect("recv failed"), 4);
            assert_eq!(buf.payload_bytes(), b"pong");
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
    let local_addr = socket.local_addr();

    let peer = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind std udp socket");
    let peer_addr = peer.local_addr().expect("peer local_addr failed");

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

#[cfg(debug_assertions)]
#[test]
fn runtime_udp_submission_errors_preserve_prefilled_iobuff_for_all_apis() {
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut socket, _peer, _peer_addr) = connected_udp_pair();

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
        })
        .expect("executor run failed");
}
