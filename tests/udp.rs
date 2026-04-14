mod common;

use common::TestIoBuffMut as IoBuffMut;
use flowio::net::udp::UdpSocket;
use flowio::runtime::executor::Executor;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};

#[test]
fn runtime_udp_ping_pong()
{
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
fn runtime_udp_send_to_recv_from_ping_pong()
{
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
fn runtime_udp_recv_rejects_oversize_iobuff()
{
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

/// UdpSocket peer_addr and socket options.
#[test]
fn runtime_udp_socket_options()
{
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
fn runtime_udp_ping_pong_iobuff()
{
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
fn runtime_udp_send_to_recv_from_iobuff()
{
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
