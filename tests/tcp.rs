mod common;

use common::{
    TestIoBuffMut as IoBuffMut, TestProjected, make_payload_chain, make_read_chain,
    make_read_only_chain, run_test,
};
use flowio::net::tcp::{TcpConnector, TcpListener, TcpStream};
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::executor::Executor;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

fn spawn_std_tcp_peer(
    addr: SocketAddr,
    expected_recv: Vec<u8>,
    response: Vec<u8>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = vec![0u8; expected_recv.len()];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(buf, expected_recv, "std peer received unexpected payload");
        stream.write_all(&response).expect("std write failed");
    })
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

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_connect_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let (mut stream, _) = listener.accept().expect("std accept failed");
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

    peer.join().expect("peer panicked");
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

    peer.join().expect("peer panicked");
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

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_read_exact_eof() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::Write;

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        stream.write_all(b"hi").expect("std write failed");
        drop(stream); // close before sending 4 bytes
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let recv = vec![0u8; 4];
            let (res, buf) = stream.read_exact(recv, 4).await;
            let err = res.expect_err("should fail with UnexpectedEof");
            assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
            assert_eq!(&buf[..2], b"hi");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// TcpStream::connect() convenience creates a connection without a TcpConnector.
#[test]
fn runtime_tcp_stream_connect() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().expect("std accept failed");
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

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_stream_connect_timeout_success() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().expect("std accept failed");
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

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_connector_connect_timeout_success() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().expect("std accept failed");
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

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_stream_connect_timeout_propagates_connect_error() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
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

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
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

/// TcpStream address queries and socket options.
#[test]
fn runtime_tcp_socket_options() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = [0u8; 1];
        use std::io::Read;
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

    peer.join().expect("peer panicked");
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

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().expect("std accept failed");
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

    peer.join().expect("peer panicked");
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

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
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

            let recv_buf = IoBuffMut::new(0, 4, 0);
            let (res, buf) = stream.read(recv_buf, 4).await;
            assert_eq!(res.expect("read failed"), 4);
            assert_eq!(buf.payload_bytes(), b"pong");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// write_all with frozen IoBuff, read_exact with IoBuffMut.
#[test]
fn runtime_tcp_write_all_read_exact_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
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

            let recv_buf = IoBuffMut::new(0, 4, 0);
            let (res, buf) = stream.read_exact(recv_buf, 4).await;
            assert_eq!(res.expect("read_exact failed"), 4);
            assert_eq!(buf.payload_bytes(), b"pong");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// Staged IoBuffMut append reads preserve previously-read payload bytes.
#[test]
fn runtime_tcp_read_exact_append_iobuff_staged() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::Write;

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
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

    peer.join().expect("peer panicked");
}

/// IoBuffMut with headroom — prepend a protocol header before sending.
#[test]
fn runtime_tcp_iobuff_headroom() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::Read;

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
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

    peer.join().expect("peer panicked");
}

/// Pool-allocated buffers through TCP transport.
#[test]
fn runtime_tcp_pool_buffers() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
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

    peer.join().expect("peer panicked");
}

/// Large payload with IoBuffMut — forces partial kernel transfers.
#[test]
fn runtime_tcp_write_all_read_exact_large_iobuff() {
    let msg_size = 256 * 1024;
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
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

    peer.join().expect("peer panicked");
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

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
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

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_writev_all_read_only_to_std_peer() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"hello world".to_vec(), b"ack".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let chain = make_read_only_chain([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
        let (res, chain) = stream.writev_all_read_only(chain).await;
        assert_eq!(res.expect("writev_all_read_only failed"), 11);
        assert_eq!(chain.segments(), 4);

        let (res, buf) = stream.read_exact(vec![0u8; 3], 3).await;
        assert_eq!(res.expect("read_exact failed"), 3);
        assert_eq!(&buf[..], b"ack");
    });

    peer.join().expect("peer panicked");
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

    peer.join().expect("peer panicked");
}
