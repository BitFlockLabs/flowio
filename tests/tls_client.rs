mod common;

use common::{DropTrackedReadOnly, assert_poll_after_ready_parks};
use flowio::net::tcp::TcpStream as FlowTcpStream;
use flowio::net::tls::{TlsClientOptions, TlsClientStream};
use flowio::runtime::executor::Executor;
#[cfg(debug_assertions)]
use flowio::runtime::timer::sleep;
use flowio::runtime::timer::timeout;
#[cfg(debug_assertions)]
use flowio::test_support::net::tls_test_peer::{drain_available_client_hello, force_reset_on_drop};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection};
use std::cell::Cell;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;
#[cfg(debug_assertions)]
use std::time::Instant;

/// Small buffers force the 8KiB test payload to span multiple TLS records and
/// exercise partial read/write pumping.
fn tls_options() -> TlsClientOptions {
    TlsClientOptions {
        rustls_buffer_limit: Some(2048),
        transport_read_buffer_size: 2048,
        transport_write_buffer_size: 2048,
    }
}

fn large_read_tls_options() -> TlsClientOptions {
    TlsClientOptions {
        rustls_buffer_limit: None,
        transport_read_buffer_size: 64 * 1024,
        transport_write_buffer_size: 2048,
    }
}

fn bulk_tls_payload() -> Vec<u8> {
    (0..48 * 1024).map(|idx| (idx % 251) as u8).collect()
}

/// Builds matched self-signed client/server configs plus the localhost server
/// name and end-entity cert DER used by the handshake assertions.
fn make_client_server_configs() -> (
    Arc<ClientConfig>,
    Arc<ServerConfig>,
    ServerName<'static>,
    Vec<u8>,
) {
    let certified = generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("failed to generate self-signed test cert");
    let cert_der = certified.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());

    let mut roots = RootCertStore::empty();
    roots
        .add(cert_der.clone())
        .expect("failed to add root certificate");

    let client = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let server = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der.into())
            .expect("failed to build rustls server config"),
    );

    (
        client,
        server,
        ServerName::try_from("localhost").expect("invalid test server name"),
        cert_der.as_ref().to_vec(),
    )
}

fn server_connection_after_handshake(
    tcp: &mut std::net::TcpStream,
    server_config: Arc<ServerConfig>,
) -> ServerConnection {
    let mut tls = ServerConnection::new(server_config).expect("server tls init failed");
    while tls.is_handshaking() {
        tls.complete_io(tcp).expect("server handshake failed");
    }
    tls
}

fn complete_server_handshake(tcp: &mut std::net::TcpStream, server_config: Arc<ServerConfig>) {
    let _ = server_connection_after_handshake(tcp, server_config);
}

#[cfg(debug_assertions)]
fn assert_tls_write_peer_close_error(err: &io::Error) {
    assert!(
        matches!(
            err.raw_os_error(),
            Some(libc::EPIPE | libc::ECONNRESET | libc::ESHUTDOWN | libc::ENOTCONN)
        ) || matches!(
            err.kind(),
            io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::NotConnected
        ),
        "unexpected tls write peer-close error: {err}"
    );
}

#[cfg(debug_assertions)]
async fn wait_for_server_reset(rx: &mpsc::Receiver<()>) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match rx.try_recv() {
            Ok(()) => return,
            Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                sleep(Duration::from_millis(1))
                    .await
                    .expect("server reset wait sleep failed");
            }
            Err(mpsc::TryRecvError::Empty) => panic!("server did not reset before timeout"),
            Err(mpsc::TryRecvError::Disconnected) => {
                panic!("server reset signal channel disconnected")
            }
        }
    }
}

#[test]
fn tls_client_round_trip_and_shutdown() {
    let (client_config, server_config, server_name, expected_cert_der) =
        make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        let mut tls = ServerConnection::new(server_config).expect("server tls init failed");

        while tls.is_handshaking() {
            tls.complete_io(&mut tcp).expect("server handshake failed");
        }

        let mut recv = vec![0u8; 8192];
        let mut filled = 0usize;
        while filled < recv.len() {
            match tls.reader().read(&mut recv[filled..]) {
                Ok(0) => panic!("server saw EOF before full payload"),
                Ok(read) => filled += read,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    tls.complete_io(&mut tcp).expect("server read pump failed");
                }
                Err(err) => panic!("server read failed: {err}"),
            }
        }
        assert!(
            recv.iter().all(|&byte| byte == 0x5A),
            "server payload mismatch"
        );

        tls.writer()
            .write_all(b"pong")
            .expect("server write_all failed");
        while tls.wants_write() {
            tls.complete_io(&mut tcp).expect("server flush failed");
        }

        let mut probe = [0u8; 1];
        loop {
            match tls.reader().read(&mut probe) {
                Ok(0) => break,
                Ok(read) => panic!("unexpected extra TLS plaintext bytes: {read}"),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    tls.complete_io(&mut tcp)
                        .expect("server close-notify wait failed");
                }
                Err(err) => panic!("server read error after response: {err}"),
            }
        }
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();

            tls.handshake().await.expect("client handshake failed");

            let payload = vec![0x5Au8; 8192];
            let (res, payload) = tls.write_all(payload).await;
            assert_eq!(res.expect("client write_all failed"), payload.len());

            let recv = vec![0u8; 4];
            let (res, recv) = tls.read_exact(recv, 4).await;
            assert_eq!(res.expect("client read_exact failed"), 4);
            assert_eq!(&recv[..], b"pong");

            assert_eq!(
                tls.peer_end_entity_certificate_der()
                    .expect("peer cert missing"),
                expected_cert_der.as_slice()
            );

            tls.shutdown().await.expect("client shutdown failed");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
fn tls_rental_futures_poll_after_ready_parks() {
    let (client_config, server_config, server_name, _) = make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let (client_done_tx, client_done_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        complete_server_handshake(&mut tcp, server_config);
        client_done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("client did not finish poll-after-ready checks");
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();

            tls.handshake().await.expect("client handshake failed");

            assert_poll_after_ready_parks(tls.read(Vec::<u8>::new(), 0)).await;
            assert_poll_after_ready_parks(tls.read_exact(Vec::<u8>::new(), 0)).await;
            assert_poll_after_ready_parks(tls.write(Vec::<u8>::new())).await;
            assert_poll_after_ready_parks(tls.write_all(Vec::<u8>::new())).await;
            client_done_tx
                .send(())
                .expect("failed to signal client poll-after-ready completion");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
fn tls_large_transport_read_buffer_handles_bulk_ciphertext() {
    let (client_config, server_config, server_name, _expected_cert_der) =
        make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let payload = bulk_tls_payload();
    let expected = payload.clone();
    let (server_sent_tx, server_sent_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        let mut tls = server_connection_after_handshake(&mut tcp, server_config);

        tls.writer()
            .write_all(&payload)
            .expect("server bulk write_all failed");
        while tls.wants_write() {
            tls.complete_io(&mut tcp).expect("server bulk flush failed");
        }
        server_sent_tx
            .send(())
            .expect("failed to signal server bulk flush");
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, large_read_tls_options())
                    .unwrap();

            tls.handshake().await.expect("client handshake failed");
            server_sent_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("server did not flush bulk TLS payload");

            let (res, recv) = tls
                .read_exact(vec![0u8; expected.len()], expected.len())
                .await;
            assert_eq!(res.expect("client bulk read_exact failed"), expected.len());
            assert_eq!(recv, expected);
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
#[cfg(debug_assertions)]
fn tls_handshake_eof_returns_unexpected_eof() {
    let (client_config, _server_config, server_name, _expected_cert_der) =
        make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let server = std::thread::spawn(move || {
        let (tcp, _) = listener.accept().expect("std accept failed");
        drain_available_client_hello(tcp);
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();

            let result = timeout(Duration::from_secs(1), tls.handshake()).await;
            let err = match result {
                Ok(Ok(())) => panic!("handshake should not succeed after peer close"),
                Ok(Err(err)) => err,
                Err(_) => panic!("handshake timed out instead of returning EOF"),
            };
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
fn tls_clean_close_read_returns_ok_zero() {
    let (client_config, server_config, server_name, _expected_cert_der) =
        make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        let mut tls = ServerConnection::new(server_config).expect("server tls init failed");
        while tls.is_handshaking() {
            tls.complete_io(&mut tcp).expect("server handshake failed");
        }

        tls.send_close_notify();
        while tls.wants_write() {
            tls.complete_io(&mut tcp)
                .expect("server close_notify flush failed");
        }
        tcp.shutdown(Shutdown::Write)
            .expect("server shutdown write failed");
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();

            tls.handshake().await.expect("client handshake failed");

            let (res, buf) = tls.read(vec![0u8; 1], 1).await;
            assert_eq!(res.expect("clean close read failed"), 0);
            assert_eq!(buf.len(), 0, "clean close should publish zero bytes");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
fn tls_truncated_close_read_exact_returns_unexpected_eof() {
    let (client_config, server_config, server_name, _expected_cert_der) =
        make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        complete_server_handshake(&mut tcp, server_config);
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();

            tls.handshake().await.expect("client handshake failed");

            let (res, buf) = tls.read_exact(vec![0u8; 4], 4).await;
            let err = res.expect_err("truncated TLS close should fail read_exact");
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
            assert_eq!(buf.len(), 0, "truncated close should publish no bytes");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
#[cfg(debug_assertions)]
fn tls_write_after_peer_reset_returns_payload_once() {
    let (client_config, server_config, server_name, _expected_cert_der) =
        make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let (server_reset_tx, server_reset_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        complete_server_handshake(&mut tcp, server_config);
        force_reset_on_drop(&tcp);
        drop(tcp);
        server_reset_tx
            .send(())
            .expect("failed to signal server reset");
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();

            tls.handshake().await.expect("client handshake failed");
            wait_for_server_reset(&server_reset_rx).await;

            let drops = Rc::new(Cell::new(0));
            let payload = DropTrackedReadOnly::new(b"after-reset".to_vec(), &drops);
            let (res, returned) = tls.write_all(payload).await;
            let err = res.expect_err("write after peer reset should fail");
            assert_tls_write_peer_close_error(&err);
            assert_eq!(drops.get(), 0, "tls write payload dropped before return");
            drop(returned);
            assert_eq!(drops.get(), 1, "tls write payload dropped exactly once");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
#[cfg(debug_assertions)]
fn tls_read_drains_staged_plaintext_after_write_failure() {
    let (client_config, server_config, server_name, _expected_cert_der) =
        make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let (client_read_done_tx, client_read_done_rx) = mpsc::channel();
    let (server_reset_tx, server_reset_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        let mut tls = ServerConnection::new(server_config).expect("server tls init failed");

        while tls.is_handshaking() {
            tls.complete_io(&mut tcp).expect("server handshake failed");
        }

        tls.writer()
            .write_all(b"abcdefgh")
            .expect("server staged plaintext write failed");
        while tls.wants_write() {
            tls.complete_io(&mut tcp)
                .expect("server plaintext flush failed");
        }

        client_read_done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("client did not read first plaintext fragment");
        force_reset_on_drop(&tcp);
        drop(tcp);
        server_reset_tx
            .send(())
            .expect("failed to signal server reset");
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();

            tls.handshake().await.expect("client handshake failed");

            let (res, first) = tls.read(vec![0u8; 4], 4).await;
            assert_eq!(res.expect("first TLS read failed"), 4);
            assert_eq!(&first[..], b"abcd");
            client_read_done_tx
                .send(())
                .expect("failed to signal first client read");
            wait_for_server_reset(&server_reset_rx).await;

            let (res, _returned) = tls.write_all(b"after-reset".to_vec()).await;
            let err = res.expect_err("write after peer reset should fail");
            assert_tls_write_peer_close_error(&err);

            let (res, second) = tls.read(vec![0u8; 4], 4).await;
            assert_eq!(
                res.expect("staged plaintext should remain readable after write failure"),
                4
            );
            assert_eq!(&second[..], b"efgh");

            let (res, _returned) = tls.write_all(b"still-failed".to_vec()).await;
            let err = res.expect_err("latched write failure should persist");
            assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
fn tls_cancelled_read_does_not_poison_shutdown() {
    let (client_config, server_config, server_name, _expected_cert_der) =
        make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        tcp.set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set_read_timeout failed");
        let mut tls = ServerConnection::new(server_config).expect("server tls init failed");

        while tls.is_handshaking() {
            tls.complete_io(&mut tcp).expect("server handshake failed");
        }

        let mut probe = [0u8; 1];
        loop {
            match tls.reader().read(&mut probe) {
                Ok(0) => break,
                Ok(read) => panic!("unexpected TLS plaintext bytes before shutdown: {read}"),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    tls.complete_io(&mut tcp)
                        .expect("server close-notify wait failed");
                }
                Err(err) => panic!("server read failed while waiting for close_notify: {err}"),
            }
        }
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();

            tls.handshake().await.expect("client handshake failed");

            let result = timeout(Duration::from_millis(50), async {
                let (res, _buf) = tls.read(vec![0u8; 1], 1).await;
                res
            })
            .await;
            assert!(result.is_err(), "silent peer read should time out");

            tls.shutdown()
                .await
                .expect("shutdown should flush close_notify after cancelled read");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[test]
fn tls_client_requires_explicit_handshake() {
    let client_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let server = std::thread::spawn(move || {
        let (_tcp, _) = listener.accept().expect("std accept failed");
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls = TlsClientStream::new(
                tcp,
                client_config,
                ServerName::try_from("localhost").expect("invalid test server name"),
                tls_options(),
            )
            .expect("tls stream init failed");

            let drops = Rc::new(Cell::new(0));
            let payload = DropTrackedReadOnly::new(b"ping".to_vec(), &drops);
            let (res, returned) = tls.write_all(payload).await;
            let err = res.expect_err("write_all should fail before handshake");
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(drops.get(), 0, "tls error payload dropped before return");
            drop(returned);
            assert_eq!(drops.get(), 1, "tls error payload dropped exactly once");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}
