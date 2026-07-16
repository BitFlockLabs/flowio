mod common;

use common::InitializationTrackedReadWrite;
use common::{DropTrackedReadOnly, assert_poll_after_ready_parks};
use flowio::net::tcp::TcpStream as FlowTcpStream;
use flowio::net::tls::{TlsClientOptions, TlsClientStream};
use flowio::runtime::buffer::{IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use flowio::runtime::executor::Executor;
#[cfg(debug_assertions)]
use flowio::runtime::timer::sleep;
use flowio::runtime::timer::timeout;
#[cfg(debug_assertions)]
use flowio::test_support::net::tls_test_peer::{drain_available_client_hello, force_reset_on_drop};
#[cfg(debug_assertions)]
use flowio::test_support::runtime::test_hooks;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection};
use std::cell::Cell;
#[cfg(debug_assertions)]
use std::cell::RefCell;
#[cfg(debug_assertions)]
use std::future::{Future, poll_fn};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc;
#[cfg(debug_assertions)]
use std::task::Poll;
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

struct NullEmptyReadOnly {
    pointer_calls: Cell<usize>,
}

impl NullEmptyReadOnly {
    fn new() -> Self {
        Self {
            pointer_calls: Cell::new(0),
        }
    }
}

// SAFETY: this fixture has a stable zero-length readable window, for which the
// buffer contract explicitly permits a null pointer.
unsafe impl IoBuffReadOnly for NullEmptyReadOnly {
    fn as_ptr(&self) -> *const u8 {
        self.pointer_calls.set(self.pointer_calls.get() + 1);
        std::ptr::null()
    }

    fn len(&self) -> usize {
        0
    }
}

struct NullEmptyReadWrite {
    pointer_calls: usize,
    publication_calls: usize,
}

impl NullEmptyReadWrite {
    fn new() -> Self {
        Self {
            pointer_calls: 0,
            publication_calls: 0,
        }
    }
}

// SAFETY: this fixture has a stable zero-length writable window, for which the
// buffer contract explicitly permits a null pointer. It never exposes bytes.
unsafe impl IoBuffReadWrite for NullEmptyReadWrite {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.pointer_calls += 1;
        std::ptr::null_mut()
    }

    fn writable_len(&self) -> usize {
        0
    }

    unsafe fn set_written_len(&mut self, len: usize) {
        assert_eq!(len, 0, "null-empty fixture cannot publish bytes");
        self.publication_calls += 1;
    }
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

            let recv = IoBuffMut::new(0, 4, 0).expect("client receive buffer allocation failed");
            let (res, recv) = tls.read_exact(recv, 4).await;
            assert_eq!(res.expect("client read_exact failed"), 4);
            assert_eq!(recv.payload_bytes(), b"pong");

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
fn tls_partial_read_publishes_relative_iobuff_and_fresh_vec() {
    let (client_config, server_config, server_name, _) = make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let (client_phase_tx, client_phase_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        let mut tls = server_connection_after_handshake(&mut tcp, server_config);
        tls.writer()
            .write_all(b"abc")
            .expect("server partial payload write failed");
        while tls.wants_write() {
            tls.complete_io(&mut tcp)
                .expect("server partial payload flush failed");
        }
        client_phase_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("client did not finish prefilled partial TLS read");

        tls.writer()
            .write_all(b"xyz")
            .expect("server fresh Vec payload write failed");
        while tls.wants_write() {
            tls.complete_io(&mut tcp)
                .expect("server fresh Vec payload flush failed");
        }
        client_phase_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("client did not finish fresh Vec partial TLS read");
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

            let mut buffer =
                IoBuffMut::new(0, 12, 0).expect("prefilled receive buffer allocation failed");
            buffer
                .payload_append(b"HEAD")
                .expect("prefilled receive buffer append failed");
            let (result, buffer) = tls.read(buffer, 8).await;
            assert_eq!(result.expect("client prefilled partial read failed"), 3);
            assert_eq!(buffer.payload_bytes(), b"HEADabc");
            client_phase_tx
                .send(())
                .expect("failed to signal prefilled partial read completion");

            let (result, buffer) = tls.read(Vec::with_capacity(8), 8).await;
            assert_eq!(result.expect("client fresh Vec partial read failed"), 3);
            assert_eq!(buffer, b"xyz");
            client_phase_tx
                .send(())
                .expect("failed to signal fresh Vec partial read completion");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[cfg(debug_assertions)]
#[test]
fn tls_staged_transport_future_drops_after_executor_teardown() {
    let (client_config, server_config, server_name, _) = make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let (release_tx, release_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        complete_server_handshake(&mut tcp, server_config);
        release_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("TLS teardown test did not release server");
    });

    let escaped = Rc::new(RefCell::new(None::<TlsClientStream>));
    let escaped_slot = Rc::clone(&escaped);
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    let err = executor
        .run(async move {
            let tcp = FlowTcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let mut tls =
                TlsClientStream::new(tcp, client_config, server_name, tls_options()).unwrap();
            tls.handshake().await.expect("client handshake failed");

            let initialization_calls = Rc::new(Cell::new(0));
            let initialized_bytes = Rc::new(Cell::new(0));
            let buffer =
                InitializationTrackedReadWrite::new(1, &initialization_calls, &initialized_bytes);
            let mut read = Box::pin(tls.read(buffer, 1));
            poll_fn(|cx| {
                for _ in 0..2 {
                    match read.as_mut().poll(cx) {
                        Poll::Pending => {}
                        Poll::Ready(_) => {
                            panic!("TLS transport read completed before staging")
                        }
                    }
                }
                Poll::Ready(())
            })
            .await;
            assert_eq!(initialization_calls.get(), 1);
            assert_eq!(initialized_bytes.get(), 1);
            drop(read);

            *escaped_slot.borrow_mut() = Some(tls);
            test_hooks::fail_next_ring_wait_errno(libc::EIO);
        })
        .expect_err("injected wait error should end the staged TLS run");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));

    drop(executor);
    drop(
        escaped
            .borrow_mut()
            .take()
            .expect("staged TLS stream did not escape"),
    );
    release_tx.send(()).expect("TLS server release failed");
    server.join().expect("TLS server thread panicked");
}

#[test]
fn tls_zero_length_prefilled_reads_preserve_prefix_and_park_after_ready() {
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

            let mut read_buffer =
                IoBuffMut::new(0, 4, 0).expect("zero read buffer allocation failed");
            read_buffer
                .payload_append(b"HEAD")
                .expect("zero read prefix append failed");
            let (result, read_buffer) = tls.read(read_buffer, 0).await;
            assert_eq!(result.expect("zero read failed"), 0);
            assert_eq!(read_buffer.payload_bytes(), b"HEAD");

            let mut exact_buffer =
                IoBuffMut::new(0, 4, 0).expect("zero exact-read buffer allocation failed");
            exact_buffer
                .payload_append(b"HEAD")
                .expect("zero exact-read prefix append failed");
            let (result, exact_buffer) = tls.read_exact(exact_buffer, 0).await;
            assert_eq!(result.expect("zero exact read failed"), 0);
            assert_eq!(exact_buffer.payload_bytes(), b"HEAD");

            let (result, source) = tls.write(NullEmptyReadOnly::new()).await;
            assert_eq!(result.expect("null-empty partial write failed"), 0);
            assert_eq!(source.pointer_calls.get(), 0);

            let (result, source) = tls.write_all(NullEmptyReadOnly::new()).await;
            assert_eq!(result.expect("null-empty exact write failed"), 0);
            assert_eq!(
                source.pointer_calls.get(),
                1,
                "exact write caches the pointer but must not consult it again"
            );

            let (result, destination) = tls.read(NullEmptyReadWrite::new(), 0).await;
            assert_eq!(result.expect("null-empty partial read failed"), 0);
            assert_eq!(destination.pointer_calls, 0);
            assert_eq!(destination.publication_calls, 0);

            let (result, destination) = tls.read_exact(NullEmptyReadWrite::new(), 0).await;
            assert_eq!(result.expect("null-empty exact read failed"), 0);
            assert_eq!(destination.pointer_calls, 0);
            assert_eq!(destination.publication_calls, 0);

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

            let mut buf =
                IoBuffMut::new(0, 8, 0).expect("clean close receive buffer allocation failed");
            buf.payload_append(b"HEAD")
                .expect("clean close receive prefix append failed");
            let (res, buf) = tls.read(buf, 4).await;
            assert_eq!(res.expect("clean close read failed"), 0);
            assert_eq!(
                buf.payload_bytes(),
                b"HEAD",
                "clean close should preserve the readable prefix"
            );

            let (res, buf) = tls.read(Vec::with_capacity(1), 1).await;
            assert_eq!(res.expect("fresh Vec clean close read failed"), 0);
            assert_eq!(buf.len(), 0, "fresh Vec clean close should stay empty");
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
        let mut tls = server_connection_after_handshake(&mut tcp, server_config);
        tls.writer()
            .write_all(b"wxyzab")
            .expect("server truncated payload write failed");
        while tls.wants_write() {
            tls.complete_io(&mut tcp)
                .expect("server truncated payload flush failed");
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

            let initialization_calls = Rc::new(Cell::new(0));
            let initialized_bytes = Rc::new(Cell::new(0));
            let buffer =
                InitializationTrackedReadWrite::new(4, &initialization_calls, &initialized_bytes);
            let (res, buf) = tls.read_exact(buffer, 4).await;
            assert_eq!(res.expect("custom exact read failed"), 4);
            assert_eq!(buf.bytes(), b"wxyz");
            assert_eq!(initialization_calls.get(), 1);
            assert_eq!(initialized_bytes.get(), 4);

            let mut buffer =
                IoBuffMut::new(0, 8, 0).expect("truncated receive buffer allocation failed");
            buffer
                .payload_append(b"HEAD")
                .expect("truncated receive prefix append failed");
            let (res, buf) = tls.read_exact(buffer, 4).await;
            let err = res.expect_err("truncated TLS close should fail read_exact");
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
            assert_eq!(buf.payload_bytes(), b"HEADab");
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}

#[cfg(debug_assertions)]
#[test]
fn tls_userspace_destination_error_preserves_unpublished_length() {
    let (client_config, server_config, server_name, _) = make_client_server_configs();
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let (release_tx, release_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().expect("std accept failed");
        complete_server_handshake(&mut tcp, server_config);
        release_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("TLS destination error test did not release server");
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

            test_hooks::fail_next_sqe_submit();
            let initialization_calls = Rc::new(Cell::new(0));
            let initialized_bytes = Rc::new(Cell::new(0));
            let buffer =
                InitializationTrackedReadWrite::new(4, &initialization_calls, &initialized_bytes);
            let (result, buffer) = tls.read(buffer, 4).await;
            let err = result.expect_err("forced custom raw read submission should fail");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
            assert!(buffer.bytes().is_empty());
            assert_eq!(initialization_calls.get(), 1);
            assert_eq!(initialized_bytes.get(), 4);

            test_hooks::fail_next_sqe_submit();
            let mut buffer =
                IoBuffMut::new(0, 8, 0).expect("destination error buffer allocation failed");
            buffer
                .payload_append(b"HEAD")
                .expect("destination error prefix append failed");
            let (result, buffer) = tls.read(buffer, 4).await;
            let err = result.expect_err("forced raw read submission should fail");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(buffer.payload_bytes(), b"HEAD");
            release_tx
                .send(())
                .expect("failed to release TLS destination error server");
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
