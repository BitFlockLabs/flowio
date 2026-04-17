use flowio::net::tcp::TcpStream as FlowTcpStream;
use flowio::net::tls::{TlsClientOptions, TlsClientStream};
use flowio::runtime::executor::Executor;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

fn tls_options() -> TlsClientOptions {
    TlsClientOptions {
        rustls_buffer_limit: Some(2048),
        transport_read_buffer_size: 2048,
        transport_write_buffer_size: 2048,
    }
}

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

            let (res, _buf) = tls.write_all(b"ping".to_vec()).await;
            let err = res.expect_err("write_all should fail before handshake");
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        })
        .expect("executor run failed");

    server.join().expect("server thread panicked");
}
