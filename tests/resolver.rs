use flowio::net::resolver::{DnsResolver, resolve_host};
use flowio::runtime::buffer::bytes::{ByteWriteAt, read_u16_be_at};
use flowio::runtime::executor::Executor;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy)]
enum TestAnswer {
    A(Ipv4Addr),
    Cname(&'static str),
    Empty,
    NxDomain,
}

#[test]
fn resolve_host_handles_ip_literals_and_localhost() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async {
            let addrs = resolve_host("127.0.0.1", 5432)
                .await
                .expect("ip literal resolution failed");
            assert_eq!(addrs, vec![SocketAddr::from((Ipv4Addr::LOCALHOST, 5432))]);

            let localhost = resolve_host("localhost", 5432)
                .await
                .expect("localhost resolution failed");
            assert!(
                localhost
                    .iter()
                    .any(|addr| *addr == SocketAddr::from((Ipv4Addr::LOCALHOST, 5432))),
                "localhost resolution did not include 127.0.0.1"
            );
        })
        .expect("executor run failed");
}

#[test]
fn resolve_host_rejects_empty_host_name() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async {
            let err = resolve_host("   ", 5432)
                .await
                .expect_err("empty host should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        })
        .expect("executor run failed");
}

#[test]
fn dns_resolver_requires_nameserver() {
    let err = DnsResolver::new(Vec::new()).expect_err("empty nameserver list should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn resolve_host_queries_custom_nameserver() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 42)),
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("custom nameserver resolution failed");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 42), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_follows_cname_to_address() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 4, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::Cname("db.internal.test"),
            ("db.example.test", 28) => TestAnswer::Empty,
            ("db.internal.test", 1) => TestAnswer::A(Ipv4Addr::new(198, 51, 100, 24)),
            ("db.internal.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("cname resolution failed");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(198, 51, 100, 24), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

fn serve_dns_queries<F>(socket: StdUdpSocket, expected_queries: usize, answer: F)
where
    F: Fn(&str, u16) -> TestAnswer,
{
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("failed to set test dns timeout");

    for _ in 0..expected_queries {
        let mut buffer = [0u8; 512];
        let (len, peer) = socket.recv_from(&mut buffer).expect("dns recv_from failed");
        let query = &buffer[..len];
        let qname = parse_qname(query).expect("failed to parse dns qname");
        let qtype = parse_qtype(query).expect("failed to parse dns qtype");
        let response = build_response(query, answer(&qname, qtype));
        socket
            .send_to(&response, peer)
            .expect("dns send_to response failed");
    }
}

fn parse_qname(packet: &[u8]) -> io::Result<String> {
    let mut offset = 12usize;
    let mut labels = Vec::new();

    while offset < packet.len() {
        let len = packet[offset] as usize;
        offset += 1;
        if len == 0 {
            return Ok(labels.join("."));
        }

        let end = offset
            .checked_add(len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "dns qname overflow"))?;
        if end > packet.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "dns qname exceeded packet length",
            ));
        }

        labels.push(String::from_utf8_lossy(&packet[offset..end]).into_owned());
        offset = end;
    }

    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "dns qname missing terminator",
    ))
}

fn parse_qtype(packet: &[u8]) -> io::Result<u16> {
    let mut offset = 12usize;
    while offset < packet.len() {
        let len = packet[offset] as usize;
        offset += 1;
        if len == 0 {
            break;
        }
        offset = offset
            .checked_add(len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "dns qtype overflow"))?;
    }

    if offset + 4 > packet.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "dns question too short",
        ));
    }

    read_u16_be_at(packet, offset).map_err(|err| io::Error::new(io::ErrorKind::UnexpectedEof, err))
}

fn push_u16_be(dst: &mut Vec<u8>, value: u16) {
    let offset = dst.len();
    dst.resize(offset + 2, 0);
    dst.write_u16_be_at(offset, value)
        .expect("test u16 big-endian write should fit");
}

fn push_u32_be(dst: &mut Vec<u8>, value: u32) {
    let offset = dst.len();
    dst.resize(offset + 4, 0);
    dst.write_u32_be_at(offset, value)
        .expect("test u32 big-endian write should fit");
}

fn build_response(query: &[u8], answer: TestAnswer) -> Vec<u8> {
    let question_end = question_end(query).expect("invalid dns question");
    let answer_count = match answer {
        TestAnswer::A(_) | TestAnswer::Cname(_) => 1u16,
        TestAnswer::Empty | TestAnswer::NxDomain => 0u16,
    };
    let flags = match answer {
        TestAnswer::NxDomain => 0x8183u16,
        _ => 0x8180u16,
    };

    let mut response = Vec::with_capacity(128);
    response.extend_from_slice(&query[0..2]);
    push_u16_be(&mut response, flags);
    push_u16_be(&mut response, 1);
    push_u16_be(&mut response, answer_count);
    push_u16_be(&mut response, 0);
    push_u16_be(&mut response, 0);
    response.extend_from_slice(&query[12..question_end]);

    match answer {
        TestAnswer::A(ip) => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());
        }
        TestAnswer::Cname(target) => {
            let mut encoded = Vec::new();
            for label in target.split('.') {
                encoded.push(label.len() as u8);
                encoded.extend_from_slice(label.as_bytes());
            }
            encoded.push(0);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded.len() as u16);
            response.extend_from_slice(&encoded);
        }
        TestAnswer::Empty | TestAnswer::NxDomain => {}
    }

    response
}

fn question_end(packet: &[u8]) -> io::Result<usize> {
    let mut offset = 12usize;
    while offset < packet.len() {
        let len = packet[offset] as usize;
        offset += 1;
        if len == 0 {
            return offset.checked_add(4).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "dns question overflow")
            });
        }
        offset = offset
            .checked_add(len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "dns question overflow"))?;
    }

    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "dns question missing terminator",
    ))
}
