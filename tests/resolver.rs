use flowio::net::resolver::{DnsResolver, resolve_host};
use flowio::runtime::buffer::bytes::{ByteWriteAt, read_u16_be_at};
use flowio::runtime::executor::Executor;
use flowio::test_support::runtime::test_hooks;
use std::cell::Cell;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

/// Response shapes emitted by the mock DNS server.
///
/// The CNAME variants encode owner-chain and poisoning cases; malformed
/// variants encode the specific parser failures under test.
#[derive(Clone, Copy)]
enum TestAnswer {
    A(Ipv4Addr),
    OversizedA(Ipv4Addr),
    Aaaa(Ipv6Addr),
    AFor(&'static str, Ipv4Addr),
    Cname(&'static str),
    CnameWithA(&'static str, Ipv4Addr),
    CompressedCnameWithA(Ipv4Addr),
    CnameWithAAndSpoof(&'static str, Ipv4Addr, &'static str, Ipv4Addr),
    CnameWithQueryOwnerA(&'static str, Ipv4Addr, Ipv4Addr),
    CnameWithAaaa(&'static str, Ipv6Addr),
    CnameWithOutOfOrderA(&'static str, Ipv4Addr),
    MultiHopCnameWithA(&'static str, &'static str, Ipv4Addr),
    CyclicCname(&'static str),
    QuestionTypeMismatch,
    QuestionClassMismatch,
    MalformedCnamePointer,
    ForwardCnamePointer,
    CnamePointerLoop,
    CnameRdataOverrun,
    CnameRdataTrailingBytes,
    Empty,
    NxDomain,
    NegativeQuestionMismatch(QuestionMismatch, NegativeRcode),
    QuestionlessNxDomain,
    ServFail,
    QuestionlessServFail,
}

#[derive(Clone, Copy)]
enum QuestionMismatch {
    Name,
    Type,
    Class,
}

#[derive(Clone, Copy)]
enum NegativeRcode {
    NxDomain,
    ServFail,
}

#[derive(Clone, Copy)]
enum FirstDnsDatagram {
    StaleQueryId,
    Undersized,
    MalformedMatchingQueryId,
    QuestionlessNxDomain,
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
fn resolve_host_rejects_overlong_name_without_sending_dns() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    server
        .set_nonblocking(true)
        .expect("failed to make test dns socket nonblocking");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let host = [
        "a".repeat(63),
        "b".repeat(63),
        "c".repeat(63),
        "d".repeat(62),
    ]
    .join(".");
    assert_eq!(host.len(), 254);

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            let err = resolver
                .resolve_host(&host, 5432)
                .await
                .expect_err("overlong host should fail before DNS send");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        })
        .expect("executor run failed");

    let mut packet = [0u8; 512];
    let err = server
        .recv_from(&mut packet)
        .expect_err("invalid host should not emit a DNS packet");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn dns_resolver_requires_nameserver() {
    let err = DnsResolver::new(Vec::new()).expect_err("empty nameserver list should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn resolver_stops_nameserver_failover_on_timer_out_of_memory() {
    let first_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind first dns socket");
    let first_nameserver = first_server
        .local_addr()
        .expect("failed to read first dns socket addr");

    let second_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind second dns socket");
    let second_nameserver = second_server
        .local_addr()
        .expect("failed to read second dns socket addr");

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![first_nameserver, second_nameserver])
                .expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_secs(1));
            test_hooks::fail_next_timer_alloc();

            let err = resolver
                .resolve_host("timer-oom.flowio.invalid", 5432)
                .await
                .expect_err("timer allocation failure should abort DNS resolution");
            assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
        })
        .expect("executor run failed");

    first_server
        .set_nonblocking(true)
        .expect("failed to make first dns socket nonblocking");
    let mut buffer = [0u8; 512];
    first_server
        .recv_from(&mut buffer)
        .expect("first nameserver did not receive the query");
    let err = first_server
        .recv_from(&mut buffer)
        .expect_err("timer allocation failure should stop before the AAAA query");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

    second_server
        .set_nonblocking(true)
        .expect("failed to make second dns socket nonblocking");
    let err = second_server
        .recv_from(&mut buffer)
        .expect_err("timer allocation failure should stop nameserver failover");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
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
fn resolve_host_keeps_a_answer_when_aaaa_is_nxdomain() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 43)),
            ("db.example.test", 28) => TestAnswer::NxDomain,
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
                .expect("A answer should survive AAAA NXDOMAIN");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 43), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_keeps_a_answer_when_aaaa_is_servfail() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 47)),
            ("db.example.test", 28) => TestAnswer::ServFail,
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
                .expect("A answer should survive AAAA SERVFAIL");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 47), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_keeps_aaaa_answer_across_recoverable_a_outcomes() {
    let cases = [
        ("empty", Some(TestAnswer::Empty), 0x60),
        ("SERVFAIL", Some(TestAnswer::ServFail), 0x61),
        ("NXDOMAIN", Some(TestAnswer::NxDomain), 0x62),
        ("timeout", None, 0x63),
    ];

    for (label, a_answer, suffix) in cases {
        let ip = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, suffix);
        let addrs =
            resolve_with_mock_dns(2, Duration::from_millis(30), move |name, qtype| {
                match (name, qtype) {
                    ("db.example.test", 1) => a_answer,
                    ("db.example.test", 28) => Some(TestAnswer::Aaaa(ip)),
                    _ => Some(TestAnswer::NxDomain),
                }
            })
            .unwrap_or_else(|err| panic!("AAAA answer should survive A {label}: {err}"));

        assert_eq!(
            addrs,
            vec![SocketAddr::from((ip, 5432))],
            "unexpected result after A {label}"
        );
    }
}

#[test]
fn resolve_host_keeps_a_answer_when_aaaa_times_out() {
    let ip = Ipv4Addr::new(192, 0, 2, 64);
    let addrs = resolve_with_mock_dns(2, Duration::from_millis(30), move |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::A(ip)),
            ("db.example.test", 28) => None,
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("A answer should survive AAAA timeout");

    assert_eq!(addrs, vec![SocketAddr::from((ip, 5432))]);
}

#[test]
fn resolve_host_follows_aaaa_cname_after_a_servfail() {
    let ip = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x65);
    let addrs = resolve_with_mock_dns(4, Duration::from_millis(200), move |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::ServFail),
            ("db.example.test", 28) => Some(TestAnswer::Cname("db.internal.test")),
            ("db.internal.test", 1) => Some(TestAnswer::Empty),
            ("db.internal.test", 28) => Some(TestAnswer::Aaaa(ip)),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("AAAA CNAME should survive A SERVFAIL");

    assert_eq!(addrs, vec![SocketAddr::from((ip, 5432))]);
}

#[test]
fn resolve_host_follows_a_cname_despite_aaaa_nxdomain() {
    let ip = Ipv4Addr::new(198, 51, 100, 66);
    let addrs = resolve_with_mock_dns(4, Duration::from_millis(200), move |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::Cname("db.internal.test")),
            ("db.example.test", 28) => Some(TestAnswer::NxDomain),
            ("db.internal.test", 1) => Some(TestAnswer::A(ip)),
            ("db.internal.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("A CNAME should survive AAAA NXDOMAIN");

    assert_eq!(addrs, vec![SocketAddr::from((ip, 5432))]);
}

#[test]
fn resolve_host_prefers_authoritative_nxdomain_over_recoverable_error() {
    let err = resolve_with_mock_dns(2, Duration::from_millis(200), |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::ServFail),
            ("db.example.test", 28) => Some(TestAnswer::NxDomain),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect_err("NXDOMAIN should win when neither family is usable");

    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn resolve_host_falls_back_promptly_after_questionless_servfail() {
    let bad_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind questionless servfail dns socket");
    let bad_nameserver = bad_server
        .local_addr()
        .expect("failed to read questionless servfail dns socket addr");
    let bad_thread = thread::spawn(move || {
        serve_dns_queries(bad_server, 2, |_name, _qtype| {
            TestAnswer::QuestionlessServFail
        })
    });

    let good_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind good dns socket");
    let good_nameserver = good_server
        .local_addr()
        .expect("failed to read good dns socket addr");
    let good_thread = thread::spawn(move || {
        serve_dns_queries(good_server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 51)),
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![bad_nameserver, good_nameserver])
                .expect("resolver init failed");
            let query_timeout = Duration::from_secs(1);
            resolver.set_query_timeout(query_timeout);

            let started = Instant::now();
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("resolver should fail over after questionless SERVFAIL");
            let elapsed = started.elapsed();

            assert!(
                elapsed < query_timeout,
                "questionless SERVFAIL should fail over without waiting for timeout: {elapsed:?}"
            );
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 51), 5432))]
            );
        })
        .expect("executor run failed");

    bad_thread.join().expect("bad dns thread panicked");
    good_thread.join().expect("good dns thread panicked");
}

#[test]
fn resolve_host_drains_questionless_nxdomain_before_matching_response() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind questionless nxdomain dns socket");
    let nameserver = server
        .local_addr()
        .expect("failed to read questionless nxdomain dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries_with_first_datagrams(
            server,
            2,
            &[FirstDnsDatagram::QuestionlessNxDomain],
            |name, qtype| {
                Some(match (name, qtype) {
                    ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 52)),
                    ("db.example.test", 28) => TestAnswer::Empty,
                    _ => TestAnswer::NxDomain,
                })
            },
        )
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("questionless NXDOMAIN should be drained before the real answer");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 52), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_merges_direct_a_and_aaaa_answers() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 46)),
            ("db.example.test", 28) => {
                TestAnswer::Aaaa(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x46))
            }
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
                .expect("direct A and AAAA answers should resolve together");
            assert_eq!(
                addrs,
                vec![
                    SocketAddr::from((Ipv4Addr::new(192, 0, 2, 46), 5432)),
                    SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x46), 5432,)),
                ]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_falls_back_after_malformed_first_nameserver() {
    let bad_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind malformed dns socket");
    let bad_nameserver = bad_server
        .local_addr()
        .expect("failed to read malformed dns socket addr");
    let bad_thread = thread::spawn(move || {
        serve_dns_queries(bad_server, 2, |_name, _qtype| {
            TestAnswer::MalformedCnamePointer
        })
    });

    let good_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind good dns socket");
    let good_nameserver = good_server
        .local_addr()
        .expect("failed to read good dns socket addr");
    let good_thread = thread::spawn(move || {
        serve_dns_queries(good_server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 44)),
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![bad_nameserver, good_nameserver])
                .expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("resolver should fall back after malformed first response");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 44), 5432))]
            );
        })
        .expect("executor run failed");

    bad_thread.join().expect("bad dns thread panicked");
    good_thread.join().expect("good dns thread panicked");
}

#[test]
fn resolve_host_falls_back_after_oversized_first_nameserver_response() {
    let bad_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind oversized dns socket");
    let bad_nameserver = bad_server
        .local_addr()
        .expect("failed to read oversized dns socket addr");
    let bad_thread = thread::spawn(move || {
        serve_dns_queries(bad_server, 2, |name, _qtype| match name {
            "db.example.test" => TestAnswer::OversizedA(Ipv4Addr::new(192, 0, 2, 200)),
            _ => TestAnswer::NxDomain,
        })
    });

    let good_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind fallback dns socket");
    let good_nameserver = good_server
        .local_addr()
        .expect("failed to read fallback dns socket addr");
    let good_thread = thread::spawn(move || {
        serve_dns_queries(good_server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 62)),
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![bad_nameserver, good_nameserver])
                .expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("resolver should fail over after oversized UDP response");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 62), 5432))]
            );
        })
        .expect("executor run failed");

    bad_thread.join().expect("bad dns thread panicked");
    good_thread.join().expect("good dns thread panicked");
}

#[test]
fn resolve_host_drains_stale_response_before_matching_query() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries_with_stale_first_response(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 45)),
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
                .expect("resolver should drain stale response and use matching response");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 45), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_drains_multiple_stale_responses_before_matching_query() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries_with_first_datagrams(
            server,
            2,
            &[
                FirstDnsDatagram::StaleQueryId,
                FirstDnsDatagram::StaleQueryId,
            ],
            |name, qtype| {
                Some(match (name, qtype) {
                    ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 50)),
                    ("db.example.test", 28) => TestAnswer::Empty,
                    _ => TestAnswer::NxDomain,
                })
            },
        )
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("resolver should drain repeated stale responses");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 50), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_drains_undersized_datagram_before_matching_query() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries_with_undersized_first_response(server, 2, |name, qtype| {
            match (name, qtype) {
                ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 48)),
                ("db.example.test", 28) => TestAnswer::Empty,
                _ => TestAnswer::NxDomain,
            }
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
                .expect("resolver should drain undersized datagram and use matching response");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 48), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_drains_malformed_matching_datagram_before_response() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries_with_malformed_matching_first_response(server, 2, |name, qtype| {
            match (name, qtype) {
                ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 49)),
                ("db.example.test", 28) => TestAnswer::Empty,
                _ => TestAnswer::NxDomain,
            }
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
                .expect("resolver should drain malformed matching datagram and use response");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 49), 5432))]
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

#[test]
fn resolve_host_accepts_exact_uncompressed_cname_rdata() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => {
                TestAnswer::CnameWithA("db.internal.test", Ipv4Addr::new(198, 51, 100, 88))
            }
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
                .expect("bundled cname resolution failed");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(198, 51, 100, 88), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_accepts_exact_compressed_cname_rdata() {
    let ip = Ipv4Addr::new(198, 51, 100, 89);
    let addrs = resolve_with_mock_dns(2, Duration::from_millis(200), move |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::CompressedCnameWithA(ip)),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("exact compressed CNAME should resolve");

    assert_eq!(addrs, vec![SocketAddr::from((ip, 5432))]);
}

#[test]
fn resolve_host_accepts_out_of_order_bundled_cname_address() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::CnameWithOutOfOrderA(
                "db.internal.test",
                Ipv4Addr::new(198, 51, 100, 90),
            ),
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
                .expect("out-of-order bundled cname resolution failed");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(198, 51, 100, 90), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_ignores_off_chain_spoofed_address_in_bundled_cname_response() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::CnameWithAAndSpoof(
                "db.internal.test",
                Ipv4Addr::new(198, 51, 100, 91),
                "unrelated.example.test",
                Ipv4Addr::new(203, 0, 113, 77),
            ),
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            // Only addresses owned by the queried CNAME chain are accepted;
            // the off-chain spoofed A must be ignored.
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("bundled cname spoof-filter resolution failed");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(198, 51, 100, 91), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_prefers_direct_query_owner_address_over_bundled_cname() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::CnameWithQueryOwnerA(
                "db.internal.test",
                Ipv4Addr::new(192, 0, 2, 123),
                Ipv4Addr::new(198, 51, 100, 92),
            ),
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            // If a response carries both a CNAME and a direct A for the query
            // owner, the direct owner answer wins before following CNAMEs.
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("direct query-owner response should resolve");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 123), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_accepts_bundled_cname_aaaa_address() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::Empty,
            ("db.example.test", 28) => TestAnswer::CnameWithAaaa(
                "db.internal.test",
                Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x53),
            ),
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
                .expect("bundled cname AAAA resolution failed");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((
                    Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x53),
                    5432,
                ))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_accepts_multi_hop_bundled_cname_address() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::MultiHopCnameWithA(
                "db.mid.test",
                "db.internal.test",
                Ipv4Addr::new(198, 51, 100, 89),
            ),
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
                .expect("multi-hop bundled cname resolution failed");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(198, 51, 100, 89), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_rejects_cyclic_in_response_cname_chain() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 1, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::CyclicCname("db.mid.test"),
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("cyclic cname should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_validates_negative_response_questions_before_rcode() {
    let cases = [
        (
            "NXDOMAIN wrong name",
            TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Name, NegativeRcode::NxDomain),
            "question name",
        ),
        (
            "NXDOMAIN wrong type",
            TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Type, NegativeRcode::NxDomain),
            "question type/class",
        ),
        (
            "NXDOMAIN wrong class",
            TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Class, NegativeRcode::NxDomain),
            "question type/class",
        ),
        (
            "SERVFAIL wrong name",
            TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Name, NegativeRcode::ServFail),
            "question name",
        ),
        (
            "SERVFAIL wrong type",
            TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Type, NegativeRcode::ServFail),
            "question type/class",
        ),
        (
            "SERVFAIL wrong class",
            TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Class, NegativeRcode::ServFail),
            "question type/class",
        ),
    ];

    for (label, answer, expected_message) in cases {
        let err = resolve_with_mock_dns(2, Duration::from_millis(200), move |_name, _qtype| {
            Some(answer)
        })
        .expect_err(label);

        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{label}");
        assert!(
            err.to_string().contains(expected_message),
            "{label} returned unexpected error: {err}"
        );
    }
}

#[test]
fn resolve_host_applies_matching_negative_response_codes() {
    let nxdomain = resolve_with_mock_dns(2, Duration::from_millis(200), |_name, _qtype| {
        Some(TestAnswer::NxDomain)
    })
    .expect_err("matching NXDOMAIN should remain authoritative");
    assert_eq!(nxdomain.kind(), io::ErrorKind::NotFound);

    let servfail = resolve_with_mock_dns(2, Duration::from_millis(200), |_name, _qtype| {
        Some(TestAnswer::ServFail)
    })
    .expect_err("matching SERVFAIL should remain a recoverable server error");
    assert_eq!(servfail.kind(), io::ErrorKind::Other);
    assert!(servfail.to_string().contains("response code 2"));
}

#[test]
fn resolve_host_rejects_question_type_mismatch() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 1, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::QuestionTypeMismatch,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("question type mismatch should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_rejects_question_class_mismatch() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 1, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::QuestionClassMismatch,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("question class mismatch should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_rejects_mismatched_answer_owner() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => {
                TestAnswer::AFor("unrelated.example.test", Ipv4Addr::new(203, 0, 113, 9))
            }
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            // A well-formed answer with an unrelated owner yields no on-chain
            // address, so the resolver surfaces NotFound rather than InvalidData.
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("mismatched answer owner should not resolve");
            assert_eq!(err.kind(), io::ErrorKind::NotFound);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_rejects_truncated_compression_pointer() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 1, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::MalformedCnamePointer,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("malformed compression pointer should fail");
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_rejects_forward_compression_pointer() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 1, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::ForwardCnamePointer,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("forward compression pointer should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_rejects_cname_compression_pointer_loop() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1 | 28) => TestAnswer::CnamePointerLoop,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("CNAME compression pointer loop should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_rejects_cname_rdata_overrun() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1 | 28) => TestAnswer::CnameRdataOverrun,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("CNAME RDATA overrun should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_rejects_cname_rdata_trailing_bytes() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1 | 28) => TestAnswer::CnameRdataTrailingBytes,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("CNAME RDATA trailing bytes should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

fn resolve_with_mock_dns<F>(
    expected_queries: usize,
    query_timeout: Duration,
    answer: F,
) -> io::Result<Vec<SocketAddr>>
where
    F: Fn(&str, u16) -> Option<TestAnswer> + Send + 'static,
{
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries_with_first_datagrams(server, expected_queries, &[], answer)
    });

    let result = Rc::new(Cell::new(None));
    let task_result = Rc::clone(&result);
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(query_timeout);
            task_result.set(Some(resolver.resolve_host("db.example.test", 5432).await));
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
    result
        .take()
        .expect("resolver task completed without recording its result")
}

/// Mock DNS server that answers exactly `expected_queries` UDP queries, then
/// returns. The count must match resolver behavior for the tested lookup.
fn serve_dns_queries<F>(socket: StdUdpSocket, expected_queries: usize, answer: F)
where
    F: Fn(&str, u16) -> TestAnswer,
{
    serve_dns_queries_with_first_datagrams(socket, expected_queries, &[], move |name, qtype| {
        Some(answer(name, qtype))
    });
}

fn serve_dns_queries_with_stale_first_response<F>(
    socket: StdUdpSocket,
    expected_queries: usize,
    answer: F,
) where
    F: Fn(&str, u16) -> TestAnswer,
{
    serve_dns_queries_with_first_datagrams(
        socket,
        expected_queries,
        &[FirstDnsDatagram::StaleQueryId],
        move |name, qtype| Some(answer(name, qtype)),
    );
}

fn serve_dns_queries_with_undersized_first_response<F>(
    socket: StdUdpSocket,
    expected_queries: usize,
    answer: F,
) where
    F: Fn(&str, u16) -> TestAnswer,
{
    serve_dns_queries_with_first_datagrams(
        socket,
        expected_queries,
        &[FirstDnsDatagram::Undersized],
        move |name, qtype| Some(answer(name, qtype)),
    );
}

fn serve_dns_queries_with_malformed_matching_first_response<F>(
    socket: StdUdpSocket,
    expected_queries: usize,
    answer: F,
) where
    F: Fn(&str, u16) -> TestAnswer,
{
    serve_dns_queries_with_first_datagrams(
        socket,
        expected_queries,
        &[FirstDnsDatagram::MalformedMatchingQueryId],
        move |name, qtype| Some(answer(name, qtype)),
    );
}

/// Handles exactly `expected_queries`; `None` deliberately leaves a query
/// unanswered so timeout behavior can be exercised without duplicating the
/// mock-server loop.
fn serve_dns_queries_with_first_datagrams<F>(
    socket: StdUdpSocket,
    expected_queries: usize,
    first_datagrams: &[FirstDnsDatagram],
    answer: F,
) where
    F: Fn(&str, u16) -> Option<TestAnswer>,
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
        let Some(answer) = answer(&qname, qtype) else {
            continue;
        };
        let response = build_response(query, answer);
        for first in first_datagrams.iter().copied() {
            send_first_dns_datagram(&socket, peer, query, &response, first);
        }
        socket
            .send_to(&response, peer)
            .expect("dns send_to response failed");
    }
}

fn send_first_dns_datagram(
    socket: &StdUdpSocket,
    peer: SocketAddr,
    query: &[u8],
    response: &[u8],
    first: FirstDnsDatagram,
) {
    match first {
        FirstDnsDatagram::StaleQueryId => {
            let mut stale = response.to_vec();
            let query_id = read_u16_be_at(&stale, 0).expect("test response query ID should exist");
            stale
                .write_u16_be_at(0, query_id.wrapping_add(1))
                .expect("test stale query ID rewrite should fit");
            socket
                .send_to(&stale, peer)
                .expect("dns send_to stale response failed");
        }
        FirstDnsDatagram::Undersized => {
            socket
                .send_to(&[0], peer)
                .expect("dns send_to undersized response failed");
        }
        FirstDnsDatagram::MalformedMatchingQueryId => {
            let malformed = query
                .get(..2)
                .expect("test query ID should exist for malformed response");
            socket
                .send_to(malformed, peer)
                .expect("dns send_to malformed matching response failed");
        }
        FirstDnsDatagram::QuestionlessNxDomain => {
            let questionless_nxdomain = build_response(query, TestAnswer::QuestionlessNxDomain);
            socket
                .send_to(&questionless_nxdomain, peer)
                .expect("dns send_to questionless NXDOMAIN response failed");
        }
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

fn push_name(dst: &mut Vec<u8>, name: &str) {
    for label in name.split('.') {
        dst.push(label.len() as u8);
        dst.extend_from_slice(label.as_bytes());
    }
    dst.push(0);
}

fn build_response(query: &[u8], answer: TestAnswer) -> Vec<u8> {
    let question_end = question_end(query).expect("invalid dns question");
    let answer_count = match answer {
        TestAnswer::CnameWithA(_, _)
        | TestAnswer::CompressedCnameWithA(_)
        | TestAnswer::CnameWithAaaa(_, _)
        | TestAnswer::CnameWithOutOfOrderA(_, _) => 2u16,
        TestAnswer::CnameWithAAndSpoof(_, _, _, _) | TestAnswer::CnameWithQueryOwnerA(_, _, _) => {
            3u16
        }
        TestAnswer::MultiHopCnameWithA(_, _, _) => 3u16,
        TestAnswer::CyclicCname(_) => 2u16,
        TestAnswer::A(_)
        | TestAnswer::OversizedA(_)
        | TestAnswer::Aaaa(_)
        | TestAnswer::AFor(_, _)
        | TestAnswer::Cname(_)
        | TestAnswer::MalformedCnamePointer
        | TestAnswer::ForwardCnamePointer
        | TestAnswer::CnamePointerLoop
        | TestAnswer::CnameRdataOverrun
        | TestAnswer::CnameRdataTrailingBytes => 1u16,
        TestAnswer::QuestionTypeMismatch
        | TestAnswer::QuestionClassMismatch
        | TestAnswer::Empty
        | TestAnswer::NxDomain
        | TestAnswer::NegativeQuestionMismatch(_, _)
        | TestAnswer::QuestionlessNxDomain
        | TestAnswer::ServFail
        | TestAnswer::QuestionlessServFail => 0u16,
    };
    let flags = match answer {
        TestAnswer::NegativeQuestionMismatch(_, NegativeRcode::NxDomain)
        | TestAnswer::NxDomain
        | TestAnswer::QuestionlessNxDomain => 0x8183u16,
        TestAnswer::NegativeQuestionMismatch(_, NegativeRcode::ServFail)
        | TestAnswer::ServFail
        | TestAnswer::QuestionlessServFail => 0x8182u16,
        _ => 0x8180u16,
    };
    let include_question = !matches!(
        answer,
        TestAnswer::QuestionlessNxDomain | TestAnswer::QuestionlessServFail
    );

    let mut response = Vec::with_capacity(128);
    response.extend_from_slice(&query[0..2]);
    push_u16_be(&mut response, flags);
    push_u16_be(&mut response, u16::from(include_question));
    push_u16_be(&mut response, answer_count);
    push_u16_be(&mut response, 0);
    push_u16_be(&mut response, 0);
    if include_question {
        response.extend_from_slice(&query[12..question_end]);
    }
    match answer {
        TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Name, _) => {
            let first_label_octet = response
                .get_mut(13)
                .expect("test question name should contain a label octet");
            *first_label_octet = if first_label_octet.eq_ignore_ascii_case(&b'x') {
                b'y'
            } else {
                b'x'
            };
        }
        TestAnswer::QuestionTypeMismatch
        | TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Type, _) => {
            let qtype_offset = response.len() - 4;
            let current_qtype =
                read_u16_be_at(&response, qtype_offset).expect("test question type should exist");
            let mismatched_qtype = if current_qtype == 1 { 28 } else { 1 };
            response
                .write_u16_be_at(qtype_offset, mismatched_qtype)
                .expect("test question type rewrite should fit");
        }
        TestAnswer::QuestionClassMismatch
        | TestAnswer::NegativeQuestionMismatch(QuestionMismatch::Class, _) => {
            let qclass_offset = response.len() - 2;
            response
                .write_u16_be_at(qclass_offset, 3)
                .expect("test question class rewrite should fit");
        }
        _ => {}
    }

    match answer {
        TestAnswer::A(ip) => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());
        }
        TestAnswer::OversizedA(ip) => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());
            response.resize(2049, 0xA5);
        }
        TestAnswer::Aaaa(ip) => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 28);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 16);
            response.extend_from_slice(&ip.octets());
        }
        TestAnswer::AFor(owner, ip) => {
            push_name(&mut response, owner);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());
        }
        TestAnswer::Cname(target) => {
            let mut encoded = Vec::new();
            push_name(&mut encoded, target);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded.len() as u16);
            response.extend_from_slice(&encoded);
        }
        TestAnswer::CnameWithA(target, ip) => {
            let mut encoded = Vec::new();
            push_name(&mut encoded, target);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded.len() as u16);
            response.extend_from_slice(&encoded);

            push_name(&mut response, target);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());
        }
        TestAnswer::CompressedCnameWithA(ip) => {
            let first_label_len = query[12] as usize;
            let suffix_offset = 13 + first_label_len;
            assert!(suffix_offset < 0x4000, "test suffix pointer should fit");
            let suffix_pointer = 0xC000 | suffix_offset as u16;

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 2);
            push_u16_be(&mut response, suffix_pointer);

            push_u16_be(&mut response, suffix_pointer);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());
        }
        TestAnswer::CnameWithAAndSpoof(target, ip, spoof_owner, spoof_ip) => {
            let mut encoded = Vec::new();
            push_name(&mut encoded, target);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded.len() as u16);
            response.extend_from_slice(&encoded);

            push_name(&mut response, target);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());

            push_name(&mut response, spoof_owner);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&spoof_ip.octets());
        }
        TestAnswer::CnameWithQueryOwnerA(target, direct_ip, target_ip) => {
            let mut encoded = Vec::new();
            push_name(&mut encoded, target);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded.len() as u16);
            response.extend_from_slice(&encoded);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&direct_ip.octets());

            push_name(&mut response, target);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&target_ip.octets());
        }
        TestAnswer::CnameWithAaaa(target, ip) => {
            let mut encoded = Vec::new();
            push_name(&mut encoded, target);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded.len() as u16);
            response.extend_from_slice(&encoded);

            push_name(&mut response, target);
            push_u16_be(&mut response, 28);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 16);
            response.extend_from_slice(&ip.octets());
        }
        TestAnswer::CnameWithOutOfOrderA(target, ip) => {
            push_name(&mut response, target);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());

            let mut encoded = Vec::new();
            push_name(&mut encoded, target);
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded.len() as u16);
            response.extend_from_slice(&encoded);
        }
        TestAnswer::MultiHopCnameWithA(mid, target, ip) => {
            let mut encoded_mid = Vec::new();
            push_name(&mut encoded_mid, mid);
            let mut encoded_target = Vec::new();
            push_name(&mut encoded_target, target);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded_mid.len() as u16);
            response.extend_from_slice(&encoded_mid);

            push_name(&mut response, mid);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded_target.len() as u16);
            response.extend_from_slice(&encoded_target);

            push_name(&mut response, target);
            push_u16_be(&mut response, 1);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            response.extend_from_slice(&ip.octets());
        }
        TestAnswer::CyclicCname(mid) => {
            let qname = parse_qname(query).expect("test query qname should parse");
            let mut encoded_mid = Vec::new();
            push_name(&mut encoded_mid, mid);
            let mut encoded_query = Vec::new();
            push_name(&mut encoded_query, &qname);

            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded_mid.len() as u16);
            response.extend_from_slice(&encoded_mid);

            push_name(&mut response, mid);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, encoded_query.len() as u16);
            response.extend_from_slice(&encoded_query);
        }
        TestAnswer::MalformedCnamePointer => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 1);
            response.push(0xC0);
        }
        TestAnswer::ForwardCnamePointer => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 2);
            let forward_target = response.len() + 2;
            push_u16_be(&mut response, 0xC000 | forward_target as u16);
        }
        TestAnswer::CnamePointerLoop => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 4);
            let rdata_offset = response.len();
            response.push(1);
            response.push(b'x');
            push_u16_be(&mut response, 0xC000 | rdata_offset as u16);
        }
        TestAnswer::CnameRdataOverrun => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 1);
            response.push(3);
            response.extend_from_slice(b"bad");
            response.push(0);
        }
        TestAnswer::CnameRdataTrailingBytes => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 3);
            push_u16_be(&mut response, 0xC00F);
            response.push(0xA5);
        }
        TestAnswer::QuestionTypeMismatch
        | TestAnswer::QuestionClassMismatch
        | TestAnswer::NegativeQuestionMismatch(_, _)
        | TestAnswer::Empty
        | TestAnswer::NxDomain
        | TestAnswer::QuestionlessNxDomain
        | TestAnswer::ServFail
        | TestAnswer::QuestionlessServFail => {}
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
