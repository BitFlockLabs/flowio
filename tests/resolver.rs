use flowio::net::resolver::{DnsResolver, resolve_host};
use flowio::runtime::buffer::bytes::{ByteWriteAt, read_u16_be_at};
use flowio::runtime::executor::Executor;
use flowio::test_support::net::resolver::{
    lookup_ipv4, parse_hosts_bytes, read_resolv_conf, resolve_host_with_hosts_path,
    resolve_local_host_with_hosts_path,
};
use flowio::test_support::runtime::test_hooks;
use std::cell::Cell;
use std::fs::{OpenOptions, remove_file};
use std::io;
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const HOSTS_FILE_MAX_BYTES: usize = 4 * 1024 * 1024;
const RESOLV_CONF_MAX_BYTES: usize = 64 * 1024;
static NEXT_TEMP_RESOLVER_FILE: AtomicUsize = AtomicUsize::new(0);

struct TempResolverFile {
    path: PathBuf,
}

impl TempResolverFile {
    fn padded(label: &str, prefix: &[u8], len: usize) -> Self {
        assert!(prefix.len() <= len, "fixture prefix must fit");
        let sequence = NEXT_TEMP_RESOLVER_FILE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "flowio-resolver-{label}-{}-{sequence}",
            std::process::id()
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("failed to create resolver fixture");
        file.write_all(prefix)
            .expect("failed to write resolver fixture prefix");

        let padding = [b' '; 8192];
        let mut remaining = len - prefix.len();
        while remaining != 0 {
            let chunk_len = remaining.min(padding.len());
            file.write_all(&padding[..chunk_len])
                .expect("failed to pad resolver fixture");
            remaining -= chunk_len;
        }

        Self { path }
    }

    fn path(&self) -> &str {
        self.path
            .to_str()
            .expect("temporary resolver path should be UTF-8")
    }
}

impl Drop for TempResolverFile {
    fn drop(&mut self) {
        let _ = remove_file(&self.path);
    }
}

/// Response shapes emitted by the mock DNS server.
///
/// The CNAME variants encode owner-chain and poisoning cases; malformed
/// variants encode the specific parser failures under test.
#[derive(Clone, Copy)]
enum TestAnswer {
    A(Ipv4Addr),
    NonQueryA(Ipv4Addr),
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
    CnameChain {
        names: &'static [&'static str],
        address: Option<Ipv4Addr>,
    },
    CyclicCname(&'static str),
    QuestionTypeMismatch,
    QuestionClassMismatch,
    MalformedCnamePointer,
    ForwardCnamePointer,
    CnamePointerLoop,
    CnameRdataOverrun,
    CnameRdataTrailingBytes,
    InvalidUtf8CnameTarget,
    LiteralDotCnameTarget,
    RootCnameTarget,
    Empty,
    NxDomain,
    NegativeQuestionMismatch(QuestionMismatch, NegativeRcode),
    QuestionlessNxDomain,
    ServFail,
    QuestionlessServFail,
    Sectioned(TestSections),
    NegativeSectioned(TestSections, NegativeRcode),
}

static CNAME_RESPONSE_BOUNDARY_CHAIN: [&str; 18] = [
    "db.example.test",
    "c01.example.test",
    "c02.example.test",
    "c03.example.test",
    "c04.example.test",
    "c05.example.test",
    "c06.example.test",
    "c07.example.test",
    "c08.example.test",
    "c09.example.test",
    "c10.example.test",
    "c11.example.test",
    "c12.example.test",
    "c13.example.test",
    "c14.example.test",
    "c15.example.test",
    "c16.example.test",
    "c17.example.test",
];

static CNAME_TOTAL_INITIAL_15: [&str; 16] = [
    "db.example.test",
    "t01.example.test",
    "t02.example.test",
    "t03.example.test",
    "t04.example.test",
    "t05.example.test",
    "t06.example.test",
    "t07.example.test",
    "t08.example.test",
    "t09.example.test",
    "t10.example.test",
    "t11.example.test",
    "t12.example.test",
    "t13.example.test",
    "t14.example.test",
    "total-followup.example.test",
];

static CNAME_TOTAL_FOLLOWUP_1: [&str; 2] =
    ["total-followup.example.test", "total-final.example.test"];

static CNAME_TOTAL_FOLLOWUP_2: [&str; 3] = [
    "total-followup.example.test",
    "total-mid.example.test",
    "total-final.example.test",
];

static PORTAL_AZURE_CNAME_CHAIN: [&str; 6] = [
    "portal.azure.com",
    "portal.azure.trafficmanager.net",
    "portal.azure.com.edgekey.net",
    "e11290.dscb.akamaiedge.net",
    "e11290.d.akamaiedge.net",
    "portal.edge.example.net",
];

/// Test-only resource record used to build explicit DNS sections.
#[derive(Clone, Copy)]
enum TestRecord {
    A(&'static str, Ipv4Addr),
    Aaaa(&'static str, Ipv6Addr),
    Cname(&'static str, &'static str),
    Truncated(&'static str),
}

/// Answer, Authority, and Additional records for a section-policy response.
#[derive(Clone, Copy)]
struct TestSections {
    answer: &'static [TestRecord],
    authority: &'static [TestRecord],
    additional: &'static [TestRecord],
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
    FullStaleQueryId,
    Undersized,
    MalformedMatchingQueryId,
    MalformedQuestionPointerLoop,
    InvalidUtf8QuestionCollision,
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
fn resolve_host_rejects_invalid_query_names_without_sending_dns() {
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
            for invalid_host in [host.as_str(), "db.example.test.."] {
                let err = resolver
                    .resolve_host(invalid_host, 5432)
                    .await
                    .expect_err("invalid host should fail before DNS send");
                assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{invalid_host}");
            }
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
    assert_eq!(err.to_string(), "resolver requires at least one nameserver");
}

#[test]
fn resolver_zero_total_timeout_preserves_local_results_and_skips_upstream() {
    let hosts = TempResolverFile::padded(
        "hosts-zero-total",
        b"192.0.2.209 local-deadline.flowio.invalid\n",
        45,
    );
    let hosts_path = hosts.path().to_owned();
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind no-query DNS socket");
    server
        .set_nonblocking(true)
        .expect("failed to make no-query DNS socket nonblocking");
    let nameserver = server
        .local_addr()
        .expect("failed to read no-query DNS socket address");

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_total_query_timeout(Duration::ZERO);

            let literal = resolver
                .resolve_host("192.0.2.208", 5432)
                .await
                .expect("a zero total timeout must not affect literal resolution");
            assert_eq!(
                literal,
                [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 208), 5432))]
            );

            let localhost = resolve_host_with_hosts_path(&resolver, &hosts_path, "localhost", 5432)
                .await
                .expect("a zero total timeout must not affect built-in localhost resolution");
            assert_eq!(
                localhost,
                [
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 5432)),
                    SocketAddr::from((Ipv6Addr::LOCALHOST, 5432)),
                ]
            );

            let local = resolve_host_with_hosts_path(
                &resolver,
                &hosts_path,
                "local-deadline.flowio.invalid",
                5432,
            )
            .await
            .expect("a zero total timeout must not affect hosts resolution");
            assert_eq!(
                local,
                [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 209), 5432))]
            );

            let err = resolve_host_with_hosts_path(
                &resolver,
                &hosts_path,
                "upstream-deadline.flowio.invalid",
                5432,
            )
            .await
            .expect_err("a zero total timeout should reject upstream DNS");
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
            assert_eq!(err.to_string(), "DNS total query timed out");
        })
        .expect("executor run failed");

    let mut packet = [0u8; 512];
    let err = server
        .recv_from(&mut packet)
        .expect_err("a zero total timeout should not emit a DNS packet");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn resolver_deduplicates_silent_nameserver_attempts() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver, nameserver, nameserver])
                .expect("duplicate nameservers should be accepted");
            resolver.set_query_timeout(Duration::from_millis(30));

            let err = lookup_ipv4(&resolver, "dedup-attempt.flowio.invalid")
                .await
                .expect_err("silent nameserver should time out");
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        })
        .expect("executor run failed");

    server
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("failed to set dns socket timeout");
    let mut buffer = [0u8; 512];
    server
        .recv_from(&mut buffer)
        .expect("unique nameserver did not receive the query");
    server
        .set_nonblocking(true)
        .expect("failed to make dns socket nonblocking");
    let err = server
        .recv_from(&mut buffer)
        .expect_err("duplicate nameservers must not cause duplicate attempts");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn resolver_fully_qualified_hosts_entry_resolves_locally_without_dns() {
    let contents =
        b"192.0.2.42 other Pinned.FlowIO.Invalid.\n192.0.2.43 Repeated.FlowIO.Invalid..\n";
    let hosts = TempResolverFile::padded("hosts-fqdn", contents, contents.len());
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind no-query DNS socket");
    server
        .set_nonblocking(true)
        .expect("failed to make no-query DNS socket nonblocking");
    let nameserver = server
        .local_addr()
        .expect("failed to read no-query DNS socket address");
    let hosts_path = hosts.path().to_owned();

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            let addrs = resolve_host_with_hosts_path(
                &resolver,
                &hosts_path,
                "PINNED.FLOWIO.INVALID.",
                5432,
            )
            .await
            .expect("a fully qualified hosts entry should resolve locally");
            assert_eq!(
                addrs,
                [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 42), 5432))]
            );
        })
        .expect("executor run failed");

    let mut packet = [0u8; 512];
    let err = server
        .recv_from(&mut packet)
        .expect_err("a local fully qualified entry must not emit a DNS packet");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

    let localhost = resolve_local_host_with_hosts_path(hosts.path(), "LOCALHOST.", 5432)
        .expect("fully qualified localhost should resolve locally");
    assert_eq!(
        localhost,
        [
            SocketAddr::from((Ipv4Addr::LOCALHOST, 5432)),
            SocketAddr::from((Ipv6Addr::LOCALHOST, 5432)),
        ]
    );

    let reverse = resolve_local_host_with_hosts_path(hosts.path(), "OTHER.", 5432)
        .expect("a dotted query should match an undotted hosts alias");
    assert_eq!(
        reverse,
        [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 42), 5432))]
    );

    let repeated =
        resolve_local_host_with_hosts_path(hosts.path(), "repeated.flowio.invalid", 5432)
            .expect("an invalid hosts alias should be ignored");
    assert!(
        repeated.is_empty(),
        "more than one trailing dot must not be treated as one root dot"
    );
}

#[test]
fn resolver_hosts_parser_skips_only_invalid_lines_and_preserves_order() {
    let contents = b"192.0.2.40 HOSTS-MIXED.FLOWIO.INVALID.\n\
\xff malformed hosts-mixed.flowio.invalid\n\
2001:db8::40 other hosts-mixed.flowio.invalid\n\
192.0.2.41 unrelated.flowio.invalid\n";

    let addrs = parse_hosts_bytes(contents, "hosts-mixed.flowio.invalid", 5432)
        .expect("valid hosts lines should survive an invalid line");
    assert_eq!(
        addrs,
        [
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 40), 5432)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x40), 5432,)),
        ]
    );
}

#[test]
fn resolver_hosts_parser_uses_hash_comments_and_keeps_semicolon_aliases() {
    let contents = b"192.0.2.50 preceding ; hosts-comments.flowio.invalid # kept\n\
192.0.2.50 hosts-comments.flowio.invalid hosts-comments.flowio.invalid\n\
192.0.2.51 preceding # hosts-comments.flowio.invalid\n\
192.0.2.52 hosts-comments.flowio.invalid;not-the-query\n\
2001:db8::50 HOSTS-COMMENTS.FLOWIO.INVALID.\n\
2001:db8::51 unrelated.flowio.invalid\n";

    let addrs = parse_hosts_bytes(contents, "hosts-comments.flowio.invalid", 6543)
        .expect("hosts comments and aliases should parse");
    assert_eq!(
        addrs,
        [
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 50), 6543)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x50), 6543,)),
        ]
    );
}

#[test]
fn resolver_all_invalid_hosts_lines_continue_to_upstream_dns() {
    let host = "all-invalid-hosts.flowio.invalid";
    let hosts = TempResolverFile::padded("hosts-all-invalid", b"\xff\n\xc0\xaf\n", 5);
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let expected = Ipv4Addr::new(192, 0, 2, 60);
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("all-invalid-hosts.flowio.invalid", 1) => TestAnswer::A(expected),
            ("all-invalid-hosts.flowio.invalid", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });
    let hosts_path = hosts.path().to_owned();

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let addrs = resolve_host_with_hosts_path(&resolver, &hosts_path, host, 5432)
                .await
                .expect("all-invalid hosts input should fall through to DNS");
            assert_eq!(addrs, [SocketAddr::from((expected, 5432))]);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolver_hosts_file_accepts_four_mib_and_rejects_the_next_byte() {
    let host = "hosts-boundary.flowio.invalid";
    let prefix = format!("192.0.2.10 {host}\n");
    let exact = TempResolverFile::padded("hosts-exact", prefix.as_bytes(), HOSTS_FILE_MAX_BYTES);
    let addrs = resolve_local_host_with_hosts_path(exact.path(), host, 5432)
        .expect("a four MiB hosts file should be accepted");
    assert_eq!(
        addrs,
        [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 5432))]
    );

    let over = TempResolverFile::padded("hosts-over", prefix.as_bytes(), HOSTS_FILE_MAX_BYTES + 1);
    let err = resolve_local_host_with_hosts_path(over.path(), host, 5432)
        .expect_err("a hosts file above four MiB should be rejected");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "/etc/hosts exceeds the 4 MiB resolver configuration limit"
    );
}

#[test]
fn resolver_hosts_invalid_lines_are_ignored_but_size_limit_still_applies() {
    let invalid = TempResolverFile::padded("hosts-invalid-utf8", &[0xff, b'\n'], 2);
    let addrs =
        resolve_local_host_with_hosts_path(invalid.path(), "invalid-utf8.flowio.invalid", 5432)
            .expect("an invalid hosts line should be skipped");
    assert!(addrs.is_empty());

    let invalid_over = TempResolverFile::padded(
        "hosts-invalid-over",
        &[0xff, b'\n'],
        HOSTS_FILE_MAX_BYTES + 1,
    );
    let err = resolve_local_host_with_hosts_path(
        invalid_over.path(),
        "invalid-over.flowio.invalid",
        5432,
    )
    .expect_err("the hosts size bound must include invalid input bytes");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "/etc/hosts exceeds the 4 MiB resolver configuration limit"
    );
}

#[test]
fn resolver_resolv_conf_accepts_64_kib_and_rejects_the_next_byte() {
    let prefix = b"nameserver 192.0.2.53\n";
    let exact = TempResolverFile::padded("resolv-conf-exact", prefix, RESOLV_CONF_MAX_BYTES);
    let nameservers =
        read_resolv_conf(exact.path()).expect("a 64 KiB resolv.conf should be accepted");
    assert_eq!(
        nameservers,
        [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 53), 53))]
    );

    let over = TempResolverFile::padded("resolv-conf-over", prefix, RESOLV_CONF_MAX_BYTES + 1);
    let err =
        read_resolv_conf(over.path()).expect_err("a resolv.conf above 64 KiB should be rejected");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "/etc/resolv.conf exceeds the 64 KiB resolver configuration limit"
    );
}

#[test]
fn resolver_resolv_conf_retains_hash_and_semicolon_comment_grammar() {
    let contents = b"nameserver 192.0.2.53; legacy comment\n\
nameserver 2001:db8::53# inline comment\n";
    let fixture = TempResolverFile::padded("resolv-conf-comments", contents, contents.len());

    let nameservers = read_resolv_conf(fixture.path()).expect("resolv.conf comments should parse");
    assert_eq!(
        nameservers,
        [
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 53), 53)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x53), 53,)),
        ]
    );
}

#[test]
fn resolver_hosts_result_accepts_64_unique_addresses_and_rejects_the_65th() {
    let host = "address-boundary.flowio.invalid";
    let mut contents = String::new();
    for octet in 1..=64 {
        contents.push_str(&format!("192.0.2.{octet} {host}\n"));
    }
    let exact =
        TempResolverFile::padded("hosts-address-exact", contents.as_bytes(), contents.len());
    let addrs = resolve_local_host_with_hosts_path(exact.path(), host, 5432)
        .expect("64 unique hosts addresses should be accepted");
    assert_eq!(addrs.len(), 64);
    for (index, addr) in addrs.iter().enumerate() {
        assert_eq!(
            *addr,
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, index as u8 + 1), 5432))
        );
    }

    contents.push_str(&format!("192.0.2.65 {host}\n"));
    let over = TempResolverFile::padded("hosts-address-over", contents.as_bytes(), contents.len());
    let err = resolve_local_host_with_hosts_path(over.path(), host, 5432)
        .expect_err("a 65th unique hosts address should be rejected");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "resolver result exceeds 64 unique addresses"
    );
}

#[test]
fn resolver_hosts_invalid_line_does_not_mask_the_address_result_bound() {
    let host = "address-utf8-precedence.flowio.invalid";
    let mut bytes = vec![0xff, b'\n'];
    for octet in 1..=65 {
        bytes.extend_from_slice(format!("192.0.2.{octet} {host}\n").as_bytes());
    }
    let invalid = TempResolverFile::padded("hosts-address-invalid", &bytes, bytes.len());

    let err = resolve_local_host_with_hosts_path(invalid.path(), host, 5432)
        .expect_err("an invalid line must not suppress the address cap");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "resolver result exceeds 64 unique addresses"
    );
}

#[test]
fn resolver_hosts_file_size_limit_precedes_deferred_address_result_bound() {
    let host = "address-size-precedence.flowio.invalid";
    let mut contents = String::new();
    for octet in 1..=65 {
        contents.push_str(&format!("192.0.2.{octet} {host}\n"));
    }

    let exact = TempResolverFile::padded(
        "hosts-address-size-exact",
        contents.as_bytes(),
        HOSTS_FILE_MAX_BYTES,
    );
    let err = resolve_local_host_with_hosts_path(exact.path(), host, 5432)
        .expect_err("the exact-size file should report its 65th unique result");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "resolver result exceeds 64 unique addresses"
    );

    let over = TempResolverFile::padded(
        "hosts-address-size-over",
        contents.as_bytes(),
        HOSTS_FILE_MAX_BYTES + 1,
    );
    let err = resolve_local_host_with_hosts_path(over.path(), host, 5432)
        .expect_err("the raw file-size bound must win over a deferred result error");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "/etc/hosts exceeds the 4 MiB resolver configuration limit"
    );
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
fn resolver_reuses_identical_query_after_nameserver_timeout() {
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
    let expected_ip = Ipv4Addr::new(192, 0, 2, 74);
    let second_thread = thread::spawn(move || {
        second_server
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("failed to set second dns socket timeout");
        let mut buffer = [0u8; 512];
        let (len, peer) = second_server
            .recv_from(&mut buffer)
            .expect("second nameserver did not receive the retry");
        let query = buffer[..len].to_vec();
        let response = build_response(&query, TestAnswer::A(expected_ip));
        second_server
            .send_to(&response, peer)
            .expect("second nameserver failed to answer");
        query
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![first_nameserver, second_nameserver])
                .expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(30));
            let addresses = lookup_ipv4(&resolver, "retry.example.test")
                .await
                .expect("second nameserver should answer after the first timeout");
            assert_eq!(addresses, vec![IpAddr::V4(expected_ip)]);
        })
        .expect("executor run failed");

    first_server
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("failed to set first dns socket timeout");
    let mut first_buffer = [0u8; 512];
    let (first_len, _) = first_server
        .recv_from(&mut first_buffer)
        .expect("first nameserver did not receive the initial query");
    let second_query = second_thread.join().expect("second dns thread panicked");
    assert_eq!(&first_buffer[..first_len], second_query);
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn resolver_restores_query_after_send_submission_failure() {
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
    let expected_ip = Ipv4Addr::new(192, 0, 2, 75);
    let second_thread = thread::spawn(move || {
        second_server
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("failed to set second dns socket timeout");
        let mut buffer = [0u8; 512];
        let (len, peer) = second_server
            .recv_from(&mut buffer)
            .expect("second nameserver did not receive the restored query");
        let response = build_response(&buffer[..len], TestAnswer::A(expected_ip));
        second_server
            .send_to(&response, peer)
            .expect("second nameserver failed to answer");
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![first_nameserver, second_nameserver])
                .expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(100));
            test_hooks::fail_next_sqe_submit();
            let addresses = lookup_ipv4(&resolver, "submission.example.test")
                .await
                .expect("second nameserver should receive the restored query");
            assert_eq!(addresses, vec![IpAddr::V4(expected_ip)]);
        })
        .expect("executor run failed");

    second_thread.join().expect("second dns thread panicked");
    first_server
        .set_nonblocking(true)
        .expect("failed to make first dns socket nonblocking");
    let mut unexpected = [0u8; 512];
    let err = first_server
        .recv_from(&mut unexpected)
        .expect_err("failed submission must not emit a first-server datagram");
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
fn resolve_host_falls_back_after_nxdomain_with_malformed_authority() {
    const MALFORMED_NXDOMAIN: TestSections = TestSections {
        answer: &[],
        authority: &[TestRecord::Truncated("db.example.test")],
        additional: &[],
    };

    let bad_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind malformed dns socket");
    let bad_nameserver = bad_server
        .local_addr()
        .expect("failed to read malformed dns socket addr");
    let bad_thread = thread::spawn(move || {
        serve_dns_queries(bad_server, 2, |_name, _qtype| {
            TestAnswer::NegativeSectioned(MALFORMED_NXDOMAIN, NegativeRcode::NxDomain)
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
                .expect("resolver should fall back after malformed NXDOMAIN Authority");
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
fn resolve_host_falls_back_after_root_cname_without_querying_root() {
    let bad_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind root-CNAME dns socket");
    let bad_nameserver = bad_server
        .local_addr()
        .expect("failed to read root-CNAME dns socket addr");
    let bad_thread = thread::spawn(move || {
        serve_dns_queries(bad_server, 2, |name, qtype| {
            assert_eq!(name, "db.example.test");
            assert!(matches!(qtype, 1 | 28));
            TestAnswer::RootCnameTarget
        })
    });

    let expected_ip = Ipv4Addr::new(192, 0, 2, 45);
    let good_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind root-CNAME fallback dns socket");
    let good_nameserver = good_server
        .local_addr()
        .expect("failed to read root-CNAME fallback dns socket addr");
    let good_thread = thread::spawn(move || {
        serve_dns_queries(good_server, 2, move |name, qtype| {
            assert_eq!(name, "db.example.test");
            match qtype {
                1 => TestAnswer::A(expected_ip),
                28 => TestAnswer::Empty,
                _ => panic!("unexpected DNS query type {qtype}"),
            }
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
                .expect("root CNAME should be an upstream failover error");
            assert_eq!(addrs, vec![SocketAddr::from((expected_ip, 5432))]);
        })
        .expect("executor run failed");

    bad_thread.join().expect("bad dns thread panicked");
    good_thread.join().expect("good dns thread panicked");
}

#[test]
fn resolve_host_falls_back_after_non_query_opcode_response() {
    let rejected_ip = Ipv4Addr::new(203, 0, 113, 57);
    let expected_ip = Ipv4Addr::new(192, 0, 2, 57);

    let bad_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind non-QUERY dns socket");
    let bad_nameserver = bad_server
        .local_addr()
        .expect("failed to read non-QUERY dns socket addr");
    let bad_thread = thread::spawn(move || {
        serve_dns_queries(bad_server, 2, move |_name, _qtype| {
            TestAnswer::NonQueryA(rejected_ip)
        })
    });

    let good_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind QUERY dns socket");
    let good_nameserver = good_server
        .local_addr()
        .expect("failed to read QUERY dns socket addr");
    let good_thread = thread::spawn(move || {
        serve_dns_queries(good_server, 2, move |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::A(expected_ip),
            ("db.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![bad_nameserver, good_nameserver])
                .expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(100));
            let addrs = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect("resolver should fail over after non-QUERY responses");
            assert_eq!(addrs, vec![SocketAddr::from((expected_ip, 5432))]);
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
fn resolve_host_drains_full_stale_response_before_matching_query() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries_with_first_datagrams(
            server,
            2,
            &[FirstDnsDatagram::FullStaleQueryId],
            |name, qtype| {
                Some(match (name, qtype) {
                    ("db.example.test", 1) => TestAnswer::A(Ipv4Addr::new(192, 0, 2, 51)),
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
                .expect("resolver should drain a full stale response");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 51), 5432))]
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
fn resolve_host_drains_matching_question_pointer_loop_before_response() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries_with_first_datagrams(
            server,
            2,
            &[FirstDnsDatagram::MalformedQuestionPointerLoop],
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
                .expect("resolver should drain the malformed question and use the real response");
            assert_eq!(
                addrs,
                vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 50), 5432))]
            );
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_drains_invalid_utf8_question_collision_before_response() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let expected = Ipv4Addr::new(192, 0, 2, 53);
    let thread = thread::spawn(move || {
        serve_dns_queries_with_first_datagrams(
            server,
            2,
            &[FirstDnsDatagram::InvalidUtf8QuestionCollision],
            move |name, qtype| {
                Some(match (name, qtype) {
                    ("\u{fffd}.example.test", 1) => TestAnswer::A(expected),
                    ("\u{fffd}.example.test", 28) => TestAnswer::Empty,
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
                .resolve_host("\u{fffd}.example.test", 5432)
                .await
                .expect("resolver should drain the invalid UTF-8 question");
            assert_eq!(addrs, vec![SocketAddr::from((expected, 5432))]);
        })
        .expect("executor run failed");

    thread.join().expect("dns thread panicked");
}

#[test]
fn resolve_host_accepts_one_cname_followup_query_round() {
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
fn resolve_host_rejects_a_second_cname_followup_query_round() {
    let err = resolve_with_mock_dns(4, Duration::from_millis(200), |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::Cname("db.mid.test")),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            ("db.mid.test", 1) => Some(TestAnswer::Cname("db.final.test")),
            ("db.mid.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect_err("a second CNAME follow-up round should be rejected");

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "DNS resolution exceeded maximum CNAME follow-up query count",
    );
}

#[test]
fn resolve_host_accepts_sixteen_total_cname_hops() {
    let expected = Ipv4Addr::new(198, 51, 100, 116);
    let addrs = resolve_with_mock_dns(4, Duration::from_millis(200), move |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::CnameChain {
                names: &CNAME_TOTAL_INITIAL_15,
                address: None,
            }),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            ("total-followup.example.test", 1) => Some(TestAnswer::CnameChain {
                names: &CNAME_TOTAL_FOLLOWUP_1,
                address: Some(expected),
            }),
            ("total-followup.example.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("a CNAME chain at the total hop limit should resolve");

    assert_eq!(addrs, vec![SocketAddr::from((expected, 5432))]);
}

#[test]
fn resolve_host_rejects_seventeen_total_cname_hops_without_nameserver_retry() {
    let first_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind first dns socket");
    let first_nameserver = first_server
        .local_addr()
        .expect("failed to read first dns socket addr");
    let first_thread = thread::spawn(move || {
        serve_dns_queries(first_server, 4, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::CnameChain {
                names: &CNAME_TOTAL_INITIAL_15,
                address: None,
            },
            ("db.example.test", 28) => TestAnswer::Empty,
            ("total-followup.example.test", 1) => TestAnswer::CnameChain {
                names: &CNAME_TOTAL_FOLLOWUP_2,
                address: Some(Ipv4Addr::new(198, 51, 100, 117)),
            },
            ("total-followup.example.test", 28) => TestAnswer::Empty,
            _ => TestAnswer::NxDomain,
        })
    });

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
            resolver.set_query_timeout(Duration::from_millis(200));
            let err = resolver
                .resolve_host("db.example.test", 5432)
                .await
                .expect_err("a CNAME chain above the total hop limit should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                err.to_string(),
                "DNS resolution exceeded maximum total CNAME hop count",
            );
        })
        .expect("executor run failed");

    first_thread.join().expect("first dns thread panicked");
    second_server
        .set_nonblocking(true)
        .expect("failed to make second dns socket nonblocking");
    let mut unexpected = [0u8; 512];
    let err = second_server
        .recv_from(&mut unexpected)
        .expect_err("CNAME hop-budget exhaustion must stop nameserver failover");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn resolve_host_keeps_in_budget_sibling_address_when_a_exceeds_total_hops() {
    let expected = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x118);
    let addrs = resolve_with_mock_dns(4, Duration::from_millis(200), move |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::CnameChain {
                names: &CNAME_TOTAL_INITIAL_15,
                address: None,
            }),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            ("total-followup.example.test", 1) => Some(TestAnswer::CnameChain {
                names: &CNAME_TOTAL_FOLLOWUP_2,
                address: Some(Ipv4Addr::new(198, 51, 100, 118)),
            }),
            ("total-followup.example.test", 28) => Some(TestAnswer::Aaaa(expected)),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("an in-budget sibling address should survive an over-budget A response");

    assert_eq!(addrs, vec![SocketAddr::from((expected, 5432))]);
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
    const CHAIN: &[&str] = &["db.example.test", "db.mid.test", "db.internal.test"];
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1) => TestAnswer::CnameChain {
                names: CHAIN,
                address: Some(Ipv4Addr::new(198, 51, 100, 89)),
            },
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
fn resolve_host_accepts_portal_azure_shaped_five_hop_chain() {
    let expected = Ipv4Addr::new(198, 51, 100, 105);
    let addrs = resolve_named_with_mock_dns(
        "portal.azure.com",
        2,
        Duration::from_millis(200),
        move |name, qtype| match (name, qtype) {
            ("portal.azure.com", 1) => Some(TestAnswer::CnameChain {
                names: &PORTAL_AZURE_CNAME_CHAIN,
                address: Some(expected),
            }),
            ("portal.azure.com", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        },
    )
    .expect("a five-hop portal.azure-shaped response should resolve");

    assert_eq!(addrs, vec![SocketAddr::from((expected, 5432))]);
}

#[test]
fn resolve_host_accepts_sixteen_cname_hops_in_one_response() {
    let expected = Ipv4Addr::new(198, 51, 100, 106);
    let addrs = resolve_with_mock_dns(2, Duration::from_millis(200), move |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::CnameChain {
                names: &CNAME_RESPONSE_BOUNDARY_CHAIN[..17],
                address: Some(expected),
            }),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("an in-response CNAME chain at the hop limit should resolve");

    assert_eq!(addrs, vec![SocketAddr::from((expected, 5432))]);
}

#[test]
fn resolve_host_rejects_seventeen_cname_hops_in_one_response() {
    let err = resolve_with_mock_dns(2, Duration::from_millis(200), |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::CnameChain {
                names: &CNAME_RESPONSE_BOUNDARY_CHAIN,
                address: Some(Ipv4Addr::new(198, 51, 100, 107)),
            }),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect_err("an in-response CNAME chain above the hop limit should fail");

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "DNS response CNAME chain exceeded maximum per-response hop count",
    );
}

#[test]
fn resolve_host_follows_answer_cname_instead_of_additional_addresses() {
    const INITIAL: TestSections = TestSections {
        answer: &[TestRecord::Cname("db.example.test", "db.internal.test")],
        authority: &[],
        additional: &[
            TestRecord::A("db.internal.test", Ipv4Addr::new(203, 0, 113, 70)),
            TestRecord::A("unrelated.example.test", Ipv4Addr::new(203, 0, 113, 71)),
        ],
    };
    let expected = Ipv4Addr::new(198, 51, 100, 93);

    let addrs = resolve_with_mock_dns(4, Duration::from_millis(200), move |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::Sectioned(INITIAL)),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            ("db.internal.test", 1) => Some(TestAnswer::A(expected)),
            ("db.internal.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("Answer CNAME should use the bounded follow-up query");

    assert_eq!(addrs, vec![SocketAddr::from((expected, 5432))]);
}

#[test]
fn resolve_host_ignores_authority_injection_and_loop() {
    const AUTHORITY_ONLY: TestSections = TestSections {
        answer: &[],
        authority: &[
            TestRecord::Cname("db.example.test", "db.mid.test"),
            TestRecord::Cname("db.mid.test", "db.example.test"),
            TestRecord::A("db.example.test", Ipv4Addr::new(203, 0, 113, 72)),
            TestRecord::A("db.mid.test", Ipv4Addr::new(203, 0, 113, 73)),
        ],
        additional: &[],
    };

    let err = resolve_with_mock_dns(2, Duration::from_millis(200), |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::Sectioned(AUTHORITY_ONLY)),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect_err("Authority records must not contribute to resolution");

    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn resolve_host_ignores_additional_cname_chain() {
    const ADDITIONAL_ONLY: TestSections = TestSections {
        answer: &[],
        authority: &[],
        additional: &[
            TestRecord::Cname("db.example.test", "db.internal.test"),
            TestRecord::A("db.internal.test", Ipv4Addr::new(203, 0, 113, 74)),
        ],
    };

    let err = resolve_with_mock_dns(2, Duration::from_millis(200), |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::Sectioned(ADDITIONAL_ONLY)),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect_err("Additional CNAME/address data must not resolve the query");

    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn resolve_host_deduplicates_duplicate_answer_names_and_addresses() {
    const DUPLICATE_ANSWERS: TestSections = TestSections {
        answer: &[
            TestRecord::Cname("db.example.test", "db.internal.test"),
            TestRecord::Cname("db.example.test", "db.internal.test"),
            TestRecord::A("db.internal.test", Ipv4Addr::new(198, 51, 100, 94)),
            TestRecord::A("db.internal.test", Ipv4Addr::new(198, 51, 100, 94)),
        ],
        authority: &[],
        additional: &[],
    };

    let addrs = resolve_with_mock_dns(2, Duration::from_millis(200), |name, qtype| {
        match (name, qtype) {
            ("db.example.test", 1) => Some(TestAnswer::Sectioned(DUPLICATE_ANSWERS)),
            ("db.example.test", 28) => Some(TestAnswer::Empty),
            _ => Some(TestAnswer::NxDomain),
        }
    })
    .expect("duplicate Answer records should remain valid");

    assert_eq!(
        addrs,
        vec![SocketAddr::from((Ipv4Addr::new(198, 51, 100, 94), 5432,))]
    );
}

#[test]
fn resolve_host_selects_requested_family_from_mixed_answer_chain() {
    const MIXED_ANSWERS: TestSections = TestSections {
        answer: &[
            TestRecord::Cname("db.example.test", "db.internal.test"),
            TestRecord::A("db.internal.test", Ipv4Addr::new(198, 51, 100, 95)),
            TestRecord::Aaaa(
                "db.internal.test",
                Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x95),
            ),
        ],
        authority: &[],
        additional: &[],
    };

    let addrs = resolve_with_mock_dns(2, Duration::from_millis(200), |name, _| match name {
        "db.example.test" => Some(TestAnswer::Sectioned(MIXED_ANSWERS)),
        _ => Some(TestAnswer::NxDomain),
    })
    .expect("mixed Answer chain should retain requested-family filtering");

    assert_eq!(
        addrs,
        vec![
            SocketAddr::from((Ipv4Addr::new(198, 51, 100, 95), 5432)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x95), 5432,)),
        ]
    );
}

#[test]
fn resolve_host_structurally_parses_ignored_sections() {
    const TRUNCATED_AUTHORITY: TestSections = TestSections {
        answer: &[],
        authority: &[TestRecord::Truncated("db.example.test")],
        additional: &[],
    };
    const TRUNCATED_ADDITIONAL: TestSections = TestSections {
        answer: &[],
        authority: &[],
        additional: &[TestRecord::Truncated("db.example.test")],
    };

    for (label, sections) in [
        ("Authority", TRUNCATED_AUTHORITY),
        ("Additional", TRUNCATED_ADDITIONAL),
    ] {
        let err = resolve_with_mock_dns(2, Duration::from_millis(200), move |_, _| {
            Some(TestAnswer::Sectioned(sections))
        })
        .expect_err("truncated ignored-section record should fail parsing");
        assert_eq!(
            err.kind(),
            io::ErrorKind::UnexpectedEof,
            "{label} record should be structurally parsed",
        );
    }
}

#[test]
fn resolve_host_rejects_cyclic_in_response_cname_chain() {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let thread = thread::spawn(move || {
        serve_dns_queries(server, 2, |name, qtype| match (name, qtype) {
            ("db.example.test", 1 | 28) => TestAnswer::CyclicCname("db.mid.test"),
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
            assert_eq!(err.to_string(), "DNS response CNAME chain contained a loop",);
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
fn resolve_host_rejects_invalid_utf8_cname_without_followup_query() {
    assert_rejected_cname_has_no_followup(
        TestAnswer::InvalidUtf8CnameTarget,
        "DNS label was not valid UTF-8",
    );
}

#[test]
fn resolve_host_rejects_literal_dot_cname_without_followup_query() {
    assert_rejected_cname_has_no_followup(
        TestAnswer::LiteralDotCnameTarget,
        "DNS literal label contained a dot",
    );
}

fn assert_rejected_cname_has_no_followup(answer: TestAnswer, expected_message: &'static str) {
    let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind test dns socket");
    let nameserver = server.local_addr().expect("failed to read dns socket addr");
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let thread = thread::spawn(move || {
        server
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("failed to set initial dns timeout");

        let mut queries = Vec::new();
        for expected_qtype in [1, 28] {
            let mut buffer = [0u8; 512];
            let (len, peer) = server.recv_from(&mut buffer).expect("dns recv_from failed");
            let query = &buffer[..len];
            let qname = parse_qname(query).expect("failed to parse dns qname");
            assert_eq!(qname, "db.example.test");
            let qtype = parse_qtype(query).expect("failed to parse dns qtype");
            assert_eq!(qtype, expected_qtype);
            queries.push((qname, qtype));
            let response = build_response(
                query,
                if qtype == 1 {
                    answer
                } else {
                    TestAnswer::Empty
                },
            );
            server
                .send_to(&response, peer)
                .expect("dns send_to response failed");
        }

        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("resolver did not finish the CNAME rejection test");
        server
            .set_nonblocking(true)
            .expect("failed to make follow-up probe nonblocking");
        loop {
            let mut unexpected = [0u8; 512];
            match server.recv_from(&mut unexpected) {
                Ok((len, _)) => {
                    let query = &unexpected[..len];
                    queries.push((
                        parse_qname(query).expect("failed to parse follow-up qname"),
                        parse_qtype(query).expect("failed to parse follow-up qtype"),
                    ));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("follow-up query probe failed: {err}"),
            }
        }
        queries
    });

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
            resolver.set_query_timeout(Duration::from_millis(200));
            let result = resolver.resolve_host("db.example.test", 5432).await;
            done_tx
                .send(())
                .expect("failed to release the DNS query recorder");
            let err = result.expect_err("invalid CNAME target should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert_eq!(err.to_string(), expected_message);
        })
        .expect("executor run failed");

    let queries = thread.join().expect("dns thread panicked");
    assert_eq!(
        queries,
        [
            ("db.example.test".to_owned(), 1),
            ("db.example.test".to_owned(), 28),
        ],
        "invalid CNAME target must not produce a follow-up query",
    );
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
    resolve_named_with_mock_dns("db.example.test", expected_queries, query_timeout, answer)
}

fn resolve_named_with_mock_dns<F>(
    host: &'static str,
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
            task_result.set(Some(resolver.resolve_host(host, 5432).await));
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
        FirstDnsDatagram::FullStaleQueryId => {
            let mut stale = response.to_vec();
            let query_id = read_u16_be_at(&stale, 0).expect("test response query ID should exist");
            stale
                .write_u16_be_at(0, query_id.wrapping_add(1))
                .expect("test stale query ID rewrite should fit");
            stale.resize(2048, 0xA5);
            assert_eq!(stale.len(), 2048);
            socket
                .send_to(&stale, peer)
                .expect("dns send_to full stale response failed");
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
        FirstDnsDatagram::MalformedQuestionPointerLoop => {
            let query_id = query
                .get(..2)
                .expect("test query ID should exist for malformed response");
            let mut malformed = [0u8; 20];
            malformed[..2].copy_from_slice(query_id);
            malformed[2..6].copy_from_slice(&[0x81, 0x80, 0x00, 0x01]);
            malformed[12..].copy_from_slice(&[0x01, b'x', 0xc0, 0x0c, 0, 1, 0, 1]);
            socket
                .send_to(&malformed, peer)
                .expect("dns send_to malformed question response failed");
        }
        FirstDnsDatagram::InvalidUtf8QuestionCollision => {
            let mut collision = response.to_vec();
            assert_eq!(
                collision.get(12..16),
                Some(&[3, 0xef, 0xbf, 0xbd][..]),
                "collision fixture requires a leading U+FFFD label",
            );
            if read_u16_be_at(&collision, 6).expect("test Answer count should exist") != 0 {
                let rdata = collision
                    .len()
                    .checked_sub(4)
                    .expect("test A response should contain RDATA");
                collision[rdata..].copy_from_slice(&[203, 0, 113, 53]);
            }
            collision[12] = 1;
            collision[13] = 0xff;
            collision.drain(14..16);
            socket
                .send_to(&collision, peer)
                .expect("dns send_to invalid UTF-8 question failed");
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
        TestAnswer::CnameChain { names, address } => {
            let cname_count = names
                .len()
                .checked_sub(1)
                .expect("test CNAME chain should contain an owner");
            u16::try_from(cname_count + usize::from(address.is_some()))
                .expect("test CNAME chain answer count should fit")
        }
        TestAnswer::CyclicCname(_) => 2u16,
        TestAnswer::A(_)
        | TestAnswer::NonQueryA(_)
        | TestAnswer::OversizedA(_)
        | TestAnswer::Aaaa(_)
        | TestAnswer::AFor(_, _)
        | TestAnswer::Cname(_)
        | TestAnswer::MalformedCnamePointer
        | TestAnswer::ForwardCnamePointer
        | TestAnswer::CnamePointerLoop
        | TestAnswer::CnameRdataOverrun
        | TestAnswer::CnameRdataTrailingBytes
        | TestAnswer::InvalidUtf8CnameTarget
        | TestAnswer::LiteralDotCnameTarget
        | TestAnswer::RootCnameTarget => 1u16,
        TestAnswer::QuestionTypeMismatch
        | TestAnswer::QuestionClassMismatch
        | TestAnswer::Empty
        | TestAnswer::NxDomain
        | TestAnswer::NegativeQuestionMismatch(_, _)
        | TestAnswer::QuestionlessNxDomain
        | TestAnswer::ServFail
        | TestAnswer::QuestionlessServFail => 0u16,
        TestAnswer::Sectioned(sections) | TestAnswer::NegativeSectioned(sections, _) => {
            u16::try_from(sections.answer.len()).expect("test Answer record count should fit")
        }
    };
    let authority_count = match answer {
        TestAnswer::Sectioned(sections) | TestAnswer::NegativeSectioned(sections, _) => {
            u16::try_from(sections.authority.len()).expect("test Authority record count should fit")
        }
        _ => 0,
    };
    let additional_count = match answer {
        TestAnswer::Sectioned(sections) | TestAnswer::NegativeSectioned(sections, _) => {
            u16::try_from(sections.additional.len())
                .expect("test Additional record count should fit")
        }
        _ => 0,
    };
    let flags = match answer {
        TestAnswer::NegativeQuestionMismatch(_, NegativeRcode::NxDomain)
        | TestAnswer::NegativeSectioned(_, NegativeRcode::NxDomain)
        | TestAnswer::NxDomain
        | TestAnswer::QuestionlessNxDomain => 0x8183u16,
        TestAnswer::NegativeQuestionMismatch(_, NegativeRcode::ServFail)
        | TestAnswer::NegativeSectioned(_, NegativeRcode::ServFail)
        | TestAnswer::ServFail
        | TestAnswer::QuestionlessServFail => 0x8182u16,
        TestAnswer::NonQueryA(_) => 0x8980u16,
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
    push_u16_be(&mut response, authority_count);
    push_u16_be(&mut response, additional_count);
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
        TestAnswer::A(ip) | TestAnswer::NonQueryA(ip) => {
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
        TestAnswer::RootCnameTarget => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 1);
            response.push(0);
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
        TestAnswer::CnameChain { names, address } => {
            let query_name = parse_qname(query).expect("test query qname should parse");
            assert_eq!(
                names.first().copied(),
                Some(query_name.as_str()),
                "test CNAME chain should start at the queried owner",
            );
            for pair in names.windows(2) {
                push_test_record(&mut response, TestRecord::Cname(pair[0], pair[1]));
            }
            if let Some(address) = address {
                let owner = names
                    .last()
                    .copied()
                    .expect("test CNAME chain should contain an address owner");
                push_test_record(&mut response, TestRecord::A(owner, address));
            }
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
        TestAnswer::InvalidUtf8CnameTarget => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 3);
            response.extend_from_slice(&[1, 0xff, 0]);
        }
        TestAnswer::LiteralDotCnameTarget => {
            push_u16_be(&mut response, 0xC00C);
            push_u16_be(&mut response, 5);
            push_u16_be(&mut response, 1);
            push_u32_be(&mut response, 60);
            push_u16_be(&mut response, 13);
            response.extend_from_slice(b"\x0bexample.com\0");
        }
        TestAnswer::QuestionTypeMismatch
        | TestAnswer::QuestionClassMismatch
        | TestAnswer::NegativeQuestionMismatch(_, _)
        | TestAnswer::Empty
        | TestAnswer::NxDomain
        | TestAnswer::QuestionlessNxDomain
        | TestAnswer::ServFail
        | TestAnswer::QuestionlessServFail => {}
        TestAnswer::Sectioned(sections) | TestAnswer::NegativeSectioned(sections, _) => {
            for record in sections
                .answer
                .iter()
                .chain(sections.authority)
                .chain(sections.additional)
            {
                push_test_record(&mut response, *record);
            }
        }
    }

    response
}

fn push_test_record(response: &mut Vec<u8>, record: TestRecord) {
    let (owner, rr_type, rdlength) = match record {
        TestRecord::A(owner, _) => (owner, 1, 4),
        TestRecord::Aaaa(owner, _) => (owner, 28, 16),
        TestRecord::Cname(owner, target) => {
            push_name(response, owner);
            push_u16_be(response, 5);
            push_u16_be(response, 1);
            push_u32_be(response, 60);

            let mut encoded_target = Vec::new();
            push_name(&mut encoded_target, target);
            push_u16_be(
                response,
                u16::try_from(encoded_target.len()).expect("test CNAME RDATA length should fit"),
            );
            response.extend_from_slice(&encoded_target);
            return;
        }
        TestRecord::Truncated(owner) => {
            push_name(response, owner);
            return;
        }
    };

    push_name(response, owner);
    push_u16_be(response, rr_type);
    push_u16_be(response, 1);
    push_u32_be(response, 60);
    push_u16_be(response, rdlength);
    match record {
        TestRecord::A(_, address) => response.extend_from_slice(&address.octets()),
        TestRecord::Aaaa(_, address) => response.extend_from_slice(&address.octets()),
        TestRecord::Cname(_, _) | TestRecord::Truncated(_) => unreachable!(),
    }
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
