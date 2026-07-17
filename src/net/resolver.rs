//! Hostname resolution helpers for FlowIO transports.
//!
//! The resolver keeps the public surface deliberately small:
//! - IP literals resolve directly without DNS traffic
//! - `localhost` and `/etc/hosts` entries are honored first
//! - all other names are resolved through UDP DNS queries using FlowIO's own
//!   transport and timer APIs
//!
//! DNS lookup is intentionally narrow:
//! - system configuration is read from `/etc/resolv.conf`
//! - only A and AAAA lookups are issued
//! - one linear CNAME chain is followed with independent per-response,
//!   cross-response, and name-compression bounds
//! - search domains and TCP fallback for truncated replies are not yet
//!   implemented
//!
//! # Fast-Path Guidance
//!
//! Preferred on setup/control paths:
//! - Resolver APIs are setup/control-plane helpers rather than steady-state
//!   data-plane APIs. Resolve host names once, keep the resulting
//!   `SocketAddr` values, and pass those addresses into transport connectors
//!   on the hot path.
//! - Use [`DnsResolver`] when resolving repeatedly so nameserver selection and
//!   timeout policy are constructed once and then reused. Reuse avoids
//!   rebuilding that setup state, but each
//!   non-local DNS query still creates a UDP socket and owned packet buffers.
//!
//! Avoid on the fast path:
//! - Avoid DNS lookup in the steady-state data path. Reuse the
//!   resolved `SocketAddr` values instead.
//! - Avoid constructing a fresh resolver for every repeated lookup. Use
//!   [`DnsResolver`] instead of the convenience [`resolve_host`] helper.
//! - Do not call [`DnsResolver::from_system`] from an async hot path: it reads
//!   `/etc/resolv.conf` synchronously. Non-literal lookups also inspect
//!   `/etc/hosts` synchronously.
//!
//! # Example
//! ```no_run
//! use flowio::net::resolver::DnsResolver;
//! use flowio::runtime::executor::Executor;
//!
//! let resolver = DnsResolver::from_system()?;
//! let mut executor = Executor::new()?;
//! executor.run(async move {
//!     let addrs = resolver.resolve_host("localhost", 5432).await.unwrap();
//!     assert!(!addrs.is_empty());
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use crate::net::udp::UdpSocket;
use crate::runtime::buffer::bytes::{
    BufferCursorMut, BufferRangeError, read_u16_be_at, write_u16_be_at,
};
use crate::runtime::timer::{TimeoutError, timeout};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DNS_PORT: u16 = 53;
const DNS_CLASS_IN: u16 = 1;
const DNS_TYPE_A: u16 = 1;
const DNS_TYPE_CNAME: u16 = 5;
const DNS_TYPE_AAAA: u16 = 28;
const DNS_FLAG_QR: u16 = 0x8000;
const DNS_FLAG_TC: u16 = 0x0200;
const DNS_RCODE_MASK: u16 = 0x000F;
const DNS_RCODE_FORMERR: u8 = 1;
const DNS_RCODE_SERVFAIL: u8 = 2;
const DNS_RCODE_NXDOMAIN: u8 = 3;
const DNS_RCODE_NOTIMP: u8 = 4;
const DNS_RCODE_REFUSED: u8 = 5;
const DNS_MAX_NAME_PRESENTATION_LEN: usize = 253;
const DNS_MAX_NAME_WIRE_LEN: usize = 255;
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_CNAME_HOPS_PER_RESPONSE: usize = 16;
const MAX_CNAME_FOLLOWUP_QUERIES: usize = 1;
const MAX_CNAME_TOTAL_HOPS: usize = 16;
const MAX_NAME_COMPRESSION_DEPTH: usize = 8;
const DNS_UDP_RESPONSE_BUFFER_SIZE: usize = 2048;
const RESOLV_CONF_PATH: &str = "/etc/resolv.conf";
const HOSTS_PATH: &str = "/etc/hosts";

static QUERY_ID_STATE: AtomicU64 = AtomicU64::new(0);

enum QueryAttemptError {
    Io(io::Error),
    Timeout(TimeoutError),
}

impl QueryAttemptError {
    fn into_io_error(self) -> io::Error {
        match self {
            Self::Io(err) | Self::Timeout(TimeoutError::Runtime(err)) => err,
            Self::Timeout(TimeoutError::Elapsed) => {
                io::Error::new(io::ErrorKind::TimedOut, "DNS query timed out")
            }
        }
    }
}

/// Reusable DNS resolver built on FlowIO UDP sockets.
///
/// Use this on setup/control-plane paths when lookups repeat and the configured
/// nameservers/timeouts should be reused. For one-off convenience resolution,
/// use [`resolve_host`]. Reuse does not make lookup allocation-free: DNS
/// attempts create UDP sockets and owned query/response buffers, and non-literal
/// names inspect `/etc/hosts` for each call.
///
/// # Example
/// ```
/// use flowio::net::resolver::DnsResolver;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut resolver = DnsResolver::new(vec![SocketAddr::from((Ipv4Addr::LOCALHOST, 53))])?;
/// resolver.set_query_timeout(std::time::Duration::from_secs(1));
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct DnsResolver {
    /// Upstream recursive resolvers queried over UDP, in retry order.
    nameservers: Box<[SocketAddr]>,
    /// Timeout for waiting on a matching response after each UDP query send
    /// completes.
    query_timeout: Duration,
}

impl DnsResolver {
    /// Builds a resolver from `/etc/resolv.conf`.
    ///
    /// This performs a synchronous filesystem read. Call it during setup, then
    /// store the resulting nameserver list for reuse across lookups.
    pub fn from_system() -> io::Result<Self> {
        let nameservers = read_resolv_conf(RESOLV_CONF_PATH)?;
        Self::new(nameservers)
    }

    /// Builds a resolver from an explicit nameserver list.
    ///
    /// Use this when the application needs deterministic or test-specific DNS
    /// behavior instead of system defaults.
    pub fn new(nameservers: Vec<SocketAddr>) -> io::Result<Self> {
        if nameservers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resolver requires at least one nameserver",
            ));
        }

        Ok(Self {
            nameservers: nameservers.into_boxed_slice(),
            query_timeout: DEFAULT_QUERY_TIMEOUT,
        })
    }

    /// Sets the matching-response wait timeout after each UDP query is sent.
    ///
    /// This does not bound socket creation or the preceding async UDP send.
    pub fn set_query_timeout(&mut self, query_timeout: Duration) -> &mut Self {
        self.query_timeout = query_timeout;
        self
    }

    /// Resolves a host name into socket addresses for the requested port.
    ///
    /// This first handles IP literals, `localhost`, and `/etc/hosts`, then
    /// falls back to UDP DNS queries if needed.
    ///
    /// This is setup/control-plane work. Keep the returned addresses and pass
    /// them to transport connectors instead of resolving on the data path.
    /// Non-literal names synchronously inspect `/etc/hosts`; each upstream DNS
    /// attempt creates a connected UDP socket plus owned query/response
    /// buffers. A true query expiry advances to the next nameserver; timer
    /// `OutOfMemory` stops that family lookup without attempting another
    /// server. A and AAAA remain sequential, in that order. Their outcomes are
    /// combined as: any address, a terminal local/runtime error, an A-first
    /// CNAME, NXDOMAIN, an A-first recoverable error, then `NotFound` for two
    /// empty answers. An A-side terminal error stops before the AAAA query; a
    /// later AAAA-side terminal error does not discard an A address.
    ///
    /// Surrounding whitespace and trailing dots are removed before lookup;
    /// root-only input is rejected. A non-literal DNS query name must contain
    /// only non-empty labels of at most 63 bytes, fit within 253 bytes in
    /// normalized dotted form, and encode to at most 255 bytes including label
    /// lengths and the terminal root. Invalid names return `InvalidInput`
    /// before a query packet is allocated or sent. A CNAME response is
    /// malformed unless its encoded (possibly compressed) name consumes its
    /// declared RDATA length exactly; malformed responses participate in the
    /// existing nameserver and address-family error selection.
    /// Every present echoed question is matched by name, type, and class before
    /// its response code is applied. Questionless FORMERR, SERVFAIL, NOTIMP,
    /// and REFUSED replies remain prompt nameserver-failover results, while a
    /// questionless NXDOMAIN is drained as an unrelated datagram.
    /// Every literal label in a response name, including a compressed suffix,
    /// must be valid UTF-8. The shared name walker rejects an invalid echoed
    /// question before comparison and rejects an invalid record owner or CNAME
    /// target with `InvalidData` before response-code or chain processing.
    /// Valid non-ASCII label text is preserved; name comparison folds ASCII case
    /// only.
    /// Every declared Answer, Authority, and Additional record is structurally
    /// validated before NXDOMAIN or another response code is applied. Known A
    /// and AAAA records require their exact wire lengths, and CNAME RDATA must
    /// contain exactly one complete encoded name, regardless of section or
    /// class. Only Internet-class Answer CNAME and address records contribute
    /// to resolution; all other valid records are ignored. An Answer CNAME
    /// without an Answer address follows one linear chain for at most 16 hops
    /// in one response, 16 hops total, and one canonical-name follow-up query
    /// round. CNAME loops are rejected explicitly. DNS name-compression
    /// recursion is bounded independently at depth 8.
    pub async fn resolve_host(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        let host = normalize_host(host)?;

        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }

        let mut addrs = resolve_local_host(host, port)?;
        if !addrs.is_empty() {
            return Ok(addrs);
        }

        let mut current = host.to_owned();
        let mut cname_followup_queries = 0usize;
        let mut total_cname_hops = 0usize;
        loop {
            let remaining_cname_hops = MAX_CNAME_TOTAL_HOPS - total_cname_hops;
            match self
                .gather_dns_addresses(&current, port, &mut addrs, remaining_cname_hops)
                .await?
            {
                ResolveHostStep::Resolved => return Ok(addrs),
                ResolveHostStep::FollowCname { next, cname_hops } => {
                    total_cname_hops += cname_hops;
                    if cname_followup_queries == MAX_CNAME_FOLLOWUP_QUERIES {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "DNS resolution exceeded maximum CNAME follow-up query count",
                        ));
                    }
                    cname_followup_queries += 1;
                    current = next;
                }
            }
        }
    }

    async fn lookup_name(
        &self,
        host: &str,
        qtype: u16,
        remaining_cname_hops: usize,
    ) -> io::Result<LookupResult> {
        let query_id = next_query_id();
        let packet = encode_query_packet(query_id, host, qtype)?;
        let mut last_err = None;

        for nameserver in self.nameservers.iter().copied() {
            match self.query_nameserver(nameserver, &packet, query_id).await {
                Ok(response) => match parse_response_packet(&response, query_id, host, qtype) {
                    Ok(result) if result.cname_hops <= remaining_cname_hops => {
                        return Ok(result);
                    }
                    Ok(_) => {
                        last_err = Some(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "DNS resolution exceeded maximum total CNAME hop count",
                        ));
                    }
                    Err(err) => last_err = Some(err),
                },
                Err(QueryAttemptError::Timeout(TimeoutError::Runtime(err)))
                    if err.kind() == io::ErrorKind::OutOfMemory =>
                {
                    return Err(err);
                }
                Err(err) => last_err = Some(err.into_io_error()),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "DNS resolution failed without a nameserver response",
            )
        }))
    }

    async fn query_nameserver(
        &self,
        nameserver: SocketAddr,
        packet: &[u8],
        query_id: u16,
    ) -> Result<Vec<u8>, QueryAttemptError> {
        let mut socket =
            UdpSocket::bind(unspecified_addr(nameserver)).map_err(QueryAttemptError::Io)?;
        socket.connect(nameserver).map_err(QueryAttemptError::Io)?;

        let (send_result, _) = socket.send(packet.to_vec()).await;
        send_result.map_err(QueryAttemptError::Io)?;
        match timeout(self.query_timeout, async {
            let mut recv = vec![0u8; DNS_UDP_RESPONSE_BUFFER_SIZE];
            loop {
                let (recv_result, returned) = socket.recv(recv, DNS_UDP_RESPONSE_BUFFER_SIZE).await;
                recv = returned;
                let recv_len = recv_result?;
                if recv_len == DNS_UDP_RESPONSE_BUFFER_SIZE {
                    // This resolver does not advertise EDNS0, so conforming
                    // UDP responses fit the legacy DNS payload size and set
                    // TC when truncated. Connected UDP recv does not expose
                    // MSG_TRUNC, so a full scratch buffer is the reliable
                    // signal for anomalous truncation.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "DNS UDP response filled the receive buffer",
                    ));
                }
                let response = &recv[..recv_len];
                if response_is_decodable_candidate(response, query_id) {
                    debug_assert_eq!(recv.len(), recv_len);
                    return Ok(recv);
                }
            }
        })
        .await
        {
            Ok(result) => result.map_err(QueryAttemptError::Io),
            Err(err) => Err(QueryAttemptError::Timeout(err)),
        }
    }

    async fn gather_dns_addresses(
        &self,
        current: &str,
        port: u16,
        addrs: &mut Vec<SocketAddr>,
        remaining_cname_hops: usize,
    ) -> io::Result<ResolveHostStep> {
        let a = match self
            .lookup_name(current, DNS_TYPE_A, remaining_cname_hops)
            .await
        {
            Err(err) if is_terminal_family_lookup_error(&err) => return Err(err),
            outcome => outcome,
        };
        let aaaa = self
            .lookup_name(current, DNS_TYPE_AAAA, remaining_cname_hops)
            .await;
        finish_dns_family_lookups(current, port, addrs, a, aaaa)
    }
}

fn is_terminal_family_lookup_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::InvalidInput | io::ErrorKind::NotConnected | io::ErrorKind::OutOfMemory
    )
}

fn finish_dns_family_lookups(
    current: &str,
    port: u16,
    addrs: &mut Vec<SocketAddr>,
    a: io::Result<LookupResult>,
    aaaa: io::Result<LookupResult>,
) -> io::Result<ResolveHostStep> {
    let mut cname = None;
    let mut saw_nx_domain = false;
    let mut terminal_error = None;
    let mut first_error = None;

    // A stays first for output ordering, conflicting CNAME selection, and
    // deterministic error precedence. Both outcomes are still considered
    // before choosing the logical lookup result.
    for outcome in [a, aaaa] {
        match outcome {
            Ok(result) => {
                saw_nx_domain |= result.nx_domain;
                extend_unique_socket_addrs(addrs, &result.addresses, port);
                if cname.is_none() {
                    cname = result.cname.map(|next| (next, result.cname_hops));
                }
            }
            Err(err) => {
                if is_terminal_family_lookup_error(&err) {
                    if terminal_error.is_none() {
                        terminal_error = Some(err);
                    }
                } else if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    if !addrs.is_empty() {
        return Ok(ResolveHostStep::Resolved);
    }
    if let Some(err) = terminal_error {
        return Err(err);
    }
    if let Some((next, cname_hops)) = cname {
        return Ok(ResolveHostStep::FollowCname { next, cname_hops });
    }
    if saw_nx_domain {
        return Err(host_not_found(current));
    }
    if let Some(err) = first_error {
        return Err(err);
    }

    Err(host_not_found(current))
}

/// Resolves a host name into socket addresses using system resolver settings.
///
/// This is the convenience resolver entry point. For repeated lookups, prefer
/// constructing and reusing a [`DnsResolver`] instead so nameserver selection
/// and timeout policy are built once. This helper calls
/// [`DnsResolver::from_system`] before resolving, so it synchronously reads
/// `/etc/resolv.conf` even when `host` is an IP literal or `localhost`.
/// See [`DnsResolver::resolve_host`] for name validation and error semantics.
pub async fn resolve_host(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    DnsResolver::from_system()?.resolve_host(host, port).await
}

/// One logical DNS lookup result before the final socket-address port is
/// applied.
pub(crate) struct LookupResult {
    /// Addresses returned directly for the requested record type.
    addresses: Vec<IpAddr>,
    /// Last in-chain CNAME target reached while answering the current query.
    cname: Option<String>,
    /// Number of CNAME edges traversed to reach `cname` or an address.
    cname_hops: usize,
    /// True when the upstream resolver returned NXDOMAIN for this name.
    nx_domain: bool,
}

/// Next action after combining one name's A and AAAA lookup results.
enum ResolveHostStep {
    /// At least one address was collected for the logical lookup.
    Resolved,
    /// Continue with one canonical target and its already-consumed hop count.
    FollowCname {
        /// Canonical target queried in the next bounded outer step.
        next: String,
        /// Hops consumed in the selected A-first family response.
        cname_hops: usize,
    },
}

/// Parsed DNS records retained while matching addresses to a CNAME chain.
enum DnsRecord {
    Address {
        /// Owner name from the resource record.
        owner: String,
        /// Address record type (`A` or `AAAA`) used for qtype matching.
        rr_type: u16,
        /// Parsed IP address from the RDATA payload.
        address: IpAddr,
    },
    Cname {
        /// Owner name that aliases to `target`.
        owner: String,
        /// Canonical name reached by following this CNAME record.
        target: String,
    },
}

/// DNS resource-record section used to keep resolution data Answer-only.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DnsRecordSection {
    Answer,
    Authority,
    Additional,
}

fn next_query_id() -> u16 {
    os_random_query_id().unwrap_or_else(fallback_query_id)
}

fn os_random_query_id() -> Option<u16> {
    let mut bytes = [0u8; 2];
    let mut filled = 0usize;
    while filled < bytes.len() {
        let remaining = bytes.len() - filled;
        let result = unsafe {
            libc::getrandom(
                bytes[filled..].as_mut_ptr().cast(),
                remaining,
                libc::GRND_NONBLOCK,
            )
        };
        if result > 0 {
            filled += result as usize;
            continue;
        }
        if result == 0 {
            return None;
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return None;
    }

    Some(u16::from_ne_bytes(bytes))
}

fn fallback_query_id() -> u16 {
    let seed = fallback_query_seed();
    let mut current = QUERY_ID_STATE.load(Ordering::Relaxed);

    loop {
        let next = next_fallback_query_state(current, seed);
        match QUERY_ID_STATE.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return query_id_from_state(next),
            Err(observed) => current = observed,
        }
    }
}

fn fallback_query_seed() -> u64 {
    // Best-effort fallback when nonblocking OS randomness is unavailable. This
    // mixer avoids a sequential process-wide counter but is not a cryptographic
    // random-number generator.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    let state_addr = (&QUERY_ID_STATE as *const AtomicU64 as usize) as u64;
    mix_query_id_state(nanos ^ pid.rotate_left(17) ^ state_addr.rotate_left(31))
}

fn next_fallback_query_state(current: u64, seed: u64) -> u64 {
    let base = if current == 0 {
        seed
    } else {
        current.wrapping_add(0x9E37_79B9_7F4A_7C15)
    };
    let mixed = mix_query_id_state(base);
    if mixed == 0 { 1 } else { mixed }
}

fn mix_query_id_state(mut value: u64) -> u64 {
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    value.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn query_id_from_state(state: u64) -> u16 {
    (state as u16) ^ ((state >> 16) as u16) ^ ((state >> 32) as u16) ^ ((state >> 48) as u16)
}

fn normalize_host(host: &str) -> io::Result<&str> {
    let host = host.trim();
    let host = host.trim_end_matches('.');
    if host.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "resolver host name was empty",
        ));
    }
    validate_query_name(host)?;
    Ok(host)
}

fn resolve_local_host(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    let mut addrs = Vec::new();
    let mut deduper = SocketAddrDeduper::new();

    if host.eq_ignore_ascii_case("localhost") {
        deduper.push_unique(&mut addrs, SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
        deduper.push_unique(&mut addrs, SocketAddr::from((Ipv6Addr::LOCALHOST, port)));
    }

    for addr in read_hosts_file(HOSTS_PATH, host, port)? {
        deduper.push_unique(&mut addrs, addr);
    }

    Ok(addrs)
}

fn read_hosts_file(path: &str, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };

    let mut addrs = Vec::new();
    let mut deduper = SocketAddrDeduper::new();
    for line in contents.lines() {
        let line = strip_comment(line);
        if line.is_empty() {
            continue;
        }

        let mut parts = line.split_whitespace();
        let Some(addr) = parts.next() else {
            continue;
        };
        let Ok(ip) = addr.parse::<IpAddr>() else {
            continue;
        };

        if parts.any(|name| name.eq_ignore_ascii_case(host)) {
            let socket = SocketAddr::new(ip, port);
            deduper.push_unique(&mut addrs, socket);
        }
    }

    Ok(addrs)
}

fn read_resolv_conf(path: &str) -> io::Result<Vec<SocketAddr>> {
    let contents = fs::read_to_string(path)?;
    let mut nameservers = Vec::new();
    let mut deduper = SocketAddrDeduper::new();

    for line in contents.lines() {
        let line = strip_comment(line);
        if line.is_empty() {
            continue;
        }

        let mut parts = line.split_whitespace();
        if parts.next() != Some("nameserver") {
            continue;
        }

        let Some(addr) = parts.next() else {
            continue;
        };
        let Ok(ip) = addr.parse::<IpAddr>() else {
            continue;
        };

        let socket = SocketAddr::new(ip, DNS_PORT);
        deduper.push_unique(&mut nameservers, socket);
    }

    if nameservers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no nameservers found in /etc/resolv.conf",
        ));
    }

    Ok(nameservers)
}

fn strip_comment(line: &str) -> &str {
    line.split_once(['#', ';'])
        .map(|(head, _)| head.trim())
        .unwrap_or_else(|| line.trim())
}

fn unspecified_addr(nameserver: SocketAddr) -> SocketAddr {
    match nameserver {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    }
}

fn host_not_found(host: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("host not found: {host}"))
}

fn byte_range_eof(err: BufferRangeError) -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, err)
}

/// Performs the bounded header/question prefilter used while draining UDP
/// responses for one query ID.
///
/// This validates enough structure to decide whether full parsing is useful;
/// it does not authenticate or fully validate the response packet.
pub(crate) fn response_is_decodable_candidate(packet: &[u8], query_id: u16) -> bool {
    if packet.len() < 12 {
        return false;
    }

    let Some(response_id) = read_u16_be_candidate(packet, 0) else {
        return false;
    };
    if response_id != query_id {
        return false;
    }

    let Some(flags) = read_u16_be_candidate(packet, 2) else {
        return false;
    };
    if flags & DNS_FLAG_QR == 0 {
        return false;
    }

    let Some(qdcount) = read_u16_be_candidate(packet, 4) else {
        return false;
    };
    match qdcount {
        0 => dns_rcode_allows_questionless_failover(dns_rcode(flags)),
        1 => {
            let Some((consumed, _)) = skip_dns_name(packet, 12, 0) else {
                return false;
            };
            let Some(question_end) = checked_add_candidate(12, consumed, packet.len()) else {
                return false;
            };
            checked_add_candidate(question_end, 4, packet.len()).is_some()
        }
        _ => false,
    }
}

/// Validated DNS header counts plus the optional echoed question.
struct DnsResponseEnvelope {
    /// Response flags containing QR, truncation, and response-code bits.
    flags: u16,
    /// Number of answer resource records declared by the header.
    ancount: usize,
    /// Number of authority resource records declared by the header.
    nscount: usize,
    /// Number of additional resource records declared by the header.
    arcount: usize,
    /// Echoed question, absent only for accepted questionless failure replies.
    question: Option<DnsResponseQuestion>,
}

/// Parsed form of the single echoed DNS question.
struct DnsResponseQuestion {
    /// Decoded question name.
    name: String,
    /// Requested record type from the echoed question.
    qtype: u16,
    /// Requested class from the echoed question.
    qclass: u16,
    /// First packet offset after the complete question section.
    end_offset: usize,
}

fn parse_response_envelope(packet: &[u8], query_id: u16) -> io::Result<DnsResponseEnvelope> {
    if packet.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "DNS response shorter than header",
        ));
    }

    let response_id = read_u16_be_at(packet, 0).map_err(byte_range_eof)?;
    if response_id != query_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response ID did not match query ID",
        ));
    }

    let flags = read_u16_be_at(packet, 2).map_err(byte_range_eof)?;
    if flags & DNS_FLAG_QR == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response packet was not marked as a response",
        ));
    }

    let qdcount = read_u16_be_at(packet, 4).map_err(byte_range_eof)? as usize;
    let ancount = read_u16_be_at(packet, 6).map_err(byte_range_eof)? as usize;
    let nscount = read_u16_be_at(packet, 8).map_err(byte_range_eof)? as usize;
    let arcount = read_u16_be_at(packet, 10).map_err(byte_range_eof)? as usize;

    let question = match qdcount {
        0 if dns_rcode_allows_questionless_failover(dns_rcode(flags)) => None,
        0 => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS response question count did not match query",
            ));
        }
        1 => {
            let mut offset = 12usize;
            let (name, consumed) = decode_name(packet, offset, 0)?;
            offset = checked_add(offset, consumed, packet.len())?;
            let qtype = read_u16_be_at(packet, offset).map_err(byte_range_eof)?;
            let qclass_offset = checked_add(offset, 2, packet.len())?;
            let qclass = read_u16_be_at(packet, qclass_offset).map_err(byte_range_eof)?;
            let end_offset = checked_add(offset, 4, packet.len())?;
            Some(DnsResponseQuestion {
                name,
                qtype,
                qclass,
                end_offset,
            })
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS response question count did not match query",
            ));
        }
    };

    Ok(DnsResponseEnvelope {
        flags,
        ancount,
        nscount,
        arcount,
        question,
    })
}

#[cfg(feature = "fuzzing")]
pub(crate) fn response_envelope_is_decodable(packet: &[u8], query_id: u16) -> bool {
    parse_response_envelope(packet, query_id).is_ok()
}

#[inline(always)]
fn dns_rcode(flags: u16) -> u8 {
    (flags & DNS_RCODE_MASK) as u8
}

#[inline(always)]
fn dns_rcode_allows_questionless_failover(rcode: u8) -> bool {
    matches!(
        rcode,
        DNS_RCODE_FORMERR | DNS_RCODE_SERVFAIL | DNS_RCODE_NOTIMP | DNS_RCODE_REFUSED
    )
}

fn extend_unique_socket_addrs(addrs: &mut Vec<SocketAddr>, ips: &[IpAddr], port: u16) {
    let mut deduper = SocketAddrDeduper::from_existing(addrs);
    for ip in ips {
        let addr = SocketAddr::new(*ip, port);
        deduper.push_unique(addrs, addr);
    }
}

/// Order-preserving helper that filters duplicate socket addresses.
struct SocketAddrDeduper {
    /// Addresses already present in the destination sequence.
    seen: HashSet<SocketAddr>,
}

impl SocketAddrDeduper {
    fn new() -> Self {
        Self {
            seen: HashSet::new(),
        }
    }

    fn from_existing(addrs: &[SocketAddr]) -> Self {
        Self {
            seen: addrs.iter().copied().collect(),
        }
    }

    fn push_unique(&mut self, addrs: &mut Vec<SocketAddr>, addr: SocketAddr) {
        if self.seen.insert(addr) {
            addrs.push(addr);
        }
    }
}

fn encode_query_packet(query_id: u16, host: &str, qtype: u16) -> io::Result<Vec<u8>> {
    validate_query_name(host)?;

    let mut packet = Vec::with_capacity(512);
    let mut header = [0u8; 12];
    {
        let mut cursor = BufferCursorMut::new(&mut header);
        cursor.put_u16_be(query_id).map_err(byte_range_eof)?;
        cursor.put_u16_be(0x0100).map_err(byte_range_eof)?;
        cursor.put_u16_be(1).map_err(byte_range_eof)?;
        cursor.put_u16_be(0).map_err(byte_range_eof)?;
        cursor.put_u16_be(0).map_err(byte_range_eof)?;
        cursor.put_u16_be(0).map_err(byte_range_eof)?;
    }
    packet.extend_from_slice(&header);

    for label in host.split('.') {
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }

    packet.push(0);
    let start = packet.len();
    packet.resize(start + 4, 0);
    write_u16_be_at(&mut packet, start, qtype).map_err(byte_range_eof)?;
    write_u16_be_at(&mut packet, start + 2, DNS_CLASS_IN).map_err(byte_range_eof)?;
    Ok(packet)
}

pub(crate) fn validate_query_name(host: &str) -> io::Result<()> {
    if host.len() > DNS_MAX_NAME_PRESENTATION_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "host name exceeded 253 presentation bytes",
        ));
    }

    // Include the terminal root octet before adding each label's length octet
    // and payload. This pass intentionally precedes query-packet allocation.
    let mut wire_len = 1usize;
    for label in host.split('.') {
        if label.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host name contained an empty DNS label",
            ));
        }
        if label.len() > 63 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host name contained a DNS label longer than 63 bytes",
            ));
        }

        wire_len = wire_len
            .checked_add(1 + label.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "host name overflowed"))?;
    }

    if wire_len > DNS_MAX_NAME_WIRE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "host name exceeded 255 encoded bytes",
        ));
    }

    Ok(())
}

pub(crate) fn parse_response_packet(
    packet: &[u8],
    query_id: u16,
    query_host: &str,
    qtype: u16,
) -> io::Result<LookupResult> {
    let envelope = parse_response_envelope(packet, query_id)?;
    let flags = envelope.flags;
    if flags & DNS_FLAG_TC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response was truncated; TCP fallback is not implemented",
        ));
    }

    if let Some(question) = envelope.question.as_ref() {
        if !dns_name_eq(&question.name, query_host) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS response question name did not match query",
            ));
        }
        if question.qtype != qtype || question.qclass != DNS_CLASS_IN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS response question type/class did not match query",
            ));
        }
    }

    let rcode = dns_rcode(flags);
    let records = parse_response_records(packet, &envelope, rcode == 0)?;
    if rcode == DNS_RCODE_NXDOMAIN {
        return Ok(LookupResult {
            addresses: Vec::new(),
            cname: None,
            cname_hops: 0,
            nx_domain: true,
        });
    }
    if rcode != 0 {
        return Err(io::Error::other(format!(
            "DNS server returned response code {rcode}"
        )));
    }

    if envelope.question.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response question count did not match query",
        ));
    }

    let mut addresses = Vec::new();
    let mut active_owner = query_host;
    let mut cname = None;
    let mut cname_hops = 0usize;
    let mut seen_owners = [""; MAX_CNAME_HOPS_PER_RESPONSE + 1];
    seen_owners[0] = query_host;
    let mut seen_owner_count = 1usize;
    loop {
        for record in &records {
            if let DnsRecord::Address {
                owner,
                rr_type,
                address,
            } = record
                && *rr_type == qtype
                && dns_name_eq(owner, active_owner)
            {
                addresses.push(*address);
            }
        }
        if !addresses.is_empty() {
            break;
        }

        let Some(target) = records.iter().find_map(|record| match record {
            DnsRecord::Cname { owner, target } if dns_name_eq(owner, active_owner) => Some(target),
            _ => None,
        }) else {
            break;
        };

        if seen_owners[..seen_owner_count]
            .iter()
            .any(|seen| dns_name_eq(seen, target))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS response CNAME chain contained a loop",
            ));
        }
        if cname_hops == MAX_CNAME_HOPS_PER_RESPONSE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS response CNAME chain exceeded maximum per-response hop count",
            ));
        }
        seen_owners[seen_owner_count] = target;
        seen_owner_count += 1;
        cname_hops += 1;
        active_owner = target;
        cname = Some(target.clone());
    }

    Ok(LookupResult {
        addresses,
        cname,
        cname_hops,
        nx_domain: false,
    })
}

/// Validates every declared resource record and retains only resolution data
/// from Internet-class Answer records.
///
/// This complete walk must finish before the caller interprets the response
/// code. Section and class affect whether a known record contributes data, but
/// never whether its owner, header, or known RDATA shape is validated.
fn parse_response_records(
    packet: &[u8],
    envelope: &DnsResponseEnvelope,
    retain_resolution_data: bool,
) -> io::Result<Vec<DnsRecord>> {
    let mut offset = envelope
        .question
        .as_ref()
        .map_or(12, |question| question.end_offset);
    let total_rrs = envelope.ancount + envelope.nscount + envelope.arcount;
    let max_rrs_by_packet = packet.len().saturating_sub(offset) / 11;
    // Bound the eager allocation by the packet's minimum possible RR density;
    // forged header counts cannot reserve independently of packet size.
    let mut records = if retain_resolution_data {
        Vec::with_capacity(total_rrs.min(max_rrs_by_packet))
    } else {
        Vec::new()
    };

    for (section, count) in [
        (DnsRecordSection::Answer, envelope.ancount),
        (DnsRecordSection::Authority, envelope.nscount),
        (DnsRecordSection::Additional, envelope.arcount),
    ] {
        for _ in 0..count {
            let (owner, consumed) =
                decode_or_validate_name(packet, offset, retain_resolution_data)?;
            offset = checked_add(offset, consumed, packet.len())?;
            let rr = parse_rr_header(packet, offset)?;
            offset = rr.data_offset + rr.rdlength as usize;
            let contributes = retain_resolution_data
                && section == DnsRecordSection::Answer
                && rr.class == DNS_CLASS_IN;

            match rr.rr_type {
                DNS_TYPE_A => {
                    if rr.rdlength != 4 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "DNS A RDATA length was not 4 bytes",
                        ));
                    }
                    if contributes {
                        let data = &packet[rr.data_offset..rr.data_offset + 4];
                        records.push(DnsRecord::Address {
                            owner,
                            rr_type: rr.rr_type,
                            address: IpAddr::V4(Ipv4Addr::new(data[0], data[1], data[2], data[3])),
                        });
                    }
                }
                DNS_TYPE_AAAA => {
                    if rr.rdlength != 16 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "DNS AAAA RDATA length was not 16 bytes",
                        ));
                    }
                    if contributes {
                        let mut octets = [0u8; 16];
                        octets.copy_from_slice(&packet[rr.data_offset..rr.data_offset + 16]);
                        records.push(DnsRecord::Address {
                            owner,
                            rr_type: rr.rr_type,
                            address: IpAddr::V6(Ipv6Addr::from(octets)),
                        });
                    }
                }
                DNS_TYPE_CNAME => {
                    let (target, consumed) =
                        decode_or_validate_name(packet, rr.data_offset, retain_resolution_data)?;
                    if consumed != rr.rdlength as usize {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "DNS CNAME RDATA did not consume its declared length",
                        ));
                    }
                    if contributes {
                        records.push(DnsRecord::Cname { owner, target });
                    }
                }
                _ => {}
            }
        }
    }

    Ok(records)
}

/// Decodes a response name when resolution data is needed, or validates it
/// without allocating when a negative response will discard all records.
fn decode_or_validate_name(
    packet: &[u8],
    offset: usize,
    materialize: bool,
) -> io::Result<(String, usize)> {
    if materialize {
        decode_name(packet, offset, 0)
    } else {
        let (consumed, _) =
            walk_dns_name(packet, offset, 0, None).map_err(DnsNameWalkError::into_io_error)?;
        Ok((String::new(), consumed))
    }
}

/// The fixed-size resource-record header fields that follow the already-decoded
/// owner name, as parsed by `parse_rr_header`.
struct RrHeader {
    /// Resource-record type.
    rr_type: u16,
    /// Resource-record class.
    class: u16,
    /// RDATA length in bytes.
    rdlength: u16,
    /// Offset of the RDATA payload inside the full DNS packet.
    data_offset: usize,
}

fn parse_rr_header(packet: &[u8], offset: usize) -> io::Result<RrHeader> {
    let end = checked_add(offset, 10, packet.len())?;
    let rr_type = read_u16_be_at(packet, offset).map_err(byte_range_eof)?;
    let class_offset = checked_add(offset, 2, packet.len())?;
    let rdlength_offset = checked_add(offset, 8, packet.len())?;
    let class = read_u16_be_at(packet, class_offset).map_err(byte_range_eof)?;
    let rdlength = read_u16_be_at(packet, rdlength_offset).map_err(byte_range_eof)?;
    let data_offset = end;
    checked_add(data_offset, rdlength as usize, packet.len())?;

    Ok(RrHeader {
        rr_type,
        class,
        rdlength,
        data_offset,
    })
}

/// Decodes one possibly compressed DNS name and returns its encoded byte count
/// at `offset`.
///
/// Compression recursion, packet bounds, backward pointers, and the maximum
/// presentation length are validated by [`walk_dns_name`]. Every label must be
/// valid UTF-8 before its exact text is added to the presentation string.
pub(crate) fn decode_name(
    packet: &[u8],
    offset: usize,
    depth: usize,
) -> io::Result<(String, usize)> {
    let mut labels = Vec::new();
    let (consumed, _) = walk_dns_name(packet, offset, depth, Some(&mut labels))
        .map_err(DnsNameWalkError::into_io_error)?;
    let name = labels.join(".");
    Ok((name, consumed))
}

/// Allocation-free failure status used by the shared DNS name walker.
///
/// The UDP candidate prefilter discards this status directly. Full decoding
/// converts it to the historical `io::Error` kind and message at its boundary.
#[derive(Clone, Copy)]
enum DnsNameWalkError {
    CompressionDepthExceeded,
    OffsetExceededPacket,
    NameExceededPacket,
    CompressionPointerTruncated,
    CompressionPointerNotBackward,
    NameLengthOverflow,
    UnsupportedLabelEncoding,
    InvalidUtf8Label,
    NameTooLong,
    PacketArithmeticOverflow,
    PacketEndedUnexpectedly,
}

impl DnsNameWalkError {
    fn into_io_error(self) -> io::Error {
        let (kind, message) = match self {
            Self::CompressionDepthExceeded => (
                io::ErrorKind::InvalidData,
                "DNS name compression exceeded maximum depth",
            ),
            Self::OffsetExceededPacket => (
                io::ErrorKind::UnexpectedEof,
                "DNS name offset exceeded packet length",
            ),
            Self::NameExceededPacket => (
                io::ErrorKind::UnexpectedEof,
                "DNS name exceeded packet length",
            ),
            Self::CompressionPointerTruncated => (
                io::ErrorKind::UnexpectedEof,
                "DNS compression pointer ended unexpectedly",
            ),
            Self::CompressionPointerNotBackward => (
                io::ErrorKind::InvalidData,
                "DNS compression pointer did not point backward",
            ),
            Self::NameLengthOverflow => (io::ErrorKind::InvalidData, "DNS name length overflowed"),
            Self::UnsupportedLabelEncoding => (
                io::ErrorKind::InvalidData,
                "DNS label used an unsupported length encoding",
            ),
            Self::InvalidUtf8Label => (io::ErrorKind::InvalidData, "DNS label was not valid UTF-8"),
            Self::NameTooLong => (
                io::ErrorKind::InvalidData,
                "DNS name exceeded maximum length",
            ),
            Self::PacketArithmeticOverflow => (
                io::ErrorKind::InvalidData,
                "DNS packet arithmetic overflowed",
            ),
            Self::PacketEndedUnexpectedly => (
                io::ErrorKind::UnexpectedEof,
                "DNS packet ended unexpectedly",
            ),
        };
        io::Error::new(kind, message)
    }
}

fn walk_dns_name(
    packet: &[u8],
    offset: usize,
    depth: usize,
    mut labels: Option<&mut Vec<String>>,
) -> Result<(usize, usize), DnsNameWalkError> {
    if depth > MAX_NAME_COMPRESSION_DEPTH {
        return Err(DnsNameWalkError::CompressionDepthExceeded);
    }
    if offset >= packet.len() {
        return Err(DnsNameWalkError::OffsetExceededPacket);
    }

    let mut pos = offset;
    let mut consumed = 0usize;
    let mut presentation_len = 0usize;

    loop {
        let len = *packet
            .get(pos)
            .ok_or(DnsNameWalkError::NameExceededPacket)?;
        if len & 0xC0 == 0xC0 {
            let next = checked_add_name_offset(pos, 1, packet.len())?;
            let next_byte = *packet
                .get(next)
                .ok_or(DnsNameWalkError::CompressionPointerTruncated)?;
            let pointer = (((len & 0x3F) as usize) << 8) | next_byte as usize;
            if pointer >= pos {
                return Err(DnsNameWalkError::CompressionPointerNotBackward);
            }
            consumed += 2;
            let child_labels = labels.as_deref_mut();
            let (_, suffix_len) = walk_dns_name(packet, pointer, depth + 1, child_labels)?;
            if suffix_len != 0 {
                if presentation_len != 0 {
                    presentation_len = presentation_len
                        .checked_add(1)
                        .ok_or(DnsNameWalkError::NameLengthOverflow)?;
                }
                presentation_len = presentation_len
                    .checked_add(suffix_len)
                    .ok_or(DnsNameWalkError::NameLengthOverflow)?;
            }
            break;
        }

        if len == 0 {
            consumed += 1;
            break;
        }

        if len & 0xC0 != 0 {
            return Err(DnsNameWalkError::UnsupportedLabelEncoding);
        }

        let label_len = len as usize;
        let label_start = checked_add_name_offset(pos, 1, packet.len())?;
        let label_end = checked_add_name_offset(label_start, label_len, packet.len())?;
        if presentation_len != 0 {
            presentation_len = presentation_len
                .checked_add(1)
                .ok_or(DnsNameWalkError::NameLengthOverflow)?;
        }
        presentation_len = presentation_len
            .checked_add(label_len)
            .ok_or(DnsNameWalkError::NameLengthOverflow)?;
        let label = std::str::from_utf8(&packet[label_start..label_end])
            .map_err(|_| DnsNameWalkError::InvalidUtf8Label)?;
        if let Some(labels) = labels.as_mut() {
            labels.push(label.to_owned());
        }
        consumed += 1 + label_len;
        pos = label_end;
    }

    // DNS name limits are defined on raw label octets plus separators, not
    // Unicode scalar or character counts.
    if presentation_len > DNS_MAX_NAME_PRESENTATION_LEN {
        return Err(DnsNameWalkError::NameTooLong);
    }

    Ok((consumed, presentation_len))
}

fn skip_dns_name(packet: &[u8], offset: usize, depth: usize) -> Option<(usize, usize)> {
    walk_dns_name(packet, offset, depth, None).ok()
}

fn checked_add_name_offset(
    base: usize,
    add: usize,
    limit: usize,
) -> Result<usize, DnsNameWalkError> {
    let value = base
        .checked_add(add)
        .ok_or(DnsNameWalkError::PacketArithmeticOverflow)?;
    if value > limit {
        return Err(DnsNameWalkError::PacketEndedUnexpectedly);
    }
    Ok(value)
}

fn dns_name_eq(left: &str, right: &str) -> bool {
    left.trim_end_matches('.')
        .eq_ignore_ascii_case(right.trim_end_matches('.'))
}

fn checked_add(base: usize, add: usize, limit: usize) -> io::Result<usize> {
    let value = base.checked_add(add).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS packet arithmetic overflowed",
        )
    })?;
    if value > limit {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "DNS packet ended unexpectedly",
        ));
    }
    Ok(value)
}

#[inline(always)]
fn checked_add_candidate(base: usize, add: usize, limit: usize) -> Option<usize> {
    let value = base.checked_add(add)?;
    (value <= limit).then_some(value)
}

#[inline(always)]
fn read_u16_be_candidate(packet: &[u8], offset: usize) -> Option<u16> {
    let end = offset.checked_add(2)?;
    let bytes = packet.get(offset..end)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

#[cfg(feature = "test-support")]
pub(crate) mod test_support {
    /// Repository-only seam for the DNS candidate allocation fixture.
    pub fn response_is_decodable_candidate(packet: &[u8], query_id: u16) -> bool {
        super::response_is_decodable_candidate(packet, query_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_query_id_mixer_is_not_sequential_counter() {
        let seed = 0x1234_5678_9ABC_DEF0;
        let mut state = 0u64;
        let mut ids = [0u16; 4];
        for id in &mut ids {
            state = next_fallback_query_state(state, seed);
            *id = query_id_from_state(state);
        }

        assert_ne!(ids, [1, 2, 3, 4]);
        assert!(
            ids.windows(2)
                .any(|pair| pair[1] != pair[0].wrapping_add(1)),
            "fallback query IDs should not retain sequential AtomicU16 behavior"
        );
    }

    #[test]
    fn encoded_query_packet_carries_selected_query_id() {
        let packet =
            encode_query_packet(0xBEEF, "db.example.test", DNS_TYPE_A).expect("query encode");
        assert_eq!(
            read_u16_be_at(&packet, 0).expect("encoded query ID should fit"),
            0xBEEF
        );
    }

    #[test]
    fn query_name_limits_accept_253_presentation_and_255_wire_bytes() {
        let host = query_name_with_final_label(61);
        assert_eq!(host.len(), DNS_MAX_NAME_PRESENTATION_LEN);

        let packet = encode_query_packet(0xBEEF, &host, DNS_TYPE_A)
            .expect("maximum-length query name should encode");
        assert_eq!(packet.len(), 12 + DNS_MAX_NAME_WIRE_LEN + 4);
    }

    #[test]
    fn query_name_limits_reject_254_presentation_and_256_wire_bytes() {
        let host = query_name_with_final_label(62);
        assert_eq!(host.len(), DNS_MAX_NAME_PRESENTATION_LEN + 1);

        let err = encode_query_packet(0xBEEF, &host, DNS_TYPE_A)
            .expect_err("overlong query name should fail before encoding");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn query_name_limits_retain_the_63_byte_label_boundary() {
        let valid = "a".repeat(63);
        encode_query_packet(0xBEEF, &valid, DNS_TYPE_A).expect("63-byte DNS label should encode");

        let invalid = "a".repeat(64);
        let err = encode_query_packet(0xBEEF, &invalid, DNS_TYPE_A)
            .expect_err("64-byte DNS label should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        for host in ["", ".example.test", "example..test", "example.test."] {
            let err = encode_query_packet(0xBEEF, host, DNS_TYPE_A)
                .expect_err("empty DNS label should fail at the encoder boundary");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn query_name_limits_apply_after_trailing_dot_normalization() {
        assert_eq!(
            normalize_host("db.example.test.").expect("trailing dot should normalize"),
            "db.example.test"
        );
        assert_eq!(
            normalize_host(".")
                .expect_err("root-only input should remain unsupported")
                .kind(),
            io::ErrorKind::InvalidInput
        );

        let max_host = query_name_with_final_label(61);
        let dotted = format!("{max_host}.");
        let normalized =
            normalize_host(&dotted).expect("maximum name plus root dot should normalize");
        encode_query_packet(0xBEEF, normalized, DNS_TYPE_A)
            .expect("normalized maximum-length name should encode");

        let overlong = query_name_with_final_label(62);
        let overlong_dotted = format!("{overlong}.");
        let err = normalize_host(&overlong_dotted)
            .expect_err("overlong name should remain invalid after root-dot normalization");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn decoded_root_name_remains_structurally_valid() {
        let (name, consumed) = decode_name(&[0], 0, 0).expect("wire root should decode");
        assert!(name.is_empty());
        assert_eq!(consumed, 1);
    }

    #[test]
    fn compression_depth_bound_accepts_eight_pointers_and_rejects_nine() {
        let (accepted_packet, accepted_offset) =
            compression_pointer_chain(MAX_NAME_COMPRESSION_DEPTH);
        let (name, consumed) = decode_name(&accepted_packet, accepted_offset, 0)
            .expect("compression chain at the depth limit should decode");
        assert!(name.is_empty());
        assert_eq!(consumed, 2);

        let (rejected_packet, rejected_offset) =
            compression_pointer_chain(MAX_NAME_COMPRESSION_DEPTH + 1);
        let err = decode_name(&rejected_packet, rejected_offset, 0)
            .expect_err("compression chain above the depth limit should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            err.to_string(),
            "DNS name compression exceeded maximum depth"
        );
    }

    #[test]
    fn compressed_name_expansion_enforces_253_byte_boundary() {
        let suffix = [
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(59),
        ]
        .join(".");
        assert_eq!(suffix.len(), 251);

        let mut packet = Vec::new();
        push_test_wire_name(&mut packet, &suffix);
        let valid_offset = packet.len();
        packet.extend_from_slice(&[1, b'x', 0xC0, 0x00]);
        let (name, consumed) =
            decode_name(&packet, valid_offset, 0).expect("253-byte compressed name should decode");
        assert_eq!(name.len(), DNS_MAX_NAME_PRESENTATION_LEN);
        assert_eq!(consumed, 4);

        let invalid_offset = packet.len();
        packet.extend_from_slice(&[2, b'x', b'x', 0xC0, 0x00]);
        let err = decode_name(&packet, invalid_offset, 0)
            .expect_err("254-byte compressed name should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn candidate_prefilter_and_envelope_parser_share_structural_acceptance() {
        const QUERY_ID: u16 = 0x1234;

        let mut cases = vec![
            (
                "valid uncompressed question",
                response_with_question_name(QUERY_ID, b"\x07example\x03com\0"),
                true,
            ),
            (
                "valid UTF-8 question",
                response_with_question_name(QUERY_ID, b"\x03\xef\xbf\xbd\0"),
                true,
            ),
            (
                "invalid UTF-8 question",
                response_with_question_name(QUERY_ID, b"\x01\xff\0"),
                false,
            ),
            (
                "valid compressed root question",
                response_with_question_name(QUERY_ID, b"\xc0\x04"),
                true,
            ),
            (
                "questionless SERVFAIL",
                response_header(QUERY_ID, DNS_FLAG_QR | DNS_RCODE_SERVFAIL as u16, 0),
                true,
            ),
            (
                "questionless NXDOMAIN",
                response_header(QUERY_ID, DNS_FLAG_QR | DNS_RCODE_NXDOMAIN as u16, 0),
                false,
            ),
            (
                "questionless NOERROR",
                response_header(QUERY_ID, DNS_FLAG_QR, 0),
                false,
            ),
            ("short header", vec![0x12, 0x34, 0x81], false),
            (
                "wrong query ID",
                response_with_question_name(QUERY_ID.wrapping_add(1), b"\0"),
                false,
            ),
            (
                "query rather than response",
                response_header(QUERY_ID, 0, 1),
                false,
            ),
            (
                "two questions",
                response_header(QUERY_ID, DNS_FLAG_QR, 2),
                false,
            ),
        ];

        let mut truncated_label = response_header(QUERY_ID, DNS_FLAG_QR, 1);
        truncated_label.extend_from_slice(&[3, b'x']);
        cases.push(("truncated label", truncated_label, false));

        let mut unsupported_label = response_header(QUERY_ID, DNS_FLAG_QR, 1);
        unsupported_label.push(0x40);
        cases.push(("unsupported label encoding", unsupported_label, false));

        cases.push((
            "forward compression pointer",
            response_with_question_name(QUERY_ID, b"\xc0\x0c"),
            false,
        ));
        cases.push((
            "backward compression pointer loop",
            response_with_question_name(QUERY_ID, b"\x01x\xc0\x0c"),
            false,
        ));

        let mut overlong_name = Vec::new();
        for label_len in [63usize, 63, 63, 62] {
            overlong_name.push(label_len as u8);
            overlong_name.extend(std::iter::repeat_n(b'x', label_len));
        }
        overlong_name.push(0);
        cases.push((
            "overlong expanded name",
            response_with_question_name(QUERY_ID, &overlong_name),
            false,
        ));

        for (case, packet, expected) in cases {
            assert_eq!(
                response_is_decodable_candidate(&packet, QUERY_ID),
                expected,
                "candidate result for {case}"
            );
            assert_eq!(
                parse_response_envelope(&packet, QUERY_ID).is_ok(),
                expected,
                "envelope result for {case}"
            );
        }
    }

    #[test]
    fn declared_rr_validation_precedes_rcode_section_and_class_filters() {
        #[derive(Clone, Copy)]
        struct MalformedShape {
            label: &'static str,
            rr_type: u16,
            rdata: &'static [u8],
            error_kind: io::ErrorKind,
            error_message: &'static str,
        }

        let malformed_shapes = [
            MalformedShape {
                label: "short A",
                rr_type: DNS_TYPE_A,
                rdata: &[192, 0, 2],
                error_kind: io::ErrorKind::InvalidData,
                error_message: "DNS A RDATA length was not 4 bytes",
            },
            MalformedShape {
                label: "long A",
                rr_type: DNS_TYPE_A,
                rdata: &[192, 0, 2, 1, 0],
                error_kind: io::ErrorKind::InvalidData,
                error_message: "DNS A RDATA length was not 4 bytes",
            },
            MalformedShape {
                label: "short AAAA",
                rr_type: DNS_TYPE_AAAA,
                rdata: &[0; 15],
                error_kind: io::ErrorKind::InvalidData,
                error_message: "DNS AAAA RDATA length was not 16 bytes",
            },
            MalformedShape {
                label: "long AAAA",
                rr_type: DNS_TYPE_AAAA,
                rdata: &[0; 17],
                error_kind: io::ErrorKind::InvalidData,
                error_message: "DNS AAAA RDATA length was not 16 bytes",
            },
            MalformedShape {
                label: "CNAME with trailing RDATA",
                rr_type: DNS_TYPE_CNAME,
                rdata: &[0, 0xA5],
                error_kind: io::ErrorKind::InvalidData,
                error_message: "DNS CNAME RDATA did not consume its declared length",
            },
        ];

        for (rcode_label, rcode) in [("NOERROR", 0), ("NXDOMAIN", DNS_RCODE_NXDOMAIN)] {
            for (section_label, section) in [
                ("Answer", DnsRecordSection::Answer),
                ("Authority", DnsRecordSection::Authority),
                ("Additional", DnsRecordSection::Additional),
            ] {
                for (class_label, class) in [("IN", DNS_CLASS_IN), ("non-IN", 3)] {
                    for shape in malformed_shapes {
                        let context = format!(
                            "{rcode_label} {section_label} {class_label} {}",
                            shape.label
                        );
                        let packet = response_with_single_test_rr(
                            section,
                            rcode,
                            true,
                            DNS_TYPE_A,
                            shape.rr_type,
                            class,
                            shape.rdata,
                        );
                        let err = match parse_response_packet(
                            &packet,
                            0x1234,
                            "db.example.test",
                            DNS_TYPE_A,
                        ) {
                            Ok(_) => panic!("{context} unexpectedly passed validation"),
                            Err(err) => err,
                        };
                        assert_eq!(err.kind(), shape.error_kind, "{context}");
                        assert_eq!(err.to_string(), shape.error_message, "{context}");
                    }
                }
            }
        }

        let mut truncated = response_with_single_test_rr(
            DnsRecordSection::Authority,
            DNS_RCODE_NXDOMAIN,
            true,
            DNS_TYPE_A,
            DNS_TYPE_A,
            DNS_CLASS_IN,
            &[192, 0, 2, 1],
        );
        truncated.pop();
        let err = match parse_response_packet(&truncated, 0x1234, "db.example.test", DNS_TYPE_A) {
            Ok(_) => panic!("truncated Authority RDATA unexpectedly produced NXDOMAIN"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(err.to_string(), "DNS packet ended unexpectedly");

        let questionless = response_with_single_test_rr(
            DnsRecordSection::Answer,
            DNS_RCODE_SERVFAIL,
            false,
            DNS_TYPE_A,
            DNS_TYPE_A,
            DNS_CLASS_IN,
            &[192, 0, 2],
        );
        let err = match parse_response_packet(&questionless, 0x1234, "db.example.test", DNS_TYPE_A)
        {
            Ok(_) => panic!("questionless SERVFAIL skipped malformed Answer validation"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "DNS A RDATA length was not 4 bytes");
    }

    #[test]
    fn declared_rr_validation_preserves_valid_rcode_and_filtering() {
        #[derive(Clone, Copy)]
        struct ValidShape {
            label: &'static str,
            rr_type: u16,
            query_type: u16,
            rdata: &'static [u8],
        }

        let valid_shapes = [
            ValidShape {
                label: "A",
                rr_type: DNS_TYPE_A,
                query_type: DNS_TYPE_A,
                rdata: &[192, 0, 2, 1],
            },
            ValidShape {
                label: "AAAA",
                rr_type: DNS_TYPE_AAAA,
                query_type: DNS_TYPE_AAAA,
                rdata: &[0; 16],
            },
            ValidShape {
                label: "CNAME",
                rr_type: DNS_TYPE_CNAME,
                query_type: DNS_TYPE_A,
                rdata: &[0],
            },
        ];

        for (rcode_label, rcode) in [("NOERROR", 0), ("NXDOMAIN", DNS_RCODE_NXDOMAIN)] {
            for (section_label, section) in [
                ("Answer", DnsRecordSection::Answer),
                ("Authority", DnsRecordSection::Authority),
                ("Additional", DnsRecordSection::Additional),
            ] {
                for (class_label, class) in [("IN", DNS_CLASS_IN), ("non-IN", 3)] {
                    for shape in valid_shapes {
                        let context = format!(
                            "{rcode_label} {section_label} {class_label} {}",
                            shape.label
                        );
                        let packet = response_with_single_test_rr(
                            section,
                            rcode,
                            true,
                            shape.query_type,
                            shape.rr_type,
                            class,
                            shape.rdata,
                        );
                        let result = parse_response_packet(
                            &packet,
                            0x1234,
                            "db.example.test",
                            shape.query_type,
                        )
                        .unwrap_or_else(|err| panic!("{context} failed validation: {err}"));
                        assert_eq!(result.nx_domain, rcode == DNS_RCODE_NXDOMAIN, "{context}");

                        let contributes = rcode == 0
                            && section == DnsRecordSection::Answer
                            && class == DNS_CLASS_IN;
                        match shape.rr_type {
                            DNS_TYPE_A | DNS_TYPE_AAAA => {
                                assert_eq!(
                                    result.addresses.len(),
                                    usize::from(contributes),
                                    "{context}"
                                );
                                assert!(result.cname.is_none(), "{context}");
                            }
                            DNS_TYPE_CNAME => {
                                assert!(result.addresses.is_empty(), "{context}");
                                assert_eq!(
                                    result.cname.as_deref(),
                                    contributes.then_some(""),
                                    "{context}"
                                );
                            }
                            _ => unreachable!("valid test shape used an unknown type"),
                        }
                    }
                }
            }
        }

        let servfail = response_with_single_test_rr(
            DnsRecordSection::Additional,
            DNS_RCODE_SERVFAIL,
            false,
            DNS_TYPE_A,
            DNS_TYPE_AAAA,
            3,
            &[0; 16],
        );
        let err = match parse_response_packet(&servfail, 0x1234, "db.example.test", DNS_TYPE_A) {
            Ok(_) => panic!("valid questionless SERVFAIL unexpectedly succeeded"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(err.to_string(), "DNS server returned response code 2");

        let non_in_answer = response_with_single_test_rr(
            DnsRecordSection::Answer,
            0,
            true,
            DNS_TYPE_A,
            DNS_TYPE_A,
            3,
            &[192, 0, 2, 1],
        );
        let result = parse_response_packet(&non_in_answer, 0x1234, "db.example.test", DNS_TYPE_A)
            .expect("valid non-IN Answer should be ignored after validation");
        assert!(!result.nx_domain);
        assert!(result.addresses.is_empty());

        let opaque_additional = response_with_single_test_rr(
            DnsRecordSection::Additional,
            0,
            true,
            DNS_TYPE_A,
            65_000,
            DNS_CLASS_IN,
            &[1, 2, 3],
        );
        let result =
            parse_response_packet(&opaque_additional, 0x1234, "db.example.test", DNS_TYPE_A)
                .expect("bounded unknown RDATA should remain opaque");
        assert!(result.addresses.is_empty());

        let mut malformed_after_answer = response_with_single_test_rr(
            DnsRecordSection::Answer,
            0,
            true,
            DNS_TYPE_A,
            DNS_TYPE_A,
            DNS_CLASS_IN,
            &[192, 0, 2, 1],
        );
        write_u16_be_at(&mut malformed_after_answer, 8, 1)
            .expect("test Authority count should fit");
        push_single_test_rr(
            &mut malformed_after_answer,
            true,
            DNS_TYPE_A,
            DNS_CLASS_IN,
            3,
            &[192, 0, 2],
        );
        let err = match parse_response_packet(
            &malformed_after_answer,
            0x1234,
            "db.example.test",
            DNS_TYPE_A,
        ) {
            Ok(_) => panic!("usable Answer hid a malformed later record"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "DNS A RDATA length was not 4 bytes");
    }

    #[test]
    fn materializing_and_non_materializing_name_walks_share_acceptance() {
        let mut overlong_name = Vec::new();
        for label_len in [63usize, 63, 63, 62] {
            overlong_name.push(label_len as u8);
            overlong_name.extend(std::iter::repeat_n(b'x', label_len));
        }
        overlong_name.push(0);

        let cases = [
            ("root", vec![0], 0usize, true),
            ("plain", b"\x03www\x07example\x03com\0".to_vec(), 0, true),
            (
                "valid UTF-8",
                b"\x03\xef\xbf\xbd\x07example\0".to_vec(),
                0,
                true,
            ),
            ("invalid UTF-8", vec![1, 0xff, 0], 0, false),
            ("compressed root", vec![0, 0xc0, 0], 1, true),
            (
                "compressed invalid UTF-8 suffix",
                vec![1, 0xff, 0, 0xc0, 0],
                3,
                false,
            ),
            ("truncated pointer", vec![0xc0], 0, false),
            ("forward pointer", vec![0xc0, 0], 0, false),
            ("backward pointer loop", vec![1, b'x', 0xc0, 0], 0, false),
            ("unsupported label", vec![0x40], 0, false),
            ("overlong name", overlong_name, 0, false),
        ];

        for (case, packet, offset, expected) in cases {
            let materialized = decode_name(&packet, offset, 0);
            let skipped = walk_dns_name(&packet, offset, 0, None);
            assert_eq!(materialized.is_ok(), expected, "decoder result for {case}");
            assert_eq!(skipped.is_ok(), expected, "skip result for {case}");
            if let (Ok((_, decoded_consumed)), Ok((skipped_consumed, _))) = (materialized, skipped)
            {
                assert_eq!(
                    decoded_consumed, skipped_consumed,
                    "consumed bytes for {case}"
                );
            }
        }
    }

    #[test]
    fn dns_name_walk_error_conversion_preserves_parser_contract() {
        let cases = [
            (
                DnsNameWalkError::CompressionDepthExceeded,
                io::ErrorKind::InvalidData,
                "DNS name compression exceeded maximum depth",
            ),
            (
                DnsNameWalkError::OffsetExceededPacket,
                io::ErrorKind::UnexpectedEof,
                "DNS name offset exceeded packet length",
            ),
            (
                DnsNameWalkError::NameExceededPacket,
                io::ErrorKind::UnexpectedEof,
                "DNS name exceeded packet length",
            ),
            (
                DnsNameWalkError::CompressionPointerTruncated,
                io::ErrorKind::UnexpectedEof,
                "DNS compression pointer ended unexpectedly",
            ),
            (
                DnsNameWalkError::CompressionPointerNotBackward,
                io::ErrorKind::InvalidData,
                "DNS compression pointer did not point backward",
            ),
            (
                DnsNameWalkError::NameLengthOverflow,
                io::ErrorKind::InvalidData,
                "DNS name length overflowed",
            ),
            (
                DnsNameWalkError::UnsupportedLabelEncoding,
                io::ErrorKind::InvalidData,
                "DNS label used an unsupported length encoding",
            ),
            (
                DnsNameWalkError::InvalidUtf8Label,
                io::ErrorKind::InvalidData,
                "DNS label was not valid UTF-8",
            ),
            (
                DnsNameWalkError::NameTooLong,
                io::ErrorKind::InvalidData,
                "DNS name exceeded maximum length",
            ),
            (
                DnsNameWalkError::PacketArithmeticOverflow,
                io::ErrorKind::InvalidData,
                "DNS packet arithmetic overflowed",
            ),
            (
                DnsNameWalkError::PacketEndedUnexpectedly,
                io::ErrorKind::UnexpectedEof,
                "DNS packet ended unexpectedly",
            ),
        ];

        for (walk_error, kind, message) in cases {
            let error = walk_error.into_io_error();
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn invalid_utf8_question_cannot_alias_replacement_character() {
        let valid = response_with_question_name(0x1234, b"\x04X\xef\xbf\xbd\x07EXAMPLE\x04TEST\0");
        parse_response_packet(&valid, 0x1234, "x\u{fffd}.example.test", DNS_TYPE_A)
            .expect("valid UTF-8 should retain ASCII-only case-insensitive matching");

        let invalid = response_with_question_name(0x1234, b"\x02X\xff\x07EXAMPLE\x04TEST\0");
        let err =
            match parse_response_packet(&invalid, 0x1234, "x\u{fffd}.example.test", DNS_TYPE_A) {
                Ok(_) => panic!("an invalid octet aliased the replacement character"),
                Err(err) => err,
            };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "DNS label was not valid UTF-8");
    }

    #[test]
    fn invalid_utf8_cname_target_is_rejected_before_chain_selection_or_rcode() {
        for rcode in [0, DNS_RCODE_NXDOMAIN] {
            let packet = response_with_single_test_rr(
                DnsRecordSection::Answer,
                rcode,
                true,
                DNS_TYPE_A,
                DNS_TYPE_CNAME,
                DNS_CLASS_IN,
                &[1, 0xff, 0],
            );
            let err = match parse_response_packet(&packet, 0x1234, "db.example.test", DNS_TYPE_A) {
                Ok(_) => panic!("an invalid CNAME target entered chain selection or RCODE"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "DNS label was not valid UTF-8");
        }
    }

    fn response_header(query_id: u16, flags: u16, qdcount: u16) -> Vec<u8> {
        let mut packet = Vec::with_capacity(12);
        for field in [query_id, flags, qdcount, 0, 0, 0] {
            packet.extend_from_slice(&field.to_be_bytes());
        }
        packet
    }

    fn response_with_question_name(query_id: u16, name: &[u8]) -> Vec<u8> {
        let mut packet = response_header(query_id, DNS_FLAG_QR, 1);
        packet.extend_from_slice(name);
        packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
        packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
        packet
    }

    fn response_with_single_test_rr(
        section: DnsRecordSection,
        rcode: u8,
        include_question: bool,
        query_type: u16,
        rr_type: u16,
        class: u16,
        rdata: &[u8],
    ) -> Vec<u8> {
        let mut packet = response_header(
            0x1234,
            DNS_FLAG_QR | u16::from(rcode),
            u16::from(include_question),
        );
        let count_offset = match section {
            DnsRecordSection::Answer => 6,
            DnsRecordSection::Authority => 8,
            DnsRecordSection::Additional => 10,
        };
        write_u16_be_at(&mut packet, count_offset, 1)
            .expect("test section count should fit in the DNS header");

        if include_question {
            push_test_wire_name(&mut packet, "db.example.test");
            packet.extend_from_slice(&query_type.to_be_bytes());
            packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
        }
        push_single_test_rr(
            &mut packet,
            include_question,
            rr_type,
            class,
            u16::try_from(rdata.len()).expect("test RDATA length should fit in u16"),
            rdata,
        );
        packet
    }

    fn push_single_test_rr(
        packet: &mut Vec<u8>,
        owner_is_question: bool,
        rr_type: u16,
        class: u16,
        rdlength: u16,
        rdata: &[u8],
    ) {
        if owner_is_question {
            packet.extend_from_slice(&0xC00Cu16.to_be_bytes());
        } else {
            packet.push(0);
        }
        packet.extend_from_slice(&rr_type.to_be_bytes());
        packet.extend_from_slice(&class.to_be_bytes());
        packet.extend_from_slice(&0u32.to_be_bytes());
        packet.extend_from_slice(&rdlength.to_be_bytes());
        packet.extend_from_slice(rdata);
    }

    fn query_name_with_final_label(final_label_len: usize) -> String {
        [
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(final_label_len),
        ]
        .join(".")
    }

    fn push_test_wire_name(packet: &mut Vec<u8>, name: &str) {
        for label in name.split('.') {
            packet.push(label.len() as u8);
            packet.extend_from_slice(label.as_bytes());
        }
        packet.push(0);
    }

    fn compression_pointer_chain(pointer_count: usize) -> (Vec<u8>, usize) {
        let mut packet = vec![0];
        let mut target = 0usize;
        for _ in 0..pointer_count {
            let pointer_offset = packet.len();
            assert!(target < 0x4000, "test compression pointer should fit");
            packet.extend_from_slice(&(0xC000 | target as u16).to_be_bytes());
            target = pointer_offset;
        }
        (packet, target)
    }

    #[test]
    fn socket_addr_dedup_preserves_first_seen_order() {
        let port = 5432;
        let mut addrs = vec![
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), port)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 10), port)),
        ];
        let ips = [
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 11)),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 10)),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 11)),
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 11)),
        ];

        extend_unique_socket_addrs(&mut addrs, &ips, port);

        assert_eq!(
            addrs,
            vec![
                SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), port)),
                SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 10), port)),
                SocketAddr::from((Ipv4Addr::new(192, 0, 2, 11), port)),
                SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 11), port)),
            ]
        );
    }

    #[test]
    fn dns_family_outcomes_use_a_first_recoverable_error() {
        let mut addrs = Vec::new();
        let result = finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "A lookup failed",
            )),
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "AAAA lookup timed out",
            )),
        );

        let err = match result {
            Err(err) => err,
            Ok(_) => panic!("two recoverable family errors should not resolve"),
        };
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn dns_family_outcomes_prefer_cname_then_nxdomain() {
        let cname_result = LookupResult {
            addresses: Vec::new(),
            cname: Some("db.internal.test".to_owned()),
            cname_hops: 3,
            nx_domain: false,
        };
        let nx_domain_result = LookupResult {
            addresses: Vec::new(),
            cname: None,
            cname_hops: 0,
            nx_domain: true,
        };
        let mut addrs = Vec::new();
        let step = finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Ok(cname_result),
            Ok(nx_domain_result),
        )
        .expect("usable CNAME should precede contradictory sibling NXDOMAIN");

        match step {
            ResolveHostStep::FollowCname { next, cname_hops } => {
                assert_eq!(next, "db.internal.test");
                assert_eq!(cname_hops, 3);
            }
            ResolveHostStep::Resolved => panic!("CNAME-only outcome should continue lookup"),
        }
    }

    #[test]
    fn dns_family_outcomes_use_a_first_conflicting_cname() {
        let mut addrs = Vec::new();
        let step = finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Ok(LookupResult {
                addresses: Vec::new(),
                cname: Some("db-v4.internal.test".to_owned()),
                cname_hops: 2,
                nx_domain: false,
            }),
            Ok(LookupResult {
                addresses: Vec::new(),
                cname: Some("db-v6.internal.test".to_owned()),
                cname_hops: 4,
                nx_domain: false,
            }),
        )
        .expect("A CNAME should win a conflicting sibling CNAME");

        match step {
            ResolveHostStep::FollowCname { next, cname_hops } => {
                assert_eq!(next, "db-v4.internal.test");
                assert_eq!(cname_hops, 2);
            }
            ResolveHostStep::Resolved => panic!("CNAME-only outcome should continue lookup"),
        }
    }

    #[test]
    fn dns_family_outcomes_prefer_nxdomain_to_recoverable_error() {
        let nx_domain_result = LookupResult {
            addresses: Vec::new(),
            cname: None,
            cname_hops: 0,
            nx_domain: true,
        };
        let mut addrs = Vec::new();
        let result = finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Err(io::Error::other("A SERVFAIL")),
            Ok(nx_domain_result),
        );

        let err = match result {
            Err(err) => err,
            Ok(_) => panic!("NXDOMAIN without a usable sibling should not resolve"),
        };
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn dns_family_outcomes_map_two_empty_answers_to_not_found() {
        let empty_a = LookupResult {
            addresses: Vec::new(),
            cname: None,
            cname_hops: 0,
            nx_domain: false,
        };
        let empty_aaaa = LookupResult {
            addresses: Vec::new(),
            cname: None,
            cname_hops: 0,
            nx_domain: false,
        };
        let mut addrs = Vec::new();
        let result = finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Ok(empty_a),
            Ok(empty_aaaa),
        );

        let err = match result {
            Err(err) => err,
            Ok(_) => panic!("two empty family answers should not resolve"),
        };
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn dns_family_outcomes_keep_terminal_error_ahead_of_cname() {
        let cname_result = LookupResult {
            addresses: Vec::new(),
            cname: Some("db.internal.test".to_owned()),
            cname_hops: 1,
            nx_domain: false,
        };
        let mut addrs = Vec::new();
        let result = finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Ok(cname_result),
            Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "timer allocation failed",
            )),
        );

        let err = match result {
            Err(err) => err,
            Ok(_) => panic!("terminal family error should stop CNAME continuation"),
        };
        assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
    }

    #[test]
    fn dns_family_outcomes_keep_address_ahead_of_terminal_error() {
        let address = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 67));
        let mut addrs = Vec::new();
        let step = finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Ok(LookupResult {
                addresses: vec![address],
                cname: None,
                cname_hops: 0,
                nx_domain: false,
            }),
            Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "timer allocation failed",
            )),
        )
        .expect("usable address should survive a later terminal family error");

        assert!(matches!(step, ResolveHostStep::Resolved));
        assert_eq!(addrs, vec![SocketAddr::new(address, 5432)]);
    }
}
