//! Hostname resolution helpers for FlowIO transports.
//!
//! The resolver keeps the public surface deliberately small:
//! - IP literals resolve directly without DNS traffic
//! - `localhost` and `/etc/hosts` entries are honored first with
//!   case-insensitive, single-root-dot-equivalent name matching
//! - all other names are resolved through UDP DNS queries using FlowIO's own
//!   transport and timer APIs
//!
//! DNS lookup is intentionally narrow:
//! - system configuration is read from `/etc/resolv.conf`
//! - only A and AAAA lookups are issued
//! - one linear CNAME chain is followed with independent per-response,
//!   cross-response, and name-compression bounds
//! - upstream asynchronous work has one five-second aggregate deadline by
//!   default, while each matching-response wait retains its independent cap
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
//!   rebuilding that setup state, but each non-local resolution still creates
//!   one owned query buffer, plus a UDP socket per nameserver attempt and a
//!   response buffer reused across completed A, AAAA, and CNAME-follow-up
//!   attempts. A timed-out receive may remain kernel-visible and requires a
//!   replacement for a later attempt.
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
use crate::runtime::timer::{Timeout, TimeoutError, timeout, timeout_at};
use std::fs;
use std::io::{self, BufRead, BufReader, Read};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DNS_PORT: u16 = 53;
const DNS_CLASS_IN: u16 = 1;
const DNS_TYPE_A: u16 = 1;
const DNS_TYPE_CNAME: u16 = 5;
const DNS_TYPE_AAAA: u16 = 28;
const DNS_FLAG_QR: u16 = 0x8000;
const DNS_FLAG_TC: u16 = 0x0200;
const DNS_OPCODE_MASK: u16 = 0x7800;
const DNS_RCODE_MASK: u16 = 0x000F;
const DNS_RCODE_FORMERR: u8 = 1;
const DNS_RCODE_SERVFAIL: u8 = 2;
const DNS_RCODE_NXDOMAIN: u8 = 3;
const DNS_RCODE_NOTIMP: u8 = 4;
const DNS_RCODE_REFUSED: u8 = 5;
const DNS_MAX_NAME_PRESENTATION_LEN: usize = 253;
const DNS_MAX_NAME_WIRE_LEN: usize = 255;
const DNS_HEADER_LEN: usize = 12;
const DNS_QUESTION_FIXED_FIELDS_LEN: usize = 4;
const DNS_RR_FIXED_FIELDS_LEN: usize = 10;
const DNS_MIN_RR_LEN: usize = 1 + DNS_RR_FIXED_FIELDS_LEN;
const DNS_MAX_QUERY_PACKET_LEN: usize =
    DNS_HEADER_LEN + DNS_MAX_NAME_WIRE_LEN + DNS_QUESTION_FIXED_FIELDS_LEN;
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_TOTAL_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const HOSTS_FILE_MAX_BYTES: usize = 4 * 1024 * 1024;
const RESOLV_CONF_MAX_BYTES: usize = 64 * 1024;
/// FlowIO's bound on DNS retry fanout and retained resolver configuration.
///
/// This is a library resource policy, not an assertion about another
/// resolver implementation's nameserver limit.
const MAX_NAMESERVERS: usize = 8;
const MAX_RESOLVED_ADDRESSES: usize = 64;
const MAX_CNAME_HOPS_PER_RESPONSE: usize = 16;
const MAX_CNAME_FOLLOWUP_QUERIES: usize = 1;
const MAX_CNAME_TOTAL_HOPS: usize = 16;
const MAX_NAME_COMPRESSION_DEPTH: usize = 8;
pub(crate) const DNS_UDP_RESPONSE_BUFFER_SIZE: usize = 2048;
const RESOLV_CONF_PATH: &str = "/etc/resolv.conf";
const HOSTS_PATH: &str = "/etc/hosts";

static QUERY_ID_STATE: AtomicU64 = AtomicU64::new(0);

enum QueryAttemptError {
    Io(io::Error),
    AttemptTimeout,
    Terminal(io::Error),
    TotalTimeout,
}

#[derive(Debug)]
struct TotalQueryTimeout;

impl std::fmt::Display for TotalQueryTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DNS total query timed out")
    }
}

impl std::error::Error for TotalQueryTimeout {}

fn total_query_timeout_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, TotalQueryTimeout)
}

#[cfg(test)]
fn is_total_query_timeout(err: &io::Error) -> bool {
    err.get_ref()
        .and_then(|source| source.downcast_ref::<TotalQueryTimeout>())
        .is_some()
}

enum DnsLookupError {
    Recoverable(io::Error),
    Terminal(io::Error),
    TotalTimeout,
}

impl DnsLookupError {
    fn classify(err: io::Error) -> Self {
        if matches!(
            err.kind(),
            io::ErrorKind::InvalidInput | io::ErrorKind::NotConnected | io::ErrorKind::OutOfMemory
        ) {
            Self::Terminal(err)
        } else {
            Self::Recoverable(err)
        }
    }

    fn into_io_error(self) -> io::Error {
        match self {
            Self::Recoverable(err) | Self::Terminal(err) => err,
            Self::TotalTimeout => total_query_timeout_error(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DnsDeadlineCheckpoint {
    Start,
    BeforeSocketSetup,
    AfterSocketBind,
    AfterSocketConnect,
    BeforeSend,
    BeforeReceive,
}

#[derive(Clone, Copy, Debug, Default)]
struct DnsDeadlineClock {
    #[cfg(test)]
    sample: Option<fn(DnsDeadlineCheckpoint) -> Instant>,
}

impl DnsDeadlineClock {
    fn sample(self, checkpoint: DnsDeadlineCheckpoint) -> Instant {
        #[cfg(test)]
        {
            self.sample
                .map_or_else(Instant::now, |sample| sample(checkpoint))
        }

        #[cfg(not(test))]
        {
            let _ = checkpoint;
            Instant::now()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DnsWaitDeadline {
    Absolute(Instant),
    Relative(Duration),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DnsWaitPlan {
    deadline: DnsWaitDeadline,
    total_limited: bool,
}

#[derive(Clone, Copy, Debug)]
struct DnsQueryDeadline {
    started_at: Instant,
    expires_at: Option<Instant>,
    timeout: Duration,
    clock: DnsDeadlineClock,
}

#[cfg(test)]
type DnsQueryHook =
    fn(SocketAddr, &[u8], u16) -> Option<Result<(Vec<u8>, usize), QueryAttemptError>>;

impl DnsQueryDeadline {
    fn new(timeout: Duration, clock: DnsDeadlineClock) -> Self {
        let started_at = clock.sample(DnsDeadlineCheckpoint::Start);
        Self {
            started_at,
            expires_at: started_at.checked_add(timeout),
            timeout,
            clock,
        }
    }

    fn remaining_at(self, now: Instant) -> Option<Duration> {
        if let Some(expires_at) = self.expires_at {
            return (now < expires_at).then(|| expires_at.duration_since(now));
        }

        let elapsed = now.saturating_duration_since(self.started_at);
        (elapsed < self.timeout).then(|| self.timeout - elapsed)
    }

    fn ensure_remaining(self, checkpoint: DnsDeadlineCheckpoint) -> Result<(), QueryAttemptError> {
        self.remaining_at(self.clock.sample(checkpoint))
            .map(|_| ())
            .ok_or(QueryAttemptError::TotalTimeout)
    }

    fn wait_plan_at(self, now: Instant, per_attempt: Option<Duration>) -> Option<DnsWaitPlan> {
        let remaining = self.remaining_at(now)?;
        let (duration, total_limited) = match per_attempt {
            Some(per_attempt) if per_attempt < remaining => (per_attempt, false),
            Some(_) | None => (remaining, true),
        };

        let deadline = if total_limited {
            self.expires_at
                .map(DnsWaitDeadline::Absolute)
                .or_else(|| now.checked_add(duration).map(DnsWaitDeadline::Absolute))
                .unwrap_or(DnsWaitDeadline::Relative(duration))
        } else {
            now.checked_add(duration)
                .map(DnsWaitDeadline::Absolute)
                .unwrap_or(DnsWaitDeadline::Relative(duration))
        };

        Some(DnsWaitPlan {
            deadline,
            total_limited,
        })
    }

    fn timeout<F: std::future::Future>(
        self,
        checkpoint: DnsDeadlineCheckpoint,
        per_attempt: Option<Duration>,
        future: F,
    ) -> Result<(Timeout<F>, bool), QueryAttemptError> {
        let plan = self
            .wait_plan_at(self.clock.sample(checkpoint), per_attempt)
            .ok_or(QueryAttemptError::TotalTimeout)?;
        let timed = match plan.deadline {
            DnsWaitDeadline::Absolute(deadline) => timeout_at(deadline, future),
            DnsWaitDeadline::Relative(duration) => timeout(duration, future),
        };
        Ok((timed, plan.total_limited))
    }
}

fn finish_dns_socket_setup<T>(
    deadline: DnsQueryDeadline,
    checkpoint: DnsDeadlineCheckpoint,
    result: io::Result<T>,
) -> Result<T, QueryAttemptError> {
    deadline.ensure_remaining(checkpoint)?;
    result.map_err(QueryAttemptError::Io)
}

fn classify_dns_timeout<T>(
    result: Result<T, TimeoutError>,
    total_limited: bool,
) -> Result<T, QueryAttemptError> {
    match result {
        Ok(output) => Ok(output),
        Err(TimeoutError::Elapsed) if total_limited => Err(QueryAttemptError::TotalTimeout),
        Err(TimeoutError::Elapsed) => Err(QueryAttemptError::AttemptTimeout),
        Err(TimeoutError::Runtime(err)) => Err(QueryAttemptError::Terminal(err)),
    }
}

/// Reusable DNS resolver built on FlowIO UDP sockets.
///
/// Use this on setup/control-plane paths when lookups repeat and the configured
/// nameservers/timeouts should be reused. For one-off convenience resolution,
/// use [`resolve_host`]. Reuse does not make lookup allocation-free: DNS
/// resolution creates one owned query and reusable response buffer shared by
/// its sequential A, AAAA, and CNAME-follow-up work, attempts create UDP
/// sockets, and non-literal names inspect `/etc/hosts` for each call. A receive
/// that times out after submission retains its buffer until the target CQE, so
/// a later attempt allocates a replacement. An aggregate timeout during send
/// likewise leaves the query owner retained until its target CQE, but starts no
/// later attempt.
///
/// # Example
/// ```
/// use flowio::net::resolver::DnsResolver;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut resolver = DnsResolver::new(vec![SocketAddr::from((Ipv4Addr::LOCALHOST, 53))])?;
/// assert_eq!(resolver.nameservers().len(), 1);
/// assert!(!resolver.system_nameservers_were_truncated());
/// resolver.set_query_timeout(std::time::Duration::from_secs(1));
/// resolver.set_total_query_timeout(std::time::Duration::from_secs(2));
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct DnsResolver {
    /// Upstream recursive resolvers queried over UDP, in retry order.
    nameservers: Box<[SocketAddr]>,
    /// Whether system configuration contained a later unique valid nameserver
    /// beyond [`MAX_NAMESERVERS`]. Explicit construction always stores false.
    system_nameservers_were_truncated: bool,
    /// Timeout for waiting on a matching response after each UDP query send
    /// completes.
    query_timeout: Duration,
    /// Aggregate budget for all asynchronous upstream work in one resolution.
    total_query_timeout: Duration,
    /// Monotonic deadline source; zero-sized in production builds.
    deadline_clock: DnsDeadlineClock,
    /// Deterministic attempt replacement used only by in-module tests.
    #[cfg(test)]
    query_hook: Option<DnsQueryHook>,
}

impl DnsResolver {
    /// Builds a resolver from `/etc/resolv.conf`.
    ///
    /// This performs a synchronous filesystem read. Call it during setup, then
    /// store the resulting nameserver list for reuse across lookups. The file
    /// read is limited to 64 KiB. The earliest raw `#` or `;` comment marker is
    /// removed before only the preceding directive is validated as UTF-8, so a
    /// malformed directive line is skipped without suppressing valid siblings,
    /// while malformed comment bytes are ignored. At most eight unique valid
    /// nameservers are retained; duplicates do not consume that bound and
    /// first-seen retry order is preserved. Use [`Self::nameservers`] to
    /// inspect the effective list and
    /// [`Self::system_nameservers_were_truncated`] to determine whether a
    /// later unique valid entry was omitted.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] when the file exceeds 64 KiB and
    /// [`io::ErrorKind::NotFound`] when no valid nameserver remains. Other file
    /// errors retain their existing classifications.
    pub fn from_system() -> io::Result<Self> {
        let configuration = read_resolv_conf(RESOLV_CONF_PATH)?;
        Ok(Self::from_effective_nameservers(
            configuration.nameservers,
            configuration.nameservers_were_truncated,
        ))
    }

    /// Builds a resolver from an explicit nameserver list.
    ///
    /// Use this when the application needs deterministic or test-specific DNS
    /// behavior instead of system defaults. Duplicate addresses are removed
    /// while retaining the first occurrence and its retry order.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] when the list is empty or
    /// contains more than eight unique nameservers.
    pub fn new(mut nameservers: Vec<SocketAddr>) -> io::Result<Self> {
        if nameservers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resolver requires at least one nameserver",
            ));
        }

        let mut unique_len = 0;
        for index in 0..nameservers.len() {
            let nameserver = nameservers[index];
            if nameservers[..unique_len].contains(&nameserver) {
                continue;
            }
            if unique_len == MAX_NAMESERVERS {
                return Err(nameserver_limit_error());
            }
            nameservers[unique_len] = nameserver;
            unique_len += 1;
        }
        nameservers.truncate(unique_len);

        Ok(Self::from_effective_nameservers(nameservers, false))
    }

    fn from_effective_nameservers(
        nameservers: Vec<SocketAddr>,
        system_nameservers_were_truncated: bool,
    ) -> Self {
        debug_assert!(!nameservers.is_empty());
        debug_assert!(nameservers.len() <= MAX_NAMESERVERS);

        Self {
            nameservers: nameservers.into_boxed_slice(),
            system_nameservers_were_truncated,
            query_timeout: DEFAULT_QUERY_TIMEOUT,
            total_query_timeout: DEFAULT_TOTAL_QUERY_TIMEOUT,
            deadline_clock: DnsDeadlineClock::default(),
            #[cfg(test)]
            query_hook: None,
        }
    }

    /// Returns the effective upstream nameservers in retry order.
    ///
    /// Explicit construction removes duplicates and retains every unique
    /// address after validation. System construction returns the first eight
    /// unique valid entries parsed from `/etc/resolv.conf`.
    pub fn nameservers(&self) -> &[SocketAddr] {
        &self.nameservers
    }

    /// Reports whether system configuration was truncated to eight nameservers.
    ///
    /// This is `true` only when [`Self::from_system`] found a later unique valid
    /// nameserver after retaining eight. Duplicate, invalid, and commented-out
    /// entries do not set it. Resolvers built with [`Self::new`] always return
    /// `false`.
    pub fn system_nameservers_were_truncated(&self) -> bool {
        self.system_nameservers_were_truncated
    }

    /// Sets the matching-response wait timeout after each UDP query is sent.
    ///
    /// This per-attempt setting does not itself bound socket creation or the
    /// preceding async UDP send. [`Self::set_total_query_timeout`] separately
    /// bounds all asynchronous upstream work in the complete resolution.
    pub fn set_query_timeout(&mut self, query_timeout: Duration) -> &mut Self {
        self.query_timeout = query_timeout;
        self
    }

    /// Sets the aggregate timeout for asynchronous upstream DNS work.
    ///
    /// The default is five seconds. This budget starts only after IP-literal,
    /// `localhost`, and `/etc/hosts` lookup produce no result, then spans all
    /// sequential A, AAAA, nameserver-failover, and CNAME-follow-up work. Each
    /// response wait is still capped by [`Self::set_query_timeout`], so its
    /// effective deadline is the earlier of the per-attempt and aggregate
    /// deadlines. A zero duration prevents upstream network I/O while leaving
    /// local resolution unaffected.
    ///
    /// The aggregate timeout bounds asynchronous sends and receives. It cannot
    /// preempt the bounded synchronous `/etc/hosts` read or a synchronous UDP
    /// socket syscall; expiry is checked immediately around socket setup so no
    /// later asynchronous attempt starts after the budget is exhausted.
    /// Aggregate expiry returns [`io::ErrorKind::TimedOut`] with the diagnostic
    /// `DNS total query timed out`.
    pub fn set_total_query_timeout(&mut self, total_query_timeout: Duration) -> &mut Self {
        self.total_query_timeout = total_query_timeout;
        self
    }

    /// Resolves a host name into socket addresses for the requested port.
    ///
    /// This first handles IP literals, `localhost`, and `/etc/hosts`, then
    /// falls back to UDP DNS queries if needed. Local aliases compare
    /// case-insensitively and treat a trailing root dot as equivalent. A
    /// hosts entry prefix before the first raw `#` byte is validated as UTF-8;
    /// a line whose entry prefix is not valid UTF-8 is skipped individually,
    /// while invalid bytes confined to the comment are ignored. Only `#`
    /// starts a hosts comment; `;` remains ordinary alias text.
    ///
    /// # Execution and failover
    ///
    /// This is setup/control-plane work. Keep the returned addresses and pass
    /// them to transport connectors instead of resolving on the data path.
    /// Non-literal names synchronously inspect `/etc/hosts`; each upstream DNS
    /// resolution creates one owned query and reusable response buffer shared
    /// across its sequential A, AAAA, and CNAME-follow-up work, and each
    /// attempt creates a connected UDP socket. A true per-attempt response
    /// expiry advances to the next nameserver; a submitted timed-out receive
    /// retains its buffer until target-CQE retirement, so a later attempt
    /// allocates a replacement. A timer-runtime failure preserves its exact
    /// `io::Error` and stops that family lookup without attempting another
    /// server; ordinary UDP I/O errors retain nameserver failover.
    ///
    /// One aggregate deadline starts only after literal, `localhost`, and
    /// hosts-file lookup miss. Its default is five seconds and it spans every
    /// asynchronous send/receive across A, AAAA, nameserver failover, and the
    /// CNAME follow-up. Each matching-response wait ends at the earlier of its
    /// per-attempt deadline and that aggregate deadline. When both deadlines
    /// are equal, expiry is aggregate and no later attempt begins. A response
    /// completion wins the timer when both become ready in the same poll,
    /// following FlowIO's general timeout contract. A completed address also
    /// keeps the existing result precedence over a later family's aggregate
    /// expiry or timer-runtime failure.
    ///
    /// The aggregate deadline cannot preempt the bounded synchronous hosts
    /// read or the synchronous socket, bind, and connect syscalls. It is
    /// sampled immediately before socket setup, after bind, and after connect;
    /// expiry observed at one of those boundaries takes precedence over that
    /// setup result and prevents the next operation. Configure the budget with
    /// [`Self::set_total_query_timeout`]. Aggregate expiry returns
    /// [`io::ErrorKind::TimedOut`] with the diagnostic
    /// `DNS total query timed out`.
    ///
    /// A and AAAA remain sequential, in that order. Their outcomes are
    /// combined as: any address, a terminal local/runtime error, an A-first
    /// CNAME, NXDOMAIN, an A-first recoverable error, then `NotFound` for two
    /// empty answers. An A-side terminal error stops before the AAAA query; a
    /// later AAAA-side terminal error does not discard an A address.
    ///
    /// # Input validation
    ///
    /// Surrounding whitespace and one optional trailing root dot are removed
    /// before lookup; root-only input and a second trailing dot are rejected.
    /// A non-literal DNS query name must contain only non-empty labels of at
    /// most 63 bytes, fit within 253 bytes in normalized dotted form, and
    /// encode to at most 255 bytes including label lengths and the terminal
    /// root. Invalid names return `InvalidInput` before a query packet is
    /// allocated or sent.
    ///
    /// # Response validation
    ///
    /// A full receive buffer is attributed to the active query only after its
    /// transaction ID matches; unrelated full datagrams are drained, while a
    /// matching-ID full datagram is rejected as anomalously truncated. A
    /// matching-ID response must be marked as a response and use the QUERY
    /// opcode. The allocation-free UDP candidate gate drains other opcodes;
    /// full parsing rejects them with `InvalidData` before question,
    /// response-code, or resource-record handling. Every present echoed
    /// question is matched by name, type, and class before its response code
    /// is applied. Questionless FORMERR, SERVFAIL, NOTIMP, and REFUSED replies
    /// remain prompt nameserver-failover results, while a questionless
    /// NXDOMAIN is drained as an unrelated datagram.
    ///
    /// Every literal label in a response name, including a compressed suffix,
    /// must be valid UTF-8 and contain no literal `.`; dots are inserted only
    /// between wire labels in the decoded presentation. The shared name walker
    /// rejects an invalid echoed question before comparison and rejects an
    /// invalid record owner or CNAME target with `InvalidData` before
    /// response-code or chain processing. Valid non-ASCII label text is
    /// preserved; name comparison folds ASCII case only.
    ///
    /// Every declared Answer, Authority, and Additional record is structurally
    /// validated before NXDOMAIN or another response code is applied. Known A
    /// and AAAA records require their exact wire lengths, and CNAME RDATA must
    /// contain exactly one complete encoded name, regardless of section or
    /// class. A CNAME is therefore malformed unless its encoded (possibly
    /// compressed) name consumes its declared RDATA length exactly. Malformed
    /// responses participate in the existing nameserver and address-family
    /// error selection.
    ///
    /// Only Internet-class Answer CNAME and address records contribute to
    /// resolution; all other valid records are ignored. A structurally valid
    /// root name remains allowed in an ignored record, but a root target
    /// reached while interpreting an Answer CNAME is upstream `InvalidData`
    /// and advances nameserver failover.
    ///
    /// # Bounds
    ///
    /// An Answer CNAME without an Answer address follows one linear chain for
    /// at most 16 hops in one response, 16 hops total, and one canonical-name
    /// follow-up query round. Exceeding the total-hop budget is local resolver
    /// policy and stops that family's remaining nameserver attempts; the
    /// sibling address family remains eligible to supply an address. CNAME
    /// loops are rejected explicitly. DNS name-compression recursion is
    /// bounded independently at depth 8.
    ///
    /// The synchronous `/etc/hosts` read is limited to 4 MiB, and the final
    /// first-seen unique result is limited to 64 socket addresses. Either
    /// over-limit condition returns `InvalidData`; results are never
    /// truncated. Invalid hosts lines still count toward the raw file-size
    /// limit.
    pub async fn resolve_host(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        self.resolve_host_with_hosts_path(HOSTS_PATH, host, port)
            .await
    }

    async fn resolve_host_with_hosts_path(
        &self,
        hosts_path: &str,
        host: &str,
        port: u16,
    ) -> io::Result<Vec<SocketAddr>> {
        let host = normalize_host(host)?;

        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }

        let mut addrs = resolve_local_host_with_hosts_path(hosts_path, host, port)?;
        if !addrs.is_empty() {
            return Ok(addrs);
        }

        let deadline = DnsQueryDeadline::new(self.total_query_timeout, self.deadline_clock);
        let mut current = host.to_owned();
        let mut cname_followup_queries = 0usize;
        let mut total_cname_hops = 0usize;
        let mut query_storage = DnsQueryStorage::new();
        loop {
            let remaining_cname_hops = MAX_CNAME_TOTAL_HOPS - total_cname_hops;
            match self
                .gather_dns_addresses(
                    &current,
                    port,
                    &mut addrs,
                    remaining_cname_hops,
                    &mut query_storage,
                    deadline,
                )
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
        query_storage: &mut DnsQueryStorage,
        deadline: DnsQueryDeadline,
    ) -> Result<LookupResult, DnsLookupError> {
        let query_id = next_query_id();
        patch_query_packet(&mut query_storage.packet, query_id, qtype)
            .map_err(DnsLookupError::classify)?;
        let mut last_err = None;

        for nameserver in self.nameservers.iter().copied() {
            match self
                .query_nameserver(
                    nameserver,
                    &mut query_storage.packet,
                    &mut query_storage.response_buffer,
                    query_id,
                    deadline,
                )
                .await
            {
                Ok((response, recv_len)) => {
                    let parsed =
                        parse_received_response_packet(&response, recv_len, query_id, host, qtype);
                    query_storage.response_buffer = Some(response);
                    match parsed {
                        Ok(result) if result.cname_hops <= remaining_cname_hops => {
                            return Ok(result);
                        }
                        Ok(_) => {
                            return Err(DnsLookupError::Recoverable(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "DNS resolution exceeded maximum total CNAME hop count",
                            )));
                        }
                        Err(err) => last_err = Some(err),
                    }
                }
                Err(QueryAttemptError::TotalTimeout) => {
                    return Err(DnsLookupError::TotalTimeout);
                }
                Err(QueryAttemptError::Terminal(err)) => {
                    return Err(DnsLookupError::Terminal(err));
                }
                Err(QueryAttemptError::Io(err)) => last_err = Some(err),
                Err(QueryAttemptError::AttemptTimeout) => {
                    last_err = Some(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "DNS query timed out",
                    ));
                }
            }
        }

        Err(DnsLookupError::classify(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "DNS resolution failed without a nameserver response",
            )
        })))
    }

    async fn query_nameserver(
        &self,
        nameserver: SocketAddr,
        packet: &mut Vec<u8>,
        response_buffer: &mut Option<Vec<u8>>,
        query_id: u16,
        deadline: DnsQueryDeadline,
    ) -> Result<(Vec<u8>, usize), QueryAttemptError> {
        deadline.ensure_remaining(DnsDeadlineCheckpoint::BeforeSocketSetup)?;
        #[cfg(test)]
        if let Some(result) = self
            .query_hook
            .and_then(|query_hook| query_hook(nameserver, packet, query_id))
        {
            return result;
        }
        let mut socket = finish_dns_socket_setup(
            deadline,
            DnsDeadlineCheckpoint::AfterSocketBind,
            UdpSocket::bind(unspecified_addr(nameserver)),
        )?;
        let connected = socket.connect(nameserver);
        finish_dns_socket_setup(
            deadline,
            DnsDeadlineCheckpoint::AfterSocketConnect,
            connected,
        )?;

        let owned_packet = std::mem::take(packet);
        let (send, total_limited) = deadline.timeout(
            DnsDeadlineCheckpoint::BeforeSend,
            None,
            socket.send(owned_packet),
        )?;
        let (send_result, returned_packet) = classify_dns_timeout(send.await, total_limited)?;
        // Restore ownership before propagating a send error so a later
        // nameserver attempt receives the same encoded query allocation.
        *packet = returned_packet;
        send_result.map_err(QueryAttemptError::Io)?;
        let (receive, total_limited) = deadline.timeout(
            DnsDeadlineCheckpoint::BeforeReceive,
            Some(self.query_timeout),
            async {
                let mut recv = response_buffer
                    .take()
                    .unwrap_or_else(new_dns_response_buffer);
                loop {
                    let (recv_result, returned) =
                        socket.recv(recv, DNS_UDP_RESPONSE_BUFFER_SIZE).await;
                    recv = returned;
                    let recv_len = match recv_result {
                        Ok(recv_len) => recv_len,
                        Err(err) => {
                            *response_buffer = Some(recv);
                            return Err(err);
                        }
                    };
                    let response = &recv[..recv_len];
                    if recv_len == DNS_UDP_RESPONSE_BUFFER_SIZE {
                        if !response_matches_query_id(response, query_id) {
                            continue;
                        }
                        // This resolver does not advertise EDNS0, so conforming
                        // UDP responses fit the legacy DNS payload size and set
                        // TC when truncated. Connected UDP recv does not expose
                        // MSG_TRUNC, so a matching-query full scratch buffer is
                        // the reliable signal for anomalous truncation.
                        let err = io::Error::new(
                            io::ErrorKind::InvalidData,
                            "DNS UDP response filled the receive buffer",
                        );
                        *response_buffer = Some(recv);
                        return Err(err);
                    }
                    if response_is_decodable_candidate(response, query_id) {
                        return Ok((recv, recv_len));
                    }
                }
            },
        )?;
        classify_dns_timeout(receive.await, total_limited)?.map_err(QueryAttemptError::Io)
    }

    async fn gather_dns_addresses(
        &self,
        current: &str,
        port: u16,
        addrs: &mut Vec<SocketAddr>,
        remaining_cname_hops: usize,
        query_storage: &mut DnsQueryStorage,
        deadline: DnsQueryDeadline,
    ) -> io::Result<ResolveHostStep> {
        encode_query_packet(&mut query_storage.packet, current)?;
        let a = match self
            .lookup_name(
                current,
                DNS_TYPE_A,
                remaining_cname_hops,
                query_storage,
                deadline,
            )
            .await
        {
            Err(DnsLookupError::Terminal(err)) => return Err(err),
            Err(DnsLookupError::TotalTimeout) => return Err(total_query_timeout_error()),
            outcome => outcome,
        };
        let aaaa = self
            .lookup_name(
                current,
                DNS_TYPE_AAAA,
                remaining_cname_hops,
                query_storage,
                deadline,
            )
            .await;
        finish_dns_family_lookups(current, port, addrs, a, aaaa)
    }
}

fn nameserver_limit_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "resolver supports at most eight unique nameservers",
    )
}

fn finish_dns_family_lookups(
    current: &str,
    port: u16,
    addrs: &mut Vec<SocketAddr>,
    a: Result<LookupResult, DnsLookupError>,
    aaaa: Result<LookupResult, DnsLookupError>,
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
                extend_unique_socket_addrs(addrs, &result.addresses, port)?;
                if cname.is_none() {
                    cname = result.cname.map(|next| (next, result.cname_hops));
                }
            }
            Err(DnsLookupError::Recoverable(err)) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(err @ (DnsLookupError::Terminal(_) | DnsLookupError::TotalTimeout)) => {
                if terminal_error.is_none() {
                    terminal_error = Some(err);
                }
            }
        }
    }

    if !addrs.is_empty() {
        return Ok(ResolveHostStep::Resolved);
    }
    if let Some(err) = terminal_error {
        return Err(err.into_io_error());
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

/// Owned DNS packet storage shared by one complete logical resolution.
struct DnsQueryStorage {
    /// Encoded query whose name is rebuilt only for a CNAME follow-up and whose
    /// ID/type fields are patched for each family lookup.
    packet: Vec<u8>,
    /// Completed receive allocation available to the next nameserver or family.
    response_buffer: Option<Vec<u8>>,
}

impl DnsQueryStorage {
    fn new() -> Self {
        Self {
            packet: Vec::with_capacity(DNS_MAX_QUERY_PACKET_LEN),
            response_buffer: None,
        }
    }
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

fn new_dns_response_buffer() -> Vec<u8> {
    Vec::with_capacity(DNS_UDP_RESPONSE_BUFFER_SIZE)
}

fn normalize_host(host: &str) -> io::Result<&str> {
    let host = host.trim();
    let host = host.strip_suffix('.').unwrap_or(host);
    if host.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "resolver host name was empty",
        ));
    }
    validate_query_name(host)?;
    Ok(host)
}

fn resolve_local_host_with_hosts_path(
    hosts_path: &str,
    host: &str,
    port: u16,
) -> io::Result<Vec<SocketAddr>> {
    let mut addrs = Vec::new();

    if dns_name_eq(host, "localhost") {
        push_unique_resolved_addr(&mut addrs, SocketAddr::from((Ipv4Addr::LOCALHOST, port)))?;
        push_unique_resolved_addr(&mut addrs, SocketAddr::from((Ipv6Addr::LOCALHOST, port)))?;
    }

    read_hosts_file(hosts_path, host, port, &mut addrs)?;

    Ok(addrs)
}

fn read_hosts_file(
    path: &str,
    host: &str,
    port: u16,
    addrs: &mut Vec<SocketAddr>,
) -> io::Result<()> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    let max_read = max_read_with_over_limit_sentinel(HOSTS_FILE_MAX_BYTES)?;
    let mut reader = BufReader::new(file.take(max_read as u64));
    let mut line_bytes = Vec::with_capacity(256);
    let mut total_bytes = 0usize;
    let mut result_error = None;

    loop {
        line_bytes.clear();
        let reached_eof = loop {
            match reader.read_until(b'\n', &mut line_bytes) {
                Ok(0) => break true,
                Ok(_) => break false,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        };
        if reached_eof && line_bytes.is_empty() {
            break;
        }

        // `reader` is capped at the configured maximum plus one sentinel byte,
        // so the accumulated count cannot overflow.
        total_bytes += line_bytes.len();
        if total_bytes > HOSTS_FILE_MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "/etc/hosts exceeds the 4 MiB resolver configuration limit",
            ));
        }

        if result_error.is_none()
            && let Err(err) = parse_hosts_line_bytes(&line_bytes, host, port, addrs)
        {
            result_error = Some(err);
        }
        if reached_eof {
            break;
        }
    }

    if let Some(err) = result_error {
        return Err(err);
    }
    Ok(())
}

#[cfg(any(feature = "fuzzing", feature = "test-support"))]
pub(crate) fn parse_hosts_bytes(
    contents: &[u8],
    host: &str,
    port: u16,
    addrs: &mut Vec<SocketAddr>,
) -> io::Result<()> {
    for line_bytes in contents.split(|byte| *byte == b'\n') {
        parse_hosts_line_bytes(line_bytes, host, port, addrs)?;
    }

    Ok(())
}

/// Returns the trimmed UTF-8 prefix before the first raw comment-boundary byte.
///
/// Only bytes before the boundary are validated as UTF-8; comment bytes are
/// ignored. An invalid, empty, or whitespace-only prefix returns `None`.
fn config_line_prefix(line_bytes: &[u8], is_comment: impl Fn(u8) -> bool) -> Option<&str> {
    let prefix_end = line_bytes
        .iter()
        .copied()
        .position(is_comment)
        .unwrap_or(line_bytes.len());
    let line = std::str::from_utf8(&line_bytes[..prefix_end]).ok()?.trim();
    if line.is_empty() { None } else { Some(line) }
}

fn parse_hosts_line_bytes(
    line_bytes: &[u8],
    host: &str,
    port: u16,
    addrs: &mut Vec<SocketAddr>,
) -> io::Result<()> {
    let Some(line) = config_line_prefix(line_bytes, |byte| byte == b'#') else {
        return Ok(());
    };

    let mut parts = line.split_whitespace();
    let Some(addr) = parts.next() else {
        return Ok(());
    };
    let Ok(ip) = addr.parse::<IpAddr>() else {
        return Ok(());
    };

    if parts.any(|name| dns_name_eq(name, host)) {
        push_unique_resolved_addr(addrs, SocketAddr::new(ip, port))?;
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedResolvConf {
    nameservers: Vec<SocketAddr>,
    nameservers_were_truncated: bool,
}

fn read_resolv_conf(path: &str) -> io::Result<ParsedResolvConf> {
    let contents = read_bounded_file_bytes(
        path,
        RESOLV_CONF_MAX_BYTES,
        "/etc/resolv.conf exceeds the 64 KiB resolver configuration limit",
    )?;
    parse_resolv_conf_configuration_bytes(&contents)
}

#[cfg(any(feature = "fuzzing", feature = "test-support"))]
pub(crate) fn parse_resolv_conf_bytes(contents: &[u8]) -> io::Result<Vec<SocketAddr>> {
    parse_resolv_conf_configuration_bytes(contents).map(|configuration| configuration.nameservers)
}

fn parse_resolv_conf_configuration_bytes(contents: &[u8]) -> io::Result<ParsedResolvConf> {
    let mut nameservers = Vec::with_capacity(MAX_NAMESERVERS);
    let mut nameservers_were_truncated = false;

    for line_bytes in contents.split(|byte| *byte == b'\n') {
        let Some(line) = config_line_prefix(line_bytes, |byte| matches!(byte, b'#' | b';')) else {
            continue;
        };

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
        if nameservers.len() == MAX_NAMESERVERS {
            if !nameservers.contains(&socket) {
                nameservers_were_truncated = true;
                break;
            }
            continue;
        }
        push_unique_socket_addr(&mut nameservers, socket);
    }

    if nameservers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no nameservers found in /etc/resolv.conf",
        ));
    }

    Ok(ParsedResolvConf {
        nameservers,
        nameservers_were_truncated,
    })
}

fn read_bounded_file_bytes(
    path: &str,
    max_bytes: usize,
    over_limit_message: &'static str,
) -> io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let max_read = max_read_with_over_limit_sentinel(max_bytes)?;
    let initial_capacity = file
        .metadata()
        .ok()
        .and_then(|metadata| usize::try_from(metadata.len()).ok())
        .unwrap_or(0)
        .min(max_read);
    let mut bytes = Vec::with_capacity(initial_capacity);
    let mut chunk = [0u8; 8192];

    while bytes.len() < max_read {
        let remaining = max_read - bytes.len();
        let chunk_len = remaining.min(chunk.len());
        let read = match file.read(&mut chunk[..chunk_len]) {
            Ok(read) => read,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        };
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }

    if bytes.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            over_limit_message,
        ));
    }

    Ok(bytes)
}

fn max_read_with_over_limit_sentinel(max_bytes: usize) -> io::Result<usize> {
    max_bytes.checked_add(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "resolver file limit cannot represent an over-limit sentinel",
        )
    })
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
/// This validates the matching ID, QR and QUERY-opcode gates plus enough
/// question structure to decide whether full parsing is useful; it does not
/// authenticate or fully validate the response packet.
pub(crate) fn response_is_decodable_candidate(packet: &[u8], query_id: u16) -> bool {
    if !response_matches_query_id(packet, query_id) {
        return false;
    }

    let Some(flags) = read_u16_be_candidate(packet, 2) else {
        return false;
    };
    if flags & DNS_FLAG_QR == 0 {
        return false;
    }
    if !dns_response_opcode_is_query(flags) {
        return false;
    }

    let Some(qdcount) = read_u16_be_candidate(packet, 4) else {
        return false;
    };
    match qdcount {
        0 => dns_rcode_allows_questionless_failover(dns_rcode(flags)),
        1 => {
            let Some((consumed, _)) = skip_dns_name(packet, DNS_HEADER_LEN, 0) else {
                return false;
            };
            let Some(question_end) = checked_add_candidate(DNS_HEADER_LEN, consumed, packet.len())
            else {
                return false;
            };
            checked_add_candidate(question_end, DNS_QUESTION_FIXED_FIELDS_LEN, packet.len())
                .is_some()
        }
        _ => false,
    }
}

/// Checks the minimum DNS header and transaction-ID gates without allocating.
fn response_matches_query_id(packet: &[u8], query_id: u16) -> bool {
    packet.len() >= DNS_HEADER_LEN
        && read_u16_be_candidate(packet, 0).is_some_and(|response_id| response_id == query_id)
}

/// Validated DNS header counts plus the optional echoed question.
struct DnsResponseEnvelope {
    /// Response flags containing QR, opcode, truncation, and response-code bits.
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
    if packet.len() < DNS_HEADER_LEN {
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
    if !dns_response_opcode_is_query(flags) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response opcode was not QUERY",
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
            let mut offset = DNS_HEADER_LEN;
            let (name, consumed) = decode_name(packet, offset, 0)?;
            offset = checked_add(offset, consumed, packet.len())?;
            let qtype = read_u16_be_at(packet, offset).map_err(byte_range_eof)?;
            let qclass_offset = checked_add(offset, 2, packet.len())?;
            let qclass = read_u16_be_at(packet, qclass_offset).map_err(byte_range_eof)?;
            let end_offset = checked_add(offset, DNS_QUESTION_FIXED_FIELDS_LEN, packet.len())?;
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
fn dns_response_opcode_is_query(flags: u16) -> bool {
    flags & DNS_OPCODE_MASK == 0
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

fn extend_unique_socket_addrs(
    addrs: &mut Vec<SocketAddr>,
    ips: &[IpAddr],
    port: u16,
) -> io::Result<()> {
    for ip in ips {
        let addr = SocketAddr::new(*ip, port);
        push_unique_resolved_addr(addrs, addr)?;
    }
    Ok(())
}

fn push_unique_resolved_addr(addrs: &mut Vec<SocketAddr>, addr: SocketAddr) -> io::Result<()> {
    if addrs.len() >= MAX_RESOLVED_ADDRESSES && !addrs.contains(&addr) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "resolver result exceeds 64 unique addresses",
        ));
    }
    push_unique_socket_addr(addrs, addr);
    Ok(())
}

/// Order-preserving helper that filters duplicate socket addresses.
fn push_unique_socket_addr(addrs: &mut Vec<SocketAddr>, addr: SocketAddr) {
    if !addrs.contains(&addr) {
        addrs.push(addr);
    }
}

fn encode_query_packet(packet: &mut Vec<u8>, host: &str) -> io::Result<()> {
    debug_assert!(validate_query_name(host).is_ok());
    packet.clear();
    let mut header = [0u8; DNS_HEADER_LEN];
    {
        let mut cursor = BufferCursorMut::new(&mut header);
        cursor.put_u16_be(0).map_err(byte_range_eof)?;
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
    packet.resize(start + DNS_QUESTION_FIXED_FIELDS_LEN, 0);
    write_u16_be_at(packet, start + 2, DNS_CLASS_IN).map_err(byte_range_eof)
}

fn patch_query_packet(packet: &mut [u8], query_id: u16, qtype: u16) -> io::Result<()> {
    let qtype_offset = packet
        .len()
        .checked_sub(DNS_QUESTION_FIXED_FIELDS_LEN)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS query packet was shorter than its fixed fields",
            )
        })?;
    write_u16_be_at(packet, 0, query_id).map_err(byte_range_eof)?;
    write_u16_be_at(packet, qtype_offset, qtype).map_err(byte_range_eof)
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
    let envelope = parse_response_query_envelope(packet, query_id, query_host, qtype)?;
    let flags = envelope.flags;

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

        if target.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS response CNAME target was the root name",
            ));
        }
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
    }
    let cname = (cname_hops != 0).then(|| active_owner.to_owned());

    Ok(LookupResult {
        addresses,
        cname,
        cname_hops,
        nx_domain: false,
    })
}

fn parse_response_query_envelope(
    packet: &[u8],
    query_id: u16,
    query_host: &str,
    qtype: u16,
) -> io::Result<DnsResponseEnvelope> {
    let envelope = parse_response_envelope(packet, query_id)?;
    if envelope.flags & DNS_FLAG_TC != 0 {
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

    Ok(envelope)
}

#[cfg(all(feature = "fuzzing", any(test, feature = "test-support")))]
pub(crate) fn response_reaches_record_parser(
    packet: &[u8],
    query_id: u16,
    query_host: &str,
    qtype: u16,
) -> bool {
    parse_response_query_envelope(packet, query_id, query_host, qtype).is_ok()
}

pub(crate) fn parse_received_response_packet(
    buffer: &[u8],
    received_len: usize,
    query_id: u16,
    query_host: &str,
    qtype: u16,
) -> io::Result<LookupResult> {
    let packet = buffer.get(..received_len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS receive length exceeded response buffer",
        )
    })?;
    parse_response_packet(packet, query_id, query_host, qtype)
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
        .map_or(DNS_HEADER_LEN, |question| question.end_offset);
    let total_rrs = envelope.ancount + envelope.nscount + envelope.arcount;
    let max_rrs_by_packet = packet.len().saturating_sub(offset) / DNS_MIN_RR_LEN;
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
            let owner_offset = offset;
            let (consumed, owner_presentation_len) =
                walk_dns_name(packet, owner_offset, 0).map_err(DnsNameWalkError::into_io_error)?;
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
                        let owner = materialize_walked_dns_name(
                            packet,
                            owner_offset,
                            owner_presentation_len,
                        )?;
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
                        let owner = materialize_walked_dns_name(
                            packet,
                            owner_offset,
                            owner_presentation_len,
                        )?;
                        records.push(DnsRecord::Address {
                            owner,
                            rr_type: rr.rr_type,
                            address: IpAddr::V6(Ipv6Addr::from(octets)),
                        });
                    }
                }
                DNS_TYPE_CNAME => {
                    let (target_consumed, target_presentation_len) =
                        walk_dns_name(packet, rr.data_offset, 0)
                            .map_err(DnsNameWalkError::into_io_error)?;
                    if target_consumed != rr.rdlength as usize {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "DNS CNAME RDATA did not consume its declared length",
                        ));
                    }
                    if contributes {
                        let owner = materialize_walked_dns_name(
                            packet,
                            owner_offset,
                            owner_presentation_len,
                        )?;
                        let target = materialize_walked_dns_name(
                            packet,
                            rr.data_offset,
                            target_presentation_len,
                        )?;
                        records.push(DnsRecord::Cname { owner, target });
                    }
                }
                _ => {}
            }
        }
    }

    Ok(records)
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
    let end = checked_add(offset, DNS_RR_FIXED_FIELDS_LEN, packet.len())?;
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
/// valid UTF-8 and contain no literal `.` before its exact text is added to the
/// presentation string.
pub(crate) fn decode_name(
    packet: &[u8],
    offset: usize,
    depth: usize,
) -> io::Result<(String, usize)> {
    let (consumed, presentation_len) =
        walk_dns_name(packet, offset, depth).map_err(DnsNameWalkError::into_io_error)?;
    let name = materialize_walked_dns_name_at_depth(packet, offset, depth, presentation_len)?;
    Ok((name, consumed))
}

/// Materializes a name immediately after a successful structural walk.
fn materialize_walked_dns_name(
    packet: &[u8],
    offset: usize,
    presentation_len: usize,
) -> io::Result<String> {
    materialize_walked_dns_name_at_depth(packet, offset, 0, presentation_len)
}

fn materialize_walked_dns_name_at_depth(
    packet: &[u8],
    offset: usize,
    depth: usize,
    presentation_len: usize,
) -> io::Result<String> {
    let mut name = String::with_capacity(presentation_len);
    // SAFETY: callers invoke this only after `walk_dns_name` validates this
    // exact offset/depth path, including every label's UTF-8, in the same
    // immutable packet.
    unsafe { materialize_validated_dns_name(packet, offset, depth, &mut name) }
        .map_err(DnsNameWalkError::into_io_error)?;
    debug_assert_eq!(name.len(), presentation_len);
    Ok(name)
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
    LiteralDotLabel,
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
            Self::LiteralDotLabel => (
                io::ErrorKind::InvalidData,
                "DNS literal label contained a dot",
            ),
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
            let (_, suffix_len) = walk_dns_name(packet, pointer, depth + 1)?;
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
        if label.as_bytes().contains(&b'.') {
            return Err(DnsNameWalkError::LiteralDotLabel);
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

/// Copies a name whose complete structure and labels were just validated by
/// [`walk_dns_name`] from the same immutable packet.
///
/// # Safety
///
/// [`walk_dns_name`] must have just succeeded for the same immutable `packet`,
/// `offset`, and `depth`. In particular, every label on the deterministic path
/// must already be known to contain valid UTF-8.
unsafe fn materialize_validated_dns_name(
    packet: &[u8],
    offset: usize,
    depth: usize,
    name: &mut String,
) -> Result<(), DnsNameWalkError> {
    if depth > MAX_NAME_COMPRESSION_DEPTH {
        return Err(DnsNameWalkError::CompressionDepthExceeded);
    }
    let mut pos = offset;

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
            // SAFETY: the caller's validated path includes this exact pointer
            // suffix at the next depth in the same immutable packet.
            return unsafe { materialize_validated_dns_name(packet, pointer, depth + 1, name) };
        }
        if len == 0 {
            return Ok(());
        }
        if len & 0xC0 != 0 {
            return Err(DnsNameWalkError::UnsupportedLabelEncoding);
        }

        let label_start = checked_add_name_offset(pos, 1, packet.len())?;
        let label_end = checked_add_name_offset(label_start, len as usize, packet.len())?;
        let label_bytes = &packet[label_start..label_end];
        // SAFETY: `decode_name` calls this helper only after `walk_dns_name`
        // validated every byte of this deterministic path as UTF-8 in the same
        // immutable packet. This pass changes no offsets or packet contents.
        let label = unsafe { std::str::from_utf8_unchecked(label_bytes) };
        if !name.is_empty() {
            name.push('.');
        }
        name.push_str(label);
        pos = label_end;
    }
}

fn skip_dns_name(packet: &[u8], offset: usize, depth: usize) -> Option<(usize, usize)> {
    walk_dns_name(packet, offset, depth).ok()
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
    let left = left.strip_suffix('.').unwrap_or(left);
    let right = right.strip_suffix('.').unwrap_or(right);
    left.eq_ignore_ascii_case(right)
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
    use std::net::{IpAddr, SocketAddr};

    /// Repository-only seam for the DNS name allocation fixture.
    pub fn decode_name(packet: &[u8], offset: usize) -> std::io::Result<(String, usize)> {
        super::decode_name(packet, offset, 0)
    }

    /// Repository-only seam for one A-family lookup and its retry behavior.
    pub async fn lookup_ipv4(
        resolver: &super::DnsResolver,
        host: &str,
    ) -> std::io::Result<Vec<IpAddr>> {
        super::validate_query_name(host)?;
        let mut query_storage = super::DnsQueryStorage::new();
        super::encode_query_packet(&mut query_storage.packet, host)?;
        resolver
            .lookup_name(
                host,
                super::DNS_TYPE_A,
                super::MAX_CNAME_TOTAL_HOPS,
                &mut query_storage,
                super::DnsQueryDeadline::new(resolver.total_query_timeout, resolver.deadline_clock),
            )
            .await
            .map(|result| result.addresses)
            .map_err(super::DnsLookupError::into_io_error)
    }

    /// Repository-only seam for the resolver deduplication allocation fixture.
    pub fn extend_unique_socket_addrs(
        addrs: &mut Vec<SocketAddr>,
        ips: &[IpAddr],
        port: u16,
    ) -> std::io::Result<()> {
        super::extend_unique_socket_addrs(addrs, ips, port)
    }

    /// Repository-only seam for bounded `/etc/hosts` fixtures.
    pub fn resolve_local_host_with_hosts_path(
        path: &str,
        host: &str,
        port: u16,
    ) -> std::io::Result<Vec<SocketAddr>> {
        super::resolve_local_host_with_hosts_path(path, host, port)
    }

    /// Repository-only seam for hosts byte-parser fixtures.
    pub fn parse_hosts_bytes(
        contents: &[u8],
        host: &str,
        port: u16,
    ) -> std::io::Result<Vec<SocketAddr>> {
        let mut addrs = Vec::new();
        super::parse_hosts_bytes(contents, host, port, &mut addrs)?;
        Ok(addrs)
    }

    /// Repository-only seam for full resolver lookup with a hosts fixture.
    pub async fn resolve_host_with_hosts_path(
        resolver: &super::DnsResolver,
        path: &str,
        host: &str,
        port: u16,
    ) -> std::io::Result<Vec<SocketAddr>> {
        resolver
            .resolve_host_with_hosts_path(path, host, port)
            .await
    }

    /// Repository-only seam for bounded `/etc/resolv.conf` fixtures.
    pub fn read_resolv_conf(path: &str) -> std::io::Result<Vec<SocketAddr>> {
        super::read_resolv_conf(path).map(|configuration| configuration.nameservers)
    }

    /// Repository-only seam for raw-byte `/etc/resolv.conf` fixtures.
    pub fn parse_resolv_conf_bytes(contents: &[u8]) -> std::io::Result<Vec<SocketAddr>> {
        super::parse_resolv_conf_bytes(contents)
    }

    /// Repository-only seam for effective `/etc/resolv.conf` metadata.
    pub fn parse_resolv_conf_configuration_bytes(
        contents: &[u8],
    ) -> std::io::Result<(Vec<SocketAddr>, bool)> {
        super::parse_resolv_conf_configuration_bytes(contents).map(|configuration| {
            (
                configuration.nameservers,
                configuration.nameservers_were_truncated,
            )
        })
    }

    /// Repository-only seam for the DNS candidate allocation fixture.
    pub fn response_is_decodable_candidate(packet: &[u8], query_id: u16) -> bool {
        super::response_is_decodable_candidate(packet, query_id)
    }

    /// Repository-only seam for response-record allocation fixtures.
    pub fn parse_ipv4_response(
        packet: &[u8],
        query_id: u16,
        query_host: &str,
    ) -> std::io::Result<()> {
        super::parse_response_packet(packet, query_id, query_host, super::DNS_TYPE_A).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(miri))]
    use crate::runtime::executor::Executor;
    #[cfg(not(miri))]
    use std::cell::RefCell;
    #[cfg(not(miri))]
    use std::collections::VecDeque;
    #[cfg(not(miri))]
    use std::net::UdpSocket as StdUdpSocket;
    #[cfg(not(miri))]
    use std::rc::Rc;

    #[cfg(not(miri))]
    struct DeadlineClockScript {
        base: Instant,
        samples: VecDeque<(DnsDeadlineCheckpoint, Duration)>,
    }

    #[cfg(not(miri))]
    enum ScriptedQueryOutcome {
        Address(IpAddr),
        Cname(&'static str),
        Empty,
        AttemptTimeout,
        TerminalRuntime(i32),
    }

    #[cfg(not(miri))]
    struct ScriptedQueryStep {
        nameserver: SocketAddr,
        host: &'static str,
        qtype: u16,
        outcome: ScriptedQueryOutcome,
    }

    #[cfg(not(miri))]
    thread_local! {
        static DEADLINE_CLOCK_SCRIPT: RefCell<Option<DeadlineClockScript>> = const {
            RefCell::new(None)
        };
        static DNS_QUERY_SCRIPT: RefCell<Option<VecDeque<ScriptedQueryStep>>> = const {
            RefCell::new(None)
        };
    }

    #[cfg(not(miri))]
    fn scripted_deadline_clock(checkpoint: DnsDeadlineCheckpoint) -> Instant {
        DEADLINE_CLOCK_SCRIPT.with(|script| {
            let mut script = script.borrow_mut();
            let script = script
                .as_mut()
                .expect("DNS deadline clock was sampled without a script");
            let (expected, offset) = script
                .samples
                .pop_front()
                .expect("DNS deadline clock consumed more samples than expected");
            assert_eq!(checkpoint, expected, "unexpected DNS deadline checkpoint");
            script
                .base
                .checked_add(offset)
                .expect("test DNS deadline offset should fit in Instant")
        })
    }

    #[cfg(not(miri))]
    fn scripted_query_hook(
        nameserver: SocketAddr,
        packet: &[u8],
        query_id: u16,
    ) -> Option<Result<(Vec<u8>, usize), QueryAttemptError>> {
        let step = DNS_QUERY_SCRIPT.with(|script| {
            script
                .borrow_mut()
                .as_mut()
                .expect("DNS query hook was called without a script")
                .pop_front()
                .expect("DNS query hook consumed more steps than expected")
        });
        assert_eq!(nameserver, step.nameserver, "unexpected DNS nameserver");
        assert_eq!(
            read_u16_be_at(packet, 0).expect("scripted query ID should fit"),
            query_id,
            "scripted query ID did not match its packet"
        );
        let (host, name_len) =
            decode_name(packet, DNS_HEADER_LEN, 0).expect("scripted query name should decode");
        assert_eq!(host, step.host, "unexpected scripted DNS query name");
        let qtype = read_u16_be_at(packet, DNS_HEADER_LEN + name_len)
            .expect("scripted query type should fit");
        assert_eq!(qtype, step.qtype, "unexpected scripted DNS query type");

        if matches!(&step.outcome, ScriptedQueryOutcome::AttemptTimeout) {
            return Some(Err(QueryAttemptError::AttemptTimeout));
        }
        if let ScriptedQueryOutcome::TerminalRuntime(errno) = &step.outcome {
            return Some(Err(QueryAttemptError::Terminal(
                io::Error::from_raw_os_error(*errno),
            )));
        }

        let mut response = packet.to_vec();
        let flags = read_u16_be_at(&response, 2).expect("scripted query flags should fit");
        write_u16_be_at(&mut response, 2, flags | DNS_FLAG_QR)
            .expect("scripted response flags should fit");
        match step.outcome {
            ScriptedQueryOutcome::Address(address) => {
                write_u16_be_at(&mut response, 6, 1).expect("scripted Answer count should fit");
                response.extend_from_slice(&0xC00Cu16.to_be_bytes());
                response.extend_from_slice(&qtype.to_be_bytes());
                response.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
                response.extend_from_slice(&0u32.to_be_bytes());
                match address {
                    IpAddr::V4(address) => {
                        response.extend_from_slice(&4u16.to_be_bytes());
                        response.extend_from_slice(&address.octets());
                    }
                    IpAddr::V6(address) => {
                        response.extend_from_slice(&16u16.to_be_bytes());
                        response.extend_from_slice(&address.octets());
                    }
                }
            }
            ScriptedQueryOutcome::Cname(target) => {
                let mut encoded_target = Vec::new();
                push_test_wire_name(&mut encoded_target, target);
                write_u16_be_at(&mut response, 6, 1).expect("scripted Answer count should fit");
                response.extend_from_slice(&0xC00Cu16.to_be_bytes());
                response.extend_from_slice(&DNS_TYPE_CNAME.to_be_bytes());
                response.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
                response.extend_from_slice(&0u32.to_be_bytes());
                response.extend_from_slice(
                    &u16::try_from(encoded_target.len())
                        .expect("scripted CNAME should fit")
                        .to_be_bytes(),
                );
                response.extend_from_slice(&encoded_target);
            }
            ScriptedQueryOutcome::Empty => {}
            ScriptedQueryOutcome::AttemptTimeout | ScriptedQueryOutcome::TerminalRuntime(_) => {
                unreachable!()
            }
        }
        let response_len = response.len();
        Some(Ok((response, response_len)))
    }

    #[cfg(not(miri))]
    struct DeadlineClockGuard;

    #[cfg(not(miri))]
    impl DeadlineClockGuard {
        fn install(samples: Vec<(DnsDeadlineCheckpoint, Duration)>) -> Self {
            DEADLINE_CLOCK_SCRIPT.with(|script| {
                let mut script = script.borrow_mut();
                assert!(
                    script.is_none(),
                    "DNS deadline clock script was already active"
                );
                *script = Some(DeadlineClockScript {
                    base: Instant::now(),
                    samples: samples.into(),
                });
            });
            Self
        }

        fn assert_exhausted(&self) {
            DEADLINE_CLOCK_SCRIPT.with(|script| {
                let script = script.borrow();
                let remaining = script
                    .as_ref()
                    .expect("DNS deadline clock script disappeared")
                    .samples
                    .len();
                assert_eq!(remaining, 0, "DNS deadline clock left unused samples");
            });
        }
    }

    #[cfg(not(miri))]
    struct QueryScriptGuard;

    #[cfg(not(miri))]
    impl QueryScriptGuard {
        fn install(steps: Vec<ScriptedQueryStep>) -> Self {
            DNS_QUERY_SCRIPT.with(|script| {
                let mut script = script.borrow_mut();
                assert!(script.is_none(), "DNS query script was already active");
                *script = Some(steps.into());
            });
            Self
        }

        fn assert_exhausted(&self) {
            DNS_QUERY_SCRIPT.with(|script| {
                let script = script.borrow();
                let remaining = script.as_ref().expect("DNS query script disappeared").len();
                assert_eq!(remaining, 0, "DNS query script left unused steps");
            });
        }
    }

    #[cfg(not(miri))]
    impl Drop for QueryScriptGuard {
        fn drop(&mut self) {
            DNS_QUERY_SCRIPT.with(|script| {
                *script.borrow_mut() = None;
            });
        }
    }

    #[cfg(not(miri))]
    impl Drop for DeadlineClockGuard {
        fn drop(&mut self) {
            DEADLINE_CLOCK_SCRIPT.with(|script| {
                *script.borrow_mut() = None;
            });
        }
    }

    #[cfg(not(miri))]
    fn resolver_with_scripted_clock(nameservers: Vec<SocketAddr>) -> DnsResolver {
        let mut resolver =
            DnsResolver::new(nameservers).expect("test resolver construction failed");
        resolver.deadline_clock = DnsDeadlineClock {
            sample: Some(scripted_deadline_clock),
        };
        resolver
    }

    #[cfg(not(miri))]
    fn resolver_with_scripted_queries(nameservers: Vec<SocketAddr>) -> DnsResolver {
        let mut resolver = resolver_with_scripted_clock(nameservers);
        resolver.query_hook = Some(scripted_query_hook);
        resolver
    }

    #[cfg(not(miri))]
    fn run_scripted_resolution(
        resolver: DnsResolver,
        host: &'static str,
    ) -> io::Result<Vec<SocketAddr>> {
        let mut executor = Executor::new().expect("test executor construction failed");
        let result = Rc::new(RefCell::new(None));
        let task_result = Rc::clone(&result);
        executor
            .run(async move {
                task_result.replace(Some(
                    resolver
                        .resolve_host_with_hosts_path(
                            "/flowio-test-hosts-file-does-not-exist",
                            host,
                            5432,
                        )
                        .await,
                ));
            })
            .expect("test executor run failed");
        result
            .borrow_mut()
            .take()
            .expect("test resolver task did not publish its result")
    }

    #[test]
    fn config_line_prefix_preserves_exact_hosts_and_resolv_conf_grammar() {
        struct Case {
            line: &'static [u8],
            hosts: Option<&'static str>,
            resolv_conf: Option<&'static str>,
        }

        const CASES: &[Case] = &[
            Case {
                line: b"",
                hosts: None,
                resolv_conf: None,
            },
            Case {
                line: b"  nameserver 192.0.2.1 \r\n",
                hosts: Some("nameserver 192.0.2.1"),
                resolv_conf: Some("nameserver 192.0.2.1"),
            },
            Case {
                line: b"\t\r\n",
                hosts: None,
                resolv_conf: None,
            },
            Case {
                line: b"# \xff comment",
                hosts: None,
                resolv_conf: None,
            },
            Case {
                line: b"; \xff comment",
                hosts: None,
                resolv_conf: None,
            },
            Case {
                line: b"value # \xff comment",
                hosts: Some("value"),
                resolv_conf: Some("value"),
            },
            Case {
                line: b"value ; \xff comment",
                hosts: None,
                resolv_conf: Some("value"),
            },
            Case {
                line: b"\xff value # comment",
                hosts: None,
                resolv_conf: None,
            },
            Case {
                line: b"value ; first # second",
                hosts: Some("value ; first"),
                resolv_conf: Some("value"),
            },
            Case {
                line: b"value # first ; second",
                hosts: Some("value"),
                resolv_conf: Some("value"),
            },
            Case {
                line: b";hosts-alias",
                hosts: Some(";hosts-alias"),
                resolv_conf: None,
            },
        ];

        for case in CASES {
            assert_eq!(
                config_line_prefix(case.line, |byte| byte == b'#'),
                case.hosts,
                "hosts grammar drifted for {:?}",
                case.line
            );
            assert_eq!(
                config_line_prefix(case.line, |byte| matches!(byte, b'#' | b';')),
                case.resolv_conf,
                "resolv.conf grammar drifted for {:?}",
                case.line
            );
        }

        let line = b"  borrowed-prefix # comment";
        let prefix = config_line_prefix(line, |byte| byte == b'#')
            .expect("valid nonempty prefix should be retained");
        assert_eq!(prefix, "borrowed-prefix");
        assert_eq!(prefix.as_ptr(), line[2..].as_ptr());
    }

    #[test]
    fn response_query_id_gate_covers_short_and_full_datagrams() {
        const QUERY_ID: u16 = 0x1234;

        assert!(!response_matches_query_id(
            &QUERY_ID.to_be_bytes(),
            QUERY_ID
        ));

        let mut header = [0u8; DNS_HEADER_LEN];
        header[..2].copy_from_slice(&QUERY_ID.wrapping_add(1).to_be_bytes());
        assert!(!response_matches_query_id(&header, QUERY_ID));
        header[..2].copy_from_slice(&QUERY_ID.to_be_bytes());
        assert!(response_matches_query_id(&header, QUERY_ID));

        let mut full = vec![0u8; DNS_UDP_RESPONSE_BUFFER_SIZE];
        full[..2].copy_from_slice(&QUERY_ID.wrapping_add(1).to_be_bytes());
        assert!(!response_matches_query_id(&full, QUERY_ID));
        full[..2].copy_from_slice(&QUERY_ID.to_be_bytes());
        assert!(response_matches_query_id(&full, QUERY_ID));
    }

    #[test]
    fn fresh_dns_response_buffer_exposes_capacity_without_initializing_bytes() {
        let response = new_dns_response_buffer();
        assert_eq!(response.len(), 0);
        assert_eq!(response.capacity(), DNS_UDP_RESPONSE_BUFFER_SIZE);
    }

    fn test_query_packet(query_id: u16, host: &str, qtype: u16) -> io::Result<Vec<u8>> {
        validate_query_name(host)?;
        let mut query_storage = DnsQueryStorage::new();
        encode_query_packet(&mut query_storage.packet, host)?;
        patch_query_packet(&mut query_storage.packet, query_id, qtype)?;
        Ok(query_storage.packet)
    }

    #[test]
    fn resolver_nameserver_bound_preserves_first_seen_unique_order() {
        let first = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 5301));
        let second = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 2), 5302));
        let third = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 3), 5303));

        let resolver = DnsResolver::new(vec![first, second, first, third, second])
            .expect("duplicate nameservers within the bound should be accepted");
        assert_eq!(resolver.nameservers(), &[first, second, third]);
        assert!(!resolver.system_nameservers_were_truncated());
    }

    #[test]
    fn resolver_nameserver_bound_accepts_eight_unique_entries() {
        let nameservers = (0..MAX_NAMESERVERS)
            .map(|index| {
                SocketAddr::from((
                    Ipv4Addr::new(192, 0, 2, index as u8 + 1),
                    5300 + index as u16,
                ))
            })
            .collect::<Vec<_>>();

        let resolver = DnsResolver::new(nameservers.clone())
            .expect("the nameserver boundary should be accepted");
        assert_eq!(resolver.nameservers(), nameservers);
        assert!(!resolver.system_nameservers_were_truncated());
    }

    #[test]
    fn resolver_nameserver_bound_rejects_ninth_unique_entry() {
        let mut nameservers = (0..MAX_NAMESERVERS)
            .map(|index| {
                SocketAddr::from((
                    Ipv4Addr::new(192, 0, 2, index as u8 + 1),
                    5300 + index as u16,
                ))
            })
            .collect::<Vec<_>>();
        nameservers.insert(1, nameservers[0]);
        nameservers.push(SocketAddr::from((Ipv4Addr::new(192, 0, 2, 9), 5309)));

        let err = DnsResolver::new(nameservers)
            .expect_err("a ninth unique nameserver should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            err.to_string(),
            "resolver supports at most eight unique nameservers"
        );
    }

    #[test]
    fn system_resolver_exposes_truncated_effective_configuration() {
        let configuration = parse_resolv_conf_configuration_bytes(
            b"nameserver 192.0.2.1\n\
nameserver 192.0.2.2\n\
nameserver 192.0.2.3\n\
nameserver 192.0.2.4\n\
nameserver 192.0.2.5\n\
nameserver 192.0.2.6\n\
nameserver 192.0.2.7\n\
nameserver 192.0.2.8\n\
nameserver 192.0.2.9\n",
        )
        .expect("system nameserver configuration should parse");
        let resolver = DnsResolver::from_effective_nameservers(
            configuration.nameservers,
            configuration.nameservers_were_truncated,
        );

        assert_eq!(resolver.nameservers().len(), MAX_NAMESERVERS);
        assert!(resolver.system_nameservers_were_truncated());
    }

    #[test]
    fn resolver_timeout_defaults_and_setters_are_independent() {
        let nameserver = SocketAddr::from((Ipv4Addr::LOCALHOST, 53));
        let mut resolver = DnsResolver::new(vec![nameserver]).expect("resolver construction");

        assert_eq!(resolver.query_timeout, Duration::from_secs(3));
        assert_eq!(resolver.total_query_timeout, Duration::from_secs(5));

        let returned = resolver
            .set_query_timeout(Duration::from_secs(7))
            .set_total_query_timeout(Duration::from_secs(11));
        assert!(std::ptr::eq(returned, &resolver));
        assert_eq!(resolver.query_timeout, Duration::from_secs(7));
        assert_eq!(resolver.total_query_timeout, Duration::from_secs(11));
    }

    #[test]
    fn dns_wait_plans_choose_per_attempt_then_equal_or_shorter_total_budget() {
        let started_at = Instant::now();
        let deadline = DnsQueryDeadline {
            started_at,
            expires_at: started_at.checked_add(Duration::from_secs(5)),
            timeout: Duration::from_secs(5),
            clock: DnsDeadlineClock::default(),
        };
        let now = started_at
            .checked_add(Duration::from_secs(1))
            .expect("test instant should fit");

        let shorter_attempt = deadline
            .wait_plan_at(now, Some(Duration::from_secs(3)))
            .expect("in-budget per-attempt wait");
        assert_eq!(
            shorter_attempt,
            DnsWaitPlan {
                deadline: DnsWaitDeadline::Absolute(
                    now.checked_add(Duration::from_secs(3))
                        .expect("test instant should fit")
                ),
                total_limited: false,
            }
        );

        for per_attempt in [Duration::from_secs(4), Duration::from_secs(9)] {
            let total_limited = deadline
                .wait_plan_at(now, Some(per_attempt))
                .expect("in-budget total wait");
            assert_eq!(
                total_limited,
                DnsWaitPlan {
                    deadline: DnsWaitDeadline::Absolute(
                        started_at
                            .checked_add(Duration::from_secs(5))
                            .expect("test instant should fit")
                    ),
                    total_limited: true,
                }
            );
        }
    }

    #[test]
    fn dns_deadline_zero_and_exact_boundary_are_expired() {
        let started_at = Instant::now();
        let zero = DnsQueryDeadline {
            started_at,
            expires_at: Some(started_at),
            timeout: Duration::ZERO,
            clock: DnsDeadlineClock::default(),
        };
        assert_eq!(zero.remaining_at(started_at), None);
        assert_eq!(zero.wait_plan_at(started_at, None), None);

        let expires_at = started_at
            .checked_add(Duration::from_secs(5))
            .expect("test instant should fit");
        let bounded = DnsQueryDeadline {
            started_at,
            expires_at: Some(expires_at),
            timeout: Duration::from_secs(5),
            clock: DnsDeadlineClock::default(),
        };
        let immediately_before = expires_at
            .checked_sub(Duration::from_nanos(1))
            .expect("test instant should fit");
        assert_eq!(
            bounded.remaining_at(immediately_before),
            Some(Duration::from_nanos(1))
        );
        assert_eq!(
            bounded.wait_plan_at(immediately_before, Some(Duration::from_secs(3))),
            Some(DnsWaitPlan {
                deadline: DnsWaitDeadline::Absolute(expires_at),
                total_limited: true,
            })
        );
        assert_eq!(bounded.remaining_at(expires_at), None);
        assert_eq!(
            bounded.remaining_at(
                expires_at
                    .checked_add(Duration::from_nanos(1))
                    .expect("test instant should fit")
            ),
            None
        );
    }

    #[test]
    fn dns_deadline_maximum_duration_uses_safe_relative_fallback() {
        let deadline = DnsQueryDeadline::new(Duration::MAX, DnsDeadlineClock::default());
        assert_eq!(deadline.expires_at, None);
        assert_eq!(
            deadline.remaining_at(deadline.started_at),
            Some(Duration::MAX)
        );
        assert_eq!(
            deadline.wait_plan_at(deadline.started_at, None),
            Some(DnsWaitPlan {
                deadline: DnsWaitDeadline::Relative(Duration::MAX),
                total_limited: true,
            })
        );
    }

    #[test]
    fn dns_timeout_classification_keeps_elapsed_distinct_and_runtime_terminal() {
        assert!(matches!(classify_dns_timeout(Ok(7u8), true), Ok(7)));
        assert!(matches!(classify_dns_timeout(Ok(7u8), false), Ok(7)));
        assert!(matches!(
            classify_dns_timeout::<()>(Err(TimeoutError::Elapsed), true),
            Err(QueryAttemptError::TotalTimeout)
        ));
        assert!(matches!(
            classify_dns_timeout::<()>(Err(TimeoutError::Elapsed), false),
            Err(QueryAttemptError::AttemptTimeout)
        ));

        for total_limited in [false, true] {
            for kind in [
                io::ErrorKind::NotConnected,
                io::ErrorKind::OutOfMemory,
                io::ErrorKind::Interrupted,
            ] {
                let outcome = classify_dns_timeout::<()>(
                    Err(TimeoutError::Runtime(io::Error::new(
                        kind,
                        "injected DNS timer-runtime failure",
                    ))),
                    total_limited,
                );
                match outcome {
                    Err(QueryAttemptError::Terminal(err)) => {
                        assert_eq!(err.kind(), kind);
                        assert_eq!(err.to_string(), "injected DNS timer-runtime failure");
                    }
                    _ => panic!("timer-runtime failure was not terminal"),
                }
            }
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn dns_socket_setup_boundary_gives_expiry_precedence_over_syscall_error() {
        let total = Duration::from_secs(5);
        {
            let clock = DeadlineClockGuard::install(vec![
                (DnsDeadlineCheckpoint::Start, Duration::ZERO),
                (DnsDeadlineCheckpoint::AfterSocketBind, total),
            ]);
            let deadline = DnsQueryDeadline::new(
                total,
                DnsDeadlineClock {
                    sample: Some(scripted_deadline_clock),
                },
            );
            let outcome = finish_dns_socket_setup::<()>(
                deadline,
                DnsDeadlineCheckpoint::AfterSocketBind,
                Err(io::Error::from(io::ErrorKind::ConnectionRefused)),
            );
            assert!(matches!(outcome, Err(QueryAttemptError::TotalTimeout)));
            clock.assert_exhausted();
        }

        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (
                DnsDeadlineCheckpoint::AfterSocketBind,
                total - Duration::from_nanos(1),
            ),
        ]);
        let deadline = DnsQueryDeadline::new(
            total,
            DnsDeadlineClock {
                sample: Some(scripted_deadline_clock),
            },
        );
        let outcome = finish_dns_socket_setup::<()>(
            deadline,
            DnsDeadlineCheckpoint::AfterSocketBind,
            Err(io::Error::from(io::ErrorKind::ConnectionRefused)),
        );
        match outcome {
            Err(QueryAttemptError::Io(err)) => {
                assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused)
            }
            _ => panic!("in-budget socket error did not retain its classification"),
        }
        clock.assert_exhausted();
    }

    #[cfg(not(miri))]
    #[test]
    fn resolver_total_deadline_after_bind_prevents_later_setup_or_send() {
        let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("test DNS bind failed");
        server
            .set_nonblocking(true)
            .expect("test DNS nonblocking setup failed");
        let nameserver = server.local_addr().expect("test DNS local addr failed");
        let total = Duration::from_secs(5);
        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, Duration::ZERO),
            (DnsDeadlineCheckpoint::AfterSocketBind, total),
        ]);
        let resolver = resolver_with_scripted_clock(vec![nameserver]);

        let err = run_scripted_resolution(resolver, "setup-delay.flowio.invalid")
            .expect_err("expired socket setup should fail");
        assert!(is_total_query_timeout(&err));
        let mut query = [0u8; DNS_MAX_QUERY_PACKET_LEN];
        let recv_err = server
            .recv_from(&mut query)
            .expect_err("expired socket setup unexpectedly sent a query");
        assert_eq!(recv_err.kind(), io::ErrorKind::WouldBlock);
        clock.assert_exhausted();
    }

    #[cfg(not(miri))]
    #[test]
    fn resolver_total_deadline_after_connect_prevents_send() {
        let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("test DNS bind failed");
        server
            .set_nonblocking(true)
            .expect("test DNS nonblocking setup failed");
        let nameserver = server.local_addr().expect("test DNS local addr failed");
        let total = Duration::from_secs(5);
        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, Duration::ZERO),
            (DnsDeadlineCheckpoint::AfterSocketBind, Duration::ZERO),
            (DnsDeadlineCheckpoint::AfterSocketConnect, total),
        ]);
        let resolver = resolver_with_scripted_clock(vec![nameserver]);

        let err = run_scripted_resolution(resolver, "connect-delay.flowio.invalid")
            .expect_err("expired connect should fail before send");
        assert!(is_total_query_timeout(&err));
        let mut query = [0u8; DNS_MAX_QUERY_PACKET_LEN];
        let recv_err = server
            .recv_from(&mut query)
            .expect_err("expired connect unexpectedly sent a query");
        assert_eq!(recv_err.kind(), io::ErrorKind::WouldBlock);
        clock.assert_exhausted();
    }

    #[cfg(not(miri))]
    #[test]
    fn resolver_total_deadline_before_send_emits_no_datagram() {
        let server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("test DNS bind failed");
        server
            .set_nonblocking(true)
            .expect("test DNS nonblocking setup failed");
        let nameserver = server.local_addr().expect("test DNS local addr failed");
        let total = Duration::from_secs(5);
        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, Duration::ZERO),
            (DnsDeadlineCheckpoint::AfterSocketBind, Duration::ZERO),
            (DnsDeadlineCheckpoint::AfterSocketConnect, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSend, total),
        ]);
        let resolver = resolver_with_scripted_clock(vec![nameserver]);

        let err = run_scripted_resolution(resolver, "send-cutoff.flowio.invalid")
            .expect_err("expired send budget should stop before submission");
        assert!(is_total_query_timeout(&err));
        let mut query = [0u8; DNS_MAX_QUERY_PACKET_LEN];
        let recv_err = server
            .recv_from(&mut query)
            .expect_err("expired send budget unexpectedly emitted a query");
        assert_eq!(recv_err.kind(), io::ErrorKind::WouldBlock);
        clock.assert_exhausted();
    }

    #[cfg(not(miri))]
    #[test]
    fn resolver_total_deadline_after_completed_send_stops_before_receive_or_retry() {
        let first = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("first test DNS bind failed");
        first
            .set_nonblocking(true)
            .expect("first test DNS nonblocking setup failed");
        let first_nameserver = first.local_addr().expect("first DNS local addr failed");
        let second = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("second test DNS bind failed");
        second
            .set_nonblocking(true)
            .expect("second test DNS nonblocking setup failed");
        let second_nameserver = second.local_addr().expect("second DNS local addr failed");
        let total = Duration::from_secs(5);
        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, Duration::ZERO),
            (DnsDeadlineCheckpoint::AfterSocketBind, Duration::ZERO),
            (DnsDeadlineCheckpoint::AfterSocketConnect, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSend, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeReceive, total),
        ]);
        let resolver = resolver_with_scripted_clock(vec![first_nameserver, second_nameserver]);

        let err = run_scripted_resolution(resolver, "send-delay.flowio.invalid")
            .expect_err("expiry after the completed send should stop before receive");
        assert!(is_total_query_timeout(&err));
        let mut query = [0u8; DNS_MAX_QUERY_PACKET_LEN];
        let (len, _) = first
            .recv_from(&mut query)
            .expect("the bounded send should have emitted exactly one query");
        assert!(len >= DNS_HEADER_LEN);
        let recv_err = first
            .recv_from(&mut query)
            .expect_err("expiry unexpectedly started another family query");
        assert_eq!(recv_err.kind(), io::ErrorKind::WouldBlock);
        let recv_err = second
            .recv_from(&mut query)
            .expect_err("expiry unexpectedly started nameserver failover");
        assert_eq!(recv_err.kind(), io::ErrorKind::WouldBlock);
        clock.assert_exhausted();
    }

    #[cfg(not(miri))]
    #[test]
    fn resolver_total_deadline_keeps_a_answer_immediately_before_aaaa_expiry() {
        let nameserver = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 53), DNS_PORT));
        let total = Duration::from_secs(5);
        let before_expiry = total - Duration::from_nanos(1);
        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, before_expiry),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, total),
        ]);
        let queries = QueryScriptGuard::install(vec![ScriptedQueryStep {
            nameserver,
            host: "family-deadline.flowio.invalid",
            qtype: DNS_TYPE_A,
            outcome: ScriptedQueryOutcome::Address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 209))),
        }]);
        let resolver = resolver_with_scripted_queries(vec![nameserver]);

        let addrs = run_scripted_resolution(resolver, "family-deadline.flowio.invalid")
            .expect("completed A address should win later AAAA aggregate expiry");
        assert_eq!(
            addrs,
            [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 209), 5432))]
        );
        queries.assert_exhausted();
        clock.assert_exhausted();
    }

    #[cfg(not(miri))]
    #[test]
    fn resolver_total_deadline_survives_attempt_timeout_and_reaches_aaaa() {
        let first = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 53), DNS_PORT));
        let second = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 54), DNS_PORT));
        let total = Duration::from_secs(5);
        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, Duration::ZERO),
            (
                DnsDeadlineCheckpoint::BeforeSocketSetup,
                Duration::from_secs(1),
            ),
            (
                DnsDeadlineCheckpoint::BeforeSocketSetup,
                total - Duration::from_nanos(1),
            ),
        ]);
        let queries = QueryScriptGuard::install(vec![
            ScriptedQueryStep {
                nameserver: first,
                host: "failover-deadline.flowio.invalid",
                qtype: DNS_TYPE_A,
                outcome: ScriptedQueryOutcome::AttemptTimeout,
            },
            ScriptedQueryStep {
                nameserver: second,
                host: "failover-deadline.flowio.invalid",
                qtype: DNS_TYPE_A,
                outcome: ScriptedQueryOutcome::Empty,
            },
            ScriptedQueryStep {
                nameserver: first,
                host: "failover-deadline.flowio.invalid",
                qtype: DNS_TYPE_AAAA,
                outcome: ScriptedQueryOutcome::Address(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            },
        ]);
        let mut resolver = resolver_with_scripted_queries(vec![first, second]);
        resolver.set_query_timeout(Duration::from_millis(500));

        let addrs = run_scripted_resolution(resolver, "failover-deadline.flowio.invalid")
            .expect("ordinary attempt expiry should preserve aggregate-budget failover");
        assert_eq!(addrs, [SocketAddr::from((Ipv6Addr::LOCALHOST, 5432))]);
        queries.assert_exhausted();
        clock.assert_exhausted();
    }

    #[cfg(not(miri))]
    #[test]
    fn resolver_terminal_timer_runtime_error_stops_before_aaaa() {
        let nameserver = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 53), DNS_PORT));
        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, Duration::ZERO),
        ]);
        let queries = QueryScriptGuard::install(vec![ScriptedQueryStep {
            nameserver,
            host: "runtime-error.flowio.invalid",
            qtype: DNS_TYPE_A,
            outcome: ScriptedQueryOutcome::TerminalRuntime(libc::ECANCELED),
        }]);
        let resolver = resolver_with_scripted_queries(vec![nameserver]);

        let err = run_scripted_resolution(resolver, "runtime-error.flowio.invalid")
            .expect_err("terminal timer runtime error should stop before AAAA");
        assert_eq!(err.raw_os_error(), Some(libc::ECANCELED));
        queries.assert_exhausted();
        clock.assert_exhausted();
    }

    #[cfg(not(miri))]
    #[test]
    fn resolver_total_deadline_reaches_cname_followup_before_expiry() {
        let nameserver = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 53), DNS_PORT));
        let total = Duration::from_secs(5);
        let clock = DeadlineClockGuard::install(vec![
            (DnsDeadlineCheckpoint::Start, Duration::ZERO),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, Duration::ZERO),
            (
                DnsDeadlineCheckpoint::BeforeSocketSetup,
                Duration::from_secs(1),
            ),
            (
                DnsDeadlineCheckpoint::BeforeSocketSetup,
                total - Duration::from_nanos(1),
            ),
            (DnsDeadlineCheckpoint::BeforeSocketSetup, total),
        ]);
        let queries = QueryScriptGuard::install(vec![
            ScriptedQueryStep {
                nameserver,
                host: "cname-deadline.flowio.invalid",
                qtype: DNS_TYPE_A,
                outcome: ScriptedQueryOutcome::Cname("target-deadline.flowio.invalid"),
            },
            ScriptedQueryStep {
                nameserver,
                host: "cname-deadline.flowio.invalid",
                qtype: DNS_TYPE_AAAA,
                outcome: ScriptedQueryOutcome::Empty,
            },
            ScriptedQueryStep {
                nameserver,
                host: "target-deadline.flowio.invalid",
                qtype: DNS_TYPE_A,
                outcome: ScriptedQueryOutcome::Address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 210))),
            },
        ]);
        let resolver = resolver_with_scripted_queries(vec![nameserver]);

        let addrs = run_scripted_resolution(resolver, "cname-deadline.flowio.invalid")
            .expect("completed CNAME follow-up address should win later family expiry");
        assert_eq!(
            addrs,
            [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 210), 5432))]
        );
        queries.assert_exhausted();
        clock.assert_exhausted();
    }

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
            test_query_packet(0xBEEF, "db.example.test", DNS_TYPE_A).expect("query encode");
        assert_eq!(
            read_u16_be_at(&packet, 0).expect("encoded query ID should fit"),
            0xBEEF
        );
    }

    #[test]
    fn query_name_limits_accept_253_presentation_and_255_wire_bytes() {
        let host = query_name_with_final_label(61);
        assert_eq!(host.len(), DNS_MAX_NAME_PRESENTATION_LEN);

        let packet = test_query_packet(0xBEEF, &host, DNS_TYPE_A)
            .expect("maximum-length query name should encode");
        assert_eq!(packet.len(), DNS_MAX_QUERY_PACKET_LEN);
        assert_eq!(packet.capacity(), DNS_MAX_QUERY_PACKET_LEN);
    }

    #[test]
    fn query_name_limits_reject_254_presentation_and_256_wire_bytes() {
        let host = query_name_with_final_label(62);
        assert_eq!(host.len(), DNS_MAX_NAME_PRESENTATION_LEN + 1);

        let err = test_query_packet(0xBEEF, &host, DNS_TYPE_A)
            .expect_err("overlong query name should fail before encoding");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn query_name_limits_retain_the_63_byte_label_boundary() {
        let valid = "a".repeat(63);
        test_query_packet(0xBEEF, &valid, DNS_TYPE_A).expect("63-byte DNS label should encode");

        let invalid = "a".repeat(64);
        let err = test_query_packet(0xBEEF, &invalid, DNS_TYPE_A)
            .expect_err("64-byte DNS label should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        for host in ["", ".example.test", "example..test", "example.test."] {
            let err = test_query_packet(0xBEEF, host, DNS_TYPE_A)
                .expect_err("empty DNS label should fail at the encoder boundary");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn query_name_limits_apply_after_single_root_dot_normalization() {
        assert_eq!(
            normalize_host("db.example.test.").expect("trailing dot should normalize"),
            "db.example.test"
        );
        assert_eq!(
            normalize_host("  db.example.test.  ")
                .expect("whitespace should be trimmed before the root dot"),
            "db.example.test"
        );
        for host in [
            "db.example.test..",
            "db.example.test...",
            "  db.example.test..  ",
            "..",
        ] {
            let err = normalize_host(host)
                .expect_err("more than one trailing root dot should remain invalid");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{host}");
        }
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
        test_query_packet(0xBEEF, normalized, DNS_TYPE_A)
            .expect("normalized maximum-length name should encode");
        let repeated_root = format!("{max_host}..");
        let err = normalize_host(&repeated_root)
            .expect_err("maximum name plus two root dots should remain invalid");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

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
    fn decode_name_materializes_direct_compressed_utf8_and_root_exactly() {
        let direct = b"\x03www\x08\xc3\xa9xample\x04test\0";
        let (name, consumed) = decode_name(direct, 0, 0).expect("direct UTF-8 name should decode");
        assert_eq!(name, "www.éxample.test");
        assert_eq!(consumed, direct.len());

        let mut compressed = b"\x08\xc3\xa9xample\x04test\0".to_vec();
        let compressed_offset = compressed.len();
        compressed.extend_from_slice(b"\x03api\xc0\x00");
        let (name, consumed) = decode_name(&compressed, compressed_offset, 0)
            .expect("compressed UTF-8 name should decode");
        assert_eq!(name, "api.éxample.test");
        assert_eq!(consumed, 6);

        let (name, consumed) =
            decode_name(&[0, 0xc0, 0], 1, 0).expect("pointer to root should decode");
        assert!(name.is_empty());
        assert_eq!(consumed, 2);
    }

    #[test]
    fn overlong_name_keeps_later_label_validation_precedence() {
        let mut prefix = Vec::new();
        for fill in *b"abc" {
            prefix.push(63);
            prefix.extend(std::iter::repeat_n(fill, 63));
        }

        let mut invalid_utf8 = prefix.clone();
        invalid_utf8.push(62);
        invalid_utf8.extend(std::iter::repeat_n(b'd', 61));
        invalid_utf8.push(0xff);
        invalid_utf8.push(0);
        let err = decode_name(&invalid_utf8, 0, 0)
            .expect_err("invalid UTF-8 should precede the final length error");
        assert_eq!(err.to_string(), "DNS label was not valid UTF-8");

        let mut literal_dot = prefix;
        literal_dot.push(62);
        literal_dot.extend(std::iter::repeat_n(b'd', 61));
        literal_dot.push(b'.');
        literal_dot.push(0);
        let err = decode_name(&literal_dot, 0, 0)
            .expect_err("literal dot should precede the final length error");
        assert_eq!(err.to_string(), "DNS literal label contained a dot");
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
                "literal-dot question",
                response_with_question_name(QUERY_ID, b"\x0bexample.com\0"),
                false,
            ),
            (
                "compressed literal-dot question",
                response_with_compressed_literal_dot_question(QUERY_ID),
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
    fn response_opcode_must_be_query_across_candidate_and_full_parsing() {
        const QUERY_ID: u16 = 0x1234;
        let shapes = [
            ("question with NOERROR", true, 0u8),
            ("question with SERVFAIL", true, DNS_RCODE_SERVFAIL),
            ("questionless SERVFAIL", false, DNS_RCODE_SERVFAIL),
        ];

        for opcode in 0u16..=15 {
            for (shape, include_question, rcode) in shapes {
                let mut packet = if include_question {
                    response_with_question_name(QUERY_ID, b"\x07example\x03com\0")
                } else {
                    response_header(QUERY_ID, DNS_FLAG_QR, 0)
                };
                let flags = DNS_FLAG_QR | (opcode << 11) | u16::from(rcode);
                write_u16_be_at(&mut packet, 2, flags).expect("test flags should fit");

                if opcode == 0 {
                    assert!(
                        response_is_decodable_candidate(&packet, QUERY_ID),
                        "QUERY candidate rejected {shape}"
                    );
                    parse_response_envelope(&packet, QUERY_ID)
                        .unwrap_or_else(|err| panic!("QUERY envelope rejected {shape}: {err}"));
                    match parse_response_packet(&packet, QUERY_ID, "example.com", DNS_TYPE_A) {
                        Ok(_) if rcode == 0 => {}
                        Err(err)
                            if rcode == DNS_RCODE_SERVFAIL
                                && err.kind() == io::ErrorKind::Other
                                && err.to_string().contains("response code 2") => {}
                        Ok(_) => panic!("QUERY {shape} ignored its response code"),
                        Err(err) => panic!("QUERY {shape} changed behavior: {err}"),
                    }
                    continue;
                }

                assert!(
                    !response_is_decodable_candidate(&packet, QUERY_ID),
                    "candidate accepted opcode {opcode} for {shape}"
                );
                for err in [
                    parse_response_envelope(&packet, QUERY_ID)
                        .err()
                        .unwrap_or_else(|| panic!("envelope accepted opcode {opcode} for {shape}")),
                    parse_response_packet(&packet, QUERY_ID, "example.com", DNS_TYPE_A)
                        .err()
                        .unwrap_or_else(|| {
                            panic!("full parser accepted opcode {opcode} for {shape}")
                        }),
                ] {
                    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                    assert_eq!(err.to_string(), "DNS response opcode was not QUERY");
                }
            }
        }
    }

    #[test]
    fn response_opcode_validation_preserves_header_precedence_and_precedes_body() {
        const QUERY_ID: u16 = 0x1234;
        const OPCODE_ONE: u16 = 1 << 11;

        let mut wrong_id = response_header(QUERY_ID.wrapping_add(1), DNS_FLAG_QR | OPCODE_ONE, 0);
        let err = parse_response_envelope(&wrong_id, QUERY_ID)
            .err()
            .expect("transaction ID mismatch should remain first");
        assert_eq!(err.to_string(), "DNS response ID did not match query ID");

        write_u16_be_at(&mut wrong_id, 0, QUERY_ID).expect("test query ID should fit");
        write_u16_be_at(&mut wrong_id, 2, OPCODE_ONE).expect("test flags should fit");
        let err = parse_response_envelope(&wrong_id, QUERY_ID)
            .err()
            .expect("QR rejection should precede opcode rejection");
        assert_eq!(
            err.to_string(),
            "DNS response packet was not marked as a response"
        );

        let mut truncated_question = response_header(QUERY_ID, DNS_FLAG_QR | OPCODE_ONE, 1);
        truncated_question.extend_from_slice(&[3, b'x']);
        assert_opcode_error(&truncated_question, QUERY_ID);

        let mut missing_record = response_with_question_name(QUERY_ID, b"\x07example\x03com\0");
        write_u16_be_at(
            &mut missing_record,
            2,
            DNS_FLAG_QR | OPCODE_ONE | u16::from(DNS_RCODE_NXDOMAIN),
        )
        .expect("test flags should fit");
        write_u16_be_at(&mut missing_record, 6, 1).expect("test Answer count should fit");
        assert_opcode_error(&missing_record, QUERY_ID);

        let mut truncated_query = response_with_question_name(QUERY_ID, b"\x07example\x03com\0");
        write_u16_be_at(&mut truncated_query, 2, DNS_FLAG_QR | DNS_FLAG_TC)
            .expect("test flags should fit");
        let err = parse_response_packet(&truncated_query, QUERY_ID, "example.com", DNS_TYPE_A)
            .err()
            .expect("QUERY truncation should remain rejected");
        assert_eq!(
            err.to_string(),
            "DNS response was truncated; TCP fallback is not implemented"
        );
    }

    fn assert_opcode_error(packet: &[u8], query_id: u16) {
        assert!(!response_is_decodable_candidate(packet, query_id));
        let err = parse_response_packet(packet, query_id, "example.com", DNS_TYPE_A)
            .err()
            .expect("non-QUERY opcode should fail before body parsing");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "DNS response opcode was not QUERY");
    }

    #[test]
    fn literal_dot_question_cannot_alias_two_wire_labels() {
        const QUERY_ID: u16 = 0x1234;

        let split_labels = response_with_question_name(QUERY_ID, b"\x07example\x03com\0");
        parse_response_packet(&split_labels, QUERY_ID, "example.com", DNS_TYPE_A)
            .expect("two wire labels should retain their dotted presentation");

        let cases = [
            (
                "direct one-label example.com",
                response_with_question_name(QUERY_ID, b"\x0bexample.com\0"),
                "example.com",
            ),
            (
                "compressed literal-dot label",
                response_with_compressed_literal_dot_question(QUERY_ID),
                "a.b",
            ),
        ];

        for (case, packet, query_host) in cases {
            assert!(
                !response_is_decodable_candidate(&packet, QUERY_ID),
                "candidate accepted {case}"
            );
            let err = match parse_response_packet(&packet, QUERY_ID, query_host, DNS_TYPE_A) {
                Ok(_) => panic!("literal-dot label aliased dotted presentation"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}");
            assert_eq!(
                err.to_string(),
                "DNS literal label contained a dot",
                "{case}"
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
    fn received_response_parser_excludes_stale_tail_bytes() {
        let buffer = response_with_single_test_rr(
            DnsRecordSection::Answer,
            0,
            true,
            DNS_TYPE_A,
            DNS_TYPE_A,
            DNS_CLASS_IN,
            &[192, 0, 2, 1],
        );
        let received_len = buffer.len() - 1;

        assert!(
            response_is_decodable_candidate(&buffer[..received_len], 0x1234),
            "the truncated record should pass the header-and-question prefilter"
        );
        assert!(
            parse_response_packet(&buffer, 0x1234, "db.example.test", DNS_TYPE_A).is_ok(),
            "the physical tail should complete the otherwise truncated record"
        );
        let err = match parse_received_response_packet(
            &buffer,
            received_len,
            0x1234,
            "db.example.test",
            DNS_TYPE_A,
        ) {
            Ok(_) => panic!("stale tail byte completed a truncated DNS response"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(err.to_string(), "DNS packet ended unexpectedly");
    }

    #[test]
    fn received_response_parser_rejects_length_beyond_buffer() {
        let buffer = response_with_single_test_rr(
            DnsRecordSection::Answer,
            0,
            true,
            DNS_TYPE_A,
            DNS_TYPE_A,
            DNS_CLASS_IN,
            &[192, 0, 2, 1],
        );

        let err = match parse_received_response_packet(
            &buffer,
            buffer.len() + 1,
            0x1234,
            "db.example.test",
            DNS_TYPE_A,
        ) {
            Ok(_) => panic!("out-of-bounds DNS receive length was accepted"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            err.to_string(),
            "DNS receive length exceeded response buffer"
        );
    }

    #[test]
    fn ignored_section_name_validation_precedes_filtering_and_rcode() {
        let malformed_name = [1, 0xff, 0];
        let question_owner = 0xC00Cu16.to_be_bytes();
        let root = [0u8];
        let malformed_sites: [(&str, &[u8], &[u8]); 2] = [
            ("owner", &malformed_name, &root),
            ("CNAME target", &question_owner, &malformed_name),
        ];

        for (rcode_label, rcode) in [("NOERROR", 0), ("NXDOMAIN", DNS_RCODE_NXDOMAIN)] {
            for (section_label, section) in [
                ("Authority", DnsRecordSection::Authority),
                ("Additional", DnsRecordSection::Additional),
            ] {
                for (site, owner, target) in malformed_sites {
                    let context = format!("{rcode_label} {section_label} malformed {site}");
                    let packet = response_with_single_wire_test_rr(section, rcode, owner, target);
                    let err =
                        match parse_response_packet(&packet, 0x1234, "db.example.test", DNS_TYPE_A)
                        {
                            Ok(_) => panic!("{context} unexpectedly passed validation"),
                            Err(err) => err,
                        };
                    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{context}");
                    assert_eq!(
                        err.to_string(),
                        "DNS label was not valid UTF-8",
                        "{context}"
                    );
                }
            }
        }
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
                        let parsed = parse_response_packet(
                            &packet,
                            0x1234,
                            "db.example.test",
                            shape.query_type,
                        );

                        let contributes = rcode == 0
                            && section == DnsRecordSection::Answer
                            && class == DNS_CLASS_IN;
                        if shape.rr_type == DNS_TYPE_CNAME && contributes {
                            let err = match parsed {
                                Ok(_) => panic!("{context} root CNAME entered follow-up selection"),
                                Err(err) => err,
                            };
                            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{context}");
                            assert_eq!(
                                err.to_string(),
                                "DNS response CNAME target was the root name",
                                "{context}"
                            );
                            continue;
                        }

                        let result = parsed
                            .unwrap_or_else(|err| panic!("{context} failed validation: {err}"));
                        assert_eq!(result.nx_domain, rcode == DNS_RCODE_NXDOMAIN, "{context}");
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
            (
                "direct literal-dot label",
                b"\x0bexample.com\0".to_vec(),
                0,
                false,
            ),
            (
                "compressed literal-dot label",
                b"\x0bexample.com\0\xc0\x00".to_vec(),
                13,
                false,
            ),
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
            let skipped = walk_dns_name(&packet, offset, 0);
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
                DnsNameWalkError::LiteralDotLabel,
                io::ErrorKind::InvalidData,
                "DNS literal label contained a dot",
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

    #[test]
    fn literal_dot_cname_target_is_rejected_before_chain_selection_or_rcode() {
        for rcode in [0, DNS_RCODE_NXDOMAIN] {
            let packet = response_with_single_test_rr(
                DnsRecordSection::Answer,
                rcode,
                true,
                DNS_TYPE_A,
                DNS_TYPE_CNAME,
                DNS_CLASS_IN,
                b"\x0bexample.com\0",
            );
            let err = match parse_response_packet(&packet, 0x1234, "db.example.test", DNS_TYPE_A) {
                Ok(_) => {
                    panic!("literal-dot CNAME entered chain selection or RCODE handling")
                }
                Err(err) => err,
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "DNS literal label contained a dot");
        }

        let packet = response_with_compressed_literal_dot_cname();
        let err = match parse_response_packet(&packet, 0x1234, "db.example.test", DNS_TYPE_A) {
            Ok(_) => panic!("compressed literal-dot CNAME target entered chain selection"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "DNS literal label contained a dot");
    }

    #[test]
    fn reachable_root_cname_target_is_upstream_invalid_data() {
        let direct_root = [0];
        // `db.example.test` starts at byte 12; its terminal root is byte 28.
        let compressed_root = 0xC01Cu16.to_be_bytes();

        for (encoding, target) in [
            ("direct", direct_root.as_slice()),
            ("compressed", compressed_root.as_slice()),
        ] {
            let packet = response_with_single_test_rr(
                DnsRecordSection::Answer,
                0,
                true,
                DNS_TYPE_A,
                DNS_TYPE_CNAME,
                DNS_CLASS_IN,
                target,
            );
            let err = match parse_response_packet(&packet, 0x1234, "db.example.test", DNS_TYPE_A) {
                Ok(_) => panic!("{encoding} root CNAME target entered follow-up selection"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{encoding}");
            assert_eq!(
                err.to_string(),
                "DNS response CNAME target was the root name",
                "{encoding}"
            );
        }
    }

    #[test]
    fn literal_dot_rr_owner_is_rejected_before_selection_or_rcode() {
        for rcode in [0, DNS_RCODE_NXDOMAIN] {
            let packet = response_with_literal_dot_rr_owner(rcode);
            let err = match parse_response_packet(&packet, 0x1234, "db.example.test", DNS_TYPE_A) {
                Ok(_) => panic!("literal-dot RR owner entered record selection or RCODE handling"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "DNS literal label contained a dot");
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

    fn response_with_compressed_literal_dot_question(query_id: u16) -> Vec<u8> {
        let mut packet = response_header(query_id, DNS_FLAG_QR, 1);
        // The question points backward into header count bytes that encode one
        // structurally valid literal label `a.b` followed by a root octet.
        packet[6..10].copy_from_slice(&[3, b'a', b'.', b'b']);
        packet.extend_from_slice(&0xC006u16.to_be_bytes());
        packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
        packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
        packet
    }

    fn response_with_compressed_literal_dot_cname() -> Vec<u8> {
        let mut packet = response_header(0x1234, DNS_FLAG_QR, 1);
        write_u16_be_at(&mut packet, 6, 2).expect("test Answer count should fit");
        push_test_wire_name(&mut packet, "db.example.test");
        packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
        packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());

        let literal_dot_name = b"\x0bexample.com\0";
        let literal_dot_offset = packet.len() + 12;
        assert!(
            literal_dot_offset < 0x4000,
            "test compression target should fit"
        );
        push_single_test_rr(
            &mut packet,
            true,
            16,
            DNS_CLASS_IN,
            u16::try_from(literal_dot_name.len()).expect("test RDATA length should fit"),
            literal_dot_name,
        );

        let pointer = (0xC000 | literal_dot_offset as u16).to_be_bytes();
        push_single_test_rr(
            &mut packet,
            true,
            DNS_TYPE_CNAME,
            DNS_CLASS_IN,
            u16::try_from(pointer.len()).expect("test CNAME RDATA length should fit"),
            &pointer,
        );
        packet
    }

    fn response_with_literal_dot_rr_owner(rcode: u8) -> Vec<u8> {
        let mut packet = response_header(0x1234, DNS_FLAG_QR | u16::from(rcode), 1);
        write_u16_be_at(&mut packet, 6, 1).expect("test Answer count should fit");
        push_test_wire_name(&mut packet, "db.example.test");
        packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
        packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());

        packet.extend_from_slice(b"\x0bexample.com\0");
        packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
        packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
        packet.extend_from_slice(&0u32.to_be_bytes());
        packet.extend_from_slice(&4u16.to_be_bytes());
        packet.extend_from_slice(&[192, 0, 2, 1]);
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

    fn response_with_single_wire_test_rr(
        section: DnsRecordSection,
        rcode: u8,
        owner: &[u8],
        cname_target: &[u8],
    ) -> Vec<u8> {
        let mut packet = response_header(0x1234, DNS_FLAG_QR | u16::from(rcode), 1);
        let count_offset = match section {
            DnsRecordSection::Answer => 6,
            DnsRecordSection::Authority => 8,
            DnsRecordSection::Additional => 10,
        };
        write_u16_be_at(&mut packet, count_offset, 1)
            .expect("test section count should fit in the DNS header");

        push_test_wire_name(&mut packet, "db.example.test");
        packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
        packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
        packet.extend_from_slice(owner);
        packet.extend_from_slice(&DNS_TYPE_CNAME.to_be_bytes());
        packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
        packet.extend_from_slice(&0u32.to_be_bytes());
        packet.extend_from_slice(
            &u16::try_from(cname_target.len())
                .expect("test CNAME RDATA length should fit")
                .to_be_bytes(),
        );
        packet.extend_from_slice(cname_target);
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

        extend_unique_socket_addrs(&mut addrs, &ips, port)
            .expect("four unique addresses should remain within the result bound");

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
    fn dns_family_merge_accepts_64_unique_addresses_and_rejects_the_65th() {
        let port = 5432;
        let ipv4 = (1..=32)
            .map(|octet| IpAddr::V4(Ipv4Addr::new(192, 0, 2, octet)))
            .collect::<Vec<_>>();
        let ipv6 = (1..=32)
            .map(|suffix| IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, suffix)))
            .collect::<Vec<_>>();
        let expected = ipv4
            .iter()
            .chain(&ipv6)
            .map(|ip| SocketAddr::new(*ip, port))
            .collect::<Vec<_>>();

        let mut ipv4_with_duplicate = ipv4.clone();
        ipv4_with_duplicate.push(ipv4[0]);
        let mut addrs = Vec::new();
        let step = finish_dns_family_lookups(
            "db.example.test",
            port,
            &mut addrs,
            Ok(LookupResult {
                addresses: ipv4_with_duplicate,
                cname: None,
                cname_hops: 0,
                nx_domain: false,
            }),
            Ok(LookupResult {
                addresses: ipv6.clone(),
                cname: None,
                cname_hops: 0,
                nx_domain: false,
            }),
        )
        .expect("64 unique DNS addresses should be accepted");
        assert!(matches!(step, ResolveHostStep::Resolved));
        assert_eq!(addrs, expected);

        let mut over_limit_ipv6 = ipv6;
        over_limit_ipv6.push(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 33)));
        let mut over_limit_addrs = Vec::new();
        let err = finish_dns_family_lookups(
            "db.example.test",
            port,
            &mut over_limit_addrs,
            Ok(LookupResult {
                addresses: ipv4,
                cname: None,
                cname_hops: 0,
                nx_domain: false,
            }),
            Ok(LookupResult {
                addresses: over_limit_ipv6,
                cname: None,
                cname_hops: 0,
                nx_domain: false,
            }),
        )
        .err()
        .expect("a 65th unique DNS address should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            err.to_string(),
            "resolver result exceeds 64 unique addresses"
        );
    }

    #[test]
    fn dns_family_outcomes_use_a_first_recoverable_error() {
        let mut addrs = Vec::new();
        let result = finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Err(DnsLookupError::Recoverable(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "A lookup failed",
            ))),
            Err(DnsLookupError::Recoverable(io::Error::new(
                io::ErrorKind::TimedOut,
                "AAAA lookup timed out",
            ))),
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
            Err(DnsLookupError::Recoverable(io::Error::other("A SERVFAIL"))),
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
            Err(DnsLookupError::Terminal(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "timer allocation failed",
            ))),
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
            Err(DnsLookupError::Terminal(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "timer allocation failed",
            ))),
        )
        .expect("usable address should survive a later terminal family error");

        assert!(matches!(step, ResolveHostStep::Resolved));
        assert_eq!(addrs, vec![SocketAddr::new(address, 5432)]);
    }

    #[test]
    fn dns_family_outcomes_keep_address_ahead_of_later_total_expiry() {
        let address = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 209));
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
            Err(DnsLookupError::TotalTimeout),
        )
        .expect("completed A address should survive later aggregate expiry");

        assert!(matches!(step, ResolveHostStep::Resolved));
        assert_eq!(addrs, vec![SocketAddr::new(address, 5432)]);
    }

    #[test]
    fn dns_family_outcomes_make_total_expiry_terminal_without_an_address() {
        let mut addrs = Vec::new();
        let err = match finish_dns_family_lookups(
            "db.example.test",
            5432,
            &mut addrs,
            Err(DnsLookupError::Recoverable(io::Error::other(
                "A lookup failed",
            ))),
            Err(DnsLookupError::TotalTimeout),
        ) {
            Ok(_) => panic!("aggregate expiry should beat an earlier recoverable error"),
            Err(err) => err,
        };

        assert!(is_total_query_timeout(&err));
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }
}
