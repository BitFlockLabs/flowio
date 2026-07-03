//! Hostname resolution helpers for FlowIO transports.
//!
//! The resolver keeps the public surface deliberately small:
//! - IP literals resolve directly without DNS traffic
//! - `localhost` and `/etc/hosts` entries are honored first
//! - all other names are resolved through UDP DNS queries using FlowIO's own
//!   transport and timer APIs
//!
//! DNS lookup is intentionally narrow in this first version:
//! - system configuration is read from `/etc/resolv.conf`
//! - only A and AAAA lookups are issued
//! - CNAME chains are followed up to a small fixed depth
//! - search domains and TCP fallback for truncated replies are not yet
//!   implemented
//!
//! # Fast-Path Guidance
//!
//! Best fast-path-adjacent choices:
//! - Resolver APIs are setup/control-plane helpers rather than steady-state
//!   data-plane APIs. Resolve host names once, keep the resulting
//!   `SocketAddr` values, and pass those addresses into transport connectors
//!   on the hot path.
//! - Use [`DnsResolver`] when resolving repeatedly so nameserver selection and
//!   timeout policy are constructed once and then reused.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to do DNS lookup in the steady-state data path. Reuse the
//!   resolved `SocketAddr` values instead.
//! - Prefer not to construct a fresh resolver for every repeated lookup. Use
//!   [`DnsResolver`] instead of the convenience [`resolve_host`] helper.
//!
//! # Example
//! ```no_run
//! use flowio::net::resolver::resolve_host;
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let addrs = resolve_host("localhost", 5432).await.unwrap();
//!     assert!(!addrs.is_empty());
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use crate::net::udp::UdpSocket;
use crate::runtime::buffer::bytes::{
    BufferCursorMut, BufferRangeError, read_u16_be_at, write_u16_be_at,
};
use crate::runtime::timer::{Elapsed, timeout};
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
const DNS_RCODE_NXDOMAIN: u8 = 3;
const DNS_MAX_NAME_PRESENTATION_LEN: usize = 253;
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_CNAME_DEPTH: usize = 4;
const RESOLV_CONF_PATH: &str = "/etc/resolv.conf";
const HOSTS_PATH: &str = "/etc/hosts";

static QUERY_ID_STATE: AtomicU64 = AtomicU64::new(0);

/// Reusable DNS resolver built on FlowIO UDP sockets.
///
/// Use this on setup/control-plane paths when lookups repeat and the configured
/// nameservers/timeouts should be reused. For one-off convenience resolution,
/// prefer [`resolve_host`].
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
    /// Timeout applied to each individual upstream query attempt.
    query_timeout: Duration,
}

impl DnsResolver {
    /// Builds a resolver from `/etc/resolv.conf`.
    ///
    /// This reads system resolver configuration once and stores the resulting
    /// nameserver list inside the resolver for reuse across lookups.
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

    /// Sets the timeout applied to each upstream DNS query attempt.
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
        for _ in 0..=MAX_CNAME_DEPTH {
            match self
                .gather_dns_addresses(&current, port, &mut addrs)
                .await?
            {
                ResolveHostStep::Resolved => return Ok(addrs),
                ResolveHostStep::FollowCname(next) => current = next,
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS CNAME chain exceeded maximum depth",
        ))
    }

    async fn lookup_name(&self, host: &str, qtype: u16) -> io::Result<LookupResult> {
        let query_id = next_query_id();
        let packet = encode_query_packet(query_id, host, qtype)?;
        let mut last_err = None;

        for nameserver in self.nameservers.iter().copied() {
            match self.query_nameserver(nameserver, &packet, query_id).await {
                Ok(response) => match parse_response_packet(&response, query_id, host, qtype) {
                    Ok(result) => return Ok(result),
                    Err(err) => last_err = Some(err),
                },
                Err(err) => last_err = Some(err),
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
    ) -> io::Result<Vec<u8>> {
        let mut socket = UdpSocket::bind(unspecified_addr(nameserver))?;
        socket.connect(nameserver)?;

        let (send_result, _) = socket.send(packet.to_vec()).await;
        send_result?;
        timeout(self.query_timeout, async {
            loop {
                let recv = vec![0u8; 2048];
                let (recv_result, recv) = socket.recv(recv, 2048).await;
                let recv_len = recv_result?;
                let response = &recv[..recv_len];
                if response_matches_query_id(response, query_id)? {
                    return Ok(response.to_vec());
                }
            }
        })
        .await
        .map_err(timeout_error)?
    }

    async fn gather_dns_addresses(
        &self,
        current: &str,
        port: u16,
        addrs: &mut Vec<SocketAddr>,
    ) -> io::Result<ResolveHostStep> {
        let a = self.lookup_name(current, DNS_TYPE_A).await?;
        if a.nx_domain {
            return Err(host_not_found(current));
        }

        extend_unique_socket_addrs(addrs, &a.addresses, port);
        let aaaa = self.lookup_name(current, DNS_TYPE_AAAA).await?;
        if aaaa.nx_domain {
            if !addrs.is_empty() {
                return Ok(ResolveHostStep::Resolved);
            }
            return Err(host_not_found(current));
        }

        extend_unique_socket_addrs(addrs, &aaaa.addresses, port);
        if !addrs.is_empty() {
            return Ok(ResolveHostStep::Resolved);
        }

        if let Some(next) = a.cname.or(aaaa.cname) {
            Ok(ResolveHostStep::FollowCname(next))
        } else {
            Err(host_not_found(current))
        }
    }
}

/// Resolves a host name into socket addresses using system resolver settings.
///
/// This is the convenience resolver entry point. For repeated lookups, prefer
/// constructing and reusing a [`DnsResolver`] instead so nameserver selection
/// and timeout policy are built once.
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
    /// True when the upstream resolver returned NXDOMAIN for this name.
    nx_domain: bool,
}

enum ResolveHostStep {
    Resolved,
    FollowCname(String),
}

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

fn timeout_error(_: Elapsed) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "DNS query timed out")
}

fn host_not_found(host: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("host not found: {host}"))
}

fn byte_range_eof(err: BufferRangeError) -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, err)
}

fn response_matches_query_id(packet: &[u8], query_id: u16) -> io::Result<bool> {
    let response_id = read_u16_be_at(packet, 0).map_err(byte_range_eof)?;
    Ok(response_id == query_id)
}

fn extend_unique_socket_addrs(addrs: &mut Vec<SocketAddr>, ips: &[IpAddr], port: u16) {
    let mut deduper = SocketAddrDeduper::from_existing(addrs);
    for ip in ips {
        let addr = SocketAddr::new(*ip, port);
        deduper.push_unique(addrs, addr);
    }
}

struct SocketAddrDeduper {
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

pub(crate) fn parse_response_packet(
    packet: &[u8],
    query_id: u16,
    query_host: &str,
    qtype: u16,
) -> io::Result<LookupResult> {
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
    if flags & DNS_FLAG_TC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response was truncated; TCP fallback is not implemented",
        ));
    }

    let rcode = (flags & DNS_RCODE_MASK) as u8;
    if rcode == DNS_RCODE_NXDOMAIN {
        return Ok(LookupResult {
            addresses: Vec::new(),
            cname: None,
            nx_domain: true,
        });
    }
    if rcode != 0 {
        return Err(io::Error::other(format!(
            "DNS server returned response code {rcode}"
        )));
    }

    let qdcount = read_u16_be_at(packet, 4).map_err(byte_range_eof)? as usize;
    let ancount = read_u16_be_at(packet, 6).map_err(byte_range_eof)? as usize;
    let nscount = read_u16_be_at(packet, 8).map_err(byte_range_eof)? as usize;
    let arcount = read_u16_be_at(packet, 10).map_err(byte_range_eof)? as usize;

    let mut offset = 12usize;
    if qdcount != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response question count did not match query",
        ));
    }
    let (question_name, consumed) = decode_name(packet, offset, 0)?;
    if !dns_name_eq(&question_name, query_host) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response question name did not match query",
        ));
    }
    offset = checked_add(offset, consumed, packet.len())?;
    let question_type = read_u16_be_at(packet, offset).map_err(byte_range_eof)?;
    let question_class_offset = checked_add(offset, 2, packet.len())?;
    let question_class = read_u16_be_at(packet, question_class_offset).map_err(byte_range_eof)?;
    if question_type != qtype || question_class != DNS_CLASS_IN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response question type/class did not match query",
        ));
    }
    offset = checked_add(offset, 4, packet.len())?;

    let total_rrs = ancount + nscount + arcount;
    let max_rrs_by_packet = packet.len().saturating_sub(offset) / 11;
    let mut records = Vec::with_capacity(total_rrs.min(max_rrs_by_packet));
    for _ in 0..total_rrs {
        let (owner, consumed) = decode_name(packet, offset, 0)?;
        offset = checked_add(offset, consumed, packet.len())?;
        let rr = parse_rr_header(packet, offset)?;
        offset = rr.data_offset + rr.rdlength as usize;

        if rr.class != DNS_CLASS_IN {
            continue;
        }

        match rr.rr_type {
            DNS_TYPE_A if rr.rdlength == 4 => {
                let data = &packet[rr.data_offset..rr.data_offset + 4];
                records.push(DnsRecord::Address {
                    owner,
                    rr_type: rr.rr_type,
                    address: IpAddr::V4(Ipv4Addr::new(data[0], data[1], data[2], data[3])),
                });
            }
            DNS_TYPE_AAAA if rr.rdlength == 16 => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&packet[rr.data_offset..rr.data_offset + 16]);
                records.push(DnsRecord::Address {
                    owner,
                    rr_type: rr.rr_type,
                    address: IpAddr::V6(Ipv6Addr::from(octets)),
                });
            }
            DNS_TYPE_CNAME => {
                let (target, consumed) = decode_name(packet, rr.data_offset, 0)?;
                if consumed > rr.rdlength as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "DNS CNAME RDATA exceeded declared length",
                    ));
                }
                records.push(DnsRecord::Cname { owner, target });
            }
            _ => {}
        }
    }

    let mut addresses = Vec::new();
    let mut active_owner = query_host;
    let mut cname = None;
    let mut cname_hops = 0usize;
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

        cname_hops += 1;
        if cname_hops > MAX_CNAME_DEPTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS CNAME chain exceeded maximum depth",
            ));
        }
        active_owner = target;
        cname = Some(target.clone());
    }

    Ok(LookupResult {
        addresses,
        cname,
        nx_domain: false,
    })
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

pub(crate) fn decode_name(
    packet: &[u8],
    offset: usize,
    depth: usize,
) -> io::Result<(String, usize)> {
    if depth > MAX_CNAME_DEPTH + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS name compression exceeded maximum depth",
        ));
    }
    if offset >= packet.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "DNS name offset exceeded packet length",
        ));
    }

    let mut labels = Vec::new();
    let mut pos = offset;
    let mut consumed = 0usize;

    loop {
        if pos >= packet.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "DNS name exceeded packet length",
            ));
        }

        let len = packet[pos];
        if len & 0xC0 == 0xC0 {
            let next = checked_add(pos, 1, packet.len())?;
            let next_byte = *packet.get(next).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "DNS compression pointer ended unexpectedly",
                )
            })?;
            let pointer = (((len & 0x3F) as usize) << 8) | next_byte as usize;
            if pointer >= pos {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DNS compression pointer did not point backward",
                ));
            }
            consumed += 2;
            let (suffix, _) = decode_name(packet, pointer, depth + 1)?;
            if !suffix.is_empty() {
                labels.push(suffix);
            }
            break;
        }

        if len == 0 {
            consumed += 1;
            break;
        }

        if len & 0xC0 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS label used an unsupported length encoding",
            ));
        }

        let label_len = len as usize;
        let label_start = checked_add(pos, 1, packet.len())?;
        let label_end = checked_add(label_start, label_len, packet.len())?;
        labels.push(String::from_utf8_lossy(&packet[label_start..label_end]).into_owned());
        consumed += 1 + label_len;
        pos = label_end;
    }

    let name = labels.join(".");
    if name.len() > DNS_MAX_NAME_PRESENTATION_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS name exceeded maximum length",
        ));
    }

    Ok((name, consumed))
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
}
