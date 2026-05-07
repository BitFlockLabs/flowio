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
//! - Resolver APIs are setup-path helpers rather than steady-state data-plane
//!   APIs. Resolve host names once, keep the resulting `SocketAddr` values,
//!   and pass those addresses into transport connectors on the hot path.
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
use std::fs;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

const DNS_PORT: u16 = 53;
const DNS_CLASS_IN: u16 = 1;
const DNS_TYPE_A: u16 = 1;
const DNS_TYPE_CNAME: u16 = 5;
const DNS_TYPE_AAAA: u16 = 28;
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_CNAME_DEPTH: usize = 4;
const RESOLV_CONF_PATH: &str = "/etc/resolv.conf";
const HOSTS_PATH: &str = "/etc/hosts";

static NEXT_QUERY_ID: AtomicU16 = AtomicU16::new(1);

/// Narrow DNS resolver built on FlowIO UDP sockets.
///
/// This is the reusable resolver API. Use it when resolution happens often
/// enough that reusing configured nameservers and timeouts matters.
///
/// This is the best resolver API to use on the setup path when lookups repeat.
/// For one-off convenience resolution, prefer [`resolve_host`] instead.
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
            let a = self.lookup_name(&current, DNS_TYPE_A).await?;
            if a.nx_domain {
                return Err(host_not_found(&current));
            }

            extend_unique_socket_addrs(&mut addrs, &a.addresses, port);
            if !addrs.is_empty() {
                let aaaa = self.lookup_name(&current, DNS_TYPE_AAAA).await?;
                if aaaa.nx_domain {
                    return Err(host_not_found(&current));
                }
                extend_unique_socket_addrs(&mut addrs, &aaaa.addresses, port);
                return Ok(addrs);
            }

            let aaaa = self.lookup_name(&current, DNS_TYPE_AAAA).await?;
            if aaaa.nx_domain {
                return Err(host_not_found(&current));
            }

            extend_unique_socket_addrs(&mut addrs, &aaaa.addresses, port);
            if !addrs.is_empty() {
                return Ok(addrs);
            }

            if let Some(next) = a.cname.or(aaaa.cname) {
                current = next;
                continue;
            }

            return Err(host_not_found(&current));
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
            match self.query_nameserver(nameserver, &packet).await {
                Ok(response) => return parse_response_packet(&response, query_id, qtype),
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

    async fn query_nameserver(&self, nameserver: SocketAddr, packet: &[u8]) -> io::Result<Vec<u8>> {
        let mut socket = UdpSocket::bind(unspecified_addr(nameserver))?;
        socket.connect(nameserver)?;

        let (send_result, _) = socket.send(packet.to_vec()).await;
        send_result?;
        let recv = vec![0u8; 2048];
        let (recv_result, recv) = timeout(self.query_timeout, socket.recv(recv, 2048))
            .await
            .map_err(timeout_error)?;
        let recv_len = recv_result?;
        Ok(recv[..recv_len].to_vec())
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
struct LookupResult {
    /// Addresses returned directly for the requested record type.
    addresses: Vec<IpAddr>,
    /// First CNAME target seen while answering the current query, if any.
    cname: Option<String>,
    /// True when the upstream resolver returned NXDOMAIN for this name.
    nx_domain: bool,
}

fn next_query_id() -> u16 {
    NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
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

    if host.eq_ignore_ascii_case("localhost") {
        addrs.push(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
        addrs.push(SocketAddr::from((Ipv6Addr::LOCALHOST, port)));
    }

    for addr in read_hosts_file(HOSTS_PATH, host, port)? {
        if !addrs.contains(&addr) {
            addrs.push(addr);
        }
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
            if !addrs.contains(&socket) {
                addrs.push(socket);
            }
        }
    }

    Ok(addrs)
}

fn read_resolv_conf(path: &str) -> io::Result<Vec<SocketAddr>> {
    let contents = fs::read_to_string(path)?;
    let mut nameservers = Vec::new();

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
        if !nameservers.contains(&socket) {
            nameservers.push(socket);
        }
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

fn extend_unique_socket_addrs(addrs: &mut Vec<SocketAddr>, ips: &[IpAddr], port: u16) {
    for ip in ips {
        let addr = SocketAddr::new(*ip, port);
        if !addrs.contains(&addr) {
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

fn parse_response_packet(packet: &[u8], query_id: u16, qtype: u16) -> io::Result<LookupResult> {
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
    if flags & 0x8000 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response packet was not marked as a response",
        ));
    }
    if flags & 0x0200 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS response was truncated; TCP fallback is not implemented",
        ));
    }

    let rcode = (flags & 0x000F) as u8;
    if rcode == 3 {
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
    for _ in 0..qdcount {
        offset = skip_name(packet, offset)?;
        offset = checked_add(offset, 4, packet.len())?;
    }

    let mut addresses = Vec::new();
    let mut cname = None;
    let total_rrs = ancount + nscount + arcount;
    for _ in 0..total_rrs {
        offset = skip_name(packet, offset)?;
        let rr = parse_rr_header(packet, offset)?;
        offset = rr.data_offset + rr.rdlength as usize;

        if rr.class != DNS_CLASS_IN {
            continue;
        }

        match rr.rr_type {
            DNS_TYPE_A if qtype == DNS_TYPE_A && rr.rdlength == 4 => {
                let data = &packet[rr.data_offset..rr.data_offset + 4];
                addresses.push(IpAddr::V4(Ipv4Addr::new(
                    data[0], data[1], data[2], data[3],
                )));
            }
            DNS_TYPE_AAAA if qtype == DNS_TYPE_AAAA && rr.rdlength == 16 => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&packet[rr.data_offset..rr.data_offset + 16]);
                addresses.push(IpAddr::V6(Ipv6Addr::from(octets)));
            }
            DNS_TYPE_CNAME if cname.is_none() => {
                let (target, _) = decode_name(packet, rr.data_offset, 0)?;
                cname = Some(target);
            }
            _ => {}
        }
    }

    Ok(LookupResult {
        addresses,
        cname,
        nx_domain: false,
    })
}

/// Reads and validates the fixed-size header fields for one DNS resource
/// record body.
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
    let class = read_u16_be_at(packet, offset + 2).map_err(byte_range_eof)?;
    let rdlength = read_u16_be_at(packet, offset + 8).map_err(byte_range_eof)?;
    let data_offset = end;
    checked_add(data_offset, rdlength as usize, packet.len())?;

    Ok(RrHeader {
        rr_type,
        class,
        rdlength,
        data_offset,
    })
}

fn skip_name(packet: &[u8], offset: usize) -> io::Result<usize> {
    let (_, consumed) = decode_name(packet, offset, 0)?;
    checked_add(offset, consumed, packet.len())
}

fn decode_name(packet: &[u8], offset: usize, depth: usize) -> io::Result<(String, usize)> {
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
    let mut jumped = false;

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
            let pointer = (((len & 0x3F) as usize) << 8) | packet[next] as usize;
            if !jumped {
                consumed += 2;
            }
            let (suffix, _) = decode_name(packet, pointer, depth + 1)?;
            if !suffix.is_empty() {
                labels.push(suffix);
            }
            break;
        }

        if len == 0 {
            if !jumped {
                consumed += 1;
            }
            break;
        }

        let label_len = len as usize;
        let label_start = checked_add(pos, 1, packet.len())?;
        let label_end = checked_add(label_start, label_len, packet.len())?;
        labels.push(String::from_utf8_lossy(&packet[label_start..label_end]).into_owned());
        if !jumped {
            consumed += 1 + label_len;
        }
        pos = label_end;
        jumped = false;
    }

    Ok((labels.join("."), consumed))
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
