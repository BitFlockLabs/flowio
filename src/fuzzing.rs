//! Fuzzing-only re-exports of internal parsers.
//!
//! Gated behind the dev-only `fuzzing` feature so the real public API is
//! unchanged. These wrappers exist purely so the out-of-source `flowio/fuzz/`
//! crate (a separate package) can reach parsers that are otherwise
//! crate-private. Not a stable API; do not depend on this module.
//!
//! Each wrapper takes raw bytes and exercises an internal parser. All targets
//! enforce no panic, out-of-bounds access, or unbounded termination; the DNS
//! prefilter target also asserts differential structural-acceptance parity
//! with the full response-envelope parser.

use std::borrow::Cow;

fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn maybe_decode_hex_seed(data: &[u8]) -> Cow<'_, [u8]> {
    let Some(hex) = data.strip_prefix(b"HEX:") else {
        return Cow::Borrowed(data);
    };
    let hex_end = hex
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(0, |index| index + 1);
    let hex = &hex[..hex_end];
    if hex.len() % 2 != 0 {
        return Cow::Borrowed(data);
    }

    let mut out = Vec::with_capacity(hex.len() / 2);
    for pair in hex.chunks_exact(2) {
        let Some(hi) = hex_val(pair[0]) else {
            return Cow::Borrowed(data);
        };
        let Some(lo) = hex_val(pair[1]) else {
            return Cow::Borrowed(data);
        };
        out.push((hi << 4) | lo);
    }
    Cow::Owned(out)
}

/// Fuzz entry: decode a DNS name from arbitrary packet bytes.
pub fn dns_decode_name(data: &[u8]) {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    if data.is_empty() {
        return;
    }
    if let Ok(host) = std::str::from_utf8(data) {
        let _ = crate::net::resolver::validate_query_name(host);
    }
    let offset = data[0] as usize % data.len();
    let _ = crate::net::resolver::decode_name(data, 0, 0);
    let _ = crate::net::resolver::decode_name(data, offset, 0);
}

const DNS_QUERY_ID_MISMATCH_CONTROL: u8 = 0x80;

struct DnsResponseCase<'a> {
    length_hint: u16,
    query_id: u16,
    qtype: u16,
    host: &'a str,
    packet: &'a [u8],
}

/// Decode `[length/id hint, control, host length, host, packet]`.
///
/// Normal cases derive the expected ID from the response packet so packet
/// mutations continue past the ID gate. Control bit 7 deliberately mismatches
/// it, preserving coverage of that rejection path. The two-byte hint remains a
/// fallback for packets shorter than an ID and drives the received-length
/// target below.
fn dns_response_case(data: &[u8]) -> DnsResponseCase<'_> {
    let length_hint = u16::from_be_bytes([
        data.first().copied().unwrap_or_default(),
        data.get(1).copied().unwrap_or_default(),
    ]);
    let control = data.get(2).copied().unwrap_or_default();
    let qtype = if control & 1 == 0 { 1 } else { 28 };

    let available = data.len().saturating_sub(4);
    let host_len = data
        .get(3)
        .map(|len| (*len as usize).min(available))
        .unwrap_or_default();
    let host_bytes = &data.get(4..4 + host_len).unwrap_or_default();
    let host = std::str::from_utf8(host_bytes)
        .ok()
        .filter(|host| !host.is_empty())
        .unwrap_or("example.com");
    let packet = data.get(4 + host_len..).unwrap_or_default();
    let packet_id = u16::from_be_bytes([
        packet.first().copied().unwrap_or_default(),
        packet.get(1).copied().unwrap_or_default(),
    ]);
    let mut query_id = if packet.len() >= 2 {
        packet_id
    } else {
        length_hint
    };
    if control & DNS_QUERY_ID_MISMATCH_CONTROL != 0 {
        query_id ^= 0x5555;
    }

    DnsResponseCase {
        length_hint,
        query_id,
        qtype,
        host,
        packet,
    }
}

fn observe_dns_parse_response_packet(data: &[u8]) -> std::io::Result<()> {
    let data = maybe_decode_hex_seed(data);
    let case = dns_response_case(data.as_ref());
    crate::net::resolver::parse_response_packet(case.packet, case.query_id, case.host, case.qtype)
        .map(drop)
}

#[cfg(any(test, feature = "test-support"))]
fn dns_response_case_reaches_records(data: &[u8]) -> bool {
    let data = maybe_decode_hex_seed(data);
    let case = dns_response_case(data.as_ref());
    crate::net::resolver::response_reaches_record_parser(
        case.packet,
        case.query_id,
        case.host,
        case.qtype,
    )
}

/// Fuzz entry: parse a DNS response packet with metadata derived from input.
pub fn dns_parse_response_packet(data: &[u8]) {
    let _ = std::hint::black_box(observe_dns_parse_response_packet(data));
}

fn observe_dns_parse_received_response_packet(data: &[u8]) -> std::io::Result<()> {
    let data = maybe_decode_hex_seed(data);
    let case = dns_response_case(data.as_ref());
    let packet_len = case
        .packet
        .len()
        .min(crate::net::resolver::DNS_UDP_RESPONSE_BUFFER_SIZE);
    let buffer = &case.packet[..packet_len];
    // The live buffer bound makes `packet_len + 2` infallible. Modulo selects
    // every in-bounds prefix plus the single `len + 1` rejection case.
    let received_len = usize::from(case.length_hint) % (packet_len + 2);
    crate::net::resolver::parse_received_response_packet(
        buffer,
        received_len,
        case.query_id,
        case.host,
        case.qtype,
    )
    .map(drop)
}

/// Fuzz entry: parse only the kernel-reported prefix of a DNS receive buffer.
pub fn dns_parse_received_response_packet(data: &[u8]) {
    let _ = std::hint::black_box(observe_dns_parse_received_response_packet(data));
}

fn observe_dns_parse_hosts_bytes(data: &[u8]) -> std::io::Result<Vec<std::net::SocketAddr>> {
    let data = maybe_decode_hex_seed(data);
    let mut addrs = Vec::new();
    crate::net::resolver::parse_hosts_bytes(
        data.as_ref(),
        "hosts-target.flowio.invalid",
        5432,
        &mut addrs,
    )?;
    Ok(addrs)
}

/// Fuzz entry: parse arbitrary hosts-file bytes, including non-UTF-8 comments,
/// without filesystem I/O.
pub fn dns_parse_hosts_bytes(data: &[u8]) {
    let _ = std::hint::black_box(observe_dns_parse_hosts_bytes(data));
}

fn observe_dns_parse_resolv_conf_bytes(data: &[u8]) -> std::io::Result<Vec<std::net::SocketAddr>> {
    let data = maybe_decode_hex_seed(data);
    crate::net::resolver::parse_resolv_conf_bytes(data.as_ref())
}

/// Fuzz entry: parse arbitrary `resolv.conf` bytes without filesystem I/O.
pub fn dns_parse_resolv_conf_bytes(data: &[u8]) {
    let _ = std::hint::black_box(observe_dns_parse_resolv_conf_bytes(data));
}

fn observe_sctp_parse_notification(data: &[u8]) -> std::io::Result<crate::net::sctp::SctpRecvMeta> {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    crate::net::sctp::parse_notification(data)
}

/// Fuzz entry: parse an SCTP notification from arbitrary control bytes.
pub fn sctp_parse_notification(data: &[u8]) {
    let _ = std::hint::black_box(observe_sctp_parse_notification(data));
}

fn observe_sctp_parse_recv_meta(data: &[u8]) -> std::io::Result<crate::net::sctp::SctpRecvMeta> {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    let flag_byte = data.first().copied().unwrap_or_default();

    let mut msg_flags = 0;
    if flag_byte & 0x01 != 0 {
        msg_flags |= libc::MSG_NOTIFICATION;
    }
    if flag_byte & 0x02 != 0 {
        msg_flags |= libc::MSG_TRUNC;
    }
    if flag_byte & 0x04 != 0 {
        msg_flags |= libc::MSG_CTRUNC;
    }
    if flag_byte & 0x08 != 0 {
        msg_flags |= libc::MSG_EOR;
    }
    let recv_rcvinfo_requested = flag_byte & 0x10 != 0;

    // Keep the kernel-reported length independent from the backing-storage
    // length so all cmsg parser branches remain reachable. Both are full-byte
    // values and are bounded before any slice is formed:
    // [flags plus receive-info policy, control storage length, reported
    // controllen, control..., data...].
    let body = data.get(3..).unwrap_or_default();
    let control_len = (data.get(1).copied().unwrap_or_default() as usize).min(body.len());
    let (control, data_slice) = body.split_at(control_len);
    let controllen = (data.get(2).copied().unwrap_or_default() as usize).min(control.len());

    crate::net::sctp::parse_recv_meta(
        control,
        controllen,
        msg_flags,
        data_slice,
        recv_rcvinfo_requested,
    )
}

/// Fuzz entry: parse SCTP recvmsg metadata with independently bounded lengths.
pub fn sctp_parse_recv_meta(data: &[u8]) {
    let _ = std::hint::black_box(observe_sctp_parse_recv_meta(data));
}

fn observe_sctp_parse_assoc_addrs(data: &[u8]) -> std::io::Result<Vec<std::net::SocketAddr>> {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    let (addr_count, payload) = sctp_assoc_addrs_case(data);

    crate::net::sctp::parse_assoc_addrs(payload, addr_count)
}

/// Fuzz entry: parse packed SCTP association-address payloads.
pub fn sctp_parse_assoc_addrs(data: &[u8]) {
    let _ = std::hint::black_box(observe_sctp_parse_assoc_addrs(data));
}

fn sctp_assoc_addrs_case(data: &[u8]) -> (usize, &[u8]) {
    let addr_count = data.first().copied().unwrap_or_default() as usize;
    // Byte one used to select synthetic entry layouts. Keep it reserved so
    // the existing corpus continues to place packed address bytes at offset 2.
    let payload = data.get(2..).unwrap_or_default();
    (addr_count, payload)
}

/// Fuzz entry: the DNS response prefilter (`response_is_decodable_candidate`),
/// which uses the non-materializing mode of the shared question-name walker
/// before `parse_response_packet` in the drain loop. The query id is derived
/// from the packet's first two bytes so the ID gate passes; QR, QUERY-opcode,
/// and name-walk acceptance remain input-controlled. The mismatch call also
/// covers the early-return path. Property: candidate and full-envelope
/// structural acceptance stay identical, pointer recursion stays bounded, and
/// arbitrary input cannot panic or read OOB.
fn observe_dns_response_prefilter(data: &[u8]) -> bool {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    let query_id = u16::from_be_bytes([
        data.first().copied().unwrap_or_default(),
        data.get(1).copied().unwrap_or_default(),
    ]);
    let candidate = crate::net::resolver::response_is_decodable_candidate(data, query_id);
    let envelope = crate::net::resolver::response_envelope_is_decodable(data, query_id);
    assert_eq!(
        candidate, envelope,
        "DNS candidate prefilter and full envelope parser diverged"
    );
    let _ = crate::net::resolver::response_is_decodable_candidate(data, query_id ^ 0x5555);
    candidate
}

pub fn dns_response_prefilter(data: &[u8]) {
    let _ = std::hint::black_box(observe_dns_response_prefilter(data));
}

fn observe_tls_server_end_point(data: &[u8]) -> Option<Vec<u8>> {
    let data = maybe_decode_hex_seed(data);
    crate::net::tls::tls_server_end_point(data.as_ref())
}

/// Fuzz entry: derive `tls-server-end-point` from arbitrary certificate DER.
///
/// The return value lets focused wrapper tests prove that the corpus reaches
/// both a supported certificate and rejected malformed input. LibFuzzer only
/// relies on this function never panicking or reading out of bounds.
pub fn tls_server_end_point(data: &[u8]) -> bool {
    observe_tls_server_end_point(data).is_some()
}

/// Repository-only observation helpers for fixture-backed integration tests.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support {
    use crate::net::sctp::SctpRecvMeta;
    use std::io;

    /// Control bit that deliberately mismatches the wrapper's expected DNS ID.
    pub const DNS_QUERY_ID_MISMATCH_CONTROL: u8 = super::DNS_QUERY_ID_MISMATCH_CONTROL;

    /// Returns the DNS response parser's exact result for a fuzz-wrapper input.
    pub fn observe_dns_parse_response_packet(data: &[u8]) -> io::Result<()> {
        super::observe_dns_parse_response_packet(data)
    }

    /// Returns the exact host extracted by the DNS response fuzz wrapper.
    pub fn observe_dns_response_case_host(data: &[u8]) -> String {
        let data = super::maybe_decode_hex_seed(data);
        super::dns_response_case(data.as_ref()).host.to_owned()
    }

    /// Reports whether a fuzz-wrapper input reaches DNS record parsing.
    pub fn dns_response_case_reaches_records(data: &[u8]) -> bool {
        super::dns_response_case_reaches_records(data)
    }

    /// Returns the received-length DNS parser's exact result.
    pub fn observe_dns_parse_received_response_packet(data: &[u8]) -> io::Result<()> {
        super::observe_dns_parse_received_response_packet(data)
    }

    /// Returns the hosts byte parser's exact result for a fuzz-wrapper input.
    pub fn observe_dns_parse_hosts_bytes(data: &[u8]) -> io::Result<Vec<std::net::SocketAddr>> {
        super::observe_dns_parse_hosts_bytes(data)
    }

    /// Returns the `resolv.conf` byte parser's exact result for a fuzz input.
    pub fn observe_dns_parse_resolv_conf_bytes(
        data: &[u8],
    ) -> io::Result<Vec<std::net::SocketAddr>> {
        super::observe_dns_parse_resolv_conf_bytes(data)
    }

    /// Returns the SCTP metadata parser's exact result.
    pub fn observe_sctp_parse_recv_meta(data: &[u8]) -> io::Result<SctpRecvMeta> {
        super::observe_sctp_parse_recv_meta(data)
    }

    /// Returns the SCTP notification parser's exact result.
    pub fn observe_sctp_parse_notification(data: &[u8]) -> io::Result<SctpRecvMeta> {
        super::observe_sctp_parse_notification(data)
    }

    /// Returns the SCTP association-address parser's exact result.
    pub fn observe_sctp_parse_assoc_addrs(data: &[u8]) -> io::Result<Vec<std::net::SocketAddr>> {
        super::observe_sctp_parse_assoc_addrs(data)
    }

    /// Returns the DNS response prefilter's decision.
    pub fn observe_dns_response_prefilter(data: &[u8]) -> bool {
        super::observe_dns_response_prefilter(data)
    }

    /// Returns the derived TLS endpoint binding, when supported.
    pub fn observe_tls_server_end_point(data: &[u8]) -> Option<Vec<u8>> {
        super::observe_tls_server_end_point(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_seed_decoder_accepts_trailing_ascii_whitespace() {
        let decoded = maybe_decode_hex_seed(b"HEX:0001aF\n\t");
        assert_eq!(decoded.as_ref(), &[0x00, 0x01, 0xAF]);
    }

    #[test]
    fn hex_seed_decoder_rejects_internal_non_hex_bytes() {
        let seed = b"HEX:00 01\n";
        let decoded = maybe_decode_hex_seed(seed);
        assert!(matches!(decoded, Cow::Borrowed(data) if data == seed));
    }

    #[test]
    fn sctp_assoc_addrs_case_uses_full_count_byte_and_reserved_offset() {
        let input = [u8::MAX, 0xA5, 0x11, 0x22, 0x33];
        let (addr_count, payload) = sctp_assoc_addrs_case(&input);

        assert_eq!(addr_count, u8::MAX as usize);
        assert!(addr_count > 7);
        assert_eq!(payload, &input[2..]);
    }
}
