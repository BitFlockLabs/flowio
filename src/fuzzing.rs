//! Fuzzing-only re-exports of internal parsers.
//!
//! Gated behind the dev-only `fuzzing` feature so the real public API is
//! unchanged. These wrappers exist purely so the out-of-source `flowio/fuzz/`
//! crate (a separate package) can reach parsers that are otherwise
//! crate-private. Not a stable API; do not depend on this module.
//!
//! Each wrapper takes raw bytes and discards the result — the fuzzers assert
//! the contract by *not crashing* (no panic, no OOB read, bounded termination)
//! across arbitrary input.

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
    let offset = data[0] as usize % data.len();
    let _ = crate::net::resolver::decode_name(data, 0, 0);
    let _ = crate::net::resolver::decode_name(data, offset, 0);
}

/// Fuzz entry: parse a DNS response packet with metadata derived from input.
pub fn dns_parse_response_packet(data: &[u8]) {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    let query_id = u16::from_be_bytes([
        data.first().copied().unwrap_or_default(),
        data.get(1).copied().unwrap_or_default(),
    ]);
    let qtype = if data.get(2).copied().unwrap_or_default() & 1 == 0 {
        1
    } else {
        28
    };

    let available = data.len().saturating_sub(4);
    let host_len = data
        .get(3)
        .map(|len| (*len as usize).min(available).min(63))
        .unwrap_or_default();
    let host_bytes = &data.get(4..4 + host_len).unwrap_or_default();
    let host = std::str::from_utf8(host_bytes)
        .ok()
        .filter(|host| !host.is_empty())
        .unwrap_or("example.com");
    let packet = data.get(4 + host_len..).unwrap_or_default();

    let _ = crate::net::resolver::parse_response_packet(packet, query_id, host, qtype);
}

/// Fuzz entry: parse an SCTP notification from arbitrary control bytes.
pub fn sctp_parse_notification(data: &[u8]) {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    let _ = crate::net::sctp::parse_notification(data);
}

/// Fuzz entry: parse SCTP recvmsg metadata with bounded derived lengths.
pub fn sctp_parse_recv_meta(data: &[u8]) {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    let Some((&flag_byte, rest)) = data.split_first() else {
        let _ = crate::net::sctp::parse_recv_meta(&[], 0, 0, &[]);
        return;
    };

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

    let split = rest
        .first()
        .map(|len| (*len as usize).min(rest.len().saturating_sub(1)))
        .unwrap_or_default();
    let control = rest.get(1..1 + split).unwrap_or_default();
    let data_slice = rest.get(1 + split..).unwrap_or_default();
    let controllen = if control.is_empty() {
        0
    } else {
        ((flag_byte >> 3) as usize).min(control.len())
    };

    let _ = crate::net::sctp::parse_recv_meta(control, controllen, msg_flags, data_slice);
}

/// Fuzz entry: parse packed SCTP association-address payloads.
pub fn sctp_parse_assoc_addrs(data: &[u8]) {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    let addr_count = data.first().map(|byte| (*byte as usize) % 8).unwrap_or(0);
    let storage_len = match data.get(1).copied().unwrap_or_default() % 4 {
        0 => 8,
        1 => std::mem::size_of::<libc::sockaddr_in>(),
        2 => std::mem::size_of::<libc::sockaddr_in6>(),
        _ => std::mem::size_of::<libc::sockaddr_storage>(),
    };
    let payload = data.get(2..).unwrap_or_default();

    let _ = crate::net::sctp::parse_assoc_addrs(payload, addr_count, storage_len);
}
