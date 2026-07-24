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

    // Keep the kernel-reported length independent from the backing-storage
    // length so all cmsg parser branches remain reachable. Both are full-byte
    // values and are bounded before any slice is formed:
    // [flags, control storage length, reported controllen, control..., data...].
    let body = data.get(3..).unwrap_or_default();
    let control_len = (data.get(1).copied().unwrap_or_default() as usize).min(body.len());
    let (control, data_slice) = body.split_at(control_len);
    let controllen = (data.get(2).copied().unwrap_or_default() as usize).min(control.len());

    crate::net::sctp::parse_recv_meta(control, controllen, msg_flags, data_slice)
}

/// Fuzz entry: parse SCTP recvmsg metadata with independently bounded lengths.
pub fn sctp_parse_recv_meta(data: &[u8]) {
    let _ = std::hint::black_box(observe_sctp_parse_recv_meta(data));
}

/// Fuzz entry: parse packed SCTP association-address payloads.
pub fn sctp_parse_assoc_addrs(data: &[u8]) {
    let data = maybe_decode_hex_seed(data);
    let data = data.as_ref();
    let (addr_count, payload) = sctp_assoc_addrs_case(data);

    let _ = crate::net::sctp::parse_assoc_addrs(payload, addr_count);
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
pub fn dns_response_prefilter(data: &[u8]) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::sctp::{SctpNotification, SctpRecvInfo, SctpRecvMeta};

    fn cmsg_align(len: usize) -> usize {
        let align = std::mem::size_of::<usize>();
        (len + align - 1) & !(align - 1)
    }

    fn write_ne<const N: usize>(buffer: &mut [u8], offset: usize, bytes: [u8; N]) {
        buffer[offset..offset + N].copy_from_slice(&bytes);
    }

    fn rcvinfo_input(info: libc::sctp_rcvinfo, payload: &[u8]) -> Vec<u8> {
        let hdr_len = std::mem::size_of::<libc::cmsghdr>();
        let data_offset = cmsg_align(hdr_len);
        let data_len = std::mem::size_of::<libc::sctp_rcvinfo>();
        let control_len = data_offset + data_len;
        let control_len_u8 =
            u8::try_from(control_len).expect("RCVINFO cmsg should fit in one byte");

        let mut input = Vec::with_capacity(3 + control_len + payload.len());
        input.extend_from_slice(&[0x08, control_len_u8, control_len_u8]);
        input.resize(3 + control_len, 0);

        let control = &mut input[3..3 + control_len];
        // Write fields rather than whole C structs so their zeroed padding is
        // deterministic in optimized tests and checked corpus comparisons.
        macro_rules! write_field {
            ($base:expr, $ty:ty, $field:ident, $value:expr) => {
                write_ne(
                    control,
                    $base + std::mem::offset_of!($ty, $field),
                    $value.to_ne_bytes(),
                );
            };
        }
        write_field!(0, libc::cmsghdr, cmsg_len, hdr_len + data_len);
        write_field!(0, libc::cmsghdr, cmsg_level, libc::IPPROTO_SCTP);
        write_field!(0, libc::cmsghdr, cmsg_type, libc::SCTP_RCVINFO);
        write_field!(data_offset, libc::sctp_rcvinfo, rcv_sid, info.rcv_sid);
        write_field!(data_offset, libc::sctp_rcvinfo, rcv_ssn, info.rcv_ssn);
        write_field!(data_offset, libc::sctp_rcvinfo, rcv_flags, info.rcv_flags);
        write_field!(data_offset, libc::sctp_rcvinfo, rcv_ppid, info.rcv_ppid);
        write_field!(data_offset, libc::sctp_rcvinfo, rcv_tsn, info.rcv_tsn);
        write_field!(data_offset, libc::sctp_rcvinfo, rcv_cumtsn, info.rcv_cumtsn);
        write_field!(
            data_offset,
            libc::sctp_rcvinfo,
            rcv_context,
            info.rcv_context
        );
        write_field!(
            data_offset,
            libc::sctp_rcvinfo,
            rcv_assoc_id,
            info.rcv_assoc_id
        );
        input.extend_from_slice(payload);
        input
    }

    fn zero_reported_control_input(flag_byte: u8, reported_len: u8, payload: &[u8]) -> Vec<u8> {
        let mut input = vec![flag_byte, 1, reported_len, 0xA5];
        input.extend_from_slice(payload);
        input
    }

    fn checked_native_seed(seed: &'static [u8], synthesized: Vec<u8>) -> Vec<u8> {
        // The checked corpus encodes the 64-bit little-endian Linux C ABI used
        // by the full-hardening host. Other Linux ABIs exercise the same
        // semantic branch with their synthesized native layout.
        #[cfg(all(target_pointer_width = "64", target_endian = "little"))]
        {
            let decoded = maybe_decode_hex_seed(seed).into_owned();
            assert_eq!(decoded, synthesized, "native fuzz seed drifted");
            decoded
        }
        #[cfg(not(all(target_pointer_width = "64", target_endian = "little")))]
        {
            let _ = seed;
            synthesized
        }
    }

    fn sample_rcvinfo() -> libc::sctp_rcvinfo {
        libc::sctp_rcvinfo {
            rcv_sid: 3,
            rcv_ssn: 4,
            rcv_flags: 5,
            rcv_ppid: 0x0607_0809u32.to_be(),
            rcv_tsn: 10,
            rcv_cumtsn: 11,
            rcv_context: 12,
            rcv_assoc_id: 13,
        }
    }

    fn sample_recv_meta() -> SctpRecvMeta {
        SctpRecvMeta::Data(SctpRecvInfo {
            stream_id: 3,
            ssn: 4,
            flags: 5,
            ppid: 0x0607_0809,
            tsn: 10,
            cumtsn: 11,
            context: 12,
            assoc_id: 13,
            end_of_record: true,
        })
    }

    fn assoc_change_input() -> Vec<u8> {
        let mut notification = [0u8; 20];
        let notification_len = notification.len() as u32;
        notification[0..2].copy_from_slice(&((1u16 << 15) | 1).to_ne_bytes());
        notification[4..8].copy_from_slice(&notification_len.to_ne_bytes());
        notification[12..14].copy_from_slice(&1u16.to_ne_bytes());
        notification[14..16].copy_from_slice(&1u16.to_ne_bytes());
        notification[16..20].copy_from_slice(&42i32.to_ne_bytes());

        let mut input = Vec::with_capacity(3 + notification.len());
        input.extend_from_slice(&[0x09, 0, 0]);
        input.extend_from_slice(&notification);
        input
    }

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

    #[test]
    fn sctp_recv_meta_wrapper_reaches_exact_data_and_notification_results() {
        let data_input = checked_native_seed(
            include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/valid_rcvinfo_unaligned"),
            rcvinfo_input(sample_rcvinfo(), b"ping"),
        );
        #[cfg(all(target_pointer_width = "64", target_endian = "little"))]
        assert_ne!(
            data_input[3..]
                .as_ptr()
                .align_offset(std::mem::align_of::<libc::cmsghdr>()),
            0,
            "the checked corpus must exercise an unaligned control address"
        );
        assert_eq!(
            observe_sctp_parse_recv_meta(&data_input).expect("valid RCVINFO should parse"),
            sample_recv_meta()
        );

        let notification_input = checked_native_seed(
            include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/notification_assoc_change"),
            assoc_change_input(),
        );
        assert_eq!(
            observe_sctp_parse_recv_meta(&notification_input)
                .expect("association-change notification should parse"),
            SctpRecvMeta::Notification(SctpNotification::AssocChange {
                state: 0,
                error: 0,
                outbound_streams: 1,
                inbound_streams: 1,
                assoc_id: 42,
            })
        );
    }

    #[test]
    fn sctp_recv_meta_wrapper_pins_zero_controllen_decision_seam() {
        let no_rcvinfo = checked_native_seed(
            include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/data_without_rcvinfo"),
            zero_reported_control_input(0x08, 0, b"payload"),
        );
        assert_eq!(
            observe_sctp_parse_recv_meta(&no_rcvinfo)
                .expect("EOR data without RCVINFO should parse"),
            SctpRecvMeta::Data(SctpRecvInfo {
                end_of_record: true,
                ..SctpRecvInfo::default()
            })
        );

        let missing_eor = checked_native_seed(
            include_bytes!(
                "../fixtures/fuzzing/sctp_parse_recv_meta/data_without_rcvinfo_missing_eor"
            ),
            zero_reported_control_input(0, 0, b"payload"),
        );
        let error = observe_sctp_parse_recv_meta(&missing_eor)
            .expect_err("nonempty data without EOR should still reject");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("end-of-record"));

        let control_truncated = checked_native_seed(
            include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/data_without_rcvinfo_ctrunc"),
            zero_reported_control_input(0x0C, 0, b"payload"),
        );
        let error = observe_sctp_parse_recv_meta(&control_truncated)
            .expect_err("MSG_CTRUNC without RCVINFO should still reject");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("control"));

        let malformed_control = checked_native_seed(
            include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/malformed_present_control"),
            zero_reported_control_input(0x08, 1, b"payload"),
        );
        let error = observe_sctp_parse_recv_meta(&malformed_control)
            .expect_err("present malformed control should still reject");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn sctp_recv_meta_wrapper_keeps_reported_length_independent_and_bounded() {
        let valid = rcvinfo_input(sample_rcvinfo(), b"x");
        let control_len = valid[1];

        let mut short_report = valid.clone();
        short_report[2] = control_len - 1;
        assert_eq!(
            observe_sctp_parse_recv_meta(&short_report)
                .expect_err("short reported controllen must reject")
                .kind(),
            std::io::ErrorKind::InvalidData
        );

        let mut oversized_report = valid.clone();
        oversized_report[2] = u8::MAX;
        assert!(observe_sctp_parse_recv_meta(&oversized_report).is_ok());

        let mut short_backing = valid;
        short_backing[1] = control_len - 1;
        short_backing[2] = u8::MAX;
        assert_eq!(
            observe_sctp_parse_recv_meta(&short_backing)
                .expect_err("reported controllen must not exceed backing storage")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn sctp_recv_meta_wrapper_reaches_truncation_and_malformed_cmsg_paths() {
        let valid = rcvinfo_input(sample_rcvinfo(), b"payload");

        let mut payload_truncated = valid.clone();
        payload_truncated[0] |= 0x02;
        let payload_error = observe_sctp_parse_recv_meta(&payload_truncated)
            .expect_err("MSG_TRUNC must reject the payload");
        assert!(payload_error.to_string().contains("payload"));

        let mut control_truncated = valid.clone();
        control_truncated[0] |= 0x04;
        control_truncated[2] = 0;
        let control_error = observe_sctp_parse_recv_meta(&control_truncated)
            .expect_err("MSG_CTRUNC with missing RCVINFO must reject control");
        assert!(control_error.to_string().contains("control"));

        let mut wrong_type = valid.clone();
        write_ne(
            &mut wrong_type[3..],
            std::mem::offset_of!(libc::cmsghdr, cmsg_type),
            libc::SCTP_SNDINFO.to_ne_bytes(),
        );
        assert_eq!(
            observe_sctp_parse_recv_meta(&wrong_type)
                .expect_err("wrong cmsg type must reject")
                .kind(),
            std::io::ErrorKind::InvalidData
        );

        let mut short_cmsg = valid;
        write_ne(
            &mut short_cmsg[3..],
            std::mem::offset_of!(libc::cmsghdr, cmsg_len),
            (std::mem::size_of::<libc::cmsghdr>() + std::mem::size_of::<libc::sctp_rcvinfo>() - 1)
                .to_ne_bytes(),
        );
        assert_eq!(
            observe_sctp_parse_recv_meta(&short_cmsg)
                .expect_err("short cmsg_len must reject")
                .kind(),
            std::io::ErrorKind::InvalidData
        );

        let short_header = checked_native_seed(
            include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/truncated_cmsg_header"),
            {
                let mut input = rcvinfo_input(sample_rcvinfo(), b"ping");
                input[0] |= 0x04;
                input[2] = (std::mem::size_of::<libc::cmsghdr>() - 1) as u8;
                input
            },
        );
        let header_error = observe_sctp_parse_recv_meta(&short_header)
            .expect_err("truncated cmsg header must reject");
        assert!(header_error.to_string().contains("control"));

        let short_rcvinfo = checked_native_seed(
            include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/truncated_rcvinfo_payload"),
            {
                let mut input = rcvinfo_input(sample_rcvinfo(), b"ping");
                input[2] -= 1;
                input
            },
        );
        assert_eq!(
            observe_sctp_parse_recv_meta(&short_rcvinfo)
                .expect_err("truncated RCVINFO must reject")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn sctp_recv_meta_wrapper_handles_empty_and_maximum_bounded_lengths() {
        assert_eq!(
            observe_sctp_parse_recv_meta(&[])
                .expect("empty data without ancillary metadata should parse"),
            SctpRecvMeta::Data(SctpRecvInfo::default())
        );

        let maximum = checked_native_seed(
            include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/maximum_bounded_control"),
            {
                let mut input = rcvinfo_input(sample_rcvinfo(), &[]);
                input[1] = u8::MAX;
                input[2] = u8::MAX;
                input.resize(3 + u8::MAX as usize, 0);
                input
            },
        );
        assert_eq!(
            observe_sctp_parse_recv_meta(&maximum).expect("maximum bounded control should parse"),
            sample_recv_meta()
        );
    }

    #[test]
    fn tls_server_end_point_wrapper_reaches_valid_and_malformed_der() {
        let rsa =
            include_bytes!("../fixtures/fuzzing/tls_server_end_point/valid_rsa_sha256_minimal");
        let ecdsa =
            include_bytes!("../fixtures/fuzzing/tls_server_end_point/valid_ecdsa_sha256_minimal");
        assert_eq!(
            observe_tls_server_end_point(rsa)
                .expect("RSA SHA-256 seed should derive a binding")
                .len(),
            32
        );
        assert_eq!(
            observe_tls_server_end_point(ecdsa)
                .expect("ECDSA SHA-256 seed should derive a binding")
                .len(),
            32
        );
        assert!(!tls_server_end_point(include_bytes!(
            "../fixtures/fuzzing/tls_server_end_point/unsupported_ed25519_minimal"
        )));
        let malformed_tag =
            include_bytes!("../fixtures/fuzzing/tls_server_end_point/malformed_signature_tag");
        let mut expected_malformed_tag = maybe_decode_hex_seed(rsa).into_owned();
        assert_eq!(expected_malformed_tag.len(), 22);
        expected_malformed_tag[19] = 0x04;
        assert_eq!(
            maybe_decode_hex_seed(malformed_tag).as_ref(),
            expected_malformed_tag
        );
        assert!(!tls_server_end_point(malformed_tag));

        let truncated_length = include_bytes!(
            "../fixtures/fuzzing/tls_server_end_point/malformed_truncated_long_length"
        );
        assert_eq!(
            maybe_decode_hex_seed(truncated_length).as_ref(),
            &[0x30, 0x82, 0x01, 0x00]
        );
        assert!(!tls_server_end_point(truncated_length));
    }
}
