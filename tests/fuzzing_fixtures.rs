#![cfg(all(feature = "fuzzing", feature = "test-support"))]

use flowio::fuzzing::test_support::{
    DNS_QUERY_ID_MISMATCH_CONTROL, dns_response_case_reaches_records,
    observe_dns_parse_hosts_bytes, observe_dns_parse_received_response_packet,
    observe_dns_parse_resolv_conf_bytes, observe_dns_parse_response_packet,
    observe_dns_response_case_host, observe_dns_response_prefilter, observe_sctp_parse_recv_meta,
    observe_tls_server_end_point,
};
use flowio::fuzzing::{dns_decode_name, tls_server_end_point};
use flowio::net::sctp::{SctpNotification, SctpRecvInfo, SctpRecvMeta};
use flowio::test_support::net::{resolver::decode_name, sctp::append_initialized_test_cmsg};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

fn decode_hex_seed(data: &[u8]) -> Vec<u8> {
    let hex = data
        .strip_prefix(b"HEX:")
        .expect("canonical fixture should use the HEX encoding");
    let hex_end = hex
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(0, |index| index + 1);
    let hex = &hex[..hex_end];
    assert_eq!(hex.len() % 2, 0, "canonical fixture has odd hex length");

    hex.chunks_exact(2)
        .map(|pair| {
            let digit = |byte| match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => panic!("canonical fixture contains a non-hex byte"),
            };
            (digit(pair[0]) << 4) | digit(pair[1])
        })
        .collect()
}

fn cmsg_align(len: usize) -> usize {
    let align = std::mem::size_of::<usize>();
    (len + align - 1) & !(align - 1)
}

fn write_ne<const N: usize>(buffer: &mut [u8], offset: usize, bytes: [u8; N]) {
    buffer[offset..offset + N].copy_from_slice(&bytes);
}

fn write_rcvinfo_fields(control: &mut [u8], data_offset: usize, info: libc::sctp_rcvinfo) {
    // Write fields rather than whole C structs so zeroed padding stays
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
}

fn recv_meta_input(flag_byte: u8, control: Vec<u8>, payload: &[u8]) -> Vec<u8> {
    let control_len =
        u8::try_from(control.len()).expect("test control chain should fit in one byte");
    let mut input = Vec::with_capacity(3 + control.len() + payload.len());
    input.extend_from_slice(&[flag_byte, control_len, control_len]);
    input.extend_from_slice(&control);
    input.extend_from_slice(payload);
    input
}

fn rcvinfo_input(info: libc::sctp_rcvinfo, payload: &[u8]) -> Vec<u8> {
    let mut control = Vec::new();
    let data_offset = append_initialized_test_cmsg(
        &mut control,
        libc::IPPROTO_SCTP,
        libc::SCTP_RCVINFO,
        std::mem::size_of::<libc::sctp_rcvinfo>(),
    );
    // Preserve the historical exact CMSG_LEN-sized fixture rather than
    // adding optional final alignment padding.
    control.truncate(data_offset + std::mem::size_of::<libc::sctp_rcvinfo>());
    write_rcvinfo_fields(&mut control, data_offset, info);
    recv_meta_input(0x08, control, payload)
}

fn timestampns_before_rcvinfo_input(payload: &[u8]) -> Vec<u8> {
    let mut control = Vec::new();
    append_initialized_test_cmsg(
        &mut control,
        libc::SOL_SOCKET,
        libc::SO_TIMESTAMPNS,
        2 * std::mem::size_of::<i64>(),
    );
    let data_offset = append_initialized_test_cmsg(
        &mut control,
        libc::IPPROTO_SCTP,
        libc::SCTP_RCVINFO,
        std::mem::size_of::<libc::sctp_rcvinfo>(),
    );
    write_rcvinfo_fields(&mut control, data_offset, sample_rcvinfo());
    recv_meta_input(0x08, control, payload)
}

fn timestampns_before_truncated_rcvinfo_input(payload: &[u8]) -> Vec<u8> {
    let mut control = Vec::new();
    append_initialized_test_cmsg(
        &mut control,
        libc::SOL_SOCKET,
        libc::SO_TIMESTAMPNS,
        2 * std::mem::size_of::<i64>(),
    );
    append_initialized_test_cmsg(&mut control, libc::IPPROTO_SCTP, libc::SCTP_RCVINFO, 0);
    recv_meta_input(0x0C, control, payload)
}

fn malformed_preceding_cmsg_len_input(payload: &[u8]) -> Vec<u8> {
    let mut input = timestampns_before_rcvinfo_input(payload);
    write_ne(
        &mut input[3..],
        std::mem::offset_of!(libc::cmsghdr, cmsg_len),
        (cmsg_align(std::mem::size_of::<libc::cmsghdr>()) - 1).to_ne_bytes(),
    );
    input
}

fn zero_reported_control_input(flag_byte: u8, reported_len: u8, payload: &[u8]) -> Vec<u8> {
    let mut input = vec![flag_byte, 1, reported_len, 0xA5];
    input.extend_from_slice(payload);
    input
}

fn checked_native_seed(seed: &[u8], synthesized: Vec<u8>) -> Vec<u8> {
    // The checked corpus encodes the 64-bit little-endian Linux C ABI used
    // by the full-hardening host. Other Linux ABIs exercise the same
    // semantic branch with their synthesized native layout.
    #[cfg(all(target_pointer_width = "64", target_endian = "little"))]
    {
        let decoded = decode_hex_seed(seed);
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

fn dns_parser_fixtures() -> [&'static [u8]; 11] {
    [
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/valid_a"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/valid_aaaa"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/malformed_a_short"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/cname_compressed_exact"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/cname_trailing_byte"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/cname_cycle"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/cname_hops_16"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/cname_hops_17"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/root_cname_target"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/question_pointer_loop"),
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/maximum_question_name"),
    ]
}

fn dns_parse_error(fixture: &[u8]) -> std::io::Error {
    observe_dns_parse_response_packet(fixture)
        .expect_err("the DNS parser fixture should be rejected")
}

#[test]
fn dns_name_decoder_fixtures_pin_validated_string_materialization() {
    let direct_fixture = include_bytes!("../fixtures/fuzzing/dns_decode_name/direct_utf8");
    dns_decode_name(direct_fixture);
    let direct = decode_hex_seed(direct_fixture);
    assert_eq!(
        decode_name(&direct, 0).expect("direct UTF-8 name should decode"),
        ("www.éxample.test".to_owned(), 19)
    );

    let compressed_fixture = include_bytes!("../fixtures/fuzzing/dns_decode_name/compressed_utf8");
    dns_decode_name(compressed_fixture);
    let compressed = decode_hex_seed(compressed_fixture);
    let compressed_offset = usize::from(compressed[0]) % compressed.len();
    assert_eq!(compressed_offset, 16);
    assert_eq!(
        decode_name(&compressed, compressed_offset).expect("compressed UTF-8 name should decode"),
        ("api.éxample.test".to_owned(), 6)
    );

    let invalid_fixture = include_bytes!("../fixtures/fuzzing/dns_decode_name/invalid_utf8");
    dns_decode_name(invalid_fixture);
    let invalid = decode_hex_seed(invalid_fixture);
    let error = decode_name(&invalid, 0).expect_err("invalid UTF-8 label should reject");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "DNS label was not valid UTF-8");

    let compressed_invalid_fixture =
        include_bytes!("../fixtures/fuzzing/dns_decode_name/compressed_invalid_utf8");
    dns_decode_name(compressed_invalid_fixture);
    let compressed_invalid = decode_hex_seed(compressed_invalid_fixture);
    let compressed_invalid_offset = usize::from(compressed_invalid[0]) % compressed_invalid.len();
    assert_eq!(compressed_invalid_offset, 4);
    let error = decode_name(&compressed_invalid, compressed_invalid_offset)
        .expect_err("compressed invalid UTF-8 label should reject");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "DNS label was not valid UTF-8");
}

#[test]
fn dns_parser_corpus_reaches_record_parsing_for_at_least_two_thirds() {
    let fixtures = dns_parser_fixtures();
    let reached = fixtures
        .iter()
        .filter(|fixture| dns_response_case_reaches_records(fixture))
        .count();

    assert!(
        reached * 3 >= fixtures.len() * 2,
        "{reached}/{} canonical DNS parser fixtures reached record parsing; \
         at least two thirds must do so",
        fixtures.len()
    );
    assert_eq!(
        reached, 10,
        "the named semantic corpus changed record-parser reachability"
    );
}

#[test]
fn dns_parser_corpus_pins_deep_record_and_chain_outcomes() {
    assert!(
        observe_dns_parse_response_packet(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/valid_a"
        ))
        .is_ok()
    );
    assert!(
        observe_dns_parse_response_packet(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/valid_aaaa"
        ))
        .is_ok()
    );

    assert_eq!(
        dns_parse_error(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/malformed_a_short"
        ))
        .to_string(),
        "DNS A RDATA length was not 4 bytes"
    );
    assert!(
        observe_dns_parse_response_packet(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/cname_compressed_exact"
        ))
        .is_ok(),
        "an exactly consumed compressed CNAME must remain valid"
    );
    assert_eq!(
        dns_parse_error(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/cname_trailing_byte"
        ))
        .to_string(),
        "DNS CNAME RDATA did not consume its declared length"
    );
    assert_eq!(
        dns_parse_error(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/cname_cycle"
        ))
        .to_string(),
        "DNS response CNAME chain contained a loop"
    );
    assert!(
        observe_dns_parse_response_packet(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/cname_hops_16"
        ))
        .is_ok()
    );
    assert_eq!(
        dns_parse_error(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/cname_hops_17"
        ))
        .to_string(),
        "DNS response CNAME chain exceeded maximum per-response hop count"
    );
    assert_eq!(
        dns_parse_error(include_bytes!(
            "../fixtures/fuzzing/dns_parse_response_packet/root_cname_target"
        ))
        .to_string(),
        "DNS response CNAME target was the root name"
    );
}

#[test]
fn dns_parser_case_preserves_maximum_host_and_explicit_id_mismatch() {
    let maximum =
        include_bytes!("../fixtures/fuzzing/dns_parse_response_packet/maximum_question_name");
    let host = observe_dns_response_case_host(maximum);
    assert_eq!(host.len(), 253);
    assert_eq!(
        host,
        [
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61)
        ]
        .join(".")
    );
    assert!(dns_response_case_reaches_records(maximum));
    assert!(observe_dns_parse_response_packet(maximum).is_ok());

    let mut packet_coupled = decode_hex_seed(include_bytes!(
        "../fixtures/fuzzing/dns_parse_response_packet/valid_a"
    ));
    packet_coupled[..2].copy_from_slice(&0u16.to_be_bytes());
    assert!(
        observe_dns_parse_response_packet(&packet_coupled).is_ok(),
        "the normal path must derive its query ID from the packet"
    );

    packet_coupled[2] |= DNS_QUERY_ID_MISMATCH_CONTROL;
    let mismatch = dns_parse_error(&packet_coupled);
    assert_eq!(mismatch.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        mismatch.to_string(),
        "DNS response ID did not match query ID"
    );
}

#[test]
fn dns_prefilter_corpus_exercises_both_differential_outcomes() {
    let accepted = include_bytes!("../fixtures/fuzzing/dns_response_prefilter/accepted_question");
    let rejected =
        include_bytes!("../fixtures/fuzzing/dns_response_prefilter/rejected_pointer_loop");

    assert!(observe_dns_response_prefilter(accepted));
    assert!(!observe_dns_response_prefilter(rejected));
}

#[test]
fn dns_received_length_corpus_pins_full_truncated_and_oversized_inputs() {
    let full =
        include_bytes!("../fixtures/fuzzing/dns_parse_received_response_packet/full_response");
    assert!(observe_dns_parse_received_response_packet(full).is_ok());

    let truncated =
        include_bytes!("../fixtures/fuzzing/dns_parse_received_response_packet/truncated_response");
    let truncated_error = observe_dns_parse_received_response_packet(truncated)
        .expect_err("the reported prefix must exclude the stale tail");
    assert_eq!(truncated_error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert_eq!(truncated_error.to_string(), "DNS packet ended unexpectedly");

    let oversized =
        include_bytes!("../fixtures/fuzzing/dns_parse_received_response_packet/oversized_length");
    let oversized_error = observe_dns_parse_received_response_packet(oversized)
        .expect_err("a reported length beyond the backing buffer must reject");
    assert_eq!(oversized_error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        oversized_error.to_string(),
        "DNS receive length exceeded response buffer"
    );
}

#[test]
fn hosts_byte_parser_corpus_pins_line_isolation_and_comment_grammar() {
    let mixed = include_bytes!("../fixtures/fuzzing/dns_parse_hosts_bytes/valid_invalid_valid");
    assert_eq!(
        observe_dns_parse_hosts_bytes(mixed)
            .expect("valid hosts lines should survive an invalid line"),
        [
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 5432)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x10), 5432,)),
        ]
    );

    let invalid = include_bytes!("../fixtures/fuzzing/dns_parse_hosts_bytes/all_invalid");
    assert!(
        observe_dns_parse_hosts_bytes(invalid)
            .expect("invalid hosts lines should be ignored")
            .is_empty()
    );

    let semicolon = include_bytes!("../fixtures/fuzzing/dns_parse_hosts_bytes/semicolon_aliases");
    assert_eq!(
        observe_dns_parse_hosts_bytes(semicolon)
            .expect("hosts semicolons should remain ordinary alias bytes"),
        [
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 20), 5432)),
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 22), 5432)),
        ]
    );

    let address_limit = include_bytes!("../fixtures/fuzzing/dns_parse_hosts_bytes/address_limit");
    let error = observe_dns_parse_hosts_bytes(address_limit)
        .expect_err("the 65th unique hosts address should be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "resolver result exceeds 64 unique addresses"
    );
}

#[test]
fn resolv_conf_byte_parser_corpus_pins_line_and_comment_isolation() {
    let all_invalid = include_bytes!("../fixtures/fuzzing/dns_parse_resolv_conf_bytes/all_invalid");
    let err = observe_dns_parse_resolv_conf_bytes(all_invalid)
        .expect_err("input without a valid nameserver should remain NotFound");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(err.to_string(), "no nameservers found in /etc/resolv.conf");

    let hash =
        include_bytes!("../fixtures/fuzzing/dns_parse_resolv_conf_bytes/invalid_hash_comment");
    assert_eq!(
        observe_dns_parse_resolv_conf_bytes(hash)
            .expect("invalid bytes after the first hash marker should be ignored"),
        [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 54), 53))]
    );

    let semicolon =
        include_bytes!("../fixtures/fuzzing/dns_parse_resolv_conf_bytes/invalid_semicolon_comment");
    assert_eq!(
        observe_dns_parse_resolv_conf_bytes(semicolon)
            .expect("invalid bytes after the first semicolon marker should be ignored"),
        [SocketAddr::from((
            Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x54),
            53,
        ))]
    );

    let mixed =
        include_bytes!("../fixtures/fuzzing/dns_parse_resolv_conf_bytes/valid_invalid_valid");
    assert_eq!(
        observe_dns_parse_resolv_conf_bytes(mixed)
            .expect("valid siblings should survive one malformed directive"),
        [
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 53), 53)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x53), 53,)),
        ]
    );

    let crlf = include_bytes!("../fixtures/fuzzing/dns_parse_resolv_conf_bytes/crlf_duplicates");
    assert_eq!(
        observe_dns_parse_resolv_conf_bytes(crlf)
            .expect("CRLF directives should retain first-seen unique addresses"),
        [
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 55), 53)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x55), 53,)),
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 56), 53)),
        ]
    );
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
fn sctp_recv_meta_wrapper_reaches_ancillary_chain_results() {
    let valid = checked_native_seed(
        include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/timestampns_before_rcvinfo"),
        timestampns_before_rcvinfo_input(b"ping"),
    );
    assert_eq!(
        observe_sctp_parse_recv_meta(&valid).expect("RCVINFO after timestamp control should parse"),
        sample_recv_meta()
    );

    let truncated = checked_native_seed(
        include_bytes!(
            "../fixtures/fuzzing/sctp_parse_recv_meta/timestampns_before_truncated_rcvinfo"
        ),
        timestampns_before_truncated_rcvinfo_input(b"ping"),
    );
    let truncated_error = observe_sctp_parse_recv_meta(&truncated)
        .expect_err("a timestamp followed by truncated RCVINFO must fail");
    assert_eq!(truncated_error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        truncated_error.to_string(),
        "SCTP_RCVINFO control message was truncated"
    );

    let malformed = checked_native_seed(
        include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/malformed_preceding_cmsg_len"),
        malformed_preceding_cmsg_len_input(b"ping"),
    );
    let malformed_error = observe_sctp_parse_recv_meta(&malformed)
        .expect_err("a malformed cmsg before RCVINFO must fail");
    assert_eq!(malformed_error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        malformed_error.to_string().contains("length")
            || malformed_error.to_string().contains("malformed")
    );
}

#[test]
fn sctp_recv_meta_wrapper_pins_zero_controllen_decision() {
    let no_rcvinfo = checked_native_seed(
        include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/data_without_rcvinfo"),
        zero_reported_control_input(0x08, 0, b"payload"),
    );
    assert_eq!(
        observe_sctp_parse_recv_meta(&no_rcvinfo).expect("EOR data without RCVINFO should parse"),
        SctpRecvMeta::Data(SctpRecvInfo {
            end_of_record: true,
            ..SctpRecvInfo::default()
        })
    );

    let requested_no_rcvinfo = checked_native_seed(
        include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/data_without_requested_rcvinfo"),
        zero_reported_control_input(0x18, 0, b"payload"),
    );
    let error = observe_sctp_parse_recv_meta(&requested_no_rcvinfo)
        .expect_err("requested RCVINFO must not silently default");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("SCTP_RCVINFO"));

    let missing_eor = checked_native_seed(
        include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/data_without_rcvinfo_missing_eor"),
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
    assert_eq!(
        error.to_string(),
        "SCTP recvmsg fixed control buffer capacity was exhausted"
    );

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
    assert_eq!(
        control_error.to_string(),
        "SCTP recvmsg fixed control buffer capacity was exhausted"
    );

    let mut wrong_type = valid.clone();
    write_ne(
        &mut wrong_type[3..],
        std::mem::offset_of!(libc::cmsghdr, cmsg_type),
        libc::SCTP_SNDINFO.to_ne_bytes(),
    );
    assert_eq!(
        observe_sctp_parse_recv_meta(&wrong_type)
            .expect("complete unrelated control should default SCTP metadata"),
        SctpRecvMeta::Data(SctpRecvInfo {
            end_of_record: true,
            ..SctpRecvInfo::default()
        })
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
    let header_error =
        observe_sctp_parse_recv_meta(&short_header).expect_err("truncated cmsg header must reject");
    assert_eq!(
        header_error.to_string(),
        "SCTP recvmsg control message header was malformed"
    );

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
    let rsa = include_bytes!("../fixtures/fuzzing/tls_server_end_point/valid_rsa_sha256_minimal");
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
    let mut expected_malformed_tag = decode_hex_seed(rsa);
    assert_eq!(expected_malformed_tag.len(), 22);
    expected_malformed_tag[19] = 0x04;
    assert_eq!(decode_hex_seed(malformed_tag), expected_malformed_tag);
    assert!(!tls_server_end_point(malformed_tag));

    let truncated_length =
        include_bytes!("../fixtures/fuzzing/tls_server_end_point/malformed_truncated_long_length");
    assert_eq!(decode_hex_seed(truncated_length), &[0x30, 0x82, 0x01, 0x00]);
    assert!(!tls_server_end_point(truncated_length));
}
