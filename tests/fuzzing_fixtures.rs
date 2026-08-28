#![cfg(all(feature = "fuzzing", feature = "test-support"))]

use flowio::fuzzing::test_support::{
    DNS_QUERY_ID_MISMATCH_CONTROL, dns_response_case_reaches_records,
    observe_dns_parse_hosts_bytes, observe_dns_parse_received_response_packet,
    observe_dns_parse_resolv_conf_bytes, observe_dns_parse_response_packet,
    observe_dns_response_case_host, observe_dns_response_prefilter, observe_sctp_parse_assoc_addrs,
    observe_sctp_parse_notification, observe_sctp_parse_recv_meta, observe_tls_server_end_point,
};
use flowio::fuzzing::{dns_decode_name, tls_server_end_point};
use flowio::net::sctp::{SctpNotification, SctpRecvInfo, SctpRecvMeta, SctpSendInfo};
use flowio::runtime::buffer::bytes::BufferRangeError;
use flowio::test_support::net::{
    resolver::{decode_name, parse_resolv_conf_configuration_bytes},
    sctp::append_initialized_test_cmsg,
};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};

const IGNORED_ASSOC_WRAPPER_SENTINEL: u8 = 1;
const NOTIFICATION_FLAGS_SENTINEL: u16 = 0x1234;

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

fn assert_fixture_inventory_is_exact(relative_dir: &str, expected: &[&str]) {
    let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_dir);
    let mut actual = Vec::new();
    for entry in std::fs::read_dir(&fixture_dir).expect("fixture directory should open") {
        let entry = entry.expect("fixture directory entry should be readable");
        assert!(
            entry
                .file_type()
                .expect("fixture file type should be readable")
                .is_file(),
            "fixture inventory must contain regular files only: {}",
            fixture_dir.display()
        );
        actual.push(
            entry
                .file_name()
                .into_string()
                .expect("fixture names should be UTF-8"),
        );
    }
    actual.sort();

    assert_eq!(
        actual,
        expected
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
        "canonical fixture inventory drifted: {}",
        fixture_dir.display()
    );
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

    let invalid_hash_comment =
        include_bytes!("../fixtures/fuzzing/dns_parse_hosts_bytes/invalid_hash_comment");
    let decoded_hash_comment = decode_hex_seed(invalid_hash_comment);
    assert!(
        !decoded_hash_comment.ends_with(b"\n"),
        "the final valid entry must exercise an unterminated line"
    );
    assert_eq!(
        observe_dns_parse_hosts_bytes(invalid_hash_comment)
            .expect("invalid UTF-8 confined to hosts comments should be ignored"),
        [
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 30), 5432)),
            SocketAddr::from((Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x30), 5432,)),
        ]
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
fn resolv_conf_byte_parser_corpus_pins_configuration_semantics() {
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

    let nameserver_truncation =
        include_bytes!("../fixtures/fuzzing/dns_parse_resolv_conf_bytes/nameserver_truncation");
    let decoded_nameserver_truncation = decode_hex_seed(nameserver_truncation);
    assert_eq!(
        decoded_nameserver_truncation,
        b"nameserver 192.0.2.4\n\
nameserver 192.0.2.1\n\
nameserver 192.0.2.4\n\
nameserver 192.0.2.7\n\
nameserver 192.0.2.2\n\
nameserver 192.0.2.8\n\
nameserver 192.0.2.3\n\
nameserver 192.0.2.6\n\
nameserver 192.0.2.5\n\
nameserver 192.0.2.1\n\
nameserver 192.0.2.9\n\
nameserver 2001:db8::10\n",
        "the canonical fixture must retain duplicates around the cap and later unique entries"
    );
    let (effective_nameservers, nameservers_were_truncated) =
        parse_resolv_conf_configuration_bytes(&decoded_nameserver_truncation)
            .expect("the canonical truncation fixture should expose effective metadata");
    assert!(nameservers_were_truncated);
    assert_eq!(
        observe_dns_parse_resolv_conf_bytes(nameserver_truncation)
            .expect("later unique nameservers should leave the effective first-eight list"),
        effective_nameservers,
        "the legacy fuzz entry must delegate to the metadata-producing parser"
    );
    assert_eq!(
        effective_nameservers,
        [4, 1, 7, 2, 8, 3, 6, 5]
            .into_iter()
            .map(|last_octet| SocketAddr::from((Ipv4Addr::new(192, 0, 2, last_octet), 53)))
            .collect::<Vec<_>>(),
        "the canonical fixture must pin exact first-seen truncation order"
    );
}

#[test]
fn sctp_recv_meta_wrapper_reaches_exact_data_and_notification_results() {
    let data_input = checked_native_seed(
        include_bytes!("../fixtures/fuzzing/sctp_parse_recv_meta/valid_rcvinfo_unaligned"),
        rcvinfo_input(sample_rcvinfo(), b"ping"),
    );
    #[cfg(all(target_pointer_width = "64", target_endian = "little"))]
    let data_input_storage = {
        let control_align = std::mem::align_of::<libc::cmsghdr>();
        let mut storage = vec![0; data_input.len() + control_align];
        let start = (0..control_align)
            .find(|start| storage[*start + 3..].as_ptr().align_offset(control_align) != 0)
            .expect("one bounded placement must make the control address unaligned");
        storage[start..start + data_input.len()].copy_from_slice(&data_input);
        (storage, start)
    };
    #[cfg(all(target_pointer_width = "64", target_endian = "little"))]
    let data_input =
        &data_input_storage.0[data_input_storage.1..data_input_storage.1 + data_input.len()];
    #[cfg(not(all(target_pointer_width = "64", target_endian = "little")))]
    let data_input = data_input.as_slice();
    #[cfg(all(target_pointer_width = "64", target_endian = "little"))]
    assert_ne!(
        data_input[3..]
            .as_ptr()
            .align_offset(std::mem::align_of::<libc::cmsghdr>()),
        0,
        "the checked corpus must exercise an unaligned control address"
    );
    assert_eq!(
        observe_sctp_parse_recv_meta(data_input).expect("valid RCVINFO should parse"),
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
fn sctp_notification_fixture_inventory_is_exact() {
    const EXPECTED: &[&str] = &[
        "adaptation",
        "assoc_change",
        "assoc_reset",
        "authentication",
        "common_header_too_short",
        "declared_length_below_header",
        "declared_length_beyond_buffer",
        "declared_short_shutdown",
        "legacy_send_failed",
        "partial_delivery",
        "partial_delivery_abort",
        "peer_addr_change",
        "peer_addr_change_ipv6",
        "peer_addr_change_unsupported_family",
        "remote_error",
        "send_failed_event",
        "sender_dry",
        "shutdown",
        "stream_change",
        "stream_reset",
        "stream_reset_with_tail",
        "unknown_notification",
    ];

    assert_fixture_inventory_is_exact("fixtures/fuzzing/sctp_parse_notification", EXPECTED);
}

#[test]
fn sctp_notification_boundary_fixtures_reach_exact_paths() {
    const HEADER_LEN: usize = 8;
    const PEER_ADDR_CHANGE_LEN: usize = 148;
    const PEER_ADDR_OFFSET: usize = HEADER_LEN;
    const PEER_ADDR_STATE_OFFSET: usize = 136;
    const PEER_ADDR_ERROR_OFFSET: usize = 140;
    const PEER_ADDR_ASSOC_ID_OFFSET: usize = 144;
    const UNKNOWN_TYPE: u16 = 0x800e;
    const PEER_ADDR_CHANGE_TYPE: u16 = (1 << 15) | 2;

    let unknown_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_notification/unknown_notification");
    let unknown = decode_hex_seed(unknown_fixture);
    assert_eq!(
        unknown,
        [0x0e, 0x80, 0x34, 0x12, 0x08, 0x00, 0x00, 0x00],
        "the unknown-notification fixture must remain one exact common header"
    );
    assert_eq!(
        observe_sctp_parse_notification(unknown_fixture)
            .expect("an exact unknown notification should remain visible"),
        SctpRecvMeta::Notification(SctpNotification::Other {
            kind: UNKNOWN_TYPE,
            flags: NOTIFICATION_FLAGS_SENTINEL,
            length: HEADER_LEN as u32,
        })
    );

    let common_short_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_notification/common_header_too_short");
    let common_short = decode_hex_seed(common_short_fixture);
    assert_eq!(
        common_short.as_slice(),
        &unknown[..HEADER_LEN - 1],
        "the short-header fixture must be exactly one byte short"
    );
    assert_eq!(
        observe_sctp_parse_notification(common_short_fixture)
            .expect_err("a seven-byte common header must be rejected")
            .kind(),
        std::io::ErrorKind::InvalidData
    );

    for (fixture, declared_len, label) in [
        (
            include_bytes!(
                "../fixtures/fuzzing/sctp_parse_notification/declared_length_below_header"
            ) as &[u8],
            HEADER_LEN - 1,
            "below-header",
        ),
        (
            include_bytes!(
                "../fixtures/fuzzing/sctp_parse_notification/declared_length_beyond_buffer"
            ) as &[u8],
            HEADER_LEN + 1,
            "beyond-buffer",
        ),
    ] {
        let decoded = decode_hex_seed(fixture);
        let mut expected = unknown.clone();
        expected[4..8].copy_from_slice(&(declared_len as u32).to_ne_bytes());
        assert_eq!(
            decoded, expected,
            "the {label} fixture must differ only in its declared length"
        );
        assert_eq!(
            observe_sctp_parse_notification(fixture)
                .expect_err("an invalid declared notification length must be rejected")
                .kind(),
            std::io::ErrorKind::InvalidData,
            "{label}"
        );
    }

    let ipv6_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_notification/peer_addr_change_ipv6");
    let ipv6 = decode_hex_seed(ipv6_fixture);
    let ipv6_addr = Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0025);
    let mut expected_ipv6 = vec![0u8; PEER_ADDR_CHANGE_LEN];
    write_ne(&mut expected_ipv6, 0, PEER_ADDR_CHANGE_TYPE.to_ne_bytes());
    write_ne(
        &mut expected_ipv6,
        2,
        NOTIFICATION_FLAGS_SENTINEL.to_ne_bytes(),
    );
    write_ne(
        &mut expected_ipv6,
        4,
        (PEER_ADDR_CHANGE_LEN as u32).to_ne_bytes(),
    );
    write_ne(
        &mut expected_ipv6,
        PEER_ADDR_OFFSET,
        (libc::AF_INET6 as u16).to_ne_bytes(),
    );
    expected_ipv6[PEER_ADDR_OFFSET + 2..PEER_ADDR_OFFSET + 4]
        .copy_from_slice(&3868u16.to_be_bytes());
    write_ne(
        &mut expected_ipv6,
        PEER_ADDR_OFFSET + 4,
        0x0102_0304u32.to_ne_bytes(),
    );
    expected_ipv6[PEER_ADDR_OFFSET + 8..PEER_ADDR_OFFSET + 24].copy_from_slice(&ipv6_addr.octets());
    write_ne(
        &mut expected_ipv6,
        PEER_ADDR_OFFSET + 24,
        0x0506_0708u32.to_ne_bytes(),
    );
    write_ne(
        &mut expected_ipv6,
        PEER_ADDR_STATE_OFFSET,
        (-10i32).to_ne_bytes(),
    );
    write_ne(
        &mut expected_ipv6,
        PEER_ADDR_ERROR_OFFSET,
        (-11i32).to_ne_bytes(),
    );
    write_ne(
        &mut expected_ipv6,
        PEER_ADDR_ASSOC_ID_OFFSET,
        (-12i32).to_ne_bytes(),
    );
    assert_eq!(
        ipv6, expected_ipv6,
        "the IPv6 peer-address-change fixture must pin the complete native layout"
    );
    assert_eq!(
        observe_sctp_parse_notification(ipv6_fixture)
            .expect("the IPv6 peer-address-change fixture should parse"),
        SctpRecvMeta::Notification(SctpNotification::PeerAddrChange {
            addr: SocketAddr::V6(SocketAddrV6::new(ipv6_addr, 3868, 0x0102_0304, 0x0506_0708,)),
            state: -10,
            error: -11,
            assoc_id: -12,
        })
    );

    let unsupported_fixture = include_bytes!(
        "../fixtures/fuzzing/sctp_parse_notification/peer_addr_change_unsupported_family"
    );
    let unsupported = decode_hex_seed(unsupported_fixture);
    let mut expected_unsupported = expected_ipv6;
    write_ne(
        &mut expected_unsupported,
        PEER_ADDR_OFFSET,
        libc::sa_family_t::MAX.to_ne_bytes(),
    );
    assert_eq!(
        unsupported, expected_unsupported,
        "the unsupported-family fixture must change only the address family"
    );
    assert_eq!(
        observe_sctp_parse_notification(unsupported_fixture)
            .expect_err("an unsupported peer address family must be rejected")
            .kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[test]
fn sctp_notification_fixtures_reach_expected_layouts() {
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/adaptation"
        ))
        .expect("adaptation notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::Adaptation {
            indication: 0x292a_2b2c,
            assoc_id: -29,
        })
    );
    let authentication_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_notification/authentication");
    assert_eq!(
        decode_hex_seed(authentication_fixture),
        [
            0x08, 0x80, 0x34, 0x12, 0x14, 0x00, 0x00, 0x00, 0x22, 0x11, 0x44, 0x33, 0x88, 0x77,
            0x66, 0x55, 0x40, 0x30, 0x20, 0x10,
        ],
        "the authentication fixture must pin the complete 20-byte Linux layout"
    );
    assert_eq!(
        observe_sctp_parse_notification(authentication_fixture)
            .expect("authentication notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::Authentication {
            flags: NOTIFICATION_FLAGS_SENTINEL,
            key_number: 0x1122,
            alternate_key_number: 0x3344,
            indication: 0x5566_7788,
            assoc_id: 0x1020_3040,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/assoc_change"
        ))
        .expect("association-change notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::AssocChange {
            state: 0,
            error: 0,
            outbound_streams: 1,
            inbound_streams: 1,
            assoc_id: 42,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/assoc_reset"
        ))
        .expect("association-reset notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::AssocReset {
            flags: NOTIFICATION_FLAGS_SENTINEL,
            assoc_id: -67,
            local_tsn: 0x4445_4647,
            remote_tsn: 0x4849_4a4b,
        })
    );

    let declared_short_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_notification/declared_short_shutdown");
    let declared_short_decoded = decode_hex_seed(declared_short_fixture);
    assert_eq!(
        declared_short_decoded,
        [
            0x05, 0x80, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        ],
        "the shutdown fixture must retain a complete 12-byte backing record"
    );
    assert_eq!(
        u32::from_ne_bytes(
            declared_short_decoded[4..8]
                .try_into()
                .expect("notification length bytes should be present")
        ),
        8,
        "the declared notification length must exclude the association id"
    );
    let declared_short = observe_sctp_parse_notification(declared_short_fixture)
        .expect_err("a declared-short shutdown notification must be rejected");
    assert_eq!(declared_short.kind(), std::io::ErrorKind::InvalidData);

    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/legacy_send_failed"
        ))
        .expect("legacy send-failed notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::SendFailed {
            error: 0x1314_1516,
            info: SctpSendInfo {
                stream_id: 0x1718,
                flags: 0x191a,
                ppid: 0x1b1c_1d1e,
                context: 0x1f20_2122,
                assoc_id: -23,
            },
            assoc_id: -24,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/partial_delivery"
        ))
        .expect("partial-delivery notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::PartialDelivery {
            indication: 0x3031_3233,
            assoc_id: -34,
            stream: 0x3536_3738,
            sequence: 0x393a_3b3c,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/partial_delivery_abort"
        ))
        .expect("partial-delivery abort notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::PartialDelivery {
            indication: 0,
            assoc_id: 0,
            stream: 0,
            sequence: 0,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/peer_addr_change"
        ))
        .expect("peer-address-change notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::PeerAddrChange {
            addr: SocketAddr::from((Ipv4Addr::new(192, 0, 2, 25), 3868)),
            state: -10,
            error: -11,
            assoc_id: -12,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/remote_error"
        ))
        .expect("remote-error notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::RemoteError {
            error: 0x2526,
            assoc_id: -27,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/send_failed_event"
        ))
        .expect("send-failed event notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::SendFailed {
            error: 0x5354_5556,
            info: SctpSendInfo {
                stream_id: 0x5758,
                flags: 0x595a,
                ppid: 0x5b5c_5d5e,
                context: 0x5f60_6162,
                assoc_id: -99,
            },
            assoc_id: -100,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/sender_dry"
        ))
        .expect("sender-dry notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::SenderDry { assoc_id: -61 })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/shutdown"
        ))
        .expect("shutdown notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::Shutdown { assoc_id: -28 })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/stream_change"
        ))
        .expect("stream-change notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::StreamChange {
            flags: NOTIFICATION_FLAGS_SENTINEL,
            assoc_id: -78,
            inbound_streams: 0x4f50,
            outbound_streams: 0x5152,
        })
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/stream_reset"
        ))
        .expect("stream-reset notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::StreamReset {
            flags: NOTIFICATION_FLAGS_SENTINEL,
            assoc_id: -64,
        })
    );

    let stream_reset_with_tail = decode_hex_seed(include_bytes!(
        "../fixtures/fuzzing/sctp_parse_notification/stream_reset_with_tail"
    ));
    assert_eq!(
        stream_reset_with_tail,
        [
            0x0a, 0x80, 0x34, 0x12, 0x10, 0x00, 0x00, 0x00, 0xc0, 0xff, 0xff, 0xff, 0x22, 0x11,
            0x44, 0x33,
        ],
        "the stream-reset-tail fixture must pin the complete declared Linux layout"
    );
    assert_eq!(
        observe_sctp_parse_notification(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_notification/stream_reset_with_tail"
        ))
        .expect("stream reset with a declared ID tail should parse"),
        SctpRecvMeta::Notification(SctpNotification::StreamReset {
            flags: NOTIFICATION_FLAGS_SENTINEL,
            assoc_id: -64,
        }),
        "FlowIO intentionally leaves the declared stream-ID tail unmaterialized"
    );
}

#[test]
fn sctp_assoc_addr_fixtures_reach_expected_forward_walk_results() {
    const CASE_PREFIX_LEN: usize = 2;
    const COMPACT_IPV4_LEN: usize = 8;

    let ipv4_loopback_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_assoc_addrs/ipv4_loopback");
    let ipv4_loopback_decoded = decode_hex_seed(ipv4_loopback_fixture);
    assert_eq!(ipv4_loopback_decoded[0], 1, "one address is declared");
    assert_eq!(
        ipv4_loopback_decoded[1], IGNORED_ASSOC_WRAPPER_SENTINEL,
        "the deliberately nonzero wrapper byte is reserved and ignored"
    );
    assert_eq!(
        observe_sctp_parse_assoc_addrs(ipv4_loopback_fixture)
            .expect("complete IPv4 association address should parse"),
        [SocketAddr::from((Ipv4Addr::LOCALHOST, 12_345))]
    );
    assert_eq!(
        observe_sctp_parse_assoc_addrs(include_bytes!(
            "../fixtures/fuzzing/sctp_parse_assoc_addrs/mixed_ipv4_ipv6_forward"
        ))
        .expect("mixed concrete association addresses should parse"),
        [
            SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 1_111)),
            SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1),
                2_222,
                7,
                9,
            )),
            SocketAddr::from((Ipv4Addr::new(5, 6, 7, 8), 3_333)),
        ]
    );

    let compact_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_assoc_addrs/compact_ipv4_rejected");
    let compact = decode_hex_seed(compact_fixture);
    assert_eq!(
        compact,
        [1, 0, 2, 0, 0x04, 0xd2, 192, 0, 2, 10],
        "the compact fixture must contain exactly one eight-byte IPv4 entry"
    );
    let compact_error = observe_sctp_parse_assoc_addrs(compact_fixture)
        .expect_err("one compact IPv4 entry must be rejected");
    assert_eq!(compact_error.kind(), std::io::ErrorKind::InvalidData);

    let dense_fixture = include_bytes!(
        "../fixtures/fuzzing/sctp_parse_assoc_addrs/two_compact_ipv4_records_rejected"
    );
    let dense = decode_hex_seed(dense_fixture);
    assert_eq!(
        dense,
        [
            2, 0, 2, 0, 0x04, 0x57, 1, 2, 3, 4, 2, 0, 0x08, 0xae, 5, 6, 7, 8,
        ],
        "the dense fixture must contain exactly two compact IPv4 entries"
    );
    let dense_payload = &dense[CASE_PREFIX_LEN..];
    assert_eq!(dense_payload.len(), COMPACT_IPV4_LEN * 2);
    assert_eq!(
        u16::from_ne_bytes(dense_payload[0..2].try_into().expect("first family bytes")),
        libc::AF_INET as u16
    );
    assert_eq!(
        u16::from_ne_bytes(
            dense_payload[8..10]
                .try_into()
                .expect("second family bytes")
        ),
        libc::AF_INET as u16
    );
    let dense_error = observe_sctp_parse_assoc_addrs(dense_fixture)
        .expect_err("two compact IPv4 entries must not satisfy two kernel-sized entries");
    assert_eq!(dense_error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        dense_error
            .get_ref()
            .and_then(|error| error.downcast_ref::<BufferRangeError>()),
        Some(&BufferRangeError {
            offset: 0,
            width: std::mem::size_of::<libc::sa_family_t>(),
            len: 0,
        })
    );

    let trailing_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_assoc_addrs/trailing_padding_rejected");
    let trailing = decode_hex_seed(trailing_fixture);
    assert_eq!(&trailing[..2], &[1, 0]);
    assert_eq!(
        trailing.len(),
        CASE_PREFIX_LEN + std::mem::size_of::<libc::sockaddr_in>() + COMPACT_IPV4_LEN
    );
    assert_eq!(
        u16::from_ne_bytes(trailing[2..4].try_into().expect("IPv4 family bytes")),
        libc::AF_INET as u16
    );
    assert!(
        trailing[CASE_PREFIX_LEN + std::mem::size_of::<libc::sockaddr_in>()..]
            .iter()
            .all(|byte| *byte == 0),
        "the trailing fixture must retain exactly eight zero padding bytes"
    );
    let trailing_error = observe_sctp_parse_assoc_addrs(trailing_fixture)
        .expect_err("one kernel-sized IPv4 entry plus padding must be rejected");
    assert_eq!(trailing_error.kind(), std::io::ErrorKind::InvalidData);

    let truncated_fixture =
        include_bytes!("../fixtures/fuzzing/sctp_parse_assoc_addrs/truncated_ipv6_rejected");
    let truncated = decode_hex_seed(truncated_fixture);
    assert_eq!(&truncated[..2], &[1, 0]);
    assert_eq!(
        truncated.len(),
        CASE_PREFIX_LEN + std::mem::size_of::<libc::sockaddr_in6>() - 1
    );
    assert_eq!(
        u16::from_ne_bytes(truncated[2..4].try_into().expect("IPv6 family bytes")),
        libc::AF_INET6 as u16
    );
    assert_eq!(
        &truncated[truncated.len() - 3..],
        &[0x09, 0x00, 0x00],
        "the one-byte-short scope id must remain pinned"
    );
    let truncated_error = observe_sctp_parse_assoc_addrs(truncated_fixture)
        .expect_err("one-byte-short IPv6 entry must be rejected");
    assert_eq!(truncated_error.kind(), std::io::ErrorKind::InvalidData);
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
fn tls_server_end_point_fixture_inventory_is_exact() {
    const EXPECTED: &[&str] = &[
        "malformed_nonminimal_outer_length",
        "malformed_signature_empty_payload",
        "malformed_signature_missing_unused_bits",
        "malformed_signature_nonzero_unused_bits_no_payload",
        "malformed_signature_nonzero_unused_bits_with_payload",
        "malformed_signature_tag",
        "malformed_truncated_long_length",
        "malformed_zero_padded_child_length",
        "unsupported_ed25519_minimal",
        "unsupported_rsa_sha256_wrong_parameters_minimal",
        "valid_ecdsa_sha256_minimal",
        "valid_rsa_sha256_absent_parameters_minimal",
        "valid_rsa_sha256_minimal",
    ];

    assert_fixture_inventory_is_exact("fixtures/fuzzing/tls_server_end_point", EXPECTED);
}

#[test]
fn tls_server_end_point_wrapper_reaches_valid_and_malformed_der() {
    let rsa = include_bytes!("../fixtures/fuzzing/tls_server_end_point/valid_rsa_sha256_minimal");
    let rsa_absent = include_bytes!(
        "../fixtures/fuzzing/tls_server_end_point/valid_rsa_sha256_absent_parameters_minimal"
    );
    let ecdsa =
        include_bytes!("../fixtures/fuzzing/tls_server_end_point/valid_ecdsa_sha256_minimal");
    assert_eq!(
        observe_tls_server_end_point(rsa)
            .expect("RSA SHA-256 seed should derive a binding")
            .len(),
        32
    );
    let rsa_der = decode_hex_seed(rsa);
    assert_eq!(
        rsa_der,
        [
            0x30, 0x15, 0x30, 0x00, 0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d,
            0x01, 0x01, 0x0b, 0x05, 0x00, 0x03, 0x02, 0x00, 0xa5,
        ],
        "minimal RSA seed bytes must stay exact"
    );
    assert_eq!(
        observe_tls_server_end_point(rsa_absent)
            .expect("RSA SHA-256 absent-parameter seed should derive a binding")
            .len(),
        32
    );
    let rsa_absent_der = decode_hex_seed(rsa_absent);
    assert_eq!(
        rsa_absent_der,
        [
            0x30, 0x13, 0x30, 0x00, 0x30, 0x0b, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d,
            0x01, 0x01, 0x0b, 0x03, 0x02, 0x00, 0xa5,
        ],
        "minimal absent-parameter RSA seed bytes must stay exact"
    );
    let rsa_wrong_parameters = include_bytes!(
        "../fixtures/fuzzing/tls_server_end_point/unsupported_rsa_sha256_wrong_parameters_minimal"
    );
    let rsa_wrong_parameters_der = decode_hex_seed(rsa_wrong_parameters);
    assert_eq!(
        rsa_wrong_parameters_der,
        [
            0x30, 0x15, 0x30, 0x00, 0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d,
            0x01, 0x01, 0x0b, 0x04, 0x00, 0x03, 0x02, 0x00, 0xa5,
        ],
        "minimal wrong-parameter RSA seed bytes must stay exact"
    );
    assert!(!tls_server_end_point(rsa_wrong_parameters));
    assert_eq!(
        observe_tls_server_end_point(ecdsa)
            .expect("ECDSA SHA-256 seed should derive a binding")
            .len(),
        32
    );
    let ecdsa_der = decode_hex_seed(ecdsa);
    assert_eq!(
        ecdsa_der,
        &[
            0x30, 0x19, 0x30, 0x00, 0x30, 0x0a, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04,
            0x03, 0x02, 0x03, 0x09, 0x00, 0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01,
        ],
        "minimal ECDSA seed bytes must stay exact"
    );

    let unsupported =
        include_bytes!("../fixtures/fuzzing/tls_server_end_point/unsupported_ed25519_minimal");
    let unsupported_der = decode_hex_seed(unsupported);
    let mut expected_unsupported = vec![
        0x30, 0x4c, 0x30, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x41, 0x00,
    ];
    expected_unsupported.extend_from_slice(&[0xa5; 64]);
    assert_eq!(
        unsupported_der, expected_unsupported,
        "minimal unsupported-algorithm seed bytes must stay exact"
    );
    assert!(!tls_server_end_point(unsupported));
    let malformed_tag =
        include_bytes!("../fixtures/fuzzing/tls_server_end_point/malformed_signature_tag");
    let mut expected_malformed_tag = rsa_der.clone();
    assert_eq!(expected_malformed_tag.len(), 23);
    expected_malformed_tag[19] = 0x04;
    assert_eq!(decode_hex_seed(malformed_tag), expected_malformed_tag);
    assert!(!tls_server_end_point(malformed_tag));

    let missing_unused_bits = include_bytes!(
        "../fixtures/fuzzing/tls_server_end_point/malformed_signature_missing_unused_bits"
    );
    let mut expected_missing_unused_bits = rsa_der.clone();
    expected_missing_unused_bits[1] = 0x13;
    expected_missing_unused_bits[20] = 0x00;
    expected_missing_unused_bits.truncate(21);
    assert_eq!(
        decode_hex_seed(missing_unused_bits),
        expected_missing_unused_bits
    );
    assert!(!tls_server_end_point(missing_unused_bits));

    let empty_payload = include_bytes!(
        "../fixtures/fuzzing/tls_server_end_point/malformed_signature_empty_payload"
    );
    let mut expected_empty_payload = rsa_der.clone();
    expected_empty_payload[1] = 0x14;
    expected_empty_payload[20] = 0x01;
    expected_empty_payload.truncate(22);
    assert_eq!(decode_hex_seed(empty_payload), expected_empty_payload);
    assert!(!tls_server_end_point(empty_payload));

    let nonzero_without_payload = include_bytes!(
        "../fixtures/fuzzing/tls_server_end_point/malformed_signature_nonzero_unused_bits_no_payload"
    );
    let mut expected_nonzero_without_payload = rsa_der.clone();
    expected_nonzero_without_payload[1] = 0x14;
    expected_nonzero_without_payload[20] = 0x01;
    expected_nonzero_without_payload[21] = 0x01;
    expected_nonzero_without_payload.truncate(22);
    assert_eq!(
        decode_hex_seed(nonzero_without_payload),
        expected_nonzero_without_payload
    );
    assert!(!tls_server_end_point(nonzero_without_payload));

    let nonzero_with_payload = include_bytes!(
        "../fixtures/fuzzing/tls_server_end_point/malformed_signature_nonzero_unused_bits_with_payload"
    );
    let mut expected_nonzero_with_payload = rsa_der.clone();
    expected_nonzero_with_payload[21] = 0x01;
    assert_eq!(
        decode_hex_seed(nonzero_with_payload),
        expected_nonzero_with_payload
    );
    assert!(!tls_server_end_point(nonzero_with_payload));

    let truncated_length =
        include_bytes!("../fixtures/fuzzing/tls_server_end_point/malformed_truncated_long_length");
    assert_eq!(decode_hex_seed(truncated_length), &[0x30, 0x82, 0x01, 0x00]);
    assert!(!tls_server_end_point(truncated_length));

    let nonminimal_outer = include_bytes!(
        "../fixtures/fuzzing/tls_server_end_point/malformed_nonminimal_outer_length"
    );
    let mut expected_nonminimal_outer = vec![0x30, 0x81, 0x15];
    expected_nonminimal_outer.extend_from_slice(&rsa_der[2..]);
    assert_eq!(decode_hex_seed(nonminimal_outer), expected_nonminimal_outer);
    assert!(!tls_server_end_point(nonminimal_outer));

    let zero_padded_child = include_bytes!(
        "../fixtures/fuzzing/tls_server_end_point/malformed_zero_padded_child_length"
    );
    let zero_padded_child_der = decode_hex_seed(zero_padded_child);
    assert_eq!(zero_padded_child_der.len(), 154);
    assert_eq!(
        &zero_padded_child_der[..7],
        &[0x30, 0x81, 0x97, 0x30, 0x82, 0x00, 0x80]
    );
    assert!(zero_padded_child_der[7..135].iter().all(|byte| *byte == 0));
    assert_eq!(&zero_padded_child_der[135..], &rsa_der[4..]);
    assert!(!tls_server_end_point(zero_padded_child));
}
