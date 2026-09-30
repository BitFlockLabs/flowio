#![no_main]
//! SCTP association-address parser over arbitrary packed address bytes.
//! Property: the family-directed forward walk remains bounded and malformed
//! address payloads never panic or read out of bounds. Fixture cases live in
//! the `flowio` package's `fixtures/fuzzing/sctp_parse_assoc_addrs/` directory.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::sctp_parse_assoc_addrs(data);
});
