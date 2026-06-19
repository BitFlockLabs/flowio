#![no_main]
//! SCTP association-address parser over arbitrary packed address bytes.
//! Property: address-count backtracking remains bounded and malformed address
//! payloads never panic or read out of bounds.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::sctp_parse_assoc_addrs(data);
});
