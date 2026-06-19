#![no_main]
//! DNS response parser over arbitrary packet bytes plus derived query metadata.
//! Property: malformed answers never panic, read out of bounds, or recurse
//! without a bound.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::dns_parse_response_packet(data);
});
