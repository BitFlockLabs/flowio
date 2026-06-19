#![no_main]
//! Reference fuzz target: SCTP notification parser over arbitrary control
//! bytes. Property: never panics, never reads out of bounds, always
//! terminates. Seed the corpus from the fixtures in flowio/tests/sctp.rs.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::sctp_parse_notification(data);
});
