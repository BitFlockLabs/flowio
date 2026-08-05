#![no_main]
//! Reference fuzz target: SCTP notification parser over arbitrary control
//! bytes. Property: never panics, never reads out of bounds, always
//! terminates. Maintained cases live in
//! `flowio/fixtures/fuzzing/sctp_parse_notification/`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::sctp_parse_notification(data);
});
