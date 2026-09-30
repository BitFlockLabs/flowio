#![no_main]
//! SCTP notification parsing and record recovery over arbitrary control bytes.
//! Exercises every bounded split of the input prefix. Properties: no panics,
//! out-of-bounds reads, or unbounded execution. Fixture cases live in the
//! `flowio` package's `fixtures/fuzzing/sctp_parse_notification/` directory.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::sctp_parse_notification(data);
});
