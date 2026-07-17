#![no_main]
//! TLS certificate DER parser and channel-binding derivation over arbitrary
//! bytes. Property: malformed tags, lengths, and certificate elements never
//! panic or read out of bounds; supported signature algorithms remain
//! reachable from the curated corpus.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    std::hint::black_box(flowio::fuzzing::tls_server_end_point(data));
});
