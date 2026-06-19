#![no_main]
//! Reference fuzz target: DNS name decompression over arbitrary packet bytes.
//! Property: never panics, never reads out of bounds, terminates (the
//! compression-pointer loop must stay bounded). Seed from flowio/tests/resolver.rs.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::dns_decode_name(data);
});
