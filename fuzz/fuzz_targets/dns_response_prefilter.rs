#![no_main]
//! DNS response prefilter over arbitrary packet bytes: the drain-loop candidate
//! check plus its question-name compression-pointer walk (`skip_dns_name`),
//! which is a distinct path from `decode_name`/`parse_response_packet`.
//! Property: the pointer walk stays bounded and never panics or reads out of
//! bounds. Seed the corpus from the DNS response fixtures in
//! flowio/tests/resolver.rs.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::dns_response_prefilter(data);
});
