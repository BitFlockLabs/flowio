#![no_main]
//! DNS response prefilter over arbitrary packet bytes.
//! Property: the drain-loop candidate mode and full response-envelope mode use
//! the same bounded question-name walker and retain identical structural
//! acceptance. Neither mode may panic, read out of bounds, or recurse without
//! a bound. Named cases live in
//! `flowio/fixtures/fuzzing/dns_response_prefilter/`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::dns_response_prefilter(data);
});
