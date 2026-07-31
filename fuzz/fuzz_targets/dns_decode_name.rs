#![no_main]
//! DNS name validation and materialization over arbitrary packet bytes.
//! Property: the shared bounded walker validates UTF-8 and compression before
//! the exact-capacity string is materialized; neither the direct nor selected
//! offset may panic, read out of bounds, or recurse without a bound. Named
//! cases live in `flowio/fixtures/fuzzing/dns_decode_name/`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::dns_decode_name(data);
});
