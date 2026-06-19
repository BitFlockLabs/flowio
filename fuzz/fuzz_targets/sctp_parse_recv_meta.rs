#![no_main]
//! SCTP recvmsg metadata parser over arbitrary control/data bytes.
//! Property: malformed cmsg/notification combinations never panic or read out
//! of bounds.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    flowio::fuzzing::sctp_parse_recv_meta(data);
});
