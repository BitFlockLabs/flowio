#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{AllocationSnapshot, CountingAllocator};
use flowio::test_support::net::resolver::response_is_decodable_candidate;
use std::hint::black_box;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const QUERY_ID: u16 = 0x1234;
const TRUNCATED_POINTER: &[u8] = &[0x12, 0x34, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0];
const FORWARD_POINTER: &[u8] = &[
    0x12, 0x34, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0, 0x0c, 0, 1, 0, 1,
];
const BACKWARD_POINTER_LOOP: &[u8] = &[
    0x12, 0x34, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 1, b'x', 0xc0, 0x0c, 0, 1, 0, 1,
];
const UNSUPPORTED_LABEL: &[u8] = &[
    0x12, 0x34, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 0x40, 0, 1, 0, 1,
];
const INVALID_UTF8_LABEL: &[u8] = &[
    0x12, 0x34, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0xff, 0, 0, 1, 0, 1,
];
const LITERAL_DOT_LABEL: &[u8] = &[
    0x12, 0x34, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 11, b'e', b'x', b'a', b'm', b'p', b'l', b'e',
    b'.', b'c', b'o', b'm', 0, 0, 1, 0, 1,
];
const COMPRESSED_LITERAL_DOT_LABEL: &[u8] = &[
    0x12, 0x34, 0x81, 0x80, 0, 1, 3, b'a', b'.', b'b', 0, 0, 0xc0, 6, 0, 1, 0, 1,
];
const NON_QUERY_OPCODE: &[u8] = &[
    0x12, 0x34, 0x89, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 7, b'e', b'x', b'a', b'm', b'p', b'l', b'e', 3,
    b'c', b'o', b'm', 0, 0, 1, 0, 1,
];

fn overlong_question() -> Vec<u8> {
    let mut packet = vec![0x12, 0x34, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0];
    for label_len in [63usize, 63, 63, 62] {
        packet.push(label_len as u8);
        packet.extend(std::iter::repeat_n(b'x', label_len));
    }
    packet.extend_from_slice(&[0, 0, 1, 0, 1]);
    packet
}

#[test]
fn malformed_dns_candidates_are_allocation_free() {
    const ROUNDS: usize = 16_384;
    let overlong_question = overlong_question();
    let malformed: [&[u8]; 9] = [
        TRUNCATED_POINTER,
        FORWARD_POINTER,
        BACKWARD_POINTER_LOOP,
        UNSUPPORTED_LABEL,
        INVALID_UTF8_LABEL,
        LITERAL_DOT_LABEL,
        COMPRESSED_LITERAL_DOT_LABEL,
        NON_QUERY_OPCODE,
        &overlong_question,
    ];

    for packet in malformed {
        assert!(!response_is_decodable_candidate(packet, QUERY_ID));
    }

    let before = AllocationSnapshot::current();
    for _ in 0..ROUNDS {
        for packet in malformed {
            assert!(!black_box(response_is_decodable_candidate(
                black_box(packet),
                QUERY_ID,
            )));
        }
    }
    let after = AllocationSnapshot::current();

    after.assert_unchanged_since(before);
}
