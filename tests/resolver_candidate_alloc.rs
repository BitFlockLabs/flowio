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

#[test]
fn malformed_dns_candidates_are_allocation_free() {
    const ROUNDS: usize = 16_384;
    const MALFORMED: [&[u8]; 4] = [
        TRUNCATED_POINTER,
        FORWARD_POINTER,
        BACKWARD_POINTER_LOOP,
        UNSUPPORTED_LABEL,
    ];

    for packet in MALFORMED {
        assert!(!response_is_decodable_candidate(packet, QUERY_ID));
    }

    let before = AllocationSnapshot::current();
    for _ in 0..ROUNDS {
        for packet in MALFORMED {
            assert!(!black_box(response_is_decodable_candidate(
                black_box(packet),
                QUERY_ID,
            )));
        }
    }
    let after = AllocationSnapshot::current();

    after.assert_unchanged_since(before);
}
