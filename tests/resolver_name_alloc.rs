#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{CountingAllocator, ThreadLocalAllocationSnapshot};
use flowio::test_support::net::resolver::decode_name;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn assert_single_exact_string_allocation(packet: &[u8], offset: usize, expected: &str) {
    let before = ThreadLocalAllocationSnapshot::current();
    let (name, consumed) = decode_name(packet, offset).expect("valid DNS name should decode");
    let live = ThreadLocalAllocationSnapshot::current();
    live.assert_delta_since(before, 1, 0);

    assert_eq!(name, expected);
    assert_eq!(name.capacity(), expected.len());
    assert!(consumed > 0);

    drop(name);
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 1);
}

fn maximum_compressed_name() -> (Vec<u8>, usize, String) {
    let labels = [(63usize, b'a'), (63, b'b'), (63, b'c'), (59, b'd')];
    let mut packet = Vec::new();
    let mut suffix = String::new();
    for (index, (len, fill)) in labels.into_iter().enumerate() {
        packet.push(len as u8);
        packet.extend(std::iter::repeat_n(fill, len));
        if index != 0 {
            suffix.push('.');
        }
        suffix.extend(std::iter::repeat_n(fill as char, len));
    }
    packet.push(0);

    let offset = packet.len();
    packet.extend_from_slice(&[1, b'x', 0xc0, 0]);
    (packet, offset, format!("x.{suffix}"))
}

#[test]
fn decoded_dns_names_allocate_only_the_exact_result_string() {
    let direct = b"\x03www\x07example\x03com\0";
    let (compressed, compressed_offset, compressed_expected) = maximum_compressed_name();

    // Warm the narrow path before taking calling-thread allocator snapshots.
    drop(decode_name(direct, 0).expect("warmup DNS name should decode"));

    let before = ThreadLocalAllocationSnapshot::current();
    let (root, consumed) = decode_name(&[0], 0).expect("wire root should decode");
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(before);
    assert!(root.is_empty());
    assert_eq!(consumed, 1);
    drop(root);
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(before);

    assert_single_exact_string_allocation(direct, 0, "www.example.com");
    assert_single_exact_string_allocation(&compressed, compressed_offset, &compressed_expected);
}
