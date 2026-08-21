#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{
    CountingAllocator, ThreadLocalAllocationSnapshot, finish_counting_allocations_of_size,
    start_counting_allocations_of_size,
};
use flowio::test_support::net::resolver::parse_ipv4_response;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const QUERY_ID: u16 = 0x1234;
const QUERY_HOST: &str = "db.example.test";
const OWNER_LEN: usize = 47;
const TARGET_LEN: usize = 59;

#[derive(Clone, Copy)]
enum RecordSection {
    Answer,
    Authority,
    Additional,
}

fn push_wire_name(packet: &mut Vec<u8>, name: &str) {
    for label in name.split('.') {
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
}

fn cname_response(section: RecordSection, owner: &str, target: &str) -> Vec<u8> {
    cname_response_with_class(section, owner, target, 1)
}

fn cname_response_with_class(
    section: RecordSection,
    owner: &str,
    target: &str,
    class: u16,
) -> Vec<u8> {
    let (answer_count, authority_count, additional_count) = match section {
        RecordSection::Answer => (1u16, 0u16, 0u16),
        RecordSection::Authority => (0u16, 1u16, 0u16),
        RecordSection::Additional => (0u16, 0u16, 1u16),
    };

    let mut packet = Vec::new();
    packet.extend_from_slice(&QUERY_ID.to_be_bytes());
    packet.extend_from_slice(&0x8180u16.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&answer_count.to_be_bytes());
    packet.extend_from_slice(&authority_count.to_be_bytes());
    packet.extend_from_slice(&additional_count.to_be_bytes());
    push_wire_name(&mut packet, QUERY_HOST);
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());

    push_wire_name(&mut packet, owner);
    packet.extend_from_slice(&5u16.to_be_bytes());
    packet.extend_from_slice(&class.to_be_bytes());
    packet.extend_from_slice(&0u32.to_be_bytes());
    let target_wire_len =
        u16::try_from(target.len() + 2).expect("test target should fit DNS RDATA");
    packet.extend_from_slice(&target_wire_len.to_be_bytes());
    push_wire_name(&mut packet, target);
    packet
}

fn zero_answer_many_ignored_response() -> (Vec<u8>, usize) {
    const AUTHORITY_RECORDS: u16 = 8;
    const ADDITIONAL_RECORDS: u16 = 8;
    const QUESTION_OFFSET: u16 = 12;
    const UNKNOWN_RR_TYPE: u16 = 65_000;

    let mut packet = Vec::new();
    packet.extend_from_slice(&QUERY_ID.to_be_bytes());
    packet.extend_from_slice(&0x8180u16.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&0u16.to_be_bytes());
    packet.extend_from_slice(&AUTHORITY_RECORDS.to_be_bytes());
    packet.extend_from_slice(&ADDITIONAL_RECORDS.to_be_bytes());
    push_wire_name(&mut packet, QUERY_HOST);
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());

    let mut last_type_offset = 0;
    for value in 0..AUTHORITY_RECORDS + ADDITIONAL_RECORDS {
        packet.extend_from_slice(&(0xC000 | QUESTION_OFFSET).to_be_bytes());
        last_type_offset = packet.len();
        packet.extend_from_slice(&UNKNOWN_RR_TYPE.to_be_bytes());
        packet.extend_from_slice(&1u16.to_be_bytes());
        packet.extend_from_slice(&0u32.to_be_bytes());
        packet.extend_from_slice(&1u16.to_be_bytes());
        packet.push(value as u8);
    }

    (packet, last_type_offset)
}

fn counted_name_allocations(packet: &[u8], name_len: usize) -> usize {
    counted_name_allocations_for_host(packet, name_len, QUERY_HOST)
}

fn counted_name_allocations_for_host(packet: &[u8], name_len: usize, query_host: &str) -> usize {
    start_counting_allocations_of_size(name_len);
    let result = parse_ipv4_response(packet, QUERY_ID, query_host);
    let allocations = finish_counting_allocations_of_size();
    result.expect("valid DNS response should parse");
    allocations
}

#[test]
fn zero_answer_many_ignored_records_validate_without_result_reservation() {
    let (packet, last_type_offset) = zero_answer_many_ignored_response();

    // Warm the exact parser seam before observing its calling-thread allocator.
    parse_ipv4_response(&packet, QUERY_ID, QUERY_HOST).expect("warmup response should parse");
    let before = ThreadLocalAllocationSnapshot::current();
    let result = parse_ipv4_response(&packet, QUERY_ID, QUERY_HOST);
    let after = ThreadLocalAllocationSnapshot::current();
    result.expect("valid ignored records should produce an empty successful result");
    // The echoed question is the sole allocation. The old all-section reserve
    // added one result-vector allocation and matching deallocation here.
    after.assert_delta_since(before, 1, 1);

    let mut malformed = packet;
    malformed[last_type_offset..last_type_offset + 2].copy_from_slice(&1u16.to_be_bytes());
    let error = parse_ipv4_response(&malformed, QUERY_ID, QUERY_HOST)
        .expect_err("the final ignored A record has invalid one-byte RDATA");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "DNS A RDATA length was not 4 bytes");
}

#[test]
fn ignored_record_sections_validate_names_without_materializing_them() {
    let owner = "o".repeat(OWNER_LEN);
    let target = "t".repeat(TARGET_LEN);
    let authority = cname_response(RecordSection::Authority, &owner, &target);
    let additional = cname_response(RecordSection::Additional, &owner, &target);
    let answer = cname_response(RecordSection::Answer, &owner, &target);
    let non_in_answer = cname_response_with_class(RecordSection::Answer, &owner, &target, 3);

    // Warm the narrow parser seam before arming the thread-local counters.
    parse_ipv4_response(&authority, QUERY_ID, QUERY_HOST).expect("warmup response should parse");

    for packet in [&authority, &additional, &non_in_answer] {
        assert_eq!(counted_name_allocations(packet, OWNER_LEN), 0);
        assert_eq!(counted_name_allocations(packet, TARGET_LEN), 0);
    }

    // The Answer control proves the fixture observes both exact-capacity
    // allocations when section policy requires materialization.
    assert_eq!(counted_name_allocations(&answer, OWNER_LEN), 1);
    assert_eq!(counted_name_allocations(&answer, TARGET_LEN), 1);
}

#[test]
fn cname_chain_clones_only_the_final_selected_target() {
    const NAME_LEN: usize = 47;
    let names = ["a", "b", "c", "d"].map(|fill| fill.repeat(NAME_LEN));
    let query_host = &names[0];

    let mut packet = Vec::new();
    packet.extend_from_slice(&QUERY_ID.to_be_bytes());
    packet.extend_from_slice(&0x8180u16.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&3u16.to_be_bytes());
    packet.extend_from_slice(&0u16.to_be_bytes());
    packet.extend_from_slice(&0u16.to_be_bytes());
    push_wire_name(&mut packet, query_host);
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());

    for pair in names.windows(2) {
        push_wire_name(&mut packet, &pair[0]);
        packet.extend_from_slice(&5u16.to_be_bytes());
        packet.extend_from_slice(&1u16.to_be_bytes());
        packet.extend_from_slice(&0u32.to_be_bytes());
        let target_wire_len =
            u16::try_from(pair[1].len() + 2).expect("test target should fit DNS RDATA");
        packet.extend_from_slice(&target_wire_len.to_be_bytes());
        push_wire_name(&mut packet, &pair[1]);
    }

    // One question, three owners, three targets, and one final selected-target
    // clone allocate at this exact name size. Cloning once per hop would make
    // this count ten instead of eight.
    assert_eq!(
        counted_name_allocations_for_host(&packet, NAME_LEN, query_host),
        8
    );
}
