#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{AllocationSnapshot, CountingAllocator};
use flowio::test_support::net::resolver::extend_unique_socket_addrs;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[test]
fn resolver_address_deduplication_needs_no_scratch_allocation() {
    let port = 5432;
    let existing_v4 = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), port));
    let new_v4 = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 11), port));
    let existing_v6 = SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 10), port));
    let new_v6 = SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 11), port));
    let mut addrs = Vec::with_capacity(4);
    addrs.extend([existing_v4, existing_v6]);
    let ips = [
        existing_v4.ip(),
        new_v4.ip(),
        existing_v6.ip(),
        new_v6.ip(),
        new_v4.ip(),
    ];

    let before = AllocationSnapshot::current();
    extend_unique_socket_addrs(&mut addrs, &ips, port);
    let after = AllocationSnapshot::current();

    after.assert_unchanged_since(before);
    assert_eq!(addrs, [existing_v4, existing_v6, new_v4, new_v6]);
}
