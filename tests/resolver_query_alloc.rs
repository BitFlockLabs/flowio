#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{
    CountingAllocator, finish_counting_allocations_of_size, start_counting_allocations_of_size,
};
use flowio::net::resolver::DnsResolver;
use flowio::runtime::executor::Executor;
use flowio::test_support::net::resolver::lookup_ipv4;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const MAXIMUM_QUERY_PACKET_LEN: usize = 271;

fn maximum_query_name() -> String {
    [(63usize, 'a'), (63, 'b'), (63, 'c'), (61, 'd')]
        .into_iter()
        .map(|(len, fill)| std::iter::repeat_n(fill, len).collect::<String>())
        .collect::<Vec<_>>()
        .join(".")
}

fn receive_query(socket: &UdpSocket) -> Vec<u8> {
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("failed to set DNS query receive timeout");
    let mut packet = [0u8; 512];
    let (len, _) = socket
        .recv_from(&mut packet)
        .expect("nameserver did not receive the DNS query");
    packet[..len].to_vec()
}

#[test]
fn dns_query_buffer_moves_across_nameserver_retries_without_cloning() {
    let first_server = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind first DNS server");
    let second_server = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind second DNS server");
    let first_nameserver = first_server
        .local_addr()
        .expect("missing first DNS address");
    let second_nameserver = second_server
        .local_addr()
        .expect("missing second DNS address");
    let host = maximum_query_name();
    assert_eq!(host.len(), 253);

    let mut resolver =
        DnsResolver::new(vec![first_nameserver, second_nameserver]).expect("resolver init failed");
    resolver.set_query_timeout(Duration::from_millis(20));
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    start_counting_allocations_of_size(MAXIMUM_QUERY_PACKET_LEN);
    executor
        .run(async move {
            let err = lookup_ipv4(&resolver, &host)
                .await
                .expect_err("two unanswered nameservers should time out");
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        })
        .expect("executor run failed");
    let query_sized_allocations = finish_counting_allocations_of_size();
    assert_eq!(
        query_sized_allocations, 1,
        "expected one exact-capacity query allocation and no retry clone"
    );

    let first_query = receive_query(&first_server);
    let second_query = receive_query(&second_server);
    assert_eq!(first_query.len(), MAXIMUM_QUERY_PACKET_LEN);
    assert_eq!(second_query, first_query);
}
