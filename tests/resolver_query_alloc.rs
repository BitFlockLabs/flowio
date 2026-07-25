#[path = "common/counting_allocator.rs"]
mod counting_allocator;
#[path = "common/dns_allocation.rs"]
mod dns_allocation;

use counting_allocator::{
    CountingAllocator, finish_counting_allocations_of_size, start_counting_allocations_of_size,
};
use dns_allocation::{
    completed_cname_followup_script, expected_followup_socket, maximum_query_name, serve_script,
};
use flowio::net::resolver::DnsResolver;
use flowio::net::udp::UdpSocket as FlowIoUdpSocket;
use flowio::runtime::executor::Executor;
use flowio::test_support::net::resolver::lookup_ipv4;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const MAXIMUM_QUERY_PACKET_LEN: usize = 271;

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

#[test]
fn one_query_allocation_serves_both_families_and_cname_followup() {
    let server = FlowIoUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind FlowIO DNS server");
    let nameserver = server
        .local_addr()
        .expect("failed to read FlowIO DNS server address");
    let resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
    let host = maximum_query_name();
    let script = completed_cname_followup_script(&host);
    let port = 4321;
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    start_counting_allocations_of_size(MAXIMUM_QUERY_PACKET_LEN);
    executor
        .run(async move {
            let server_task = Executor::spawn(serve_script(server, script))
                .expect("failed to spawn scripted DNS responder");
            let addrs = resolver
                .resolve_host(&host, port)
                .await
                .expect("CNAME follow-up should resolve");
            assert_eq!(addrs, [expected_followup_socket(port)]);
            server_task
                .await
                .expect("scripted DNS responder task was cancelled")
                .expect("scripted DNS responder failed");
        })
        .expect("executor run failed");
    let query_allocations = finish_counting_allocations_of_size();

    assert_eq!(
        query_allocations, 1,
        "the complete A/AAAA plus CNAME follow-up should share one query allocation"
    );
}
