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
use flowio::net::udp::UdpSocket;
use flowio::runtime::executor::Executor;
use flowio::test_support::net::resolver::lookup_ipv4;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::time::Duration;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const DNS_RESPONSE_BUFFER_SIZE: usize = 2048;
const DNS_FLAG_QR: u16 = 0x8000;
const DNS_RCODE_SERVFAIL: u16 = 2;
const DNS_RCODE_REFUSED: u16 = 5;

async fn serve_error_responses(mut server: UdpSocket, rcodes: &'static [u16]) -> io::Result<()> {
    for &rcode in rcodes {
        let (recv_result, query) = server.recv_from(vec![0u8; 512], 512).await;
        let (query_len, peer) = recv_result?;
        if query_len < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS query was shorter than its ID",
            ));
        }

        let mut response = vec![0u8; 12];
        response[..2].copy_from_slice(&query[..2]);
        response[2..4].copy_from_slice(&(DNS_FLAG_QR | rcode).to_be_bytes());
        let (send_result, _) = server.send_to(response, peer).await;
        send_result?;
    }
    Ok(())
}

#[test]
fn completed_nameserver_failover_reuses_one_response_buffer() {
    let first_server = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind first FlowIO DNS server");
    let first_nameserver = first_server
        .local_addr()
        .expect("failed to read first FlowIO DNS server address");
    let second_server = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind second FlowIO DNS server");
    let second_nameserver = second_server
        .local_addr()
        .expect("failed to read second FlowIO DNS server address");
    let resolver =
        DnsResolver::new(vec![first_nameserver, second_nameserver]).expect("resolver init failed");
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    start_counting_allocations_of_size(DNS_RESPONSE_BUFFER_SIZE);
    executor
        .run(async move {
            let first_server_task =
                Executor::spawn(serve_error_responses(first_server, &[DNS_RCODE_SERVFAIL]))
                    .expect("failed to spawn first DNS responder");
            let second_server_task =
                Executor::spawn(serve_error_responses(second_server, &[DNS_RCODE_REFUSED]))
                    .expect("failed to spawn second DNS responder");
            lookup_ipv4(&resolver, "completed-failover.flowio.invalid")
                .await
                .expect_err("two DNS error responses should fail lookup");
            first_server_task
                .await
                .expect("first DNS responder task was cancelled")
                .expect("first DNS responder failed");
            second_server_task
                .await
                .expect("second DNS responder task was cancelled")
                .expect("second DNS responder failed");
        })
        .expect("executor run failed");
    let response_allocations = finish_counting_allocations_of_size();

    assert_eq!(
        response_allocations, 1,
        "completed nameserver failover should reuse one response allocation"
    );
}

#[test]
fn timed_out_receive_uses_a_distinct_response_buffer_for_failover() {
    let silent_server = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind silent DNS server");
    let silent_nameserver = silent_server
        .local_addr()
        .expect("failed to read silent DNS server address");
    let responding_server = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind responding FlowIO DNS server");
    let responding_nameserver = responding_server
        .local_addr()
        .expect("failed to read responding FlowIO DNS server address");
    let mut resolver = DnsResolver::new(vec![silent_nameserver, responding_nameserver])
        .expect("resolver init failed");
    resolver.set_query_timeout(Duration::from_millis(20));
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    start_counting_allocations_of_size(DNS_RESPONSE_BUFFER_SIZE);
    executor
        .run(async move {
            let server_task = Executor::spawn(serve_error_responses(
                responding_server,
                &[DNS_RCODE_REFUSED],
            ))
            .expect("failed to spawn DNS responder");
            lookup_ipv4(&resolver, "timeout-failover.flowio.invalid")
                .await
                .expect_err("timeout followed by DNS refusal should fail lookup");
            server_task
                .await
                .expect("DNS responder task was cancelled")
                .expect("DNS responder failed");
        })
        .expect("executor run failed");
    let response_allocations = finish_counting_allocations_of_size();

    assert_eq!(
        response_allocations, 2,
        "a timed-out kernel-visible receive buffer must not be reused"
    );
    silent_server
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("failed to set silent DNS receive timeout");
    let mut query = [0u8; 512];
    silent_server
        .recv_from(&mut query)
        .expect("silent nameserver did not receive the first query");
}

#[test]
fn one_completed_response_allocation_serves_families_and_cname_followup() {
    let server = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("failed to bind FlowIO DNS server");
    let nameserver = server
        .local_addr()
        .expect("failed to read FlowIO DNS server address");
    let resolver = DnsResolver::new(vec![nameserver]).expect("resolver init failed");
    let host = maximum_query_name();
    let script = completed_cname_followup_script(&host);
    let port = 4321;
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    start_counting_allocations_of_size(DNS_RESPONSE_BUFFER_SIZE);
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
    let response_allocations = finish_counting_allocations_of_size();

    assert_eq!(
        response_allocations, 1,
        "the complete A/AAAA plus CNAME follow-up should share one completed response allocation"
    );
}
