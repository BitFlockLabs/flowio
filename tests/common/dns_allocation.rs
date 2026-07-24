use flowio::net::udp::UdpSocket;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};

const DNS_CLASS_IN: u16 = 1;
const DNS_TYPE_A: u16 = 1;
const DNS_TYPE_CNAME: u16 = 5;
const DNS_TYPE_AAAA: u16 = 28;
const DNS_QUERY_FLAGS: u16 = 0x0100;
const DNS_RESPONSE_FLAGS: u16 = 0x8180;

pub const FOLLOWUP_HOST: &str = "canonical.flowio.invalid";
pub const FOLLOWUP_ADDRESS: Ipv4Addr = Ipv4Addr::new(203, 0, 113, 7);

pub fn maximum_query_name() -> String {
    [(63usize, 'a'), (63, 'b'), (63, 'c'), (61, 'd')]
        .into_iter()
        .map(|(len, fill)| std::iter::repeat_n(fill, len).collect::<String>())
        .collect::<Vec<_>>()
        .join(".")
}

pub struct ScriptedDnsReply {
    expected_query: Vec<u8>,
    response: Vec<u8>,
}

pub fn completed_cname_followup_script(initial_host: &str) -> Vec<ScriptedDnsReply> {
    vec![
        ScriptedDnsReply {
            expected_query: query_packet(initial_host, DNS_TYPE_A),
            response: cname_response(initial_host, DNS_TYPE_A, FOLLOWUP_HOST),
        },
        ScriptedDnsReply {
            expected_query: query_packet(initial_host, DNS_TYPE_AAAA),
            response: cname_response(initial_host, DNS_TYPE_AAAA, FOLLOWUP_HOST),
        },
        ScriptedDnsReply {
            expected_query: query_packet(FOLLOWUP_HOST, DNS_TYPE_A),
            response: address_response(FOLLOWUP_HOST, FOLLOWUP_ADDRESS),
        },
        ScriptedDnsReply {
            expected_query: query_packet(FOLLOWUP_HOST, DNS_TYPE_AAAA),
            response: empty_response(FOLLOWUP_HOST, DNS_TYPE_AAAA),
        },
    ]
}

pub async fn serve_script(mut server: UdpSocket, script: Vec<ScriptedDnsReply>) -> io::Result<()> {
    for mut exchange in script {
        let (recv_result, query) = server.recv_from(vec![0u8; 512], 512).await;
        let (query_len, peer) = recv_result?;
        let query = query.get(..query_len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "scripted DNS query length exceeded its receive buffer",
            )
        })?;
        if query.len() < 2
            || query.len() != exchange.expected_query.len()
            || query[2..] != exchange.expected_query[2..]
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "resolver query did not match the scripted host/type order",
            ));
        }

        exchange.response[..2].copy_from_slice(&query[..2]);
        let (send_result, _) = server.send_to(exchange.response, peer).await;
        send_result?;
    }
    Ok(())
}

fn query_packet(host: &str, qtype: u16) -> Vec<u8> {
    let mut packet = dns_header(0, DNS_QUERY_FLAGS, 1, 0);
    push_wire_name(&mut packet, host);
    packet.extend_from_slice(&qtype.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet
}

fn cname_response(query_host: &str, qtype: u16, target: &str) -> Vec<u8> {
    let mut packet = dns_header(0, DNS_RESPONSE_FLAGS, 1, 1);
    push_wire_name(&mut packet, query_host);
    packet.extend_from_slice(&qtype.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet.extend_from_slice(&[0xC0, 0x0C]);
    packet.extend_from_slice(&DNS_TYPE_CNAME.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet.extend_from_slice(&0u32.to_be_bytes());
    let target_wire_len = u16::try_from(target.len() + 2)
        .expect("scripted DNS CNAME target should fit its RDATA length");
    packet.extend_from_slice(&target_wire_len.to_be_bytes());
    push_wire_name(&mut packet, target);
    packet
}

fn address_response(query_host: &str, address: Ipv4Addr) -> Vec<u8> {
    let mut packet = dns_header(0, DNS_RESPONSE_FLAGS, 1, 1);
    push_wire_name(&mut packet, query_host);
    packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet.extend_from_slice(&[0xC0, 0x0C]);
    packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet.extend_from_slice(&0u32.to_be_bytes());
    packet.extend_from_slice(&4u16.to_be_bytes());
    packet.extend_from_slice(&address.octets());
    packet
}

fn empty_response(query_host: &str, qtype: u16) -> Vec<u8> {
    let mut packet = dns_header(0, DNS_RESPONSE_FLAGS, 1, 0);
    push_wire_name(&mut packet, query_host);
    packet.extend_from_slice(&qtype.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet
}

fn dns_header(query_id: u16, flags: u16, questions: u16, answers: u16) -> Vec<u8> {
    let mut packet = Vec::new();
    for field in [query_id, flags, questions, answers, 0, 0] {
        packet.extend_from_slice(&field.to_be_bytes());
    }
    packet
}

fn push_wire_name(packet: &mut Vec<u8>, name: &str) {
    for label in name.split('.') {
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
}

pub fn expected_followup_socket(port: u16) -> SocketAddr {
    SocketAddr::from((FOLLOWUP_ADDRESS, port))
}
