use flowio::net::resolver::DnsResolver;
use std::net::{Ipv4Addr, SocketAddr};

#[test]
fn resolver_configuration_accessors_are_nameable_downstream() {
    let nameservers: for<'a> fn(&'a DnsResolver) -> &'a [SocketAddr] = DnsResolver::nameservers;
    let system_nameservers_were_truncated: fn(&DnsResolver) -> bool =
        DnsResolver::system_nameservers_were_truncated;
    let first = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 53));
    let second = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 2), 53));
    let resolver =
        DnsResolver::new(vec![first, second, first]).expect("explicit resolver should be valid");

    assert_eq!(nameservers(&resolver), &[first, second]);
    assert!(!system_nameservers_were_truncated(&resolver));
}
