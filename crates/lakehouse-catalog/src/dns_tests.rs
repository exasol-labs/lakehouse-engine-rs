use std::net::IpAddr;

use aws_smithy_runtime_api::client::dns::ResolveDns;

use super::CachingResolver;

fn assert_loopback(addresses: &[IpAddr]) {
    assert!(!addresses.is_empty());
    assert!(addresses.iter().all(IpAddr::is_loopback), "{addresses:?}");
}

#[tokio::test]
async fn localhost_resolves_to_loopback_addresses() {
    let resolver = CachingResolver::default();

    assert_loopback(&resolver.lookup("localhost").await.unwrap());
    assert_loopback(&resolver.resolve_dns("localhost").await.unwrap());
}

/// `.invalid` never resolves and never reaches a DNS server (RFC 6761).
#[tokio::test]
async fn an_unresolvable_host_is_an_error() {
    let resolver = CachingResolver::default();

    assert!(resolver.lookup("host.invalid").await.is_err());
    assert!(resolver.resolve_dns("host.invalid").await.is_err());
}
