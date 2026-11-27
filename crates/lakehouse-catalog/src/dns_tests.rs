use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use aws_smithy_runtime_api::client::dns::ResolveDns;
use hickory_resolver::net::NetError;
use tokio::sync::OnceCell;

use super::{CachingResolver, resolver_from};

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

#[tokio::test]
async fn a_node_without_a_dns_server_still_resolves_localhost() {
    let no_dns_server = Err(NetError::from(io::Error::other(
        "no nameservers found in config",
    )));
    let resolver = CachingResolver {
        resolver: Arc::new(OnceCell::new_with(Some(
            resolver_from(no_dns_server).unwrap(),
        ))),
    };

    assert_loopback(&resolver.lookup("localhost").await.unwrap());
}

/// `.invalid` never resolves and never reaches a DNS server (RFC 6761).
#[tokio::test]
async fn an_unresolvable_host_is_an_error() {
    let resolver = CachingResolver::default();

    assert!(resolver.lookup("host.invalid").await.is_err());
    assert!(resolver.resolve_dns("host.invalid").await.is_err());
}
