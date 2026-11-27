use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicUsize, Ordering};

use aws_smithy_runtime_api::client::dns::ResolveDns;

use super::*;

fn assert_loopback(addresses: &[IpAddr]) {
    assert!(!addresses.is_empty());
    assert!(addresses.iter().all(IpAddr::is_loopback), "{addresses:?}");
}

/// `localhost` comes from the hosts file, so the system resolver sends no DNS query for it.
#[tokio::test]
async fn every_client_hook_resolves_localhost_to_loopback_addresses() {
    assert_loopback(&CachingResolver.lookup("localhost").await.unwrap());
    assert_loopback(&CachingResolver.resolve_dns("localhost").await.unwrap());
    let options = caching_client_options();
    let resolver = options
        .dns_resolver()
        .expect("the options carry a resolver");
    assert_loopback(&resolver.resolve("localhost").await.unwrap());
    let name: reqwest::dns::Name = "localhost".parse().unwrap();
    let addresses: Vec<IpAddr> = reqwest::dns::Resolve::resolve(&CachingResolver, name)
        .await
        .unwrap()
        .map(|address| address.ip())
        .collect();
    assert_loopback(&addresses);
}

static ANSWERED: AtomicUsize = AtomicUsize::new(0);

fn answering(_: String) -> BoxFuture<'static, io::Result<Vec<IpAddr>>> {
    ANSWERED.fetch_add(1, Ordering::SeqCst);
    Box::pin(async {
        // Lets every concurrent request reach the cache before the answer arrives.
        tokio::task::yield_now().await;
        Ok(vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        ])
    })
}

/// Scenario: Concurrent and repeated requests for one host share one lookup
#[tokio::test]
async fn concurrent_and_later_requests_for_one_host_share_one_lookup() {
    let cache = DnsCache::new(answering);

    let answers = futures::future::join_all((0..8).map(|_| cache.lookup("store.example"))).await;
    let later = cache.lookup("store.example").await.unwrap();

    assert_eq!(ANSWERED.load(Ordering::SeqCst), 1);
    for mut addresses in answers.into_iter().map(Result::unwrap).chain([later]) {
        addresses.sort();
        assert_eq!(addresses.len(), 2, "{addresses:?}");
        assert_eq!(addresses[0], IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    }
}

static FAILED: AtomicUsize = AtomicUsize::new(0);

fn failing(_: String) -> BoxFuture<'static, io::Result<Vec<IpAddr>>> {
    FAILED.fetch_add(1, Ordering::SeqCst);
    Box::pin(async { Err(io::Error::other("Name or service not known")) })
}

/// Scenario: A failed lookup is cached and names the host
#[tokio::test]
async fn a_failed_lookup_is_cached_and_names_the_host() {
    let cache = DnsCache::new(failing);

    let first = cache.lookup("missing.example").await.unwrap_err();
    let second = cache.lookup("missing.example").await.unwrap_err();

    assert_eq!(FAILED.load(Ordering::SeqCst), 1);
    for error in [first, second] {
        assert_eq!(
            error.to_string(),
            "could not resolve the host missing.example: Name or service not known"
        );
    }
}
