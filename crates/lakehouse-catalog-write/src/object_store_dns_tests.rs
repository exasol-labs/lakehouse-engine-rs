use super::caching_client_options;

#[tokio::test]
async fn an_object_store_client_resolves_localhost_to_loopback_addresses() {
    let options = caching_client_options();
    let resolver = options
        .dns_resolver()
        .expect("the options carry a resolver");

    let addresses = resolver.resolve("localhost").await.unwrap();

    assert!(!addresses.is_empty());
    assert!(
        addresses.iter().all(|address| address.is_loopback()),
        "{addresses:?}"
    );
}
