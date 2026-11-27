//! Gives an `object_store` client the engine's DNS cache (#693).

use lakehouse_catalog::CachingResolver;
use object_store::ClientOptions;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::client::{DnsFuture, DnsResolver};
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::Arc;

#[derive(Debug, Default)]
struct CachedDns(CachingResolver);

// `object_store` demands both of a `DnsResolver`, and a lookup leaves no state a panic could tear.
impl UnwindSafe for CachedDns {}
impl RefUnwindSafe for CachedDns {}

impl DnsResolver for CachedDns {
    fn resolve(&self, host: &str) -> DnsFuture {
        let (resolver, host) = (self.0.clone(), host.to_string());
        Box::pin(async move { resolver.lookup(&host).await })
    }
}

/// The options every `object_store` client starts from: the default ones with the DNS cache,
/// which replaces `object_store`'s own uncached address shuffle.
#[expect(
    clippy::disallowed_methods,
    reason = "the factory of object_store client options"
)]
pub fn caching_client_options() -> ClientOptions {
    ClientOptions::new().with_dns_resolver(Arc::new(CachedDns::default()))
}

/// An S3 store builder that already holds [`caching_client_options`].
#[expect(clippy::disallowed_methods, reason = "the factory of S3 stores")]
pub fn s3_store_builder() -> AmazonS3Builder {
    AmazonS3Builder::new().with_client_options(caching_client_options())
}

/// An ADLS Gen2 store builder that already holds [`caching_client_options`].
#[expect(clippy::disallowed_methods, reason = "the factory of ADLS stores")]
pub fn adls_store_builder() -> MicrosoftAzureBuilder {
    MicrosoftAzureBuilder::new().with_client_options(caching_client_options())
}

#[cfg(test)]
#[path = "object_store_dns_tests.rs"]
mod tests;
