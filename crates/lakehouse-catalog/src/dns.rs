//! The DNS resolver of every client that does not resolve through reqwest's own (#693).

use std::error::Error;
use std::net::IpAddr;
use std::sync::Arc;

use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::{self, rustls_provider::CryptoMode};
use aws_smithy_http_client::{Builder, ConnectorBuilder};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use hickory_resolver::config::{LookupIpStrategy, ResolverConfig};
use hickory_resolver::net::NetError;
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::{ResolverBuilder, TokioResolver};
use rand::seq::SliceRandom;
use tokio::sync::OnceCell;

/// Caches each answer for its DNS TTL and shuffles it, so connections spread over a host's
/// addresses. A clone shares the cache.
#[derive(Clone, Debug, Default)]
pub struct CachingResolver {
    // Built on first lookup, because a client can be built outside the Tokio runtime it runs on.
    resolver: Arc<OnceCell<TokioResolver>>,
}

impl CachingResolver {
    /// The addresses of `host`, in random order.
    pub async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, Box<dyn Error + Send + Sync>> {
        let resolver = self
            .resolver
            .get_or_try_init(|| async { resolver_from(TokioResolver::builder_tokio()) })
            .await?;
        let mut addresses: Vec<IpAddr> = resolver.lookup_ip(host).await?.iter().collect();
        addresses.shuffle(&mut rand::thread_rng());
        Ok(addresses)
    }
}

/// The resolver of the node's `/etc/resolv.conf`. A node whose file names no DNS server still
/// resolves the names of its `/etc/hosts`, as it did through glibc.
fn resolver_from(
    system: Result<ResolverBuilder<TokioRuntimeProvider>, NetError>,
) -> Result<TokioResolver, NetError> {
    let mut builder = system.unwrap_or_else(|_| {
        TokioResolver::builder_with_config(
            ResolverConfig::from_parts(None, vec![], vec![]),
            TokioRuntimeProvider::default(),
        )
    });
    // Both families, as reqwest's own hickory resolver asks, for happy eyeballs.
    builder.options_mut().ip_strategy = LookupIpStrategy::Ipv6AndIpv4;
    builder.build()
}

impl ResolveDns for CachingResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        DnsFuture::new(async move { self.lookup(name).await.map_err(ResolveDnsError::new) })
    }
}

/// The HTTP client of an AWS SDK client: the SDK's default one with a [`CachingResolver`].
#[expect(
    clippy::disallowed_methods,
    reason = "the one place that builds an AWS SDK HTTP client"
)]
pub(crate) fn aws_http_client() -> SharedHttpClient {
    let resolver = CachingResolver::default();
    Builder::new().build_with_connector_fn(move |settings, components| {
        let mut connector =
            ConnectorBuilder::default().tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc));
        connector.set_connector_settings(settings.cloned());
        if let Some(components) = components {
            connector.set_sleep_impl(components.sleep_impl());
        }
        // Only the SDK's default client reads the proxy variables by itself (#539).
        connector.set_proxy_config(Some(ProxyConfig::from_env()));
        connector.build_with_resolver(resolver.clone())
    })
}

#[cfg(test)]
#[path = "dns_tests.rs"]
mod tests;
