//! The one DNS cache of a UDF VM process, and the factories of every client that resolves through
//! it (#701). Lookups go through the system resolver, so `nsswitch.conf`, `/etc/hosts`, and
//! `resolv.conf` apply as for any other process on the node.

use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::{self, rustls_provider::CryptoMode};
use aws_smithy_http_client::{Builder, ConnectorBuilder};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use futures::future::BoxFuture;
use object_store::ClientOptions;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use rand::seq::SliceRandom;

/// The JVM's defaults for `networkaddress.cache.ttl` and `networkaddress.cache.negative.ttl`.
const ANSWER_TTL: Duration = Duration::from_secs(30);
const FAILURE_TTL: Duration = Duration::from_secs(10);

/// Holds only data and locks, no Tokio handle, so every invocation's runtime can use it.
static CACHE: LazyLock<DnsCache> = LazyLock::new(|| DnsCache::new(system_lookup));

type Lookup = fn(String) -> BoxFuture<'static, io::Result<Vec<IpAddr>>>;

fn system_lookup(host: String) -> BoxFuture<'static, io::Result<Vec<IpAddr>>> {
    Box::pin(async move {
        let addresses = tokio::net::lookup_host((host.as_str(), 0)).await?;
        Ok(addresses.map(|address| address.ip()).collect())
    })
}

/// One slot per host. A lookup holds its host's slot, so concurrent requests for that host wait
/// for its answer instead of sending their own.
struct DnsCache {
    lookup: Lookup,
    hosts: Mutex<HashMap<String, Arc<tokio::sync::Mutex<Option<Answer>>>>>,
}

struct Answer {
    at: Instant,
    addresses: Result<Arc<[IpAddr]>, String>,
}

impl Answer {
    fn ttl(&self) -> Duration {
        if self.addresses.is_ok() {
            ANSWER_TTL
        } else {
            FAILURE_TTL
        }
    }
}

impl DnsCache {
    fn new(lookup: Lookup) -> Self {
        Self {
            lookup,
            hosts: Mutex::default(),
        }
    }

    async fn lookup(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        let slot = self
            .hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(host.to_string())
            .or_default()
            .clone();
        let mut answer = slot.lock().await;
        let held = match answer.as_ref() {
            Some(held) if held.at.elapsed() < held.ttl() => held.addresses.clone(),
            _ => {
                let addresses: Result<Arc<[IpAddr]>, String> = (self.lookup)(host.to_string())
                    .await
                    .map(Arc::from)
                    .map_err(|error| format!("could not resolve the host {host}: {error}"));
                *answer = Some(Answer {
                    at: Instant::now(),
                    addresses: addresses.clone(),
                });
                addresses
            }
        };
        drop(answer);
        let mut addresses = held.map_err(io::Error::other)?.to_vec();
        // Spreads connections across a host's addresses, as object_store's own resolver does.
        addresses.shuffle(&mut rand::thread_rng());
        Ok(addresses)
    }
}

/// A handle on the process's DNS cache, for every client kind's resolver hook.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CachingResolver;

impl CachingResolver {
    /// The addresses of `host`, in random order.
    pub(crate) async fn lookup(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        CACHE.lookup(host).await
    }
}

impl ResolveDns for CachingResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        DnsFuture::new(async move { self.lookup(name).await.map_err(ResolveDnsError::new) })
    }
}

impl reqwest::dns::Resolve for CachingResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let addresses = CACHE.lookup(name.as_str()).await?;
            let addresses: reqwest::dns::Addrs = Box::new(
                addresses
                    .into_iter()
                    .map(|address| SocketAddr::new(address, 0)),
            );
            Ok(addresses)
        })
    }
}

impl object_store::client::DnsResolver for CachingResolver {
    fn resolve(&self, host: &str) -> object_store::client::DnsFuture {
        let host = host.to_string();
        Box::pin(async move {
            CACHE
                .lookup(&host)
                .await
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        })
    }
}

/// The builder of every reqwest client, with the DNS cache.
#[expect(clippy::disallowed_methods, reason = "the factory of reqwest clients")]
pub(crate) fn http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().dns_resolver(CachingResolver)
}

/// The options every `object_store` client starts from: the default ones with the DNS cache.
#[expect(
    clippy::disallowed_methods,
    reason = "the factory of object_store client options"
)]
pub fn caching_client_options() -> ClientOptions {
    ClientOptions::new().with_dns_resolver(Arc::new(CachingResolver))
}

/// An S3 store builder that already holds [`caching_client_options`].
#[expect(clippy::disallowed_methods, reason = "the factory of S3 stores")]
pub(crate) fn s3_store_builder() -> AmazonS3Builder {
    AmazonS3Builder::new().with_client_options(caching_client_options())
}

/// An ADLS Gen2 store builder that already holds [`caching_client_options`].
#[expect(clippy::disallowed_methods, reason = "the factory of ADLS stores")]
pub fn adls_store_builder() -> MicrosoftAzureBuilder {
    MicrosoftAzureBuilder::new().with_client_options(caching_client_options())
}

/// The Glue client of `config`, on the caching HTTP client.
#[expect(clippy::disallowed_methods, reason = "the factory of Glue clients")]
pub(crate) fn glue_client(config: aws_sdk_glue::config::Builder) -> aws_sdk_glue::Client {
    aws_sdk_glue::Client::from_conf(config.http_client(aws_http_client()).build())
}

/// The STS client of `config`, on the caching HTTP client.
#[expect(clippy::disallowed_methods, reason = "the factory of STS clients")]
pub(crate) fn sts_client(config: aws_sdk_sts::config::Builder) -> aws_sdk_sts::Client {
    aws_sdk_sts::Client::from_conf(config.http_client(aws_http_client()).build())
}

/// The SDK's default HTTP client with a [`CachingResolver`].
#[expect(
    clippy::disallowed_methods,
    reason = "the factory of AWS SDK HTTP clients"
)]
fn aws_http_client() -> SharedHttpClient {
    Builder::new().build_with_connector_fn(move |settings, components| {
        let mut connector =
            ConnectorBuilder::default().tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc));
        connector.set_connector_settings(settings.cloned());
        if let Some(components) = components {
            connector.set_sleep_impl(components.sleep_impl());
        }
        // Only the SDK's default client reads the proxy variables by itself (#539).
        connector.set_proxy_config(Some(ProxyConfig::from_env()));
        connector.build_with_resolver(CachingResolver)
    })
}

#[cfg(test)]
#[path = "dns_tests.rs"]
mod tests;
