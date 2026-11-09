//! The vended storage credentials of an export table, renewed through the catalog's
//! `loadCredentials` endpoint before they expire. The source sits inside the table's FileIO, so
//! every storage request of a writer, its commit's manifests included, signs with fresh keys.

use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use lakehouse_catalog::{
    AtRenewalPoint, AwsCredential, CatalogSession, CustomAwsCredentialLoader, EpochClock,
    ExpiringCredential, Lease, RenewalPoint, VendedS3Credentials, redact_error_text, scheme_of,
};
use reqsign_core::time::Timestamp;
use reqsign_core::{Context, ProvideCredential};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Iceberg Java's `VendedCredentialsProvider` prefetches this long before expiry.
const RENEWAL_SKEW: Duration = Duration::from_secs(5 * 60);
/// Lakekeeper serves a cached credential for half its lifetime, so a renewal can return the
/// credential it replaces; this pause bounds the requests that would then follow.
const MIN_RENEWAL_INTERVAL_MS: u64 = 30_000;

const CREDENTIALS_ROUTE: &str =
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials";

/// What a credential source does with its credentials when a storage request needs them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshDecision {
    Keep,
    Renew,
    Expired,
}

/// What to do at the holder's renewal point, within five minutes of the expiry: renew when the
/// catalog offers the credentials endpoint, but not within 30 s of the previous renewal; without
/// the endpoint, keep the credentials until they expire.
pub(crate) fn refresh_decision(point: RenewalPoint, endpoint_offered: bool) -> RefreshDecision {
    if !endpoint_offered {
        return if point.now_ms >= point.expires_at_ms {
            RefreshDecision::Expired
        } else {
            RefreshDecision::Keep
        };
    }
    let renewed_recently = point
        .renewed_at_ms
        .is_some_and(|last| point.now_ms.saturating_sub(last) < MIN_RENEWAL_INTERVAL_MS);
    if renewed_recently {
        return RefreshDecision::Keep;
    }
    RefreshDecision::Renew
}

/// The table whose vended credentials a source renews, and how: through the writer's own catalog
/// session, for the credentials that cover `location`, with the export's plain-HTTP consent.
pub(crate) struct VendedTable {
    pub(crate) session: Arc<CatalogSession>,
    pub(crate) table: TableIdent,
    pub(crate) location: String,
    pub(crate) allow_http: bool,
}

/// The vended storage credentials of one table, as a `ProvideCredential` source for its FileIO.
/// It renews them through `loadCredentials` on the CONNECTION's catalog address, so the catalog
/// token never goes to an address the catalog chose. No error and no `Debug` output shows a
/// credential value.
#[derive(Clone)]
pub(crate) struct VendedCredentials {
    inner: Arc<Source>,
}

struct Source {
    holder: ExpiringCredential<AwsCredential>,
    vended: VendedTable,
    endpoint_offered: bool,
}

impl VendedCredentials {
    /// Seeded from the credentials of a create, stage-create, or load answer; `clock` reads epoch
    /// milliseconds.
    pub(crate) fn seeded(
        vended: VendedTable,
        seed: VendedS3Credentials,
        clock: EpochClock,
    ) -> Result<Self, UdfError> {
        let lease = vended_lease(seed, &vended).map_err(UdfError::User)?;
        let endpoint_offered = vended.session.offers_endpoint(CREDENTIALS_ROUTE);
        Ok(Self {
            inner: Arc::new(Source {
                holder: ExpiringCredential::seeded(lease, RENEWAL_SKEW, clock),
                vended,
                endpoint_offered,
            }),
        })
    }

    /// The source as the FileIO's credential hook.
    pub(crate) fn loader(&self) -> CustomAwsCredentialLoader {
        CustomAwsCredentialLoader::new(self.clone())
    }

    /// Every credential value the source has held, for value-based error redaction.
    pub(crate) fn secret_values(&self) -> Vec<String> {
        self.inner
            .holder
            .held_values()
            .into_iter()
            .flat_map(|credential| {
                [
                    Some(credential.access_key_id),
                    Some(credential.secret_access_key),
                    credential.session_token,
                ]
            })
            .flatten()
            .filter(|value| !value.is_empty())
            .collect()
    }

    async fn credential(&self) -> Result<AwsCredential, String> {
        let source = &self.inner;
        source
            .holder
            .get_or_keep(|point| self.decide(point), |_| self.renew())
            .await
    }

    fn decide(&self, point: RenewalPoint) -> Result<AtRenewalPoint, String> {
        let source = &self.inner;
        match refresh_decision(point, source.endpoint_offered) {
            RefreshDecision::Keep => Ok(AtRenewalPoint::Keep),
            RefreshDecision::Renew => Ok(AtRenewalPoint::Renew),
            RefreshDecision::Expired => Err(format!(
                "the vended storage credentials of table {} expired at {}, and the catalog {} \
                 offers no credentials endpoint to refresh them",
                source.vended.table,
                epoch_time(point.expires_at_ms),
                source.vended.session.catalog_uri()
            )),
        }
    }

    async fn renew(&self) -> Result<Lease<AwsCredential>, String> {
        let vended = &self.inner.vended;
        let credentials = vended
            .session
            .load_credentials(&vended.table, &vended.location)
            .await
            .map_err(|error| self.refresh_failure(&error.to_string()))?;
        vended_lease(credentials, vended).map_err(|cause| self.refresh_failure(&cause))
    }

    fn refresh_failure(&self, cause: &str) -> String {
        let vended = &self.inner.vended;
        let held = self.secret_values();
        let held: Vec<&str> = held.iter().map(String::as_str).collect();
        format!(
            "could not refresh the vended storage credentials of table {} through the catalog \
             {}: {}",
            vended.table,
            vended.session.catalog_uri(),
            redact_error_text(cause, &held)
        )
    }
}

impl ProvideCredential for VendedCredentials {
    type Credential = AwsCredential;

    async fn provide_credential(&self, _: &Context) -> reqsign_core::Result<Option<AwsCredential>> {
        self.credential()
            .await
            .map(Some)
            .map_err(reqsign_core::Error::unexpected)
    }
}

impl fmt::Debug for VendedCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let vended = &self.inner.vended;
        f.debug_struct("VendedCredentials")
            .field("table", &vended.table.to_string())
            .field("location", &vended.location)
            .finish_non_exhaustive()
    }
}

/// Refuses a vended storage endpoint that is plain HTTP without the export's consent.
fn refuse_plaintext_endpoint(endpoint: Option<&str>, vended: &VendedTable) -> Result<(), String> {
    match endpoint {
        Some(endpoint) if !vended.allow_http && scheme_of(endpoint) == "http" => Err(format!(
            "the catalog vended the plaintext storage endpoint {endpoint} for table {}, but the \
             export option ALLOW_HTTP is not 'true'",
            vended.table
        )),
        _ => Ok(()),
    }
}

/// The credentials and their expiry, once their endpoint passes the plaintext gate.
fn vended_lease(
    credentials: VendedS3Credentials,
    vended: &VendedTable,
) -> Result<Lease<AwsCredential>, String> {
    refuse_plaintext_endpoint(credentials.endpoint.as_deref(), vended)?;
    Ok(Lease {
        value: AwsCredential {
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: credentials.session_token,
            expires_in: credentials.expires_at_ms.and_then(timestamp),
        },
        expires_at_ms: credentials.expires_at_ms,
    })
}

fn timestamp(epoch_ms: u64) -> Option<Timestamp> {
    Timestamp::from_millisecond(i64::try_from(epoch_ms).ok()?).ok()
}

fn epoch_time(epoch_ms: u64) -> String {
    timestamp(epoch_ms).map_or_else(
        || format!("{epoch_ms} ms after the Unix epoch"),
        Timestamp::format_rfc3339_zulu,
    )
}

#[cfg(test)]
#[path = "vended_refresh_tests.rs"]
mod tests;
