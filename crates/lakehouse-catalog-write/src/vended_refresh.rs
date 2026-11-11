//! The storage credentials of an export table, as the one `ProvideCredential` source of its FileIO,
//! so every storage request of a writer, its commit's manifests included, signs with a current
//! credential. Each kind of credential is renewed its own way: CONNECTION keys never, an assumed
//! role by assuming it again, vended credentials through the catalog's `loadCredentials` endpoint.

use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use lakehouse_catalog::{
    AssumedRole, AtRenewalPoint, AwsCredential, CatalogSession, CustomAwsCredentialLoader,
    EpochClock, ExpiringCredential, Lease, RenewalPoint, VendedS3Credentials, aws_lease,
    redact_error_text, scheme_of,
};
use reqsign_core::{Context, ProvideCredential};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Iceberg Java's `VendedCredentialsProvider` prefetches this long before expiry.
const RENEWAL_SKEW: Duration = Duration::from_secs(5 * 60);
/// Lakekeeper serves a cached credential for half its lifetime, so a renewal can return the
/// credential it replaces; this pause bounds the requests that would then follow.
const MIN_RENEWAL_INTERVAL_MS: u64 = 30_000;

/// What to do at the holder's renewal point, within five minutes of the expiry: still-valid
/// credentials wait 30 s after the previous renewal, expired ones always renew.
pub(crate) fn keep_at_renewal_point(point: RenewalPoint) -> bool {
    let renewed_recently = point
        .renewed_at_ms
        .is_some_and(|last| point.now_ms.saturating_sub(last) < MIN_RENEWAL_INTERVAL_MS);
    renewed_recently && point.now_ms < point.expires_at_ms
}

/// The table whose vended credentials a source renews, and how: through the writer's own catalog
/// session, for the credentials that cover `location`, with the export's plain-HTTP consent.
pub(crate) struct VendedTable {
    pub(crate) session: Arc<CatalogSession>,
    pub(crate) table: TableIdent,
    pub(crate) location: String,
    pub(crate) allow_http: bool,
}

/// The storage credentials of one table, as a `ProvideCredential` source for its FileIO. Vended
/// credentials renew through `loadCredentials` on the CONNECTION's catalog address, so the catalog
/// token never goes to an address the catalog chose. No error and no `Debug` output shows a
/// credential value, and only the value held now is redacted.
#[derive(Clone)]
pub(crate) struct StorageCredentials {
    inner: Arc<Source>,
}

enum Source {
    /// The CONNECTION's own keys: a lease with no expiry, never renewed.
    Keys(ExpiringCredential<AwsCredential>),
    /// An assumed role, shared with the catalog signing that reads the same holder.
    Role(AssumedRole),
    Vended {
        holder: ExpiringCredential<AwsCredential>,
        vended: VendedTable,
    },
}

impl StorageCredentials {
    /// The CONNECTION's keys, which do not expire.
    pub(crate) fn keys(
        access_key: &str,
        secret_key: &str,
        session_token: Option<&str>,
        clock: EpochClock,
    ) -> Self {
        let lease = aws_lease(
            access_key.to_string(),
            secret_key.to_string(),
            session_token.map(str::to_string),
            None,
        );
        Self {
            inner: Arc::new(Source::Keys(ExpiringCredential::seeded(
                lease,
                RENEWAL_SKEW,
                clock,
            ))),
        }
    }

    /// The credentials of an assumed role, renewed by assuming it again.
    pub(crate) fn role(role: AssumedRole) -> Self {
        Self {
            inner: Arc::new(Source::Role(role)),
        }
    }

    /// Seeded from the credentials of a create, stage-create, or load answer; `clock` reads epoch
    /// milliseconds.
    pub(crate) fn vended(
        vended: VendedTable,
        seed: VendedS3Credentials,
        clock: EpochClock,
    ) -> Result<Self, UdfError> {
        let lease = vended_lease(seed, &vended).map_err(UdfError::User)?;
        Ok(Self {
            inner: Arc::new(Source::Vended {
                holder: ExpiringCredential::seeded(lease, RENEWAL_SKEW, clock),
                vended,
            }),
        })
    }

    /// The source as the FileIO's credential hook.
    pub(crate) fn loader(&self) -> CustomAwsCredentialLoader {
        CustomAwsCredentialLoader::new(self.clone())
    }

    /// The credential values held now, for value-based error redaction.
    pub(crate) fn secret_values(&self) -> Vec<String> {
        match &*self.inner {
            Source::Role(role) => role.secret_values(),
            Source::Keys(holder) | Source::Vended { holder, .. } => holder
                .current()
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
                .collect(),
        }
    }

    async fn credential(&self) -> Result<AwsCredential, String> {
        match &*self.inner {
            Source::Role(role) => role.credential().await.map_err(|error| error.to_string()),
            Source::Keys(holder) => {
                holder
                    .get(|_| async {
                        Err("the CONNECTION's storage keys cannot be renewed".into())
                    })
                    .await
            }
            Source::Vended { holder, vended } => {
                holder
                    .get_or_keep(
                        |point| {
                            Ok(if keep_at_renewal_point(point) {
                                AtRenewalPoint::Keep
                            } else {
                                AtRenewalPoint::Renew
                            })
                        },
                        |_| self.renew_vended(vended),
                    )
                    .await
            }
        }
    }

    async fn renew_vended(&self, vended: &VendedTable) -> Result<Lease<AwsCredential>, String> {
        let credentials = vended
            .session
            .load_credentials(&vended.table, &vended.location)
            .await
            .map_err(|error| self.refresh_failure(vended, &error.to_string()))?;
        vended_lease(credentials, vended).map_err(|cause| self.refresh_failure(vended, &cause))
    }

    fn refresh_failure(&self, vended: &VendedTable, cause: &str) -> String {
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

impl ProvideCredential for StorageCredentials {
    type Credential = AwsCredential;

    async fn provide_credential(&self, _: &Context) -> reqsign_core::Result<Option<AwsCredential>> {
        self.credential()
            .await
            .map(Some)
            .map_err(reqsign_core::Error::unexpected)
    }
}

impl fmt::Debug for StorageCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self.inner {
            Source::Vended { vended, .. } => f
                .debug_struct("StorageCredentials")
                .field("table", &vended.table.to_string())
                .field("location", &vended.location)
                .finish_non_exhaustive(),
            Source::Keys(_) | Source::Role(_) => {
                f.debug_struct("StorageCredentials").finish_non_exhaustive()
            }
        }
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
    Ok(aws_lease(
        credentials.access_key_id,
        credentials.secret_access_key,
        credentials.session_token,
        credentials.expires_at_ms,
    ))
}

#[cfg(test)]
#[path = "vended_refresh_tests.rs"]
mod tests;
