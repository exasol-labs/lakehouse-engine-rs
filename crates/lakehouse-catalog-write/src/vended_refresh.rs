//! The storage credentials of an export table, as the one credential source of its FileIO, so
//! every storage request of a writer, its commit's manifests included, signs with a current
//! credential. Each backend has its own source: S3 keys for the S3 FileIO, an ADLS account key or
//! SAS for the ADLS storage. Each kind of credential is renewed its own way: CONNECTION credentials
//! never, an assumed role by assuming it again, vended credentials through the catalog's
//! `loadCredentials` endpoint.

use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use lakehouse_catalog::{
    AdlsCred, AssumedRole, AtRenewalPoint, AwsCredential, CatalogSession,
    CustomAwsCredentialLoader, EpochClock, ExpiringCredential, Lease, RenewalPoint, VendedAdlsSas,
    VendedCredentialSet, VendedS3Credentials, aws_lease, redact_error_text, scheme_of,
};
use object_store::CredentialProvider;
use object_store::azure::{AzureAccessKey, AzureCredential, split_sas};
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

/// [`keep_at_renewal_point`] as the decision of a vended holder's `get_or_keep`.
fn vended_decision(point: RenewalPoint) -> Result<AtRenewalPoint, String> {
    Ok(if keep_at_renewal_point(point) {
        AtRenewalPoint::Keep
    } else {
        AtRenewalPoint::Renew
    })
}

/// The credential source of a table's FileIO, of the backend its location selects.
pub(crate) enum TableCredentials {
    S3(StorageCredentials),
    Adls(AdlsCredentials),
}

impl TableCredentials {
    /// The credential values held now, for value-based error redaction.
    pub(crate) fn secret_values(&self) -> Vec<String> {
        match self {
            Self::S3(credentials) => credentials.secret_values(),
            Self::Adls(credentials) => credentials.secret_values(),
        }
    }
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
    /// The CONNECTION's own keys, which never expire.
    Keys(AwsCredential),
    /// An assumed role, shared with the catalog signing that reads the same holder.
    Role(AssumedRole),
    Vended {
        holder: Box<ExpiringCredential<AwsCredential>>,
        vended: VendedTable,
    },
}

impl StorageCredentials {
    /// The CONNECTION's keys, which do not expire.
    pub(crate) fn keys(access_key: &str, secret_key: &str, session_token: Option<&str>) -> Self {
        let lease = aws_lease(
            access_key.to_string(),
            secret_key.to_string(),
            session_token.map(str::to_string),
            None,
        );
        Self {
            inner: Arc::new(Source::Keys(lease.value)),
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
                holder: Box::new(ExpiringCredential::seeded(lease, RENEWAL_SKEW, clock)),
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
            Source::Keys(keys) => aws_secret_values(Some(keys.clone())),
            Source::Vended { holder, .. } => aws_secret_values(holder.current()),
        }
    }

    async fn credential(&self) -> Result<AwsCredential, String> {
        match &*self.inner {
            Source::Role(role) => role.credential().await.map_err(|error| error.to_string()),
            Source::Keys(keys) => Ok(keys.clone()),
            Source::Vended { holder, vended } => {
                holder
                    .get_or_keep(vended_decision, |_| self.renew_vended(vended))
                    .await
            }
        }
    }

    async fn renew_vended(&self, vended: &VendedTable) -> Result<Lease<AwsCredential>, String> {
        let renewal = renewed(vended, VendedCredentialSet::s3)
            .await
            .and_then(|credentials| vended_lease(credentials, vended));
        renewal.map_err(|cause| refresh_failure(vended, &self.secret_values(), &cause))
    }
}

/// The non-empty values of an AWS credential, none before the first one.
fn aws_secret_values(credential: Option<AwsCredential>) -> Vec<String> {
    credential
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

/// Credentials renewed through `loadCredentials` and read as one backend's by `read`. The cause of
/// a failure is in the words of the S3 and ADLS sources alike.
async fn renewed<T>(
    vended: &VendedTable,
    read: impl FnOnce(&VendedCredentialSet, &str) -> Result<T, UdfError>,
) -> Result<T, String> {
    let set = vended
        .session
        .load_credentials(&vended.table, &vended.location)
        .await
        .map_err(|error| error.to_string())?;
    read(&set, &vended.location).map_err(|error| {
        format!(
            "loadCredentials of table '{}' failed: {error}",
            vended.table
        )
    })
}

/// Only the credential values held now are redacted.
fn refresh_failure(vended: &VendedTable, held: &[String], cause: &str) -> String {
    let held: Vec<&str> = held.iter().map(String::as_str).collect();
    format!(
        "could not refresh the vended storage credentials of table {} through the catalog {}: {}",
        vended.table,
        vended.session.catalog_uri(),
        redact_error_text(cause, &held)
    )
}

/// An ADLS credential as object_store signs with it, beside the value it was made from, which
/// error redaction needs.
#[derive(Clone)]
struct AdlsSecret {
    signing: Arc<AzureCredential>,
    value: String,
}

/// The ADLS credentials of one table, as the credential provider of its ADLS storage, which asks
/// it on every request. A vended SAS renews through `loadCredentials` on the CONNECTION's catalog
/// address. No error and no `Debug` output shows a credential value.
#[derive(Clone)]
pub(crate) struct AdlsCredentials {
    inner: Arc<AdlsSource>,
}

enum AdlsSource {
    /// The CONNECTION's account key or SAS, which never expires.
    Connection(AdlsSecret),
    Vended {
        holder: Box<ExpiringCredential<AdlsSecret>>,
        vended: VendedTable,
    },
}

impl AdlsCredentials {
    /// The CONNECTION's account key or SAS, which do not expire.
    pub(crate) fn connection(cred: &AdlsCred) -> Result<Self, UdfError> {
        let secret = match cred {
            AdlsCred::AccountKey(key) => account_key_secret(key),
            AdlsCred::Sas(sas) => sas_secret(sas),
        }
        .map_err(|cause| UdfError::User(format!("the CONNECTION's {cause}")))?;
        Ok(Self {
            inner: Arc::new(AdlsSource::Connection(secret)),
        })
    }

    /// Seeded from the SAS of a create, stage-create, or load answer; `clock` reads epoch
    /// milliseconds.
    pub(crate) fn vended(
        vended: VendedTable,
        seed: VendedAdlsSas,
        clock: EpochClock,
    ) -> Result<Self, UdfError> {
        let lease = sas_lease(seed).map_err(|cause| {
            UdfError::User(format!("the vended {cause} (table {})", vended.table))
        })?;
        Ok(Self {
            inner: Arc::new(AdlsSource::Vended {
                holder: Box::new(ExpiringCredential::seeded(lease, RENEWAL_SKEW, clock)),
                vended,
            }),
        })
    }

    /// The credential value held now, for value-based error redaction.
    pub(crate) fn secret_values(&self) -> Vec<String> {
        let held = match &*self.inner {
            AdlsSource::Connection(secret) => Some(secret.clone()),
            AdlsSource::Vended { holder, .. } => holder.current(),
        };
        held.map(|secret| secret.value)
            .filter(|value| !value.is_empty())
            .into_iter()
            .collect()
    }

    async fn credential(&self) -> Result<AdlsSecret, String> {
        match &*self.inner {
            AdlsSource::Connection(secret) => Ok(secret.clone()),
            AdlsSource::Vended { holder, vended } => {
                holder
                    .get_or_keep(vended_decision, |_| self.renew_vended(vended))
                    .await
            }
        }
    }

    async fn renew_vended(&self, vended: &VendedTable) -> Result<Lease<AdlsSecret>, String> {
        // Boxed: object_store's `Send` check on the unboxed request future overflows the
        // compiler's query depth.
        let renewal = Box::pin(renewed(vended, |set, location| {
            set.adls_sas(location).and_then(|seed| {
                sas_lease(seed).map_err(|cause| UdfError::User(format!("the vended {cause}")))
            })
        }))
        .await;
        renewal.map_err(|cause| refresh_failure(vended, &self.secret_values(), &cause))
    }
}

#[async_trait::async_trait]
impl CredentialProvider for AdlsCredentials {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
        self.credential()
            .await
            .map(|secret| secret.signing)
            .map_err(|cause| object_store::Error::Generic {
                store: "MicrosoftAzure",
                source: cause.into(),
            })
    }
}

impl fmt::Debug for AdlsCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self.inner {
            AdlsSource::Vended { vended, .. } => f
                .debug_struct("AdlsCredentials")
                .field("table", &vended.table.to_string())
                .field("location", &vended.location)
                .finish_non_exhaustive(),
            AdlsSource::Connection(_) => f.debug_struct("AdlsCredentials").finish_non_exhaustive(),
        }
    }
}

fn sas_lease(seed: VendedAdlsSas) -> Result<Lease<AdlsSecret>, String> {
    Ok(Lease {
        value: sas_secret(&seed.sas)?,
        expires_at_ms: seed.expires_at_ms,
    })
}

fn account_key_secret(key: &str) -> Result<AdlsSecret, String> {
    let signing = AzureAccessKey::try_new(key)
        .map_err(|_| "account_key is no base64-encoded storage account key".to_string())?;
    Ok(AdlsSecret {
        signing: Arc::new(AzureCredential::AccessKey(signing)),
        value: key.to_string(),
    })
}

fn sas_secret(sas: &str) -> Result<AdlsSecret, String> {
    let pairs = split_sas(sas)
        .map_err(|_| "SAS token is no query string of key=value pairs".to_string())?;
    Ok(AdlsSecret {
        signing: Arc::new(AzureCredential::SASToken(pairs)),
        value: sas.to_string(),
    })
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
