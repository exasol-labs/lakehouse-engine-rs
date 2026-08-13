//! The credential material that catalog and storage access need, declared once.
//!
//! These three types are the shapes that cross the `lakehouse-engine` ↔
//! `lakehouse-catalog` edge in both directions: the engine parses an Exasol
//! CONNECTION into [`ConnectionCreds`] and projects it into [`StorageProps`] and
//! [`CatalogProps`], while catalog access reads all three and vends a fresh
//! [`StorageProps`] back out of a `loadTable` response. They are declared here
//! rather than in the engine because the dependency edge points engine →
//! catalog, so a type both crates name must live on the catalog side; the engine
//! re-exports each at its pre-move path (`scan::spec`, `adapter::connection`) and
//! no consumer names this module.
//!
//! [`StorageProps`] is a wire type. It is a `CommonScanSpec` field serialized
//! into the scan-driving SQL literal, so its field names, field order, and serde
//! attributes are a compatibility contract rather than an implementation detail.
use serde::{Deserialize, Serialize};

/// Parsed credential fields from a CONNECTION password JSON object.
///
/// Carries all optional flags so later work (SigV4 signing, credential vending,
/// catalog token/OAuth2 auth) can read them without touching the module again.
///
/// Secret-bearing fields (`secret_key`, `client_secret`, `token`, `account_key`,
/// `sas_token`) are excluded from the derived `Debug` output via a manual impl to
/// prevent accidental leaks.
#[derive(Clone)]
pub struct ConnectionCreds {
    pub warehouse: String,
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    /// Optional STS session token. Absent when not supplied.
    pub session_token: Option<String>,
    /// Use path-style S3 access. Defaults to `true` to preserve MinIO behaviour.
    pub path_style: bool,
    /// Sign catalog REST requests with AWS SigV4. Defaults to `false` so
    /// existing MinIO/REST stacks behave exactly as before.
    pub use_sigv4: bool,
    /// Request short-lived vended S3 credentials via `load_table`. Defaults to
    /// `false` so existing stacks behave exactly as before.
    pub use_vended_credentials: bool,
    /// Static bearer token for catalog authentication. Absent when not supplied.
    pub token: Option<String>,
    /// OAuth2 client ID for catalog client-credentials flow. Absent when not supplied.
    pub client_id: Option<String>,
    /// OAuth2 client secret for catalog client-credentials flow. Absent when not supplied.
    pub client_secret: Option<String>,
    /// Optional OAuth2 token endpoint URI. Absent when not supplied.
    pub oauth2_server_uri: Option<String>,
    /// Optional OAuth2 scope. Absent when not supplied.
    pub scope: Option<String>,
    /// Azure storage account name. Absent when not supplied.
    pub account_name: Option<String>,
    /// Azure shared storage-account key. Absent when not supplied.
    pub account_key: Option<String>,
    /// Azure inline shared-access-signature token. Absent when not supplied.
    pub sas_token: Option<String>,
}

impl std::fmt::Debug for ConnectionCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionCreds")
            .field("warehouse", &self.warehouse)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key", &self.access_key)
            .field("secret_key", &"[redacted]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[redacted]"),
            )
            .field("path_style", &self.path_style)
            .field("use_sigv4", &self.use_sigv4)
            .field("use_vended_credentials", &self.use_vended_credentials)
            .field("token", &self.token.as_ref().map(|_| "[redacted]"))
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "[redacted]"),
            )
            .field("oauth2_server_uri", &self.oauth2_server_uri)
            .field("scope", &self.scope)
            .field("account_name", &self.account_name)
            .field(
                "account_key",
                &self.account_key.as_ref().map(|_| "[redacted]"),
            )
            .field("sas_token", &self.sas_token.as_ref().map(|_| "[redacted]"))
            .finish()
    }
}

/// Which of the three mutually exclusive catalog-auth modes a CONNECTION
/// supplied.
///
/// The three variants are exactly the three field shapes `validate_creds`
/// accepts; every other combination of `token`, `client_id`, and
/// `client_secret` is a CONNECTION it rejects.
pub(crate) enum SuppliedCatalogAuth<'a> {
    /// No catalog credentials — the catalog is queried unauthenticated.
    Unauthenticated,
    /// A static bearer `token`, applied verbatim.
    StaticToken(&'a str),
    /// A complete OAuth2 client-credentials pair, exchanged for a bearer.
    ClientCredentials {
        client_id: &'a str,
        client_secret: &'a str,
    },
}

impl ConnectionCreds {
    /// Returns `true` when catalog authentication credentials are present:
    /// either a static bearer `token` OR any OAuth2 client-credential field
    /// (`client_id` and/or `client_secret`). Partial OAuth still signals
    /// catalog-auth intent, so the SigV4 guard rejects it.
    ///
    /// Used exclusively for the SigV4 mutual-exclusivity check.
    pub fn has_catalog_auth(&self) -> bool {
        self.token.is_some() || self.client_id.is_some() || self.client_secret.is_some()
    }

    /// Names the one catalog-auth mode this CONNECTION's fields describe.
    ///
    /// The sole owner of that decision: every consumer matches on the answer
    /// rather than re-deriving it from the fields, so no two catalog kinds can
    /// read one field shape two different ways. An empty field is an absent
    /// field.
    ///
    /// Exactly one mode is ever describable because `validate_creds` rejects
    /// every other shape before a session exists — rule 6 a `token` beside a
    /// complete client-credentials pair, rule 7 a partial pair.
    ///
    /// Rule 6 tests field presence (`is_some()`) while this method tests
    /// non-emptiness (`non_empty`); the two coincide only because the engine's
    /// `parse_creds` normalizes every empty credential field to `None` before
    /// validation ever runs, so a present-but-empty `Some("")` never reaches
    /// either side.
    pub(crate) fn supplied_catalog_auth(&self) -> SuppliedCatalogAuth<'_> {
        match (
            non_empty(&self.token),
            non_empty(&self.client_id),
            non_empty(&self.client_secret),
        ) {
            (None, Some(client_id), Some(client_secret)) => {
                SuppliedCatalogAuth::ClientCredentials {
                    client_id,
                    client_secret,
                }
            }
            (Some(token), None, None) => SuppliedCatalogAuth::StaticToken(token),
            // Every remaining shape is one `validate_creds` rejects before a
            // session exists: rule 6 for a token beside a complete pair, rule 7
            // for a partial pair. Reaching one here means validation was
            // bypassed, so the honest answer is "this describes no auth mode" —
            // the request then fails on the catalog's own 401 rather than on a
            // credential the operator never unambiguously supplied.
            (None, None, None)
            | (None, Some(_), None)
            | (None, None, Some(_))
            | (Some(_), Some(_), None)
            | (Some(_), None, Some(_))
            | (Some(_), Some(_), Some(_)) => SuppliedCatalogAuth::Unauthenticated,
        }
    }
}

/// Borrow the inner value of an `Option<String>` only when it is non-empty.
///
/// An empty credential field is an absent one for the purpose of deciding
/// which catalog-auth mode was supplied: `supplied_catalog_auth` calls this
/// rather than testing `is_some()`. `has_catalog_auth` deliberately does not
/// — it asks whether catalog auth was INTENDED at all, not which mode, so a
/// partial (and therefore still-rejected) OAuth2 shape must still count.
pub(crate) fn non_empty(field: &Option<String>) -> Option<&str> {
    field.as_deref().filter(|value| !value.is_empty())
}

/// Storage connection properties (S3-compatible / MinIO).
/// Fields are plain Strings so serde handles them uniformly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProps {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    /// Enable HTTP (MinIO local dev typically uses HTTP, not HTTPS).
    #[serde(default)]
    pub allow_http: bool,
    /// Use path-style access (required for MinIO).
    #[serde(default = "default_true")]
    pub path_style: bool,
}

fn default_true() -> bool {
    true
}

impl Default for StorageProps {
    /// Mirrors serde's field-absent defaults: empty connection fields, no session
    /// token, HTTPS (`allow_http` false), and path-style access ON (`default_true`).
    /// So `StorageProps::default()` equals deserializing a `StorageProps` whose
    /// optional fields are all absent — the single source of truth is the same
    /// `default_true` seam serde uses. A placeholder for tests, which override the
    /// connection fields that matter to a given scenario.
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            session_token: None,
            allow_http: false,
            path_style: default_true(),
        }
    }
}

impl StorageProps {
    /// The non-empty secret values (access key, secret key, session token).
    ///
    /// Used for value-based error redaction: any error string containing one of
    /// these literal values has it stripped before the error is surfaced.
    pub fn secret_values(&self) -> Vec<&str> {
        let mut secrets = Vec::new();
        for candidate in [self.access_key.as_str(), self.secret_key.as_str()] {
            if !candidate.is_empty() {
                secrets.push(candidate);
            }
        }
        if let Some(token) = self.session_token.as_deref()
            && !token.is_empty()
        {
            secrets.push(token);
        }
        secrets
    }
}

/// Iceberg REST catalog connection properties: which warehouse holds which table.
///
/// The catalog URI is deliberately NOT carried here. Every consumer receives it as
/// an explicit parameter — `build_rest_catalog(catalog_uri, ...)`,
/// `CatalogSession::resolve(catalog_uri, ...)`, `list_namespace_tables(catalog_uri, ...)`
/// — so a copy on this struct would be a second source of truth for the same value,
/// free to disagree with the one the request is actually issued against.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogProps {
    pub warehouse: String,
    /// Fully-qualified table identifier: "<namespace>.<table>".
    pub table: String,
}

#[cfg(test)]
#[path = "creds_tests.rs"]
mod tests;
