//! The credential material that catalog and storage access need, declared once.
//!
//! These four types are the shapes that cross the `lakehouse-engine` ↔
//! `lakehouse-catalog` edge in both directions: the engine parses an Exasol
//! CONNECTION into [`ConnectionCreds`] and projects it into [`StorageProps`] and
//! [`CatalogProps`], while catalog access reads those three and vends a fresh
//! [`StorageProps`] back out of a `loadTable` response. [`StorageCreds`] is the
//! fourth — the storage-only projection of a `ConnectionCreds`, and the one
//! reader of the nine storage key spellings, so the adapter's plan-time
//! derivation and the scan UDF's own read of the same CONNECTION cannot drift.
//! They are declared here
//! rather than in the engine because the dependency edge points engine →
//! catalog, so a type both crates name must live on the catalog side; the engine
//! re-exports each at its pre-move path (`scan::spec`, `adapter::connection`) and
//! no consumer names this module.
//!
//! [`StorageProps`] is a wire type. It is a `CommonScanSpec` field serialized
//! into the scan-driving SQL literal, so its field names, field order, and serde
//! attributes are a compatibility contract rather than an implementation detail.
use crate::{AdlsCred, StorageBackend};
use serde::{Deserialize, Serialize};

/// Parsed credential fields from a CONNECTION password JSON object.
///
/// Carries all optional flags so later work (SigV4 signing, credential vending,
/// catalog token/OAuth2 auth) can read them without touching the module again.
///
/// Secret-bearing fields (`access_key`, `secret_key`, `session_token`, `token`,
/// `client_secret`, `account_key`, `sas_token`) are excluded from the derived
/// `Debug` output via a manual impl to prevent accidental leaks.
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
            .field("access_key", &"[redacted]")
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
///
/// `access_key`, `secret_key`, and `session_token` are excluded from the
/// derived `Debug` output via a manual impl below to prevent accidental leaks.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl std::fmt::Debug for StorageProps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageProps")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key", &"[redacted]")
            .field("secret_key", &"[redacted]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[redacted]"),
            )
            .field("allow_http", &self.allow_http)
            .field("path_style", &self.path_style)
            .finish()
    }
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

/// The storage half of a CONNECTION password: exactly the nine fields that
/// decide WHICH object store a scan reads through, and with WHAT credential.
///
/// It exists to make an exclusion structural rather than a discipline six
/// modules must honour. The scan UDF derives its store from a CONNECTION it
/// resolves itself, so whichever type carries that derivation decides what the
/// UDF is able to materialize at all. A projection declaring no `token`,
/// `client_id`, or `client_secret` field CANNOT carry a catalog secret onto a
/// shard invocation — where [`ConnectionCreds`], which declares all seventeen
/// password fields, would materialize every catalog-auth value on up to 300 of
/// them, outside the storage-only redaction set.
///
/// `allow_http` is deliberately NOT a tenth field: it originates from the
/// adapter's own `PROP_ALLOW_HTTP` property rather than from the password, so
/// it arrives as a [`Self::backend`] parameter and cannot become a second
/// source of truth free to disagree with that property.
///
/// All nine fields are `pub` because both readers construct and read them
/// across the `lakehouse-engine` ↔ `lakehouse-catalog` edge. There is
/// deliberately no DERIVED `Debug`: a derived one would print `access_key`,
/// `secret_key`, `session_token`, `account_key`, and `sas_token` verbatim into
/// any error a caller formats. A manual impl below redacts them instead.
pub struct StorageCreds {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub path_style: bool,
    pub account_name: Option<String>,
    pub account_key: Option<String>,
    pub sas_token: Option<String>,
}

impl std::fmt::Debug for StorageCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageCreds")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key", &"[redacted]")
            .field("secret_key", &"[redacted]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[redacted]"),
            )
            .field("path_style", &self.path_style)
            .field("account_name", &self.account_name)
            .field(
                "account_key",
                &self.account_key.as_ref().map(|_| "[redacted]"),
            )
            .field("sas_token", &self.sas_token.as_ref().map(|_| "[redacted]"))
            .finish()
    }
}

impl StorageCreds {
    /// Read the nine storage fields out of a CONNECTION password JSON object.
    ///
    /// The ONE reader of these nine key spellings, so a spelling cannot drift
    /// between the adapter's plan-time parse and the scan UDF's own read of the
    /// same CONNECTION. A field counts only when it is a non-empty JSON STRING
    /// — an empty one is an absent one, the normalization every CONNECTION
    /// field gets — and `path_style` counts only as a JSON bool, defaulting to
    /// `true` to preserve MinIO behaviour.
    ///
    /// Hand reads rather than a `Deserialize` derive: serde would reject a
    /// wrong-typed field instead of defaulting it, and would keep an empty
    /// string as a present credential.
    pub fn from_json(json: &serde_json::Value) -> Self {
        Self {
            endpoint: non_empty_json_str(json, "endpoint")
                .unwrap_or("")
                .to_string(),
            region: non_empty_json_str(json, "region").unwrap_or("").to_string(),
            access_key: non_empty_json_str(json, "access_key")
                .unwrap_or("")
                .to_string(),
            secret_key: non_empty_json_str(json, "secret_key")
                .unwrap_or("")
                .to_string(),
            session_token: non_empty_json_str(json, "session_token").map(str::to_string),
            path_style: json
                .get("path_style")
                .and_then(|value| value.as_bool())
                .unwrap_or(true),
            account_name: non_empty_json_str(json, "account_name").map(str::to_string),
            account_key: non_empty_json_str(json, "account_key").map(str::to_string),
            sas_token: non_empty_json_str(json, "sas_token").map(str::to_string),
        }
    }

    /// Build the [`StorageBackend`] these credentials describe.
    ///
    /// The ONE site that selects a storage backend from input, and TOTAL by
    /// construction. The Azure branch needs an account name AND a resolvable
    /// [`AdlsCred`] — exactly one of `account_key` and `sas_token` — and falls
    /// through to S3 when either is absent. `validate_creds` runs before any
    /// production caller reaches here, so that fall-through is unreachable in
    /// production; it is a deterministic answer rather than a panic because a
    /// panic inside a UDF is an abnormal VM exit, and the engine responds by
    /// SIGKILLing every sibling VM of the statement part — turning a defensive
    /// assertion into a cluster-wide failure. Returning `Result` instead would
    /// push a new error path through every caller for a state that cannot occur.
    ///
    /// `allow_http` is an S3-only knob (see this type's own note): the Azure
    /// backend carries no HTTP-scheme field, so an Azure CONNECTION ignores it.
    pub fn backend(&self, allow_http: bool) -> StorageBackend {
        let azure_cred = match (self.account_key.as_deref(), self.sas_token.as_deref()) {
            (Some(account_key), None) => Some(AdlsCred::AccountKey(account_key.to_string())),
            (None, Some(sas_token)) => Some(AdlsCred::Sas(sas_token.to_string())),
            (Some(_), Some(_)) | (None, None) => None,
        };
        if let (Some(account_name), Some(cred)) = (self.account_name.as_deref(), azure_cred) {
            return StorageBackend::Adls {
                account_name: account_name.to_string(),
                cred,
            };
        }

        StorageBackend::S3(StorageProps {
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            session_token: self.session_token.clone(),
            allow_http,
            path_style: self.path_style,
        })
    }
}

impl From<&ConnectionCreds> for StorageCreds {
    /// Project the storage half of an already-parsed CONNECTION.
    ///
    /// A field-for-field copy, deliberately re-normalizing nothing:
    /// [`StorageCreds::from_json`] already normalized whatever
    /// [`ConnectionCreds`] was parsed from, and normalizing a second time here
    /// would let this projection's [`StorageCreds::backend`] disagree with the
    /// backend the same `ConnectionCreds` describes — an empty `account_key`
    /// still names an Azure credential to the selection rule.
    fn from(creds: &ConnectionCreds) -> Self {
        Self {
            endpoint: creds.endpoint.clone(),
            region: creds.region.clone(),
            access_key: creds.access_key.clone(),
            secret_key: creds.secret_key.clone(),
            session_token: creds.session_token.clone(),
            path_style: creds.path_style,
            account_name: creds.account_name.clone(),
            account_key: creds.account_key.clone(),
            sas_token: creds.sas_token.clone(),
        }
    }
}

/// Borrow a JSON object's field only when it is a non-empty JSON string.
///
/// The same reading the adapter's own `nonempty_str` applies to the eight
/// non-storage CONNECTION fields. Duplicated rather than shared because the
/// dependency edge points engine → catalog, so the adapter's helper cannot be
/// named from here.
fn non_empty_json_str<'a>(json: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    json.get(key)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
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
