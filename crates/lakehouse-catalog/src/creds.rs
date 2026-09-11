use crate::{AdlsCred, StorageBackend};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub struct ConnectionCreds {
    pub warehouse: String,
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub path_style: Option<bool>,
    pub use_sigv4: bool,
    pub use_vended_credentials: bool,
    pub token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub oauth2_server_uri: Option<String>,
    pub scope: Option<String>,
    pub account_name: Option<String>,
    pub account_key: Option<String>,
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

pub(crate) enum SuppliedCatalogAuth<'a> {
    Unauthenticated,
    StaticToken(&'a str),
    ClientCredentials {
        client_id: &'a str,
        client_secret: &'a str,
    },
}

impl ConnectionCreds {
    pub fn has_catalog_auth(&self) -> bool {
        self.token.is_some() || self.client_id.is_some() || self.client_secret.is_some()
    }

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
            (None, None, None)
            | (None, Some(_), None)
            | (None, None, Some(_))
            | (Some(_), Some(_), None)
            | (Some(_), None, Some(_))
            | (Some(_), Some(_), Some(_)) => SuppliedCatalogAuth::Unauthenticated,
        }
    }
}

pub(crate) fn non_empty(field: &Option<String>) -> Option<&str> {
    field.as_deref().filter(|value| !value.is_empty())
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProps {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
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

#[derive(Default)]
pub struct StorageCreds {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub path_style: Option<bool>,
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
            path_style: json.get("path_style").and_then(|value| value.as_bool()),
            account_name: non_empty_json_str(json, "account_name").map(str::to_string),
            account_key: non_empty_json_str(json, "account_key").map(str::to_string),
            sas_token: non_empty_json_str(json, "sas_token").map(str::to_string),
        }
    }

    /// Resolves an unstated `path_style` to `false`.
    ///
    /// This is the one selector both the adapter-side (`ConnectionCreds`-derived)
    /// and scan-side (`from_json`-derived) readers call, so resolving here — not in
    /// `from_json` — guarantees they can never disagree. A parse-time default would
    /// also destroy the tri-state the vended-credentials path needs to distinguish
    /// "the operator said nothing" from "the operator said false", since the vended
    /// resolution must not silently override a response's `s3.path-style-access`
    /// on a CONNECTION that stated no preference at all.
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
            path_style: self.path_style.unwrap_or(false),
        })
    }
}

impl From<&ConnectionCreds> for StorageCreds {
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

fn non_empty_json_str<'a>(json: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    json.get(key)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogProps {
    pub warehouse: String,
    /// Fully-qualified table identifier: "<namespace>.<table>".
    pub table: String,
}

#[cfg(test)]
#[path = "creds_tests.rs"]
mod tests;
