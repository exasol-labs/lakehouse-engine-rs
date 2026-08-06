/// Resolve an Exasol CONNECTION object into catalog and storage configuration.
///
/// The CONNECTION's `address` is the Iceberg REST catalog URI; the `password`
/// is a JSON object carrying credential and behavioural fields. Credential
/// values NEVER appear in any error message produced by this module.
use crate::scan::spec::{AdlsCred, CatalogProps, StorageBackend, StorageProps};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;

use super::nonempty_str;

/// The only unconditionally-required field in the CONNECTION password JSON.
///
/// The four S3 fields (`endpoint`, `region`, `access_key`, `secret_key`) are
/// optional at the base level; they are orthogonal to catalog authentication and
/// credential vending. `region`/`access_key`/`secret_key` become required only
/// when `use_sigv4` is enabled (see `read_connection`).
pub const REQUIRED_KEY: &str = "warehouse";

/// Parsed credential fields from a CONNECTION password JSON object, declared once
/// in the `lakehouse-catalog` crate and re-exported here at its pre-move path.
///
/// The type lives in the catalog crate because that crate is what consumes it —
/// catalog authentication, prefix resolution, and credential vending all read
/// these fields — and the dependency edge points engine → catalog, so a type both
/// crates name must be declared on the catalog side. What stays in this module is
/// everything that interprets the Exasol CONNECTION delivery mechanism:
/// [`read_connection`], `parse_creds`, `validate_creds`, [`storage_block`],
/// [`catalog_block`], and [`REQUIRED_KEY`]. The catalog crate must not name that
/// mechanism.
pub use lakehouse_catalog::ConnectionCreds;

/// Resolved CONNECTION: catalog URI plus parsed credentials.
#[derive(Debug)]
pub struct Resolved {
    pub uri: String,
    pub creds: ConnectionCreds,
}

/// Resolve a named Exasol CONNECTION into a catalog URI and credentials.
///
/// Credential-safe: the password value is never embedded in any returned error.
pub fn read_connection(ctx: &dyn UdfContext, name: Option<&str>) -> Result<Resolved, UdfError> {
    let name = match name {
        Some(n) if !n.is_empty() => n,
        _ => {
            return Err(UdfError::User("CATALOG_CONNECTION is required".into()));
        }
    };

    let conn = ctx
        .connection(name)
        .map_err(|_| UdfError::User(format!("CONNECTION '{name}' could not be resolved")))?;

    let uri = conn.address;
    if uri.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' has no address; expected the catalog URI"
        )));
    }

    // Never embed the password in the error message.
    let json: serde_json::Value = serde_json::from_str(&conn.password).map_err(|_| {
        UdfError::User(format!(
            "CONNECTION '{name}' password is not a valid JSON object"
        ))
    })?;

    if !json.is_object() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' password is not a valid JSON object"
        )));
    }

    let creds = parse_creds(&json);
    validate_creds(name, &creds)?;
    Ok(Resolved { uri, creds })
}

/// Validate parsed credentials against the mode-aware credential contract.
///
/// Credential-safe: only field names — never values — appear in any error.
///
/// Rules, in precedence order:
/// 1. `warehouse` is the only unconditionally-required field.
/// 2. Azure and static S3 storage credentials cannot both be supplied. An
///    undeclared precedence between two credential sets would resolve an
///    ambiguous credentials input silently, which is the misconfiguration the
///    rest of these rules exist to prevent.
/// 3. A CONNECTION supplying ANY Azure field is an Azure CONNECTION, and an
///    Azure CONNECTION requires `account_name` plus EXACTLY ONE of `account_key`
///    and `sas_token`. Keying on any-of-three rather than on `account_name`
///    alone is what turns a CONNECTION that supplies a credential and forgets
///    the account name into a named-field error instead of a silent fall back to
///    S3 with the credential ignored.
/// 4. SigV4 and catalog token/OAuth authentication are mutually exclusive.
/// 5. When `use_sigv4` is enabled, `access_key`, `secret_key`, and `region` are
///    required (they sign the catalog `load_table` request ahead of any vended
///    credentials); this holds regardless of `use_vended_credentials`. `endpoint`
///    stays optional.
/// 6. OAuth2 client credentials require both `client_id` and `client_secret`.
///
/// Rules 2 and 3 sit ahead of 4-6 because they decide WHICH storage backend the
/// credential set describes; reporting a catalog-authentication defect first
/// would leave a malformed storage-credential set unreported until the operator
/// fixed an unrelated field. Rule 2 sits ahead of rule 3 because a CONNECTION
/// carrying both credential sets has no single well-formed shape for rule 3 to
/// check it against. `use_sigv4` together with Azure fields needs no rule of its
/// own: rule 2 rejects it when the SigV4 fields are supplied, and rule 5 rejects
/// it when they are not.
fn validate_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    if creds.warehouse.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' password is missing required field: {REQUIRED_KEY}"
        )));
    }

    let azure_fields = supplied_azure_fields(creds);
    if !azure_fields.is_empty() {
        let s3_fields = supplied_s3_fields(creds);
        if !s3_fields.is_empty() {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' supplies Azure storage credential field(s) {} together \
                 with S3 storage credential field(s) {}; Azure and S3 storage credentials \
                 cannot both be supplied on one CONNECTION",
                azure_fields.join(", "),
                s3_fields.join(", ")
            )));
        }

        let mut defects: Vec<&str> = Vec::new();
        if creds.account_name.is_none() {
            defects.push("account_name is missing");
        }
        match (creds.account_key.is_some(), creds.sas_token.is_some()) {
            (true, true) => defects.push("account_key and sas_token are both present"),
            (false, false) => defects.push("neither account_key nor sas_token is present"),
            (true, false) | (false, true) => {}
        }
        if !defects.is_empty() {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' supplies Azure storage credential field(s) {}; an Azure \
                 CONNECTION requires account_name and exactly one of account_key and sas_token: {}",
                azure_fields.join(", "),
                defects.join("; ")
            )));
        }
    }

    if creds.use_sigv4 && creds.has_catalog_auth() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' enables SigV4 signing together with catalog \
             token/OAuth authentication; these cannot both be enabled"
        )));
    }

    if creds.use_sigv4 {
        let mut missing: Vec<&str> = Vec::new();
        if creds.access_key.is_empty() {
            missing.push("access_key");
        }
        if creds.secret_key.is_empty() {
            missing.push("secret_key");
        }
        if creds.region.is_empty() {
            missing.push("region");
        }
        if !missing.is_empty() {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' enables SigV4 signing but is missing field(s) \
                 required when SigV4 signing is enabled: {}",
                missing.join(", ")
            )));
        }
    }

    match (creds.client_id.is_some(), creds.client_secret.is_some()) {
        (true, false) => {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' OAuth2 client credentials require both \
                 client_id and client_secret; missing field: client_secret"
            )));
        }
        (false, true) => {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' OAuth2 client credentials require both \
                 client_id and client_secret; missing field: client_id"
            )));
        }
        _ => {}
    }

    Ok(())
}

/// The Azure storage-credential field names this CONNECTION supplies. An empty
/// result is what makes a CONNECTION an S3 one: naming no Azure field describes
/// no Azure backend.
fn supplied_azure_fields(creds: &ConnectionCreds) -> Vec<&'static str> {
    [
        ("account_name", creds.account_name.is_some()),
        ("account_key", creds.account_key.is_some()),
        ("sas_token", creds.sas_token.is_some()),
    ]
    .into_iter()
    .filter_map(|(field, supplied)| supplied.then_some(field))
    .collect()
}

/// The static S3 storage-credential field names this CONNECTION supplies.
///
/// The four string fields use the empty string as "absent", the convention
/// `parse_creds` applies to every field it reads through `nonempty_str`;
/// `session_token` uses `None`.
fn supplied_s3_fields(creds: &ConnectionCreds) -> Vec<&'static str> {
    [
        ("endpoint", !creds.endpoint.is_empty()),
        ("region", !creds.region.is_empty()),
        ("access_key", !creds.access_key.is_empty()),
        ("secret_key", !creds.secret_key.is_empty()),
        ("session_token", creds.session_token.is_some()),
    ]
    .into_iter()
    .filter_map(|(field, supplied)| supplied.then_some(field))
    .collect()
}

fn parse_creds(json: &serde_json::Value) -> ConnectionCreds {
    ConnectionCreds {
        warehouse: nonempty_str(json, "warehouse").unwrap_or("").to_string(),
        endpoint: nonempty_str(json, "endpoint").unwrap_or("").to_string(),
        region: nonempty_str(json, "region").unwrap_or("").to_string(),
        access_key: nonempty_str(json, "access_key").unwrap_or("").to_string(),
        secret_key: nonempty_str(json, "secret_key").unwrap_or("").to_string(),
        session_token: nonempty_str(json, "session_token").map(|s| s.to_string()),
        path_style: json
            .get("path_style")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        use_sigv4: json
            .get("use_sigv4")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        use_vended_credentials: json
            .get("use_vended_credentials")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        token: nonempty_str(json, "token").map(|s| s.to_string()),
        client_id: nonempty_str(json, "client_id").map(|s| s.to_string()),
        client_secret: nonempty_str(json, "client_secret").map(|s| s.to_string()),
        oauth2_server_uri: nonempty_str(json, "oauth2_server_uri").map(|s| s.to_string()),
        scope: nonempty_str(json, "scope").map(|s| s.to_string()),
        account_name: nonempty_str(json, "account_name").map(|s| s.to_string()),
        account_key: nonempty_str(json, "account_key").map(|s| s.to_string()),
        sas_token: nonempty_str(json, "sas_token").map(|s| s.to_string()),
    }
}

/// Build a `StorageBackend` from resolved credentials. `allow_http` arrives as
/// a parameter rather than a `ConnectionCreds` field because it originates
/// from the adapter's `PROP_ALLOW_HTTP` property, read in
/// `resolve_connection_config`, not from the connection creds themselves;
/// taking it here lets this function finish building the `StorageBackend`
/// payload in one step, so no caller has to mutate the constructed payload
/// afterwards to apply it. It is an S3-only knob: the Azure backend carries no
/// HTTP-scheme field, so an Azure CONNECTION ignores it.
///
/// This is the ONE site that selects a storage backend from input, and it is
/// TOTAL by construction. The Azure branch needs an account name AND a
/// resolvable [`AdlsCred`] — exactly one of `account_key` and `sas_token` —
/// and falls through to S3 when either is absent. `read_connection` always runs
/// `validate_creds` first, so that fall-through is unreachable in production;
/// it is a deterministic answer rather than a panic because a panic inside a
/// UDF is an abnormal VM exit, and the engine responds by SIGKILLing every
/// sibling VM of the statement part — turning a defensive assertion into a
/// cluster-wide failure. Returning `Result` instead would push a new error path
/// through the caller for a state that cannot occur.
pub fn storage_block(creds: &ConnectionCreds, allow_http: bool) -> StorageBackend {
    let azure_cred = match (creds.account_key.as_deref(), creds.sas_token.as_deref()) {
        (Some(account_key), None) => Some(AdlsCred::AccountKey(account_key.to_string())),
        (None, Some(sas_token)) => Some(AdlsCred::Sas(sas_token.to_string())),
        (Some(_), Some(_)) | (None, None) => None,
    };
    if let (Some(account_name), Some(cred)) = (creds.account_name.as_deref(), azure_cred) {
        return StorageBackend::Adls {
            account_name: account_name.to_string(),
            cred,
        };
    }

    StorageBackend::S3(StorageProps {
        endpoint: creds.endpoint.clone(),
        region: creds.region.clone(),
        access_key: creds.access_key.clone(),
        secret_key: creds.secret_key.clone(),
        session_token: creds.session_token.clone(),
        allow_http,
        path_style: creds.path_style,
    })
}

/// Build `CatalogProps` from resolved credentials and table name.
///
/// Takes no catalog URI: `CatalogProps` does not carry one, because every consumer
/// of it already receives the URI as its own explicit parameter.
pub fn catalog_block(creds: &ConnectionCreds, table: &str) -> CatalogProps {
    CatalogProps {
        warehouse: creds.warehouse.clone(),
        table: table.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
