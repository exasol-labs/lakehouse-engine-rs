/// Resolve an Exasol CONNECTION object into catalog and storage configuration.
///
/// The CONNECTION's `address` is the Iceberg REST catalog URI; the `password`
/// is a JSON object carrying credential and behavioural fields. Credential
/// values NEVER appear in any error message produced by this module.
use crate::scan::sealed::{
    SealedStorageKey, connection_password_carries_key_material, derive_sealed_storage_key,
};
use crate::scan::spec::{CatalogProps, StorageBackend};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageCreds;

use super::catalog_kind::CatalogKind;
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

/// Resolved CONNECTION: catalog URI, parsed credentials, and the sealing key the
/// password is entitled to.
#[derive(Debug)]
pub struct Resolved {
    pub uri: String,
    pub creds: ConnectionCreds,
    /// The sealing key HKDF-derived from this CONNECTION's RAW password bytes,
    /// present IFF that password carries secret material.
    ///
    /// Both the derivation AND the decision, taken here where the password
    /// already lives: the plaintext never travels further than
    /// [`read_connection`]'s own body, and no reader of a `Resolved` can obtain a
    /// key for a password holding no secret, because for such a password no key
    /// was ever constructed. `Option` rather than a caller-side test is what makes
    /// that structural — see [`read_connection`] for the predicate that decides
    /// it.
    pub(crate) sealed_storage_key: Option<SealedStorageKey>,
}

/// Resolve a named Exasol CONNECTION into a catalog URI, credentials, and the
/// sealing key that password is entitled to.
///
/// Credential-safe: the password value is never embedded in any returned error.
///
/// This is the SINGLE site that calls
/// [`connection_password_carries_key_material`]. The condition is not inlined
/// anywhere else, because the refusal `scan_storage_for` raises is written from a
/// fact only that predicate knows, and a second copy of the test is how the
/// refusal's stated reason and the outcome start to disagree. Gating HERE rather
/// than at a consumer is what makes the guarantee structural: a password carrying
/// no secret produces no [`SealedStorageKey`] at all, so no later reader of the
/// returned [`Resolved`] can seal under one.
pub fn read_connection(
    ctx: &dyn UdfContext,
    name: Option<&str>,
    kind: CatalogKind,
) -> Result<Resolved, UdfError> {
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
    validate_creds(name, &creds, kind)?;
    let sealed_storage_key = connection_password_carries_key_material(&creds)
        .then(|| derive_sealed_storage_key(&conn.password));
    Ok(Resolved {
        uri,
        creds,
        sealed_storage_key,
    })
}

/// Validate parsed credentials against the mode-aware credential contract,
/// parameterized by the resolved [`CatalogKind`].
///
/// Credential-safe: only field names — never values — appear in any error.
///
/// Under `CatalogKind::UnityCatalogNative` the kind first rejects `use_sigv4`
/// (the native Unity Catalog API authenticates with a bearer token or Databricks
/// OAuth, not a signed AWS request) and then applies rules 2-7 below; rule 1's
/// `warehouse` requirement does not apply. Rejecting `use_sigv4` ahead of rules
/// 4-5 keeps the operator from seeing a generic missing-SigV4-field error for a
/// signing mode that does not apply to Unity Catalog.
///
/// Rules, in precedence order:
/// 1. `warehouse` is required under `CatalogKind::IcebergRest` — the only
///    unconditionally-required field under that kind; a native Unity Catalog is
///    addressed by `catalog.schema.table` and carries no warehouse identifier.
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
/// 6. A `token` together with a complete `client_id`/`client_secret` pair is
///    rejected. This rule sits after the SigV4 rules so every SigV4 error stays
///    byte-identical, and ahead of rule 7 for readability only — the two are
///    disjoint (rule 6 requires all three fields; rule 7 requires exactly one of
///    the pair), so their relative order has no behavioural consequence.
/// 7. OAuth2 client credentials require both `client_id` and `client_secret`.
///
/// Rules 2 and 3 sit ahead of 4-7 because they decide WHICH storage backend the
/// credential set describes; reporting a catalog-authentication defect first
/// would leave a malformed storage-credential set unreported until the operator
/// fixed an unrelated field. Rule 2 sits ahead of rule 3 because a CONNECTION
/// carrying both credential sets has no single well-formed shape for rule 3 to
/// check it against. `use_sigv4` together with Azure fields needs no rule of its
/// own: rule 2 rejects it when the SigV4 fields are supplied, and rule 5 rejects
/// it when they are not.
///
/// Each rule-group is delegated to a focused helper; this function fixes only
/// their precedence order (`?` short-circuits on the first defect).
fn validate_creds(name: &str, creds: &ConnectionCreds, kind: CatalogKind) -> Result<(), UdfError> {
    validate_kind_preconditions(name, creds, kind)?;
    validate_azure_storage_creds(name, creds)?;
    validate_sigv4_creds(name, creds)?;
    validate_exclusive_catalog_auth_creds(name, creds)?;
    validate_oauth2_creds(name, creds)?;
    Ok(())
}

/// Rule 1 (`IcebergRest`) and the native Unity Catalog SigV4 rejection: the
/// per-kind preconditions that run ahead of the kind-agnostic rules 2-6.
fn validate_kind_preconditions(
    name: &str,
    creds: &ConnectionCreds,
    kind: CatalogKind,
) -> Result<(), UdfError> {
    match kind {
        CatalogKind::IcebergRest => {
            if creds.warehouse.is_empty() {
                return Err(UdfError::User(format!(
                    "CONNECTION '{name}' password is missing required field: {REQUIRED_KEY}"
                )));
            }
        }
        CatalogKind::UnityCatalogNative => {
            if creds.use_sigv4 {
                return Err(UdfError::User(format!(
                    "CONNECTION '{name}' enables SigV4 signing, but AWS SigV4 signing is not a \
                     Unity Catalog authentication mode; a native Unity Catalog authenticates \
                     with a bearer token or Databricks OAuth"
                )));
            }
        }
    }
    Ok(())
}

/// Rules 2 and 3: Azure and S3 storage credentials are mutually exclusive, and a
/// CONNECTION supplying any Azure field must supply `account_name` plus exactly
/// one of `account_key` and `sas_token`.
fn validate_azure_storage_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    let azure_fields = supplied_azure_fields(creds);
    if azure_fields.is_empty() {
        return Ok(());
    }

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
    Ok(())
}

/// Rules 4 and 5: SigV4 signing is mutually exclusive with catalog token/OAuth
/// authentication, and when enabled requires `access_key`, `secret_key`, and
/// `region`.
fn validate_sigv4_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
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
    Ok(())
}

/// Rule 6: a `token` together with a complete `client_id`/`client_secret`
/// pair is rejected. Fires only when all three fields are present — a
/// `token` beside HALF a pair is already rejected by rule 7 (OAuth2
/// completeness), so the two rules are disjoint and share no error text.
fn validate_exclusive_catalog_auth_creds(
    name: &str,
    creds: &ConnectionCreds,
) -> Result<(), UdfError> {
    if creds.token.is_some() && creds.client_id.is_some() && creds.client_secret.is_some() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' supplies a token together with a complete \
             client_id/client_secret pair; these are mutually exclusive, \
             remove one: token, client_id, client_secret"
        )));
    }
    Ok(())
}

/// Rule 7: OAuth2 client credentials require both `client_id` and
/// `client_secret`, or neither.
fn validate_oauth2_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    match (creds.client_id.is_some(), creds.client_secret.is_some()) {
        (true, false) => Err(UdfError::User(format!(
            "CONNECTION '{name}' OAuth2 client credentials require both \
             client_id and client_secret; missing field: client_secret"
        ))),
        (false, true) => Err(UdfError::User(format!(
            "CONNECTION '{name}' OAuth2 client credentials require both \
             client_id and client_secret; missing field: client_id"
        ))),
        _ => Ok(()),
    }
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
/// [`StorageCreds::from_json`] applies to every storage field `parse_creds`
/// reads through it; `session_token` uses `None`.
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
    let StorageCreds {
        endpoint,
        region,
        access_key,
        secret_key,
        session_token,
        path_style,
        account_name,
        account_key,
        sas_token,
    } = StorageCreds::from_json(json);
    ConnectionCreds {
        warehouse: nonempty_str(json, "warehouse").unwrap_or("").to_string(),
        endpoint,
        region,
        access_key,
        secret_key,
        session_token,
        path_style,
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
        account_name,
        account_key,
        sas_token,
    }
}

/// Build a `StorageBackend` from resolved credentials, by projecting them onto
/// their storage half and asking that projection which backend it describes.
///
/// The selection rule itself lives on [`StorageCreds::backend`], not here, so
/// the adapter's plan-time derivation and the scan UDF's own read of the same
/// CONNECTION cannot select two different backends from one password. What
/// stays here is the Exasol-CONNECTION-facing entry point: `catalog-crate-structure`
/// records that every function interpreting that delivery mechanism belongs to
/// this module, because the catalog crate must not name it. So this is a
/// deliberate projection-and-delegate rather than a layer to inline away.
///
/// `allow_http` arrives as a parameter rather than a `ConnectionCreds` field
/// because it originates from the adapter's `PROP_ALLOW_HTTP` property, read in
/// `resolve_connection_config`, not from the connection creds themselves; it is
/// an S3-only knob, so an Azure CONNECTION ignores it.
pub fn storage_block(creds: &ConnectionCreds, allow_http: bool) -> StorageBackend {
    StorageCreds::from(creds).backend(allow_http)
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
