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

#[derive(Debug)]
pub struct Resolved {
    pub uri: String,
    pub creds: ConnectionCreds,
    /// Present iff the password carries secret material — `Option` makes the gate structural.
    pub(crate) sealed_storage_key: Option<SealedStorageKey>,
}

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

fn validate_creds(name: &str, creds: &ConnectionCreds, kind: CatalogKind) -> Result<(), UdfError> {
    validate_kind_preconditions(name, creds, kind)?;
    validate_azure_storage_creds(name, creds)?;
    validate_sigv4_creds(name, creds)?;
    validate_exclusive_catalog_auth_creds(name, creds)?;
    validate_oauth2_creds(name, creds)?;
    Ok(())
}

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

pub fn storage_block(creds: &ConnectionCreds, allow_http: bool) -> StorageBackend {
    StorageCreds::from(creds).backend(allow_http)
}

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
