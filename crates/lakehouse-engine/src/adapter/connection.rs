//! Credential values never appear in an error message produced by this module.

use crate::connection::{
    parse_creds, password_object, supplied_azure_fields, supplied_s3_fields, validate_shared_creds,
};
use crate::exasol::catalog_connection_name;
use crate::scan::sealed::{
    SealedStorageKey, connection_password_carries_key_material, derive_sealed_storage_key,
};
use crate::scan::spec::{CatalogProps, StorageBackend};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{StorageCreds, scheme_of};

use super::catalog_kind::CatalogKind;

pub const REQUIRED_KEY: &str = "warehouse";

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
    let name = catalog_connection_name(name)
        .ok_or_else(|| UdfError::User("CATALOG_CONNECTION is required".into()))?;

    let conn = ctx.connection(name).map_err(|cause| {
        UdfError::User(format!(
            "CONNECTION '{name}' could not be resolved: {cause}"
        ))
    })?;

    let uri = conn.address;
    if uri.is_empty() && kind != CatalogKind::DirectStorage {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' has no address; expected the catalog URI"
        )));
    }

    let json = password_object(name, &conn.password)?;
    let creds = parse_kind_creds(name, &json, kind)?;
    validate_kind_preconditions(name, &creds, kind, &uri)?;
    validate_shared_creds(name, &creds, &uri)?;
    let sealed_storage_key = connection_password_carries_key_material(&creds)
        .then(|| derive_sealed_storage_key(&conn.password));
    Ok(Resolved {
        uri,
        creds,
        sealed_storage_key,
    })
}

fn parse_kind_creds(
    name: &str,
    json: &serde_json::Value,
    kind: CatalogKind,
) -> Result<ConnectionCreds, UdfError> {
    let mut creds = parse_creds(json);
    if kind == CatalogKind::Glue {
        if json.get("use_sigv4").and_then(|v| v.as_bool()) == Some(false) {
            return Err(UdfError::User(format!(
                "CONNECTION '{name}' sets use_sigv4 to false, but the Glue catalog kind always \
                 signs with AWS SigV4; remove use_sigv4"
            )));
        }
        creds.use_sigv4 = true;
    }
    Ok(creds)
}

fn validate_kind_preconditions(
    name: &str,
    creds: &ConnectionCreds,
    kind: CatalogKind,
    address: &str,
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
        CatalogKind::DirectStorage => {
            validate_direct_storage_preconditions(name, creds, address)?;
        }
        CatalogKind::Glue => validate_glue_preconditions(name, creds)?,
    }
    Ok(())
}

fn validate_glue_preconditions(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    if creds.use_vended_credentials {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' sets use_vended_credentials, but the Glue catalog kind has no \
             native credential vending; remove it and supply storage credentials"
        )));
    }
    let rejected = supplied_catalog_auth_fields(creds);
    if !rejected.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' supplies field(s) {} but the Glue catalog kind signs with AWS \
             SigV4 and uses no token or OAuth2 authentication; remove them",
            rejected.join(", ")
        )));
    }
    Ok(())
}

fn validate_direct_storage_preconditions(
    name: &str,
    creds: &ConnectionCreds,
    address: &str,
) -> Result<(), UdfError> {
    if address.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' has no address; a storage base path is expected"
        )));
    }

    // Catalog-auth fields are meaningless under direct storage; reject rather than ignore them.
    let mut rejected: Vec<&str> = Vec::new();
    if !creds.warehouse.is_empty() {
        rejected.push("warehouse");
    }
    rejected.extend(supplied_catalog_auth_fields(creds));
    if creds.use_sigv4 {
        rejected.push("use_sigv4");
    }
    if creds.use_vended_credentials {
        rejected.push("use_vended_credentials");
    }
    if !rejected.is_empty() {
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' supplies field(s) {} but the direct-storage catalog kind \
             reaches no catalog service; remove them",
            rejected.join(", ")
        )));
    }

    let scheme = scheme_of(address);
    let backend = storage_block(creds, false);
    if !backend.addresses_scheme(&scheme) {
        let azure_fields = supplied_azure_fields(creds);
        let (shape, accepted): (String, &[&str]) = if azure_fields.is_empty() {
            (
                format!(
                    "S3 credential field(s) {}",
                    supplied_s3_fields(creds).join(", ")
                ),
                &["s3", "s3a"],
            )
        } else {
            (
                format!("Azure credential field(s) {}", azure_fields.join(", ")),
                &["abfss"],
            )
        };
        return Err(UdfError::User(format!(
            "CONNECTION '{name}' address scheme '{scheme}' does not agree with its {shape}; \
             the two must address the same storage backend; accepted scheme(s) for these \
             credentials: {}",
            accepted.join(", ")
        )));
    }
    Ok(())
}

fn supplied_catalog_auth_fields(creds: &ConnectionCreds) -> Vec<&'static str> {
    [
        ("token", creds.token.is_some()),
        ("client_id", creds.client_id.is_some()),
        ("client_secret", creds.client_secret.is_some()),
        ("oauth2_server_uri", creds.oauth2_server_uri.is_some()),
        ("scope", creds.scope.is_some()),
    ]
    .into_iter()
    .filter_map(|(field, supplied)| supplied.then_some(field))
    .collect()
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

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
