
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageCreds;

use crate::scan::sealed::{derive_sealed_storage_key, unseal_storage};
use crate::scan::spec::{CommonScanSpec, ScanStorage, StorageBackend};

#[derive(Debug)]
pub struct ResolvedScanStorage {
    primary: StorageBackend,
    join: Option<StorageBackend>,
}

impl ResolvedScanStorage {
    pub fn from_backends(primary: StorageBackend, join: Option<StorageBackend>) -> Self {
        Self { primary, join }
    }

    pub(crate) fn primary(&self) -> &StorageBackend {
        &self.primary
    }

    pub(crate) fn join(&self) -> Option<&StorageBackend> {
        self.join.as_ref()
    }

    pub(crate) fn all_secret_values(&self) -> Vec<&str> {
        let mut secrets = self.primary.secret_values();
        if let Some(join) = &self.join {
            secrets.extend(join.secret_values());
        }
        secrets
    }
}

pub(crate) fn resolve_scan_storage(
    common: &CommonScanSpec,
    ctx: &dyn UdfContext,
) -> Result<ResolvedScanStorage, UdfError> {
    let primary = resolve_side(&common.storage, ctx)?;
    let join = match &common.join {
        Some(join) => Some(resolve_side(&join.storage, ctx)?),
        None => None,
    };
    Ok(ResolvedScanStorage { primary, join })
}

fn resolve_side(storage: &ScanStorage, ctx: &dyn UdfContext) -> Result<StorageBackend, UdfError> {
    match storage {
        ScanStorage::Inline(backend) => Ok(backend.clone()),
        ScanStorage::Connection { name, allow_http } => {
            let password = connection_password(ctx, name)?;
            Ok(StorageCreds::from_json(&password_object(name, &password)?).backend(*allow_http))
        }
        ScanStorage::Sealed { name, payload } => {
            let password = connection_password(ctx, name)?;
            let key = derive_sealed_storage_key(&password);
            unseal_storage(payload, &key).map_err(|failure| {
                UdfError::User(format!(
                    "the scan cannot open the storage envelope sealed under CONNECTION \
                     '{name}' ({failure}); the expected cause is the CONNECTION's password \
                     having been rotated after this query was planned"
                ))
            })
        }
    }
}

/// Exasol checks the grant against the VS OWNER, not the querying user.
fn connection_password(ctx: &dyn UdfContext, name: &str) -> Result<String, UdfError> {
    match ctx.connection(name) {
        Ok(connection) => Ok(connection.password),
        Err(cause) => {
            let script = script_reference(ctx);
            Err(UdfError::User(format!(
                "the scan cannot access CONNECTION '{name}' ({cause}); the scan script needs \
                 GRANT ACCESS ON CONNECTION {name} FOR SCRIPT {script} TO <vs_owner>, granted \
                 directly or through a role, to the OWNER of the virtual schema being queried \
                 (one deployment-time grant — a querying user needs no connection privilege)"
            )))
        }
    }
}

fn script_reference(ctx: &dyn UdfContext) -> String {
    let schema = ctx.script_schema();
    let script = ctx.script_name();
    format!(
        "{}.{}",
        if schema.is_empty() {
            UNREPORTED_SCRIPT_SCHEMA
        } else {
            schema.as_str()
        },
        if script.is_empty() {
            UNREPORTED_SCRIPT_NAME
        } else {
            script.as_str()
        }
    )
}

const UNREPORTED_SCRIPT_SCHEMA: &str = "<schema>";
const UNREPORTED_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";

fn password_object(name: &str, password: &str) -> Result<serde_json::Value, UdfError> {
    let refusal = || {
        UdfError::User(format!(
            "CONNECTION '{name}' password is not a JSON object, so the scan cannot derive \
             its storage credentials"
        ))
    };
    let json: serde_json::Value = serde_json::from_str(password).map_err(|_| refusal())?;
    if json.is_object() {
        Ok(json)
    } else {
        Err(refusal())
    }
}

#[cfg(test)]
#[path = "storage_ref_tests.rs"]
mod tests;
