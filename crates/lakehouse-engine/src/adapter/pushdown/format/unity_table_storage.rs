use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogTable, ConnectionCreds, StaticStoreAddress, StorageBackend, UnityCatalogSession,
    redact_error_text, resolve_uc_vended_storage,
};
use object_store::ObjectStore;

use super::{ConnectionStorage, checked_storage_location};
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::build_table_root_store;
use crate::scan::spec::DEFAULT_S3_MAX_CONNECTIONS;

#[cfg(test)]
#[path = "unity_table_storage_tests.rs"]
mod tests;

/// Planning never writes, so it asks for a read-scoped credential.
const READ_OPERATION: &str = "READ";

/// One Unity table's checked root and read backend: the single owner of the no-fallback
/// vending rule across every Unity-hosted format.
pub(in crate::adapter::pushdown) struct UnityTableStorage<'a> {
    session: &'a UnityCatalogSession,
    table: &'a CatalogTable,
    storage: &'a StorageBackend,
    creds: &'a ConnectionCreds,
    allow_http: bool,
}

impl<'a> UnityTableStorage<'a> {
    pub(in crate::adapter::pushdown) fn new(
        session: &'a UnityCatalogSession,
        table: &'a CatalogTable,
        connection: &ConnectionStorage<'a>,
    ) -> Self {
        Self {
            session,
            table,
            storage: connection.storage,
            creds: connection.creds,
            allow_http: connection.allow_http,
        }
    }

    /// The table root, the backend that reads it, and a store over it. A store error is redacted
    /// against that backend's secrets, because this layer decided which credential it carries.
    pub(super) async fn open_store(
        &self,
    ) -> Result<(&'a str, StorageBackend, Arc<dyn ObjectStore>), UdfError> {
        let table_root = checked_storage_location(self.table, "Unity Catalog")?;
        let effective_storage = self.effective_storage(table_root).await?;
        let secrets = effective_storage.secret_values();
        let store = build_table_root_store(
            &effective_storage,
            table_root,
            DEFAULT_S3_MAX_CONNECTIONS,
            &secrets,
        )
        .map_err(|error| redacted(error, &secrets))?;
        Ok((table_root, effective_storage, store))
    }

    /// Under vending, a table with no (or an empty) vending key fails rather than falling
    /// back to the static credential.
    async fn effective_storage(&self, table_root: &str) -> Result<StorageBackend, UdfError> {
        if !self.creds.use_vended_credentials {
            return Ok(self.storage.clone());
        }

        let vending_key = self
            .table
            .vended_credential_key
            .as_deref()
            .filter(|key| !key.trim().is_empty())
            .ok_or_else(|| {
                UdfError::User(format!(
                    "USE_VENDED_CREDENTIALS is enabled, but Unity Catalog reported no \
                     storage-credential vending key for table {}; reading it through the \
                     CONNECTION's static credential instead would use a credential the \
                     operator did not select for this table",
                    self.table_name()
                ))
            })?;

        let vended = self
            .session
            .temporary_table_credentials(vending_key, READ_OPERATION)
            .await?;
        resolve_uc_vended_storage(
            &vended,
            table_root,
            self.allow_http,
            &StaticStoreAddress::from(self.creds),
        )
    }

    fn table_name(&self) -> String {
        catalog_identifier_string(&self.table.ident)
    }
}

/// Collapses to [`UdfError::User`] via `Display`, so the variant prefix survives while no
/// future variant payload can carry a secret unmasked.
pub(super) fn redacted(error: UdfError, secrets: &[&str]) -> UdfError {
    UdfError::User(redact_error_text(&error.to_string(), secrets))
}
