use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogTable, ConnectionCreds, StaticStoreAddress, StorageBackend, UnityCatalogSession,
    redact_error_text, resolve_uc_vended_storage,
};

use super::ConnectionStorage;
use crate::adapter::tables::catalog_identifier_string;

#[cfg(test)]
#[path = "unity_table_storage_tests.rs"]
mod tests;

/// Planning never writes, so it asks for a read-scoped credential.
const READ_OPERATION: &str = "READ";

/// One Unity table's checked root and read backend: the single owner of the no-fallback
/// vending rule across every Unity-hosted format.
pub(super) struct UnityTableStorage<'a> {
    session: &'a UnityCatalogSession,
    table: &'a CatalogTable,
    storage: &'a StorageBackend,
    creds: &'a ConnectionCreds,
    allow_http: bool,
}

impl<'a> UnityTableStorage<'a> {
    pub(super) fn new(
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

    pub(super) async fn resolve(&self) -> Result<(&'a str, StorageBackend), UdfError> {
        let table_root = self.checked_table_root()?;
        let effective_storage = self.effective_storage(table_root).await?;
        Ok((table_root, effective_storage))
    }

    /// Checked before any credential or storage access; neither the catalog URI nor the
    /// CONNECTION endpoint may substitute for an empty location.
    fn checked_table_root(&self) -> Result<&'a str, UdfError> {
        match self.table.storage_location.as_deref() {
            Some(location) if !location.trim().is_empty() => Ok(location),
            _ => Err(UdfError::User(format!(
                "the Unity Catalog metadata for table {} carries an EMPTY storage location; \
                 the catalog URI and the CONNECTION endpoint name no table location and are \
                 not valid substitutes",
                self.table_name()
            ))),
        }
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

/// Re-raise `error` as [`UdfError::User`] with every value in `secrets` masked.
pub(super) fn redacted(error: UdfError, secrets: &[&str]) -> UdfError {
    UdfError::User(redact_error_text(&error.to_string(), secrets))
}
