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

/// The Unity Catalog credential-vending operation a plan-time read asks for.
/// Planning never writes, and a write-scoped credential would grant the scan more
/// than it needs.
const READ_OPERATION: &str = "READ";

/// One Unity Catalog table's storage decision: its checked table root and the
/// backend its files are read through. The single owner of that decision across
/// every format Unity Catalog hosts, so the no-fallback vending rule can't drift
/// between readers; scoped to this table's key, so it can't be hoisted to a shared caller.
pub(super) struct UnityTableStorage<'a> {
    session: &'a UnityCatalogSession,
    table: &'a CatalogTable,
    storage: &'a StorageBackend,
    creds: &'a ConnectionCreds,
    allow_http: bool,
}

impl<'a> UnityTableStorage<'a> {
    /// `connection` is the CONNECTION's static storage decision: its static storage
    /// backend and resolved credentials, plus the resolved `ALLOW_HTTP` property,
    /// which under vending is the operator's consent gate for plaintext transport.
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

    /// This table's catalog-reported storage location and the backend its files are
    /// read THROUGH, the location checked before any credential is decided.
    pub(super) async fn resolve(&self) -> Result<(&'a str, StorageBackend), UdfError> {
        let table_root = self.checked_table_root()?;
        let effective_storage = self.effective_storage(table_root).await?;
        Ok((table_root, effective_storage))
    }

    /// This table's own catalog-reported storage location, checked before the
    /// vended/static split and before any object-storage access — neither the
    /// catalog URI nor the CONNECTION endpoint may substitute for one left empty.
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

    /// The vended backend under vending, the CONNECTION's static one otherwise.
    /// A table with no vending key (empty counts as none) fails rather than
    /// falling back to a credential the operator didn't select for it.
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

/// Re-raise `error` with every value in `secrets` masked. Collapses onto
/// [`UdfError::User`] since every error reaching here is a plan-time refusal a
/// user must read, and masking through `Display` leaves no payload a future
/// SDK variant could smuggle a secret through unmasked.
pub(super) fn redacted(error: UdfError, secrets: &[&str]) -> UdfError {
    UdfError::User(redact_error_text(&error.to_string(), secrets))
}
