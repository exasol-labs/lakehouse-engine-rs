//! Where an export table's storage credentials come from, and the FileIO each kind signs with.

use crate::adls_storage::adls_file_io;
use crate::vended_refresh::{AdlsCredentials, StorageCredentials, TableCredentials, VendedTable};
use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg_catalog_rest::LoadTableResult;
use lakehouse_catalog::{
    AssumedRole, CatalogSession, ConnectionCreds, EpochClock, StaticStoreAddress, StorageBackend,
    StorageCreds, VendedBackendKind, VendedCredentialSet, classify_vended_scheme,
    resolve_vended_storage, scheme_of,
};
use std::sync::Arc;

/// Where an export table's storage credentials come from. Every kind reaches the table's FileIO
/// through one credential source, which renews the credentials its own way.
pub enum WriteStorage {
    /// The CONNECTION's own storage credentials, S3 keys or an Azure account key or SAS, which
    /// do not expire.
    Static(StorageBackend),
    /// The credentials of an IAM role the CONNECTION names, assumed again before they expire.
    /// `backend` carries the store's address.
    AssumedRole {
        backend: StorageBackend,
        role: AssumedRole,
    },
    /// Credentials the catalog vends per table, renewed before they expire. The CONNECTION
    /// contributes at most the store's address.
    Vended(StaticStoreAddress),
}

impl WriteStorage {
    /// Vended per table when the CONNECTION sets `use_vended_credentials`, else the credentials of
    /// `role` when the CONNECTION names one, else its own keys.
    pub fn of(creds: &ConnectionCreds, allow_http: bool, role: Option<AssumedRole>) -> Self {
        if creds.use_vended_credentials {
            return Self::Vended(StaticStoreAddress::from(creds));
        }
        let backend = StorageCreds::from(creds).backend(allow_http);
        match role {
            Some(role) => Self::AssumedRole { backend, role },
            None => Self::Static(backend),
        }
    }
}

/// The FileIO of `table`, which `answer` describes, and the credential source it signs with: the
/// CONNECTION's own credentials, the credentials of its role, or the credentials `answer` vends.
/// Each is seeded into a source that renews it its own way. A location the storage cannot address
/// is refused before any storage is configured.
pub(crate) fn storage_of(
    session: &Arc<CatalogSession>,
    storage: &WriteStorage,
    allow_http: bool,
    clock: &EpochClock,
    table: &TableIdent,
    answer: &LoadTableResult,
) -> Result<(FileIO, TableCredentials), UdfError> {
    let location = answer.metadata.location().to_string();
    let vended_kind = refuse_unaddressable(storage, allow_http, table, &location)?;
    match storage {
        WriteStorage::Static(StorageBackend::Adls { cred, .. }) => {
            Ok(adls_storage(&location, AdlsCredentials::connection(cred)?))
        }
        WriteStorage::Static(backend @ StorageBackend::S3(props)) => {
            let keys = StorageCredentials::keys(
                &props.access_key,
                &props.secret_key,
                props.session_token.as_deref(),
            );
            s3_storage(backend, keys)
        }
        WriteStorage::AssumedRole { backend, role } => {
            s3_storage(backend, StorageCredentials::role(role.clone()))
        }
        WriteStorage::Vended(address) => {
            let vended = VendedTable {
                session: session.clone(),
                table: table.clone(),
                location: location.clone(),
                allow_http,
            };
            let set = VendedCredentialSet::of(answer, &location);
            if let Some(VendedBackendKind::Adls) = vended_kind {
                let seed = set.adls_sas(&location)?;
                let credentials = AdlsCredentials::vended(vended, seed, clock.clone())?;
                return Ok(adls_storage(&location, credentials));
            }
            let seed = set.s3(&location)?;
            let credentials = StorageCredentials::vended(vended, seed, clock.clone())?;
            let backend = resolve_vended_storage(answer, &location, allow_http, address)?;
            s3_storage(&backend, credentials)
        }
    }
}

/// Refuses a location the storage cannot address: the CONNECTION's S3 storage, by keys or a role,
/// addresses `s3` and `s3a`, its Azure storage `abfss`, and vended storage all four schemes,
/// `abfs` only under the export's `ALLOW_HTTP`. Returns the backend a vended location selects.
fn refuse_unaddressable(
    storage: &WriteStorage,
    allow_http: bool,
    table: &TableIdent,
    location: &str,
) -> Result<Option<VendedBackendKind>, UdfError> {
    let scheme = scheme_of(location);
    let storage = match storage {
        WriteStorage::Static(backend) | WriteStorage::AssumedRole { backend, .. } => {
            if backend.addresses_scheme(&scheme) {
                return Ok(None);
            }
            match backend {
                StorageBackend::S3(_) => "the CONNECTION's S3 storage",
                StorageBackend::Adls { .. } => "the CONNECTION's Azure storage",
            }
        }
        WriteStorage::Vended(_) => match classify_vended_scheme(&scheme) {
            // object_store writes `abfs` over HTTPS too; ALLOW_HTTP acknowledges the upgrade, as
            // the virtual schema's property does.
            Some(VendedBackendKind::Adls) if scheme == "abfs" && !allow_http => {
                return Err(UdfError::User(format!(
                    "table {table} has the location {location}, whose plaintext scheme abfs needs \
                     the export option ALLOW_HTTP = 'true', which acknowledges that the export \
                     writes the location over HTTPS"
                )));
            }
            Some(kind) => return Ok(Some(kind)),
            None => "vended storage",
        },
    };
    Err(UdfError::User(format!(
        "table {table} has the location {location}, whose scheme {scheme} {storage} cannot address"
    )))
}

fn s3_storage(
    backend: &StorageBackend,
    credentials: StorageCredentials,
) -> Result<(FileIO, TableCredentials), UdfError> {
    let file_io = backend.file_io_with_credential_loader(credentials.loader())?;
    Ok((file_io, TableCredentials::S3(credentials)))
}

fn adls_storage(location: &str, credentials: AdlsCredentials) -> (FileIO, TableCredentials) {
    (
        adls_file_io(location, credentials.clone()),
        TableCredentials::Adls(credentials),
    )
}

#[cfg(test)]
#[path = "write_storage_tests.rs"]
mod tests;
