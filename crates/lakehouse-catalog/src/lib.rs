//! Iceberg REST catalog access: session resolution, authentication, namespace
//! enumeration, vended-storage-credential resolution, SigV4 request signing,
//! credential redaction, the three shared credential types (`CatalogProps`,
//! `ConnectionCreds`, `StorageProps`), and the `StorageBackend` that selects which
//! object storage a scan reads through.

mod auth;
mod client;
mod creds;
mod iceberg_io;
mod namespace;
mod redaction;
mod session;
mod sigv4;
mod storage;
mod unity;
mod vended;

#[cfg(test)]
#[path = "test_support_tests.rs"]
mod test_support;

pub use client::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogTable, CatalogTableIdent,
    CatalogTableType, ColumnSourceType, IcebergRestCatalogClient, SkipReason, SkippedTable,
};
pub use creds::{CatalogProps, ConnectionCreds, StorageProps};
pub use namespace::parse_table_ident;
pub use redaction::{redact_credentials, redact_error_text, redact_secret_values};
pub use session::{CatalogSession, load_table_any_auth};
pub use storage::{AdlsCred, StaticStoreAddress, StorageBackend};

pub use vended::resolve_vended_storage;

pub use unity::{TemporaryTableCredentials, UnityCatalogSession, resolve_uc_vended_storage};
