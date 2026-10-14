//! Iceberg REST and Unity Catalog access for the lakehouse engine, including AWS STS role assumption.

mod auth;
mod aws_error;
mod client;
mod creds;
mod glue;
mod iceberg_io;
mod lakekeeper;
mod namespace;
mod redaction;
mod session;
mod sigv4;
mod storage;
mod sts;
mod unity;
mod vended;

#[cfg(test)]
#[path = "test_support_tests.rs"]
mod test_support;

pub use client::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogPartition, CatalogTable,
    CatalogTableIdent, CatalogTableType, ColumnSourceType, HIVE_DEFAULT_PARTITION,
    IcebergRestCatalogClient, PartitionFormat, SkipReason, SkippedTable, TableFormat,
    catalog_identifier_string,
};
pub use creds::{CatalogProps, ConnectionCreds, StorageCreds, StorageProps};
pub use glue::{GlueCatalogSession, parse_glue_table_ident};
pub use iceberg_io::read_iceberg_metadata_file;
pub use lakekeeper::{TableReadDecision, lakekeeper_batch_check, lakekeeper_management_url};
pub use namespace::parse_table_ident;
pub use redaction::{redact_credentials, redact_error_text, redact_secret_values};
pub use session::{CatalogSession, load_table_any_auth};
pub use storage::{AdlsCred, StaticStoreAddress, StorageBackend, scheme_of};
pub use sts::resolve_aws_identity;

pub use vended::resolve_vended_storage;

pub use unity::{TemporaryTableCredentials, UnityCatalogSession, resolve_uc_vended_storage};
