//! Iceberg REST catalog access: session resolution, authentication, namespace
//! enumeration, vended-storage-credential resolution, SigV4 request signing,
//! credential redaction, and the three shared credential types
//! (`CatalogProps`, `ConnectionCreds`, `StorageProps`).

mod auth;
mod creds;
mod iceberg_io;
mod namespace;
mod redaction;
mod session;
mod sigv4;
mod vended;

#[cfg(test)]
mod test_support;

pub use creds::{CatalogProps, ConnectionCreds, StorageProps};
pub use iceberg_io::build_s3_file_io;
pub use namespace::{list_namespace_tables, parse_table_ident};
pub use redaction::{redact_credentials, redact_secret_values};
pub use session::{CatalogSession, load_table_any_auth};

pub use vended::resolve_vended_storage;
