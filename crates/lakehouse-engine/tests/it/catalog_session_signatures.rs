//! Compile-time proof that `ScanSource::Iceberg` carries a shared
//! `&CatalogSession`, proven by signature because building a live session
//! needs real catalog I/O.

use exasol_udf_sdk::error::UdfError;
use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::{
    ConnectionStorage, FormatReader, ScanSource, format_reader,
};
use lakehouse_engine::scan::spec::{CatalogProps, StorageBackend};

#[allow(dead_code)]
fn accepts_shared_session_for_iceberg_scan_source<'a>(
    session: &'a lakehouse_catalog::CatalogSession,
    catalog_props: &'a CatalogProps,
    storage: &'a StorageBackend,
    creds: &'a ConnectionCreds,
    allow_http: bool,
) -> Result<Box<dyn FormatReader + 'a>, UdfError> {
    format_reader(
        ScanSource::Iceberg {
            session,
            catalog_props,
        },
        &ConnectionStorage {
            storage,
            creds,
            allow_http,
        },
    )
}

#[test]
fn iceberg_scan_source_carries_a_shared_session() {}
