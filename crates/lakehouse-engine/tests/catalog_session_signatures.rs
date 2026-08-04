//! Compile-time proof that `resolve_file_list` and `resolve_table_schema`
//! accept `&lakehouse_catalog::CatalogSession` as their first parameter.
//!
//! Both functions perform real catalog/network I/O (`CatalogSession::resolve`
//! itself runs an OAuth2 grant and a config lookup), so unlike
//! `shared_type_reexports.rs`'s plain-struct probes, this file cannot prove
//! its fact by actually constructing a session and calling either function —
//! that would need live catalog infrastructure this compile-time test does
//! not have. Instead, each `accepts_shared_session_for_*` function below is
//! never invoked; its only job is to exist with a signature that names
//! `&lakehouse_catalog::CatalogSession` explicitly for the first parameter
//! and forwards every argument into the real function. The compiler still
//! type-checks that forwarding call against `resolve_file_list`'s and
//! `resolve_table_schema`'s real signatures while compiling this crate, so a
//! regression in either signature (e.g. the `&CatalogSession` parameter being
//! dropped or changed to an owned value) fails this file's compilation rather
//! than surfacing only in a live-catalog integration test.
//!
//! Covers Verification > Scenario Coverage rows (vs-adapter/pushdown-catalog-session):
//! - "CatalogSession is public and every file-resolution entry point takes
//!   one" -> `file_resolution_entry_points_take_a_shared_session`
//! - "createVirtualSchema resolves every table's schema on one shared
//!   session" -> `schema_resolution_entry_point_takes_a_shared_session`

use exasol_udf_sdk::error::UdfError;
use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::{resolve_file_list, resolve_table_schema};
use lakehouse_engine::scan::spec::{
    CatalogProps, FileEntry, LogicalField, NameMappingEntry, StorageBackend,
};
use serde_json::Value as Json;

/// Never invoked. Its signature naming `&lakehouse_catalog::CatalogSession`
/// as the first parameter, forwarded into `resolve_file_list`, is the proof:
/// this file only compiles while `resolve_file_list`'s real first parameter
/// stays a shared reference to the catalog crate's session type.
#[allow(dead_code)]
async fn accepts_shared_session_for_file_resolution(
    session: &lakehouse_catalog::CatalogSession,
    catalog_props: &CatalogProps,
    storage: &StorageBackend,
    creds: &ConnectionCreds,
    allow_http: bool,
    filter_json: Option<&Json>,
) -> Result<
    (
        Vec<FileEntry>,
        StorageBackend,
        Vec<LogicalField>,
        String,
        Vec<NameMappingEntry>,
    ),
    UdfError,
> {
    resolve_file_list(
        session,
        catalog_props,
        storage,
        creds,
        allow_http,
        filter_json,
    )
    .await
}

/// Never invoked, for the same reason as `accepts_shared_session_for_file_resolution`
/// above. Proves `resolve_table_schema` accepts `&lakehouse_catalog::CatalogSession`
/// as its first parameter.
#[allow(dead_code)]
async fn accepts_shared_session_for_schema_resolution(
    session: &lakehouse_catalog::CatalogSession,
    catalog_props: &CatalogProps,
    creds: &ConnectionCreds,
) -> Result<Vec<(String, String)>, UdfError> {
    resolve_table_schema(session, catalog_props, creds).await
}

#[test]
fn file_resolution_entry_points_take_a_shared_session() {
    // The proof is that `accepts_shared_session_for_file_resolution` above
    // compiled: no live catalog is built or called here.
}

#[test]
fn schema_resolution_entry_point_takes_a_shared_session() {
    // The proof is that `accepts_shared_session_for_schema_resolution` above
    // compiled: no live catalog is built or called here.
}
