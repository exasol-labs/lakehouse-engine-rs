//! Compile-time proof that `ScanSource::Iceberg` carries a shared
//! `&lakehouse_catalog::CatalogSession` reference, not an owned session.
//!
//! `format_reader` performs no catalog/network I/O itself (only the
//! [`FormatReader::resolve_scan`] it answers does), but the session it wraps
//! IS built from real catalog I/O (`CatalogSession::resolve` runs an OAuth2
//! grant and a config lookup under most auth modes), so this file still
//! avoids constructing a live one: like `shared_type_reexports.rs`'s
//! plain-struct probes, it proves the fact by SIGNATURE alone.
//! `accepts_shared_session_for_iceberg_scan_source` below is never invoked;
//! its only job is to exist with a signature that names
//! `&lakehouse_catalog::CatalogSession` explicitly and forwards it into
//! `ScanSource::Iceberg`, which `format_reader` accepts. The compiler still
//! type-checks that forwarding call against `ScanSource`'s and
//! `format_reader`'s real signatures while compiling this crate, so a
//! regression (e.g. the session becoming an owned value, or the resolver
//! matching a table format some other way) fails this file's compilation
//! rather than surfacing only in a live-catalog integration test.
//!
//! Covers Verification > Scenario Coverage rows
//! (vs-adapter/pushdown-format-neutral-resolution):
//! - "The Iceberg scan source carries a shared catalog session" ->
//!   `iceberg_scan_source_carries_a_shared_session`
//!
//! The companion façade-departure claim (that collapsing the resolver leaves
//! the pushdown façade unchanged) is NOT pinned here: that is
//! `tests/pushdown_public_surface.rs` and
//! `src/adapter/pushdown_surface_probe_tests.rs`'s compile-time `use` probes'
//! job.

use exasol_udf_sdk::error::UdfError;
use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::{
    ConnectionStorage, FormatReader, ScanSource, format_reader,
};
use lakehouse_engine::scan::spec::{CatalogProps, StorageBackend};

/// Never invoked. Its signature naming `&lakehouse_catalog::CatalogSession`
/// as `session`, forwarded into `ScanSource::Iceberg` and then `format_reader`,
/// is the proof: this file only compiles while the Iceberg scan source's
/// session field stays a shared reference to the catalog crate's session type.
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
fn iceberg_scan_source_carries_a_shared_session() {
    // The proof is that `accepts_shared_session_for_iceberg_scan_source` above
    // compiled: no live catalog is built or called here.
}
