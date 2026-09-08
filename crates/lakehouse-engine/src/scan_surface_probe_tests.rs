//! Compile-time reachability probe for the `scan` module's public surface.
//!
//! This is primarily a pure `use` list with no behavior. It exists at a
//! vantage point OUTSIDE `scan`'s private submodules — a sibling file at the
//! crate root, mirroring `adapter/pushdown_surface_probe_tests.rs`'s
//! placement one level above `adapter::pushdown`'s own submodules — so that
//! it only sees items at their declared visibility, not the elevated
//! visibility a `mod tests` nested inside
//! `raw_scan`/`join_scan`/`partial_agg`/`object_store`/
//! `field_id_projection`/`sql_support` would enjoy as a descendant module of
//! `scan`. It names the 13 always-available `pub`/`pub(crate)` items plus the
//! one `#[cfg(test)]`-gated `pub` item (`build_partial_agg_sql`) from the
//! `refactor-scan-modules` plan's Migration table
//! (`specs/_plans/refactor-scan-modules/plan.md`), plus `ResolvedScanStorage`
//! and its `from_backends` constructor, which MUST stay `pub`: three of the
//! facade entries below take a `&ResolvedScanStorage`, and a `pub(crate)` type
//! in a `pub` signature both trips `private_interfaces` and leaves no external
//! caller able to construct the argument. If any of them is
//! narrowed below its declared visibility during the module split, this file
//! fails to compile.
//!
//! Task 6.1 (plan `fix-connection-credential-exposure`) adds one exception to
//! the "no behavior" rule: `ScanStorage`'s declared type on both `storage`
//! fields is pinned at compile time below (a fn-pointer-style probe, the same
//! technique `_FROM_BACKENDS` already uses), and one source-level `#[test]`
//! asserts that `ScanStorage` declares neither a `secret_values` method nor a
//! payload accessor — the guarantee that redaction must build its secret set
//! from the RESOLVED credentials (`ResolvedScanStorage::all_secret_values`),
//! never from the wire type itself.
#![allow(unused_imports)]

use crate::scan::spec::{CommonScanSpec, JoinSpec, ScanStorage};
use crate::scan::{
    FieldIdExprAdapterFactory, FieldIdResolution, PARQUET_FIELD_ID_META_KEY, ResolvedScanStorage,
    build_alias_items, build_grouped_partial_agg_sql, build_join_physical_plan,
    build_partial_agg_sql_filtered, build_raw_scan_physical_plan, int96_coerced_parquet_format,
    reconstruct_abs_uri, register_files, run_join_scan_with_session, run_raw_scan_with_session,
};

const _FROM_BACKENDS: fn(
    crate::scan::spec::StorageBackend,
    Option<crate::scan::spec::StorageBackend>,
) -> ResolvedScanStorage = ResolvedScanStorage::from_backends;

use crate::scan::build_partial_agg_sql;

fn _common_scan_spec_storage_is_scan_storage(spec: &CommonScanSpec) -> &ScanStorage {
    &spec.storage
}

fn _join_spec_storage_is_scan_storage(spec: &JoinSpec) -> &ScanStorage {
    &spec.storage
}

const SCAN_SPEC_SOURCE: &str = include_str!("scan/spec.rs");

#[test]
fn scan_storage_declares_no_secret_or_payload_accessor() {
    assert!(
        SCAN_SPEC_SOURCE.contains("pub enum ScanStorage"),
        "the probed source must still declare `ScanStorage` here — the probe's own \
         anchor is broken, not just failing to find a match"
    );
    for method in ["fn secret_values", "fn payload"] {
        assert!(
            !SCAN_SPEC_SOURCE.contains(method),
            "`scan/spec.rs` must declare no `{method}` method on `ScanStorage`: a wrapper \
             accessor would compile at every redaction feed site while returning nothing \
             for a referenced or sealed credential, silently disarming redaction"
        );
    }
}
