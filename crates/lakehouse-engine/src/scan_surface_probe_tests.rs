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

/// `ResolvedScanStorage::from_backends` pinned at its declared visibility AND
/// signature.
///
/// Rust has no `use` path for an inherent associated function, so a fn-pointer
/// binding is the only way to name one from outside its own module. It is the
/// stronger probe anyway: narrowing the constructor below `pub`, or reshaping
/// either parameter, fails to compile here — and the three `pub` facade entries
/// that take a `&ResolvedScanStorage` are unreachable from an external test
/// without it.
const _FROM_BACKENDS: fn(
    crate::scan::spec::StorageBackend,
    Option<crate::scan::spec::StorageBackend>,
) -> ResolvedScanStorage = ResolvedScanStorage::from_backends;

// `build_partial_agg_sql` is re-exported flat only under `#[cfg(test)]` (it
// backs the scan crate's own unit tests, never an external consumer). This
// whole file is test-only via its `#[cfg(test)] mod scan_surface_probe;`
// declaration in `lib.rs`, so no further gate is needed to reach it.
use crate::scan::build_partial_agg_sql;

/// `CommonScanSpec.storage`'s declared type, pinned by field access with an
/// explicit return type — a change back to a bare `StorageBackend`, or to any
/// other type, fails to compile here rather than only at whichever call site
/// happens to be edited next.
fn _common_scan_spec_storage_is_scan_storage(spec: &CommonScanSpec) -> &ScanStorage {
    &spec.storage
}

/// `JoinSpec.storage`'s declared type, pinned the same way. The dimension
/// side's storage rides here, never in `CommonScanSpec.storage` — see
/// `JoinSpec`'s own doc comment — so this is a SEPARATE pin, not a duplicate
/// of the one above.
fn _join_spec_storage_is_scan_storage(spec: &JoinSpec) -> &ScanStorage {
    &spec.storage
}

/// `scan/spec.rs`'s own source, so
/// [`scan_storage_declares_no_secret_or_payload_accessor`] can search it for a
/// method that would restore the credential exposure plan
/// `fix-connection-credential-exposure` closed.
const SCAN_SPEC_SOURCE: &str = include_str!("scan/spec.rs");

/// `ScanStorage` must expose no way to read a secret, or the `Sealed` variant's
/// `payload` string, directly off the wire type. Every error-redaction feed
/// site in the scan builds its secret set from the RESOLVED credentials
/// (`ResolvedScanStorage::all_secret_values`), never from `ScanStorage`
/// itself: a `secret_values()` or a `payload` accessor added here would
/// compile at every one of those sites while silently returning nothing for a
/// referenced or sealed credential, disarming redaction without a compile
/// error to catch it.
///
/// Scoped to `scan/spec.rs`, where `ScanStorage` is declared and where this
/// codebase's convention colocates a type with its own `impl` blocks — a text
/// search, not a parse, so it also fails loudly (rather than passing
/// vacuously) if `ScanStorage`'s own declaration ever moves out of this file
/// without this probe's `include_str!` path being updated to follow it.
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
