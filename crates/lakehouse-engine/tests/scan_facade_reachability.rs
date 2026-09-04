//! Compile-time reachability probe for the `scan` module's public surface,
//! from an external-crate vantage.
//!
//! This is a pure `use` list with no behavior. Unlike
//! `src/scan_surface_probe_tests.rs` (in-crate vantage, sees all 13
//! always-available `pub`/`pub(crate)` items plus the one `#[cfg(test)]`-gated
//! `pub` item), this file lives in the `tests/` crate and so can only see
//! items that are actually `pub` AND reachable outside the defining crate's
//! own test build (`#[cfg(test)]` items never cross that boundary). It names
//! the 9 externally-reachable `pub` items from the `refactor-scan-modules`
//! plan's Migration table (`specs/_plans/refactor-scan-modules/plan.md`), plus
//! `ResolvedScanStorage` and its `from_backends` constructor. Those two are the
//! argument three of the facade entries now take: without both being `pub`, an
//! external caller can name the entry but cannot build what it asks for, so a
//! reachability list naming only the entries would pass while the facade was
//! unusable. If any of them is narrowed to `pub(crate)` or less during the
//! module split, this file fails to compile.
#![allow(unused_imports)]

use lakehouse_engine::scan::spec::StorageBackend;
use lakehouse_engine::scan::{
    ResolvedScanStorage, build_alias_items, build_grouped_partial_agg_sql,
    build_join_physical_plan, build_partial_agg_sql_filtered, build_raw_scan_physical_plan,
    int96_coerced_parquet_format, register_files, run_join_scan_with_session,
    run_raw_scan_with_session,
};

/// `ResolvedScanStorage::from_backends` pinned at its declared visibility AND
/// signature from outside the defining crate. Rust has no `use` path for an
/// inherent associated function, so a fn-pointer binding is the only way to name
/// one — and it is the stronger probe, catching a reshaped parameter as well as a
/// narrowed visibility.
const _FROM_BACKENDS: fn(StorageBackend, Option<StorageBackend>) -> ResolvedScanStorage =
    ResolvedScanStorage::from_backends;
