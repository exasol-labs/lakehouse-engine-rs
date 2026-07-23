//! Compile-time reachability probe for the `scan` module's public surface,
//! from an external-crate vantage.
//!
//! This is a pure `use` list with no behavior. Unlike
//! `src/scan_surface_probe.rs` (in-crate vantage, sees all 13
//! always-available `pub`/`pub(crate)` items plus the one `#[cfg(test)]`-gated
//! `pub` item), this file lives in the `tests/` crate and so can only see
//! items that are actually `pub` AND reachable outside the defining crate's
//! own test build (`#[cfg(test)]` items never cross that boundary). It names
//! the 9 externally-reachable `pub` items from the `refactor-scan-modules`
//! plan's Migration table (`specs/_plans/refactor-scan-modules/plan.md`). If
//! any of them is narrowed to `pub(crate)` or less during the module split,
//! this file fails to compile.
#![allow(unused_imports)]

use lakehouse_engine::scan::{
    build_alias_items, build_grouped_partial_agg_sql, build_join_physical_plan,
    build_partial_agg_sql_filtered, build_raw_scan_physical_plan, int96_coerced_parquet_format,
    register_files, run_join_scan_with_session, run_raw_scan_with_session,
};
