//! Compile-time reachability probe for the `scan` module's public surface.
//!
//! This is a pure `use` list with no behavior. It exists at a vantage point
//! OUTSIDE `scan`'s private submodules — a sibling file at the crate root,
//! mirroring `adapter/pushdown_surface_probe_tests.rs`'s placement one level above
//! `adapter::pushdown`'s own submodules — so that it only sees items at
//! their declared visibility, not the elevated visibility a `mod tests`
//! nested inside `raw_scan`/`join_scan`/`partial_agg`/`object_store`/
//! `field_id_projection`/`sql_support` would enjoy as a descendant module of
//! `scan`. It names the 13 always-available `pub`/`pub(crate)` items plus the
//! one `#[cfg(test)]`-gated `pub` item (`build_partial_agg_sql`) from the
//! `refactor-scan-modules` plan's Migration table
//! (`specs/_plans/refactor-scan-modules/plan.md`). If any of them is
//! narrowed below its declared visibility during the module split, this file
//! fails to compile.
#![allow(unused_imports)]

use crate::scan::{
    FieldIdExprAdapterFactory, FieldIdResolution, PARQUET_FIELD_ID_META_KEY, build_alias_items,
    build_grouped_partial_agg_sql, build_join_physical_plan, build_partial_agg_sql_filtered,
    build_raw_scan_physical_plan, int96_coerced_parquet_format, reconstruct_abs_uri,
    register_files, run_join_scan_with_session, run_raw_scan_with_session,
};

// `build_partial_agg_sql` is re-exported flat only under `#[cfg(test)]` (it
// backs the scan crate's own unit tests, never an external consumer). This
// whole file is test-only via its `#[cfg(test)] mod scan_surface_probe;`
// declaration in `lib.rs`, so no further gate is needed to reach it.
use crate::scan::build_partial_agg_sql;
