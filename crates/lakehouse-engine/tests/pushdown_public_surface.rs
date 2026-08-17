//! Compile-time reachability probe for the `pushdown` module's public surface,
//! from an external-crate vantage.
//!
//! This is a pure `use` list with no behavior. Unlike
//! `src/adapter/pushdown_surface_probe_tests.rs`, which shares the crate and so also
//! sees `pub(crate)` items, this file lives in the `tests/` crate and can only
//! see items that are genuinely `pub`. Its 15 items are therefore a subset of
//! that probe's 25.
//!
//! The `use` list below IS the baseline for the externally-`pub` half of the
//! façade. There is no separate baseline file to consult, so there is nothing
//! for the surface contract to drift from. Narrowing any of these items to
//! `pub(crate)` or less fails this file's compilation. As with the in-crate
//! probe, the count is stated because the compiler catches only narrowing, not
//! deletion. Changing the set or the count requires a spec delta against
//! `vs-adapter/pushdown-module-structure`.
#![allow(unused_imports)]

use lakehouse_engine::adapter::pushdown::{
    ConnectionStorage, FormatReader, GroupedAggregateDetection, GroupedSelectItem, ResolvedScan,
    ScanSource, build_fan_out_inner, build_grouped_aggregate_scan_sql, build_scan_driving_sql,
    detect_aggregates, detect_group_by_aggregates, format_reader, handle_pushdown, shard_count,
    validate_agg_col_types,
};
