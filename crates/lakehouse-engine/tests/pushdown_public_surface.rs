//! Compile-time reachability probe for the `pushdown` module's public surface,
//! from an external-crate vantage.
//!
//! This is a pure `use` list with no behavior. Unlike
//! `src/adapter/pushdown_surface_probe.rs` (in-crate vantage, sees all 25
//! items including `pub(crate)`), this file lives in the `tests/` crate and so
//! can only see items that are actually `pub`. It names the 15 `pub` items
//! from the public-surface baseline
//! (`specs/_plans/refactor-adapter-pushdown-modules/public-surface-baseline.txt`).
//! If any of them is narrowed to `pub(crate)` or less, this file fails to
//! compile.
#![allow(unused_imports)]

use lakehouse_engine::adapter::pushdown::{
    GroupedAggregateDetection, GroupedSelectItem, build_fan_out_inner,
    build_grouped_aggregate_scan_sql, build_scan_driving_sql, detect_aggregates,
    detect_group_by_aggregates, extract_vended_keys, handle_pushdown, list_namespace_tables,
    merge_vended_into_storage, resolve_file_list, resolve_table_schema, shard_count,
    validate_agg_col_types,
};
