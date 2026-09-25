//! External-crate reachability probe: only genuinely `pub` items of `pushdown` compile here
//! (17 items, a subset of the in-crate probe's 27). The count is stated because the compiler
//! catches narrowing, not deletion; changing it requires a spec delta against
//! `vs-adapter/pushdown-module-structure`.
#![allow(unused_imports)]

use lakehouse_engine::adapter::pushdown::{
    AggregateMergeInputs, ConnectionStorage, FormatReader, GroupedAggregateDetection,
    GroupedSelectItem, RefusedColumn, ResolvedScan, ScanSource, build_fan_out_inner,
    build_grouped_aggregate_scan_sql, build_scan_driving_sql, detect_aggregates,
    detect_group_by_aggregates, format_reader, handle_pushdown, shard_count,
    validate_agg_col_types,
};
