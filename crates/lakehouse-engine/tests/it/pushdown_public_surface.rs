//! External-crate reachability probe: only genuinely `pub` items of `pushdown` compile here
//! (16 items, a subset of the in-crate probe's 26). The count is stated because the compiler
//! catches narrowing, not deletion.
#![allow(unused_imports)]

use lakehouse_engine::adapter::pushdown::{
    AggregateMergeInputs, ColumnTypes, ConnectionStorage, FanOut, GroupedAggregateDetection,
    GroupedSelectItem, LoadedTable, ProjectedColumn, RefusedColumn, ResolvedScan,
    build_fan_out_inner, build_grouped_aggregate_scan_sql, build_scan_driving_sql,
    detect_aggregates, detect_group_by_aggregates, shard_count,
};
