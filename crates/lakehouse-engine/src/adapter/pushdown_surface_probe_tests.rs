//! Compile-time probe outside `adapter::pushdown`, so it sees items at their declared visibility:
//! narrowing any item below `pub(crate)` fails compilation. The list holds 26 items; the count
//! makes a removal visible in review, since a deleted item still compiles.
#![allow(unused_imports)]

use crate::adapter::pushdown::{
    AggregateMergeInputs, ColumnTypes, ConnectionStorage, DetectedJoin, FanOut,
    GroupedAggregateDetection, GroupedSelectItem, JoinLeaf, JoinShape, LoadedTable,
    ProjectedColumn, RefusedColumn, RenderedJoinPushdown, ResolvedJoinSide, ResolvedScan, ScanUdfs,
    Shards, build_fan_out_inner, build_grouped_aggregate_scan_sql, build_scan_driving_sql,
    declare_columns, detect_aggregates, detect_group_by_aggregates, detect_join, handle_pushdown,
    shard_count,
};
