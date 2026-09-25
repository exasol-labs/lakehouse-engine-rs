//! Compile-time probe outside `adapter::pushdown`, so it sees items at their declared visibility:
//! narrowing any item below `pub(crate)` fails compilation. The list holds 27 items; the count
//! makes a removal visible in review, since a deleted item still compiles. Changing the set or
//! count requires a spec delta against `vs-adapter/pushdown-module-structure`.
#![allow(unused_imports)]

use crate::adapter::pushdown::{
    AggregateMergeInputs, ConnectionStorage, DetectedJoin, FormatReader, GroupedAggregateDetection,
    GroupedSelectItem, IneligibleJoinReason, JoinLeaf, JoinShape, JoinSides, RefusedColumn,
    RenderedJoinPushdown, ResolvedJoinSide, ResolvedScan, ScanSource, build_fan_out_inner,
    build_grouped_aggregate_scan_sql, build_logical_schema, build_scan_driving_sql,
    detect_aggregates, detect_group_by_aggregates, detect_join, format_reader, handle_pushdown,
    render_broadcast_join, shard_count, validate_agg_col_types,
};
