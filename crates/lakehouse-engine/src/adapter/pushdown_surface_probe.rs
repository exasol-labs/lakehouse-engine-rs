//! Compile-time reachability probe for the `pushdown` module's public surface.
//!
//! This is a pure `use` list with no behavior. It exists at a vantage point
//! OUTSIDE `adapter::pushdown` (a sibling file in `adapter/`) so that it only
//! sees items at their declared visibility, not the elevated visibility that
//! `pushdown`'s own `mod tests` enjoys as a descendant module. If any of the 25
//! items in the public-surface baseline
//! (`specs/_plans/refactor-adapter-pushdown-modules/public-surface-baseline.txt`)
//! is narrowed below `pub(crate)`, this file fails to compile — turning an
//! effective visibility regression into a build failure rather than a silent
//! gap that only a `pub use` text diff would miss.
#![allow(unused_imports)]

use crate::adapter::pushdown::{
    DetectedJoin, GroupedAggregateDetection, GroupedSelectItem, IneligibleJoinReason, JoinLeaf,
    JoinShape, JoinSides, RenderedJoinPushdown, ResolvedJoinSide, build_fan_out_inner,
    build_grouped_aggregate_scan_sql, build_logical_schema, build_scan_driving_sql,
    detect_aggregates, detect_group_by_aggregates, detect_join, extract_vended_keys,
    handle_pushdown, list_namespace_tables, merge_vended_into_storage, render_broadcast_join,
    resolve_file_list, resolve_table_schema, shard_count, validate_agg_col_types,
};
