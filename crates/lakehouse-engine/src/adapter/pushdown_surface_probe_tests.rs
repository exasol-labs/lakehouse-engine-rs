//! Compile-time reachability probe for the `pushdown` module's public surface.
//!
//! This is a pure `use` list with no behavior. It exists at a vantage point
//! OUTSIDE `adapter::pushdown` (a sibling file in `adapter/`) so that it only
//! sees items at their declared visibility, not the elevated visibility that
//! `pushdown`'s own `mod tests` enjoys as a descendant module.
//!
//! The 21-item `use` list below IS the façade's frozen baseline. There is no
//! separate baseline file to consult, so there is nothing for the surface
//! contract to drift from. Narrowing any of those items below `pub(crate)`
//! fails this file's compilation, turning an effective visibility regression
//! into a build failure rather than a silent gap that only a `pub use` text
//! diff would miss. The count is stated because the compiler catches only
//! narrowing — a deleted item leaves the list compiling — so the count is what
//! makes a removal visible in review. Changing the set or the count requires a
//! spec delta against `vs-adapter/pushdown-module-structure`.
#![allow(unused_imports)]

use crate::adapter::pushdown::{
    DetectedJoin, GroupedAggregateDetection, GroupedSelectItem, IneligibleJoinReason, JoinLeaf,
    JoinShape, JoinSides, RenderedJoinPushdown, ResolvedJoinSide, build_fan_out_inner,
    build_grouped_aggregate_scan_sql, build_logical_schema, build_scan_driving_sql,
    detect_aggregates, detect_group_by_aggregates, detect_join, handle_pushdown,
    render_broadcast_join, resolve_file_list, shard_count, validate_agg_col_types,
};
