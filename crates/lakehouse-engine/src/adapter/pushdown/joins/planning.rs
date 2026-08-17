use crate::scan::spec::{FileEntry, LogicalField, NameMappingEntry, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::super::ResolvedScan;
use super::super::scan_resolution::TableScanResolver;
use super::super::support::{column_types, extract_limit, extract_offset};
use super::super::topn::{ParsedSortKey, parse_sort_key_element};

/// Why a join `from` clause cannot be rendered by the join path at all.
///
/// The unified join path serves EVERY inner join of any arity (broadcast or the
/// N-scan fallback), so an `Ineligible` shape is the genuine last resort — a shape
/// the adapter cannot render, routed to a hard client-facing error. Each variant
/// names the specific reason so a caller can log or test it; every variant carries
/// no data because the shape check alone explains the decline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IneligibleJoinReason {
    /// A join node ANYWHERE in the tree has `join_type` other than `"inner"` (e.g.
    /// an outer join); a cross-join + conjunctive WHERE cannot reproduce its
    /// semantics.
    NotInnerJoinType,
    /// A join node is missing a `left`/`right`/`condition` field, or a leaf is
    /// neither a `join` nor a `table` node — a shape the planner does not recognize.
    UnsupportedShape,
}

/// One base-table leaf of a detected inner-join tree, with its original-cased
/// catalog identifier already recovered from `TABLE_MAP`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinLeaf {
    /// The Exasol virtual table name (a `from`-tree leaf's `name`).
    pub table_name: String,
    /// `table_name`'s original-cased catalog identifier, from `TABLE_MAP`.
    pub table_identifier: String,
}

/// A detected all-inner join tree over N ≥ 2 involved tables — the single unified
/// join shape (the two-involved-table case is simply N = 2).
///
/// `tables` are the base-table leaves in stable left-to-right tree order; every
/// leaf's catalog identifier is resolved from `TABLE_MAP` at detection time (a
/// missing leaf is a hard `Err`, not a value here). `conditions` are the N-1
/// join-node `condition` expressions collected while walking the tree —
/// AND-conjoined by the N-scan fallback, which is order-agnostic for an all-inner
/// join.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DetectedJoin {
    /// The N ≥ 2 base-table leaves in stable left-to-right tree order.
    pub tables: Vec<JoinLeaf>,
    /// The N-1 raw join-node `condition` expressions, unrendered.
    pub conditions: Vec<Json>,
}

/// The result of inspecting a pushdown request's `from` clause for the inner
/// equi-join shape this phase plans.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JoinShape {
    /// The `from` clause is a plain table reference (or absent) — today's
    /// single-table pushdown path applies unchanged.
    NotAJoin,
    /// The `from` clause is a join the adapter cannot render at all (a non-inner
    /// join node in the tree, or a malformed shape). Routed to a hard error — the
    /// genuine last resort, never a native re-plan.
    Ineligible(IneligibleJoinReason),
    /// An all-inner join tree spanning N ≥ 2 involved tables, every leaf's catalog
    /// identifier resolved from `TABLE_MAP`. Served by the SINGLE unified join path
    /// ([`plan_join`]): broadcast when the two-table (N = 2) case is eligible,
    /// otherwise the N-scan unaccelerated fallback. The two-table case is simply
    /// N = 2 — there is no separate two-table shape.
    Join(DetectedJoin),
}

/// Recursively collect a join tree's base-table leaf names (into `tables`, stable
/// left-to-right order) and every join node's `condition` (into `conditions`,
/// post-order).
///
/// Returns the specific [`IneligibleJoinReason`] on the first non-inner join node
/// ([`IneligibleJoinReason::NotInnerJoinType`]), a join node missing a
/// `left`/`right`/`condition` field or a leaf missing its `name`, or a leaf that is
/// neither a `join` nor a `table` node ([`IneligibleJoinReason::UnsupportedShape`]).
fn collect_join_tree(
    node: &Json,
    tables: &mut Vec<String>,
    conditions: &mut Vec<Json>,
) -> Result<(), IneligibleJoinReason> {
    match node.get("type").and_then(|t| t.as_str()) {
        Some("join") => {
            let is_inner = node
                .get("join_type")
                .and_then(|t| t.as_str())
                .is_some_and(|t| t.eq_ignore_ascii_case("inner"));
            if !is_inner {
                return Err(IneligibleJoinReason::NotInnerJoinType);
            }
            let (left, right) = match (node.get("left"), node.get("right")) {
                (Some(left), Some(right)) => (left, right),
                _ => return Err(IneligibleJoinReason::UnsupportedShape),
            };
            let condition = match node.get("condition").filter(|c| !c.is_null()) {
                Some(condition) => condition.clone(),
                None => return Err(IneligibleJoinReason::UnsupportedShape),
            };
            collect_join_tree(left, tables, conditions)?;
            collect_join_tree(right, tables, conditions)?;
            conditions.push(condition);
            Ok(())
        }
        Some("table") => match node.get("name").and_then(|n| n.as_str()) {
            Some(name) => {
                tables.push(name.to_string());
                Ok(())
            }
            None => Err(IneligibleJoinReason::UnsupportedShape),
        },
        _ => Err(IneligibleJoinReason::UnsupportedShape),
    }
}

/// Detect whether a pushdown request's `from` clause is an inner-join tree the
/// unified join path serves, over N ≥ 2 involved tables.
///
/// Per the Exasol virtual-schema-common-java pushdown JSON shape, a join `from`
/// node looks like:
/// ```json
/// {"type": "join", "join_type": "inner", "left": {...}, "right": {...}, "condition": {...}}
/// ```
/// where `left`/`right` are each a base-table reference (`{"name": ..., "type": "table"}`)
/// or a nested `join` node. The whole tree is walked ONCE by [`collect_join_tree`]:
/// it collects the base-table leaves (stable left-to-right order) and every join
/// node's `condition`, asserting every join node is `join_type = "inner"`. The
/// two-involved-table case is simply N = 2 — there is no separate two-table shape,
/// and no equi-condition gate here (broadcast eligibility, computed later in
/// [`plan_join`], is what requires an equi condition; the N-scan fallback renders
/// any inner-join condition into its WHERE).
///
/// A request whose `from` clause is absent or a plain table reference is
/// [`JoinShape::NotAJoin`]: today's single-table pushdown path, unaffected.
///
/// A non-inner join node or a malformed node is [`JoinShape::Ineligible`] (a hard
/// error, the genuine last resort). Once the tree is a valid all-inner join, every
/// involved table's original-cased catalog identifier MUST be recoverable from
/// `TABLE_MAP` — a virtual table absent from `TABLE_MAP` is the same "stale virtual
/// schema" condition the single-table path reports, so it is a hard `Err`, not a
/// decline.
pub(crate) fn detect_join(request: &Json, pushdown_req: &Json) -> Result<JoinShape, UdfError> {
    let from = match pushdown_req.get("from") {
        Some(from) => from,
        None => return Ok(JoinShape::NotAJoin),
    };
    if from.get("type").and_then(|t| t.as_str()) != Some("join") {
        return Ok(JoinShape::NotAJoin);
    }

    let mut table_names = Vec::new();
    let mut conditions = Vec::new();
    if let Err(reason) = collect_join_tree(from, &mut table_names, &mut conditions) {
        return Ok(JoinShape::Ineligible(reason));
    }

    let table_map = crate::adapter::read_table_map(request);
    let mut tables = Vec::with_capacity(table_names.len());
    for table_name in table_names {
        let table_identifier = table_map.get(&table_name).cloned().ok_or_else(|| {
            UdfError::User(format!(
                "pushdown: virtual table '{table_name}' is not in TABLE_MAP; \
                 drop and recreate the virtual schema"
            ))
        })?;
        tables.push(JoinLeaf {
            table_name,
            table_identifier,
        });
    }

    Ok(JoinShape::Join(DetectedJoin { tables, conditions }))
}

/// One fully-resolved side of a two-table inner equi-join.
///
/// Every field is resolved ONCE per query in the VS planning layer, through the
/// same `TableScanResolver` seam the single-table scan uses — never per shard
/// and never per node (mission.md "resolve metadata once per query"). `total_bytes`
/// is the sum of every file's `file_size_in_bytes` (the catalog-manifest byte
/// size, NO Parquet read), the quantity the broadcast threshold is evaluated
/// against.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedJoinSide {
    /// The Exasol virtual table name (a detected join leaf).
    pub table_name: String,
    /// The original-cased catalog identifier this side was resolved from.
    pub table_identifier: String,
    /// The table's storage root; empty ⇒ every `files` path is absolute.
    pub table_root: String,
    /// This side's FULL file list as [`FileEntry`] values (path,
    /// `file_size_in_bytes`, and any associated positional-delete files). Deletes
    /// are resolved once here — the same resolver seam the single-table scan
    /// uses — and travel with the side so the scan applies them per side.
    pub files: Vec<FileEntry>,
    /// Full logical schema of this side's table at query time.
    pub logical_schema: Vec<LogicalField>,
    /// This side's flattened Iceberg `schema.name-mapping.default` entries
    /// (empty when the table has no name-mapping property, and on every Delta
    /// side). Resolved ONCE per query alongside `logical_schema`.
    pub name_mapping: Vec<NameMappingEntry>,
    /// Effective storage for this side (vended STS creds when applicable).
    pub effective_storage: StorageBackend,
    /// This side's ordered partition-column names — the same neutral concept as
    /// [`crate::scan::spec::CommonScanSpec::partition_columns`]. Empty on every
    /// Iceberg side.
    pub partition_columns: Vec<String>,
    /// Sum of every file's `file_size_in_bytes` — the broadcast-threshold metric.
    pub total_bytes: u64,
}

impl ResolvedJoinSide {
    /// Assemble a resolved side, computing `total_bytes` from the file list with a
    /// saturating sum (a byte total that overflows `u64` is clamped to `u64::MAX`,
    /// which is correctly treated as "far over any broadcast threshold").
    pub(super) fn new(
        table_name: String,
        table_identifier: String,
        resolved: ResolvedScan,
    ) -> Self {
        let ResolvedScan {
            files,
            effective_storage,
            logical_schema,
            table_root,
            name_mapping,
            partition_columns,
        } = resolved;
        let total_bytes = files
            .iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.size));
        Self {
            table_name,
            table_identifier,
            table_root,
            files,
            logical_schema,
            name_mapping,
            effective_storage,
            partition_columns,
            total_bytes,
        }
    }
}

/// The outcome of resolving BOTH sides of an eligible inner equi-join once and
/// deciding broadcast eligibility from Iceberg-manifest byte sizes.
///
/// Both sides are always carried fully resolved: the broadcast path shards `fact`
/// and replicates `dimension`; the unaccelerated fallback scans BOTH sides through
/// their own fan-outs, so it needs both here too. The only role of
/// `broadcast_eligible` is to route between those two SQL builders — it is NEVER an
/// error: an ineligible join takes the deterministic N-scan fallback, not a native
/// re-plan.
///
/// # Edge cases
///
/// - **Self-join** (both sides the same Iceberg table): resolved and sized like
///   any other pair — both sides carry identical file lists and equal byte totals,
///   so the tie-break makes the LEFT side the dimension. Broadcasting a table
///   against itself is a *correct* inner join (every fact-shard row is matched
///   against the full table). No special case is needed here; the disjoint-
///   column-name guard independently declines a self-join to the unaccelerated
///   path because its two sides share every column name.
/// - **Empty side** (either side resolves to zero files): its `total_bytes` is 0,
///   so an empty side is always the (trivially broadcast-eligible) dimension. An
///   inner join with an empty side yields zero rows either way; the caller may
///   short-circuit to an empty result by testing `fact.files.is_empty() ||
///   dimension.files.is_empty()`. Selection deliberately does not special-case it
///   — sizing and role assignment stay total and deterministic.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinSides {
    /// The LARGER side by total bytes — sharded across the cluster exactly like
    /// the single-table scan path.
    pub fact: ResolvedJoinSide,
    /// The SMALLER side by total bytes — the broadcast/dimension candidate.
    pub dimension: ResolvedJoinSide,
    /// `true` when `dimension.total_bytes <= join_broadcast_max_bytes`: plan a
    /// broadcast join. `false`: the smaller side is still too big to replicate to
    /// every shard, so the caller builds the unaccelerated two-scan fallback SQL.
    pub broadcast_eligible: bool,
}

/// Choose the fact (sharded) and dimension (broadcast) roles from two resolved
/// sides and gate broadcast eligibility on the dimension's byte size.
///
/// The SMALLER side by total Iceberg-manifest bytes is the dimension; the larger
/// is the fact. On an exact byte-size tie the first argument (`a`) becomes the
/// dimension — deterministic and arbitrary, since equal-sized candidates are
/// interchangeable. The join is broadcast-eligible iff the chosen dimension's
/// total bytes are at or below `join_broadcast_max_bytes`.
///
/// This is the pure, catalog-free core of side selection so it is unit-testable
/// without a live Iceberg catalog; [`plan_join`] resolves each side and delegates
/// here for the two-table broadcast role/threshold decision.
pub(super) fn select_broadcast_sides(
    a: ResolvedJoinSide,
    b: ResolvedJoinSide,
    join_broadcast_max_bytes: u64,
) -> JoinSides {
    let (dimension, fact) = if a.total_bytes <= b.total_bytes {
        (a, b)
    } else {
        (b, a)
    };
    let broadcast_eligible = dimension.total_bytes <= join_broadcast_max_bytes;
    JoinSides {
        fact,
        dimension,
        broadcast_eligible,
    }
}

/// Resolve ONE join side's file list, logical schema, table root, effective
/// storage, and partition columns, through the SAME per-request
/// `TableScanResolver` the single-table scan uses.
///
/// `table_identifier` (the original-cased identifier recovered from `TABLE_MAP`)
/// is this side's table identifier; both sides resolve through the same
/// `resolver`, which is built once per request, so a two-leg join performs no
/// more catalog authentication round-trips than a single-table scan.
///
/// `filter_json` is this side's SIDE-LOCAL sub-predicate (see [`side_local_filter`])
/// — the conjuncts of the WHERE every column of which is this table's — forwarded
/// for format-level file pruning exactly as `filter_json_raw` is on the single-table
/// path. For an inner join a side-local conjunct is a necessary condition for that
/// side's rows to survive, so pruning by it is sound; cross-table and OR-spanning
/// conjuncts are already excluded from `filter_json`. `None` (no side-local
/// conjunct) prunes nothing — every file is kept.
pub(super) async fn resolve_one_join_side(
    table_name: &str,
    table_identifier: &str,
    resolver: &TableScanResolver<'_>,
    filter_json: Option<&Json>,
) -> Result<ResolvedJoinSide, UdfError> {
    let resolved = resolver.resolve(table_identifier, filter_json).await?;
    Ok(ResolvedJoinSide::new(
        table_name.to_string(),
        table_identifier.to_string(),
        resolved,
    ))
}

/// The (folded name, Exasol type) columns of the named involved table.
///
/// Locates the `involvedTables[]` entry whose `name` equals `table_name` (the
/// Exasol virtual table name carried in a [`JoinLeaf`]) and maps its columns to
/// `support::column_types`' folded names plus Exasol types from `dataType`.
/// Returns an empty vec when the table or its columns are absent.
///
/// A partial application of `support::column_types`, supplying the find-by-name
/// selection.
///
/// CROSS-FOLD SEAM: this output travels into `referenced_side_columns`
/// (`joins/rendering.rs`) as `full_cols`, where it is string-matched against the name
/// set `collect_side_column_names` builds with the ASCII-only `to_ascii_uppercase`.
/// The two folds are different BY DESIGN and MUST NOT be reconciled by changing
/// either one: `column_types` owns this side's fold, and unifying the collect walks'
/// is forbidden by `walk_column_nodes`' doc comment and by
/// `vs-adapter/pushdown-module-structure`'s "One blind traversal primitive backs every
/// column-collecting walk" scenario. The two sides agree not by construction but by
/// premise — `build_listing_virtual_tables` (`adapter/mod.rs`) Unicode-uppercases every name it declares, so no
/// LOWERCASE name reaches either side. Non-ASCII letters can still reach both sides
/// (e.g. `über` uppercases to `ÜBER`, not to an ASCII form); the folds still agree
/// there because `to_ascii_uppercase` only touches ASCII `a`-`z`, none of which
/// remain once a name is already Unicode-uppercased. The E2E test
/// `non_ascii_table_and_column_stay_queryable` guards that premise.
pub(super) fn involved_table_columns(request: &Json, table_name: &str) -> Vec<(String, String)> {
    column_types(request, |tables: &[Json]| {
        tables
            .iter()
            .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(table_name))
    })
}

/// The disjoint-column-name guard for reusing the `vs-expression` translator
/// unchanged on a two-table join.
///
/// Returns `true` when no column NAME appears on both sides. Only then do bare,
/// non-table-qualified column references (which is all the translator renders —
/// see `render_expression`) resolve unambiguously against the COMBINED DataFusion
/// schema of both registered tables. A single shared name makes a bare reference
/// ambiguous, so the join is NOT eligible for translator-reuse rendering; the
/// caller declines to the unaccelerated two-scan path (this is a clean decline,
/// never an error). Comparison is by name only — a name collision breaks
/// resolution regardless of the columns' types. Both inputs already carry
/// uppercased names, so the check is exact.
pub(super) fn disjoint_schema_guard(left: &[(String, String)], right: &[(String, String)]) -> bool {
    let left_names: std::collections::HashSet<&str> =
        left.iter().map(|(n, _)| n.as_str()).collect();
    !right.iter().any(|(n, _)| left_names.contains(n.as_str()))
}

/// Whether a join pushdown request carries an aggregation Exasol must execute over
/// the materialized two-scan join: an aggregate select item, a GROUP BY, a group-by
/// aggregation, or a HAVING. The broadcast in-UDF join renders only projection,
/// filter, and join condition, so none of these can ride along with it.
fn carries_aggregation_clause(pushdown_req: &Json) -> bool {
    let has_aggregate_item = pushdown_req
        .get("selectList")
        .and_then(|v| v.as_array())
        .is_some_and(|list| {
            list.iter()
                .any(|item| item.get("type").and_then(|t| t.as_str()) == Some("function_aggregate"))
        });
    let has_group_by = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty());
    let is_group_by_aggregation =
        pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by");
    let has_having = pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .is_some();
    has_aggregate_item || has_group_by || is_group_by_aggregation || has_having
}

/// What a join pushdown request's window clauses oblige the broadcast path to
/// render, and the single decision on whether that path may be taken at all.
///
/// [`Self::ExasolPostProcessed`] is a fall-through to the qualified two-scan
/// fallback, which renders every one of these clauses as ordinary Exasol SQL over
/// the materialized join — never an error.
#[derive(Debug)]
pub(in super::super) enum JoinWindowPlan {
    /// No `limit` and no `orderBy`: the broadcast fan-out is the whole answer.
    Unbounded,
    /// A `limit` with no ordering, so the cap composes per shard: each shard may
    /// truncate its own joined output at `n` and the merge truncate again at `n`.
    BareLimit(u64),
    /// A bare-column ordering, served by an outer wrapper over the merged fan-out.
    /// The window rides on that wrapper, never per shard: a per-shard `OFFSET`
    /// would skip each shard's OWN first rows.
    Ordered {
        keys: Vec<ParsedSortKey>,
        limit: Option<u64>,
        offset: u64,
    },
    /// Exasol executes the request's remaining work over the two-scan join.
    ExasolPostProcessed,
}

/// Classify what the broadcast join path would have to render for `pushdown_req`,
/// from the REQUEST alone.
///
/// Whether the rendered projection can bind an `Ordered` key is deliberately NOT
/// decided here: no projection exists yet at classification time, and rendering one
/// first would reverse `plan_join`'s short-circuit — an aggregate-carrying join
/// would then reach a render that can hard-`Err` on absent column metadata. The
/// construction site owns that one downgrade instead.
pub(super) fn classify_join_window(pushdown_req: &Json) -> JoinWindowPlan {
    if carries_aggregation_clause(pushdown_req) {
        return JoinWindowPlan::ExasolPostProcessed;
    }
    let limit = extract_limit(pushdown_req);
    let offset = extract_offset(pushdown_req);
    let Some(order_by) = pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|elements| !elements.is_empty())
    else {
        // Without an ordering there is no wrapper to carry an OFFSET — Exasol's
        // grammar rejects one without an ORDER BY, and a per-shard offset does not
        // compose — so only a bare cap survives here.
        if offset != 0 {
            return JoinWindowPlan::ExasolPostProcessed;
        }
        return match limit {
            Some(n) => JoinWindowPlan::BareLimit(n),
            None => JoinWindowPlan::Unbounded,
        };
    };

    let mut keys = Vec::with_capacity(order_by.len());
    for element in order_by {
        // The wrapper binds its ORDER BY against the fan-out's emitted columns and
        // this path appends no hidden ones, so only a flagged bare column is
        // servable; an expression, an aggregate, or a missing direction / NULL
        // placement flag falls back rather than guessing an order.
        let Some(key) = parse_sort_key_element(element) else {
            return JoinWindowPlan::ExasolPostProcessed;
        };
        keys.push(ParsedSortKey::Column(key));
    }
    JoinWindowPlan::Ordered {
        keys,
        limit,
        offset,
    }
}

#[cfg(test)]
#[path = "planning_tests.rs"]
mod tests;
