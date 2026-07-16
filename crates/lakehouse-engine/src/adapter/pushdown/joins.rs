use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{
    CatalogProps, FileEntry, JoinSpec, JoinType, LogicalField, NameMappingEntry, ProjectionItem,
    ScanSpec, StorageProps,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use std::collections::HashMap;
use vs_expression::{render_df_filter_safe, render_expression_safe};

use super::file_resolution::{empty_result_sql, relativize_shards_to_root, resolve_file_list};
use super::support::{
    DISTINCT_MERGE_UDF_NAME, DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, build_scan_driving_sql,
    exasol_type_from_json, extract_limit, order_by_present, project_columns, quote_ident,
    shard_count,
};
use super::topn::parse_sort_key_element;

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
/// Iceberg identifier already recovered from `TABLE_MAP`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinLeaf {
    /// The Exasol virtual table name (a `from`-tree leaf's `name`).
    pub table_name: String,
    /// `table_name`'s original-cased Iceberg identifier, from `TABLE_MAP`.
    pub iceberg_ident: String,
}

/// A detected all-inner join tree over N ≥ 2 involved tables — the single unified
/// join shape (the two-involved-table case is simply N = 2).
///
/// `tables` are the base-table leaves in stable left-to-right tree order; every
/// leaf's Iceberg identifier is resolved from `TABLE_MAP` at detection time (a
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
    /// An all-inner join tree spanning N ≥ 2 involved tables, every leaf's Iceberg
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
/// involved table's original-cased Iceberg identifier MUST be recoverable from
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
        let iceberg_ident = table_map.get(&table_name).cloned().ok_or_else(|| {
            UdfError::User(format!(
                "pushdown: virtual table '{table_name}' is not in TABLE_MAP; \
                 drop and recreate the virtual schema"
            ))
        })?;
        tables.push(JoinLeaf {
            table_name,
            iceberg_ident,
        });
    }

    Ok(JoinShape::Join(DetectedJoin { tables, conditions }))
}

/// One fully-resolved side of a two-table inner equi-join.
///
/// Every field is resolved ONCE per query in the VS planning layer from Iceberg
/// manifest metadata — the same `resolve_file_list` path the single-table scan
/// uses — never per shard and never per node (mission.md "resolve metadata once
/// per query"). `total_bytes` is the sum of every file's `file_size_in_bytes`
/// (the Iceberg-manifest byte size, NO Parquet read), the quantity the broadcast
/// threshold is evaluated against.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedJoinSide {
    /// The Exasol virtual table name (a detected join leaf).
    pub table_name: String,
    /// The original-cased Iceberg identifier this side was resolved from.
    pub iceberg_ident: String,
    /// The Iceberg table root (`table.metadata().location()`); empty ⇒ every
    /// `files` path is absolute.
    pub table_root: String,
    /// This side's FULL file list as [`FileEntry`] values (path,
    /// `file_size_in_bytes`, and any associated positional-delete files). Deletes
    /// are resolved once here — the same `resolve_file_list` path the single-table
    /// scan uses — and travel with the side so the scan applies them per side.
    pub files: Vec<FileEntry>,
    /// Full logical schema of this side's Iceberg table at query time.
    pub logical_schema: Vec<LogicalField>,
    /// This side's flattened Iceberg `schema.name-mapping.default` entries
    /// (empty when the table has no name-mapping property). Resolved ONCE per
    /// query on the same `resolve_file_list` path as `logical_schema`.
    pub name_mapping: Vec<NameMappingEntry>,
    /// Effective storage for this side (vended STS creds when applicable).
    pub effective_storage: StorageProps,
    /// Sum of every file's `file_size_in_bytes` — the broadcast-threshold metric.
    pub total_bytes: u64,
}

impl ResolvedJoinSide {
    /// Assemble a resolved side, computing `total_bytes` from the file list with a
    /// saturating sum (a byte total that overflows `u64` is clamped to `u64::MAX`,
    /// which is correctly treated as "far over any broadcast threshold").
    fn new(
        table_name: String,
        iceberg_ident: String,
        table_root: String,
        files: Vec<FileEntry>,
        logical_schema: Vec<LogicalField>,
        name_mapping: Vec<NameMappingEntry>,
        effective_storage: StorageProps,
    ) -> Self {
        let total_bytes = files
            .iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.size));
        Self {
            table_name,
            iceberg_ident,
            table_root,
            files,
            logical_schema,
            name_mapping,
            effective_storage,
            total_bytes,
        }
    }
}

/// The outcome of resolving BOTH sides of an eligible inner equi-join once and
/// deciding broadcast eligibility from Iceberg-manifest byte sizes.
///
/// Both sides are always carried fully resolved: the broadcast path (task 3.4)
/// shards `fact` and replicates `dimension`; the unaccelerated fallback (task 3.5)
/// scans BOTH sides through their own fan-outs, so it needs both here too. The
/// only role of `broadcast_eligible` is to route between those two SQL builders —
/// it is NEVER an error (decision-log [2]: an ineligible join takes the
/// deterministic N-scan fallback, not a native re-plan).
///
/// # Edge cases (decision-log has no explicit ruling; choices made here)
///
/// - **Self-join** (both sides the same Iceberg table): resolved and sized like
///   any other pair — both sides carry identical file lists and equal byte totals,
///   so the tie-break makes the LEFT side the dimension. Broadcasting a table
///   against itself is a *correct* inner join (every fact-shard row is matched
///   against the full table). No special case is needed here; the disjoint-
///   column-name guard (task 3.3) independently declines a self-join to the
///   unaccelerated path because its two sides share every column name.
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
fn select_broadcast_sides(
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

/// Resolve ONE join side's file list, logical schema, table root, and effective
/// storage from the Iceberg catalog, reusing the single-table `resolve_file_list`
/// path unchanged.
///
/// `iceberg_ident` (the original-cased identifier recovered from `TABLE_MAP`)
/// replaces only the `table` field of the shared `catalog` template, so both
/// sides resolve against the same catalog URI and warehouse.
///
/// `filter_json` is this side's SIDE-LOCAL sub-predicate (see [`side_local_filter`])
/// — the conjuncts of the WHERE every column of which is this table's — forwarded
/// for Iceberg manifest pruning exactly as `filter_json_raw` is on the single-table
/// path. For an inner join a side-local conjunct is a necessary condition for that
/// side's rows to survive, so pruning by it is sound; cross-table and OR-spanning
/// conjuncts are already excluded from `filter_json`. `to_iceberg_predicate`
/// resolves it against this table's OWN schema, and sound-drops anything it cannot
/// translate. `None` (no side-local conjunct) prunes nothing — every file is kept.
async fn resolve_one_join_side(
    table_name: &str,
    iceberg_ident: &str,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
    filter_json: Option<&Json>,
) -> Result<ResolvedJoinSide, UdfError> {
    let side_catalog = CatalogProps {
        table: iceberg_ident.to_string(),
        ..catalog.clone()
    };
    let (files, effective_storage, logical_schema, table_root, name_mapping) =
        resolve_file_list(catalog_uri, &side_catalog, storage, creds, filter_json).await?;
    Ok(ResolvedJoinSide::new(
        table_name.to_string(),
        iceberg_ident.to_string(),
        table_root,
        files,
        logical_schema,
        name_mapping,
        effective_storage,
    ))
}

/// The `(UPPERCASE name, Exasol type)` columns of the named involved table.
///
/// Locates the `involvedTables[]` entry whose `name` equals `table_name` (the
/// Exasol virtual table name carried in a [`JoinLeaf`]) and maps its columns
/// exactly as the single-table projection does — uppercased names, Exasol types
/// from `dataType`. Returns an empty vec when the table or its columns are absent.
fn involved_table_columns(request: &Json, table_name: &str) -> Vec<(String, String)> {
    request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|tables| {
            tables
                .iter()
                .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(table_name))
        })
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .map(|cols| {
            cols.iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?.to_uppercase();
                    let dt_json = c.get("dataType")?;
                    Some((name, exasol_type_from_json(dt_json)))
                })
                .collect()
        })
        .unwrap_or_default()
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
fn disjoint_schema_guard(left: &[(String, String)], right: &[(String, String)]) -> bool {
    let left_names: std::collections::HashSet<&str> =
        left.iter().map(|(n, _)| n.as_str()).collect();
    !right.iter().any(|(n, _)| left_names.contains(n.as_str()))
}

/// Render a join's equi-condition node to a DataFusion SQL boolean expression via
/// the `vs-expression` translator (bare column names). `None` when the node cannot
/// be rendered — a defensive decline, since [`plan_join`] only reaches the broadcast
/// path for a `predicate_equal` condition. Uses `render_expression` (not the filter
/// renderer) so the boolean expression is returned verbatim, never suppressed as
/// trivially true.
fn render_join_condition(condition: &Json) -> Option<String> {
    render_expression_safe(condition)
}

/// The cross-table projection and Exasol EMITS types for a broadcast join.
///
/// Reuses [`project_columns`] against the disjoint union of both involved tables'
/// columns, so a projected column spanning either side is typed from whichever
/// side owns it. The caller must have already passed the [`disjoint_schema_guard`]
/// so the union carries no name collision. Broadcast is a two-table optimization,
/// so `join.tables[0]`/`[1]` are the two involved tables.
fn extract_join_projection(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
) -> Result<(Vec<ProjectionItem>, Vec<String>), UdfError> {
    let mut combined = involved_table_columns(request, &join.tables[0].table_name);
    combined.extend(involved_table_columns(request, &join.tables[1].table_name));
    project_columns(pushdown_req, combined)
}

/// The translator-reuse artifacts for a broadcast inner equi-join, rendered once
/// in the VS planning layer and consumed by the broadcast fan-out SQL builder.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedJoinPushdown {
    /// The rendered DataFusion SQL boolean join condition (→ [`JoinSpec::condition`]).
    pub condition: String,
    /// The rendered cross-table WHERE filter, or `None` when the request carries
    /// none (or it is trivially true and Exasol keeps it as a backstop).
    pub filter: Option<String>,
    /// The cross-table projection, spanning columns from both tables, in order.
    pub projection: Vec<ProjectionItem>,
    /// The Exasol EMITS type per projected column, positionally aligned with
    /// `projection`.
    pub projection_types: Vec<String>,
}

/// Render every `vs-expression` artifact a broadcast inner equi-join needs, after
/// enforcing the disjoint-column-name guard.
///
/// Broadcast is a two-table optimization, so `join.tables[0]`/`[1]` are the two
/// involved tables and `join.conditions[0]` is the equi-condition. Returns
/// `Ok(None)` — a clean decline, NOT an error — when the two tables share any
/// column name (the guard fails) or the equi-condition cannot be rendered; the
/// caller then falls through to the deterministic N-scan fallback, exactly as for
/// any other join off the broadcast path. `Ok(Some(..))` carries the rendered join
/// condition, the cross-table WHERE filter, and the cross-table projection with its
/// EMITS types. `Err` is reserved for a genuinely malformed request with no column
/// metadata at all (the same contract [`project_columns`] enforces for the
/// single-table path).
///
/// Rendering is side-agnostic: the translator emits bare column names, so the
/// result does not depend on which side is later selected as fact vs dimension.
pub(crate) fn render_broadcast_join(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
) -> Result<Option<RenderedJoinPushdown>, UdfError> {
    let left_cols = involved_table_columns(request, &join.tables[0].table_name);
    let right_cols = involved_table_columns(request, &join.tables[1].table_name);
    if !disjoint_schema_guard(&left_cols, &right_cols) {
        return Ok(None);
    }

    let condition = match render_join_condition(&join.conditions[0]) {
        Some(condition) => condition,
        None => return Ok(None),
    };

    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(render_df_filter_safe);

    let (projection, projection_types) = extract_join_projection(request, pushdown_req, join)?;

    Ok(Some(RenderedJoinPushdown {
        condition,
        filter,
        projection,
        projection_types,
    }))
}

/// Schema-qualify a UDF/script name for a pushdown-driving query.
///
/// The generated SQL runs outside the adapter script's own schema, so an
/// unqualified name would fail to resolve. Shared by the single-table path and the
/// join planner so both qualify identically.
pub(super) fn qualify_udf(scan_schema: Option<&str>, udf: &str) -> String {
    match scan_schema {
        Some(schema) if !schema.is_empty() => format!("{}.{}", quote_ident(schema), udf),
        _ => udf.to_string(),
    }
}

/// The `User` decline error for a join `from` clause the adapter cannot render at
/// all — the genuine last resort.
///
/// Spanning more than two tables, needing Exasol postprocessing, or overlapping
/// column names are NEVER reasons to reach here — every such inner join is served
/// by the unified fallback. Only a non-inner join node in the tree or a malformed
/// shape lands here, and falling through to the single-table path would scan only
/// the first involved table and silently drop the join. So the only safe outcome is
/// a `User` error — surfaced by the FFI shim as a hard `F-UDF-CL-RUST-9001` client
/// error with no native re-plan (`vs-adapter/pushdown-planning-join` "declined
/// safely", last resort).
pub(super) fn ineligible_join_decline(reason: IneligibleJoinReason) -> UdfError {
    let detail = match reason {
        IneligibleJoinReason::NotInnerJoinType => "the join is not an inner join",
        IneligibleJoinReason::UnsupportedShape => "the join `from` clause has an unsupported shape",
    };
    UdfError::User(format!(
        "join pushdown declined: {detail}; the adapter cannot render this join shape, \
         so this is a hard error, not a native re-plan"
    ))
}

/// Render one projection item as an outer-query SELECT expression: a bare column is
/// double-quoted, an already-rendered scalar expression is spliced verbatim.
fn projection_item_select_sql(item: &ProjectionItem) -> String {
    match item {
        ProjectionItem::Column(name) => quote_ident(name),
        ProjectionItem::Expr { expr } => expr.clone(),
    }
}

/// Deep-clone an expression node, tagging every `column` node with the subquery
/// alias its `tableName` maps to (`tableAlias`), so `vs-expression` renders it as a
/// table-qualified reference (`"ALIAS"."NAME"`).
///
/// This is the seam that makes the two-scan wrapper correct regardless of whether
/// the two joined tables share a column name: bare-name rendering (the broadcast
/// path) is ambiguous on a collision, but a table-qualified reference resolved
/// against each side's OWN fan-out subquery never is. A `column` whose `tableName`
/// is not in `alias_of` is left unqualified (it belongs to neither joined table —
/// which cannot happen for a well-formed two-table request).
fn annotate_columns_with_alias(expr: &Json, alias_of: &HashMap<String, String>) -> Json {
    match expr {
        Json::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len() + 1);
            for (key, value) in map {
                out.insert(key.clone(), annotate_columns_with_alias(value, alias_of));
            }
            if map.get("type").and_then(|t| t.as_str()) == Some("column")
                && let Some(alias) = map
                    .get("tableName")
                    .and_then(|t| t.as_str())
                    .and_then(|t| alias_of.get(&t.to_ascii_uppercase()))
            {
                out.insert("tableAlias".to_string(), Json::String(alias.clone()));
            }
            Json::Object(out)
        }
        Json::Array(items) => Json::Array(
            items
                .iter()
                .map(|item| annotate_columns_with_alias(item, alias_of))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Render an expression node to table-qualified DataFusion/Exasol SQL for the
/// two-scan wrapper: annotate each `column` with its side alias, then reuse the
/// `vs-expression` translator. `None` when the node cannot be rendered.
fn render_expression_qualified(expr: &Json, alias_of: &HashMap<String, String>) -> Option<String> {
    render_expression_safe(&annotate_columns_with_alias(expr, alias_of))
}

/// Render a WHERE filter to a table-qualified Exasol boolean expression for the
/// two-scan wrapper. `None` when the filter is absent-shaped, trivially true, or
/// unrenderable — mirroring the single-table `render_df_filter_safe` contract, so a
/// dropped predicate is Exasol's own backstop responsibility exactly as elsewhere.
fn render_df_filter_qualified(filter: &Json, alias_of: &HashMap<String, String>) -> Option<String> {
    render_df_filter_safe(&annotate_columns_with_alias(filter, alias_of))
}

/// Walk an expression tree, recording every `column` node's owning side: its
/// UPPERCASE `tableName` into `tables`, or `has_untagged` when a `column` carries
/// no `tableName`. `any_column` becomes true on the first `column` node seen.
///
/// `tableName` is the SAME attribution signal [`annotate_columns_with_alias`] uses,
/// so conjunct-to-side attribution is by table identity — never by column name,
/// which keeps the shared-column-name case (both tables carry an `ID`) correct.
fn collect_column_tables(
    expr: &Json,
    tables: &mut std::collections::HashSet<String>,
    has_untagged: &mut bool,
    any_column: &mut bool,
) {
    match expr {
        Json::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("column") {
                *any_column = true;
                match map.get("tableName").and_then(|t| t.as_str()) {
                    Some(tn) => {
                        tables.insert(tn.to_ascii_uppercase());
                    }
                    None => *has_untagged = true,
                }
            }
            for value in map.values() {
                collect_column_tables(value, tables, has_untagged, any_column);
            }
        }
        Json::Array(items) => items
            .iter()
            .for_each(|item| collect_column_tables(item, tables, has_untagged, any_column)),
        _ => {}
    }
}

/// The single side a conjunct is local to — `Some(UPPERCASE table name)` iff every
/// `column` node it references is tagged with that ONE `tableName`. `None` when the
/// conjunct spans two tables, carries an untagged column, or references no column at
/// all (a bare literal). Such a conjunct is withheld from BOTH sides' pruning; only
/// the outer wrapper's WHERE (which renders the full predicate) applies it.
///
/// Sound for an inner equi-join: a conjunct over one side alone is a necessary
/// condition for that side's rows to survive the join, so using it to prune that
/// side can never drop a row the join would have kept.
fn conjunct_single_side(conjunct: &Json) -> Option<String> {
    let mut tables = std::collections::HashSet::new();
    let mut has_untagged = false;
    let mut any_column = false;
    collect_column_tables(conjunct, &mut tables, &mut has_untagged, &mut any_column);
    if has_untagged || !any_column || tables.len() != 1 {
        return None;
    }
    tables.into_iter().next()
}

/// Flatten a top-level `predicate_and` chain into its individual conjuncts,
/// recursing through nested `predicate_and` nodes (AND is associative). A non-AND
/// node (including a top-level `predicate_or`) is a single opaque conjunct — an OR
/// is never split, so an OR spanning both tables stays withheld from both sides.
fn flatten_conjuncts<'a>(filter: &'a Json, out: &mut Vec<&'a Json>) {
    if filter.get("type").and_then(|t| t.as_str()) == Some("predicate_and")
        && let Some(exprs) = filter.get("expressions").and_then(|e| e.as_array())
    {
        for expr in exprs {
            flatten_conjuncts(expr, out);
        }
        return;
    }
    out.push(filter);
}

/// The side-local sub-predicate of `filter` for `table_name`: the AND of exactly
/// those top-level conjuncts every column of which is attributed to `table_name`.
/// `None` when no conjunct is side-local to it.
///
/// This is what is threaded into (a) that side's `resolve_file_list` for Iceberg
/// manifest pruning and (b) that side's fan-out `ScanSpec.filter` for DataFusion
/// row-group pruning + row filtering. Cross-table conjuncts and OR-spanning
/// conjuncts are withheld here and applied only by the outer wrapper's WHERE.
fn side_local_filter(filter: &Json, table_name: &str) -> Option<Json> {
    let target = table_name.to_ascii_uppercase();
    let mut conjuncts = Vec::new();
    flatten_conjuncts(filter, &mut conjuncts);
    let mut kept: Vec<Json> = conjuncts
        .into_iter()
        .filter(|c| conjunct_single_side(c).as_deref() == Some(target.as_str()))
        .cloned()
        .collect();
    match kept.len() {
        0 => None,
        1 => kept.pop(),
        _ => Some(serde_json::json!({
            "type": "predicate_and",
            "expressions": kept,
        })),
    }
}

/// The cross-side residual sub-predicate of `filter`: the AND of exactly those
/// top-level conjuncts that are NOT side-local to a single table — i.e. cross-table,
/// OR-spanning, untagged, or column-free conjuncts (`conjunct_single_side` is
/// `None`). `None` when every conjunct is side-local.
///
/// This is the exact set-complement of the per-side [`side_local_filter`] slices:
/// every conjunct is either side-local to exactly one table (pushed into that side's
/// fan-out leg) or cross-side residual (kept here, in the outer wrapper's WHERE), so
/// the partition is total and disjoint — no conjunct is dropped or double-applied
/// (decision-log [7]).
fn cross_side_residual_filter(filter: &Json) -> Option<Json> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(filter, &mut conjuncts);
    let mut kept: Vec<Json> = conjuncts
        .into_iter()
        .filter(|c| conjunct_single_side(c).is_none())
        .cloned()
        .collect();
    match kept.len() {
        0 => None,
        1 => kept.pop(),
        _ => Some(serde_json::json!({
            "type": "predicate_and",
            "expressions": kept,
        })),
    }
}

/// Deep-clone `expr` with every `tableAlias` key removed, so the reused
/// `vs-expression` translator renders BARE column names.
///
/// Exasol sends each column node with BOTH its `tableName` and the query's
/// `tableAlias` (e.g. `FROM fact_orders o` yields `tableAlias: "O"`), and the
/// translator emits `"ALIAS"."NAME"` whenever `tableAlias` is present. A single-table
/// fan-out ([`build_side_fan_out_sql`]) scans one relation exposing BARE uppercase
/// column names, so an alias-qualified reference would not resolve against it — the
/// fan-out's pushed filter must be bare, exactly like the single-table scan path.
/// `tableName` is left intact (the translator ignores it; conjunct attribution has
/// already read it upstream).
fn strip_table_alias(expr: &Json) -> Json {
    match expr {
        Json::Object(map) => Json::Object(
            map.iter()
                .filter(|(key, _)| key.as_str() != "tableAlias")
                .map(|(key, value)| (key.clone(), strip_table_alias(value)))
                .collect(),
        ),
        Json::Array(items) => Json::Array(items.iter().map(strip_table_alias).collect()),
        other => other.clone(),
    }
}

/// Record the UPPERCASE name of every `column` node in `expr` attributed (by
/// `tableName`, case-insensitive) to `table_name`, recursively.
fn collect_side_column_names(
    expr: &Json,
    table_name: &str,
    out: &mut std::collections::HashSet<String>,
) {
    match expr {
        Json::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("column") {
                let tn = map.get("tableName").and_then(|t| t.as_str());
                let name = map.get("name").and_then(|n| n.as_str());
                if let (Some(tn), Some(name)) = (tn, name)
                    && tn.eq_ignore_ascii_case(table_name)
                {
                    out.insert(name.to_ascii_uppercase());
                }
            }
            for value in map.values() {
                collect_side_column_names(value, table_name, out);
            }
        }
        Json::Array(items) => items
            .iter()
            .for_each(|item| collect_side_column_names(item, table_name, out)),
        _ => {}
    }
}

/// The subset of `full_cols` this side actually contributes to the outer two-scan
/// wrapper — dropping columns the wrapper never references, so each fan-out leg
/// ships narrow rows instead of the table's full column set.
///
/// The kept set is every column of this side referenced by any clause the wrapper
/// renders: the SELECT list, the join condition, the WHERE (the FULL predicate —
/// the outer wrapper renders all of it, so a side-local *or* cross-table filter
/// column must survive), GROUP BY, HAVING, and ORDER BY. Order and Exasol types are
/// preserved from `full_cols`.
///
/// Two total-safety fallbacks keep the wrapper buildable: an absent/empty SELECT
/// list means `SELECT *` over both fan-outs, so every column is kept; and an
/// (unreachable) empty result keeps `full_cols` rather than emit a zero-column leg.
fn referenced_side_columns(
    pushdown_req: &Json,
    condition: &Json,
    table_name: &str,
    full_cols: &[(String, String)],
) -> Vec<(String, String)> {
    let mut names = std::collections::HashSet::new();
    match pushdown_req.get("selectList") {
        Some(Json::Array(list)) if !list.is_empty() => {
            for item in list {
                collect_side_column_names(item, table_name, &mut names);
            }
        }
        // Absent/empty select list ⇒ the wrapper projects every column (SELECT *).
        _ => return full_cols.to_vec(),
    }
    collect_side_column_names(condition, table_name, &mut names);
    if let Some(f) = pushdown_req.get("filter").filter(|f| !f.is_null()) {
        collect_side_column_names(f, table_name, &mut names);
    }
    for key in ["groupBy", "orderBy"] {
        if let Some(v) = pushdown_req.get(key) {
            collect_side_column_names(v, table_name, &mut names);
        }
    }
    if let Some(h) = pushdown_req.get("having").filter(|h| !h.is_null()) {
        collect_side_column_names(h, table_name, &mut names);
    }
    let narrowed: Vec<(String, String)> = full_cols
        .iter()
        .filter(|(name, _)| names.contains(name))
        .cloned()
        .collect();
    if narrowed.is_empty() {
        full_cols.to_vec()
    } else {
        narrowed
    }
}

/// Render one select-list item to a table-qualified outer-SELECT expression through
/// the SINGLE `vs-expression` path — columns, literals, scalar expressions, a
/// top-level `function_aggregate`, AND a `function_aggregate` nested inside a scalar
/// function all render through the same recursive translator.
///
/// The translator splices an Exasol aggregate `name` verbatim (Exasol pushed it, so
/// it is a valid Exasol aggregate — `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, the
/// STDDEV/VARIANCE family), renders each argument by recursion (table-qualifying any
/// column argument via its `tableAlias`), handles `COUNT(*)`, and honors `DISTINCT`.
/// This is byte-compatible with the former top-level `render_aggregate_qualified`
/// (single-arg aggregate → `NAME(<arg>)`, `COUNT(*)` → `COUNT(*)`), and additionally
/// renders a scalar expression that wraps aggregates (e.g.
/// `ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`) instead of declining. `None` only when
/// the node genuinely cannot be rendered.
fn render_selectlist_item_qualified(
    item: &Json,
    alias_of: &HashMap<String, String>,
) -> Option<String> {
    render_expression_qualified(item, alias_of)
}

/// Whether a join pushdown request carries work Exasol must execute over the
/// materialized two-scan join rather than inside the broadcast in-UDF join: an
/// aggregate (single-group or grouped), a GROUP BY, an ORDER BY, a LIMIT, or a
/// HAVING. The broadcast path renders only projection + filter + join condition, so
/// any of these routes the join to the qualified two-scan fallback (which renders
/// them as ordinary Exasol SQL over the join), reproducing pre-`JOIN`-capability
/// behavior exactly.
fn join_requires_exasol_postprocessing(pushdown_req: &Json) -> bool {
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
    has_aggregate_item
        || has_group_by
        || is_group_by_aggregation
        || has_having
        || order_by_present(pushdown_req)
        || extract_limit(pushdown_req).is_some()
}

/// Plan an inner join (N ≥ 2 involved tables) through the SINGLE unified join path.
///
/// Resolves each involved table's file list, logical schema, and byte size ONCE
/// (one catalog resolution per table, never per shard), pruned by that table's
/// side-local WHERE conjuncts. An inner join with any empty side yields zero rows,
/// so an empty side short-circuits to the shape-correct empty result over the
/// combined N-table column universe (in stable side order, matching the fallback's
/// full-row projection).
///
/// Broadcast is an OPTIMIZATION selected inside this one path — never a second
/// implementation. It is taken only for a two-table (N = 2) equi-join whose smaller
/// side fits `join_broadcast_max_bytes`, whose request needs no Exasol
/// postprocessing (the in-UDF join renders only projection + filter + condition),
/// and whose bare-name broadcast render succeeds (disjoint column names + renderable
/// condition — `render_broadcast_join` returns `Ok(None)` otherwise, a clean
/// fall-through, never an error). Every other inner join — N ≥ 3, above threshold,
/// non-equi, overlapping columns, or needing postprocessing — takes the SOLE
/// fallback renderer, [`build_n_scan_join_sql`], which scans each table through its
/// own sharded fan-out and reconstructs the join in Exasol's core engine. A hard
/// `Err` (a client-facing error, no native re-plan) is the last resort, delegated to
/// the builder for a wrapper that genuinely cannot be built.
#[allow(clippy::too_many_arguments)]
pub(super) async fn plan_join(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
    scan_schema: Option<&str>,
    cluster_nodes: usize,
    parallelism_factor: usize,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
    join_broadcast_max_bytes: u64,
) -> Result<Json, UdfError> {
    // Resolve each side ONCE (one catalog resolution per involved table, never per
    // shard), each pruned by its own side-local WHERE conjuncts for Iceberg manifest
    // pruning — attributed by `tableName`, so a shared-column-name case stays correct.
    let filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let mut sides = Vec::with_capacity(join.tables.len());
    for leaf in &join.tables {
        let side_filter = filter.and_then(|f| side_local_filter(f, &leaf.table_name));
        let side = resolve_one_join_side(
            &leaf.table_name,
            &leaf.iceberg_ident,
            catalog_uri,
            storage,
            catalog,
            creds,
            side_filter.as_ref(),
        )
        .await?;
        sides.push(side);
    }

    // An inner join with any empty side is empty regardless of the plan. Emit the
    // shape-correct empty result over the combined N-table column universe (stable
    // side order) rather than a fan-out over an empty file list.
    if sides.iter().any(|s| s.files.is_empty()) {
        let mut combined = Vec::new();
        for leaf in &join.tables {
            combined.extend(involved_table_columns(request, &leaf.table_name));
        }
        let (proj_cols, proj_types) = project_columns(pushdown_req, combined.clone())?;
        return empty_result_sql(pushdown_req, &proj_cols, &proj_types, &combined);
    }

    let udf_name = qualify_udf(scan_schema, SCAN_UDF_NAME);
    let merge_udf_name = qualify_udf(scan_schema, DISTINCT_MERGE_UDF_NAME);
    let distribute_udf_name = qualify_udf(scan_schema, DISTRIBUTE_FILES_UDF_NAME);
    let tuning = JoinScanTuning {
        cluster_nodes,
        parallelism_factor,
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
    };

    // Broadcast eligibility is a PROPERTY of the request, computed here: exactly two
    // involved tables, a `predicate_equal` condition, and no Exasol postprocessing.
    // When it holds, size the two sides (smaller = dimension) and take the broadcast
    // fan-out iff the dimension fits the threshold AND the bare-name render succeeds.
    // Any miss falls through to the N-scan fallback below — never an error.
    let is_equi =
        join.conditions[0].get("type").and_then(|t| t.as_str()) == Some("predicate_equal");
    if join.tables.len() == 2 && is_equi && !join_requires_exasol_postprocessing(pushdown_req) {
        let candidate =
            select_broadcast_sides(sides[0].clone(), sides[1].clone(), join_broadcast_max_bytes);
        if candidate.broadcast_eligible
            && let Some(rendered) = render_broadcast_join(request, pushdown_req, join)?
        {
            let sql = build_broadcast_join_sql(
                &candidate,
                &rendered,
                &tuning,
                &udf_name,
                &merge_udf_name,
                &distribute_udf_name,
            );
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        }
    }

    let sql = build_n_scan_join_sql(
        request,
        pushdown_req,
        join,
        &sides,
        &tuning,
        &udf_name,
        &merge_udf_name,
        &distribute_udf_name,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// Side `i`'s Exasol virtual table name (UPPERCASE) maps to `aliases[i]`
/// (`LHS_T{i}`), so every column reference the N-scan wrapper renders is
/// table-qualified from its `tableName`.
fn build_n_scan_alias_map(
    sides: &[ResolvedJoinSide],
    aliases: &[String],
) -> HashMap<String, String> {
    sides
        .iter()
        .zip(aliases)
        .map(|(side, alias)| (side.table_name.to_ascii_uppercase(), alias.clone()))
        .collect()
}

/// Render the N-scan fallback's FROM as a left-to-right `INNER JOIN … ON` chain and
/// return it together with any join conditions that could not be attached to a join
/// point (untagged, or referencing no known leg). Those unattachable conditions
/// become outer-WHERE residual conjuncts — for an inner join a condition in the
/// WHERE is result-equivalent to the same condition in an `ON` clause, so this is a
/// safe last-resort backstop (decision-log [7]).
///
/// `conditions[i]` is the pre-rendered, table-qualified SQL for `raw_conditions[i]`.
/// Each condition GREEDILY attaches to the earliest join point where every table it
/// touches is in scope — the join point that brings its highest-indexed leg in.
/// Scope is resolved by the SET of `tableName`s the raw condition references
/// (via [`collect_column_tables`]), NEVER by column name, so two legs sharing a
/// column name can never fool the attachment. A join point with no attached
/// condition renders `ON 1=1`.
fn build_n_scan_join_from(
    fan_outs: &[String],
    aliases: &[String],
    raw_conditions: &[Json],
    conditions: &[String],
    sides: &[ResolvedJoinSide],
) -> (String, Vec<String>) {
    let leg_index: HashMap<String, usize> = sides
        .iter()
        .enumerate()
        .map(|(i, s)| (s.table_name.to_ascii_uppercase(), i))
        .collect();
    let last_join_point = aliases.len().saturating_sub(1);

    let mut on_at: Vec<Vec<String>> = vec![Vec::new(); aliases.len()];
    let mut residual: Vec<String> = Vec::new();
    for (raw, rendered) in raw_conditions.iter().zip(conditions) {
        let mut tables = std::collections::HashSet::new();
        let mut has_untagged = false;
        let mut any_column = false;
        collect_column_tables(raw, &mut tables, &mut has_untagged, &mut any_column);
        let resolvable =
            any_column && !has_untagged && tables.iter().all(|t| leg_index.contains_key(t));
        match resolvable
            .then(|| tables.iter().map(|t| leg_index[t]).max())
            .flatten()
        {
            // The earliest join point in scope is the one bringing the
            // highest-indexed leg in; clamp to a real join point (≥ 1, ≤ last).
            // Guard `last_join_point >= 1` (i.e. at least one join exists) first:
            // with a single leg there is no join point to attach to (and
            // `clamp(1, 0)` would panic since min > max), so fall through to
            // residual; behavior for N≥2 is unchanged.
            Some(m) if last_join_point >= 1 => {
                on_at[m.clamp(1, last_join_point)].push(rendered.clone())
            }
            _ => residual.push(rendered.clone()),
        }
    }

    let mut from = format!("({}) AS {}", fan_outs[0], quote_ident(&aliases[0]));
    for k in 1..aliases.len() {
        let on = if on_at[k].is_empty() {
            "1=1".to_string()
        } else {
            on_at[k]
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        from.push_str(&format!(
            " INNER JOIN ({}) AS {} ON {on}",
            fan_outs[k],
            quote_ident(&aliases[k])
        ));
    }
    (from, residual)
}

/// Every column of all involved tables as a table-qualified projection item, in
/// side order. `cols_per_side[i]` belongs to the side aliased `aliases[i]`.
fn n_full_row_qualified_items(
    aliases: &[String],
    cols_per_side: &[Vec<(String, String)>],
) -> Vec<ProjectionItem> {
    aliases
        .iter()
        .zip(cols_per_side)
        .flat_map(|(alias, cols)| {
            cols.iter().map(move |(name, _)| ProjectionItem::Expr {
                expr: format!("{}.{}", quote_ident(alias), quote_ident(name)),
            })
        })
        .collect()
}

/// The N-scan wrapper's outer SELECT list, table-qualified. An absent/empty select
/// list projects every column of all involved tables in side order. An item that
/// cannot be rendered is a last-resort hard error (no native re-plan).
fn n_scan_join_select_items(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
    aliases: &[String],
    cols_per_side: &[Vec<(String, String)>],
) -> Result<Vec<ProjectionItem>, UdfError> {
    match pushdown_req.get("selectList") {
        Some(Json::Array(list)) if !list.is_empty() => {
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                let sql = render_selectlist_item_qualified(item, alias_of).ok_or_else(|| {
                    UdfError::User(
                        "join pushdown declined: a select-list item could not be rendered for the \
                         qualified N-scan join; this is a hard error, not a native re-plan"
                            .into(),
                    )
                })?;
                items.push(ProjectionItem::Expr { expr: sql });
            }
            Ok(items)
        }
        _ => Ok(n_full_row_qualified_items(aliases, cols_per_side)),
    }
}

/// Build the N-scan (N ≥ 2) unaccelerated inner-join SQL — the SOLE unaccelerated
/// fallback renderer (the two-involved-table case is simply N = 2). Each involved
/// table is scanned through its own sharded fan-out and reconstructed into the
/// original inner join by Exasol's core engine via a left-to-right `INNER JOIN … ON`
/// chain.
///
/// Each side emits its full column set (narrowed to the columns the wrapper actually
/// references across all clauses), so the outer wrapper's SELECT, every join
/// condition, WHERE, aggregate, GROUP BY, HAVING, and ORDER BY can reference any
/// column the join needs — all rendered TABLE-QUALIFIED (`"LHS_T{i}"."COL"`) from
/// each `column` node's `tableName`, so the wrapper is correct whether or not any
/// two involved tables share a column name (decision-log [2]).
///
/// The FROM is a left-to-right `INNER JOIN … ON` chain (decision-log [6]): each join
/// condition greedily attaches to the earliest join point where every table it
/// touches is in scope, resolved by the SET of `tableName`s the condition references
/// (never by column name, so shared column names cannot misroute scope); a join
/// point with no newly-resolvable condition renders `ON 1=1`. Each side's side-local
/// WHERE conjuncts are pushed into that side's fan-out leg; only cross-table /
/// OR-spanning / untagged residual conjuncts (and any untaggable join condition)
/// remain in the outer WHERE, each parenthesized so a top-level `OR` cannot bind
/// across the ANDs. For an inner join this is result-equivalent to single-node
/// evaluation, independent of join order and of shared column names (decision-log
/// [7]).
///
/// Returns an `Err` (a hard client-facing error, no native re-plan) only when the
/// wrapper genuinely cannot be built: an involved table carries no column metadata,
/// or a join condition (or a pushed select/GROUP BY/HAVING/ORDER BY element) cannot
/// be rendered at all.
#[allow(clippy::too_many_arguments)]
fn build_n_scan_join_sql(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    tuning: &JoinScanTuning,
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    let cols_per_side: Vec<Vec<(String, String)>> = sides
        .iter()
        .map(|s| involved_table_columns(request, &s.table_name))
        .collect();
    if cols_per_side.iter().any(|c| c.is_empty()) {
        return Err(UdfError::User(
            "join pushdown declined: an involved table carries no column metadata, so the \
             unaccelerated N-scan fallback cannot be built; this is a hard error, not a \
             native re-plan"
                .into(),
        ));
    }

    let aliases: Vec<String> = (0..sides.len()).map(|i| format!("LHS_T{i}")).collect();
    let alias_of = build_n_scan_alias_map(sides, &aliases);

    // Every join-tree condition, table-qualified. A condition is the one clause with
    // no lower fallback: if it cannot be rendered even qualified, no correct join SQL
    // exists → last-resort hard error (no native re-plan).
    let mut conditions = Vec::with_capacity(join.conditions.len());
    for cond in &join.conditions {
        let rendered = render_expression_qualified(cond, &alias_of).ok_or_else(|| {
            UdfError::User(
                "join pushdown declined: a join condition could not be rendered against the \
                 qualified N-scan schema; this is a hard error, not a native re-plan"
                    .into(),
            )
        })?;
        conditions.push(rendered);
    }

    // Task 4.2: the outer WHERE keeps ONLY the residual conjuncts NOT side-local to a
    // single leg (cross-table, OR-spanning, or untagged); every side-local conjunct
    // is pushed into its leg's fan-out below and never re-applied here. The partition
    // is exact and total (see `side_local_filter` vs `cross_side_residual_filter`).
    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(cross_side_residual_filter)
        .and_then(|residual| render_df_filter_qualified(&residual, &alias_of));

    let select_items = n_scan_join_select_items(pushdown_req, &alias_of, &aliases, &cols_per_side)?;
    let group_by = qualified_join_group_by(pushdown_req, &alias_of)?;
    let having = qualified_join_having(pushdown_req, &alias_of)?;
    let order_by = qualified_join_order_by(pushdown_req, &alias_of)?;
    let limit = extract_limit(pushdown_req);

    // Per-side fan-out: narrow each leg's projection to the columns the wrapper
    // references (across the SELECT list, ALL join conditions, WHERE, GROUP BY,
    // HAVING, and ORDER BY), and push each side's side-local WHERE conjuncts down as a
    // DataFusion filter. Cross-table and OR-spanning conjuncts stay only in the outer
    // WHERE (`filter`), the correctness backstop. All N-1 conditions are passed as one
    // JSON array so `referenced_side_columns` (which walks arbitrary nodes) keeps a
    // side's column referenced by ANY condition.
    let where_filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let all_conditions = Json::Array(join.conditions.clone());
    let mut fan_outs = Vec::with_capacity(sides.len());
    for (i, side) in sides.iter().enumerate() {
        let narrowed = referenced_side_columns(
            pushdown_req,
            &all_conditions,
            &side.table_name,
            &cols_per_side[i],
        );
        let side_filter = where_filter.and_then(|f| side_local_filter(f, &side.table_name));
        fan_outs.push(build_side_fan_out_sql(
            side,
            &narrowed,
            side_filter.as_ref(),
            tuning,
            udf_name,
            merge_udf_name,
            distribute_udf_name,
        ));
    }

    // Assemble the INNER JOIN … ON chain (decision-log [6]). FROM is the chain of
    // aliased fan-out legs with each condition greedily attached by table-name set;
    // the outer WHERE carries the residual filter plus any untaggable join condition.
    let select = if select_items.is_empty() {
        "*".to_string()
    } else {
        select_items
            .iter()
            .map(projection_item_select_sql)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let (from, residual_conditions) =
        build_n_scan_join_from(&fan_outs, &aliases, &join.conditions, &conditions, sides);

    let mut where_parts: Vec<String> = residual_conditions
        .iter()
        .map(|c| format!("({c})"))
        .collect();
    if let Some(f) = &filter {
        where_parts.push(format!("({f})"));
    }

    let mut sql = format!("SELECT {select} FROM {from}");
    if !where_parts.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
    }
    if let Some(clause) = group_by {
        sql.push_str(&format!(" GROUP BY {clause}"));
    }
    if let Some(clause) = having {
        sql.push_str(&format!(" HAVING {clause}"));
    }
    if let Some(clause) = order_by {
        sql.push_str(&format!(" ORDER BY {clause}"));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

/// The DataFusion execution + sharding knobs threaded into join SQL building.
///
/// Bundled so the two join SQL builders take one config parameter instead of eight
/// positional numbers whose order is easy to transpose (guardrails: few arguments,
/// config at high levels).
struct JoinScanTuning {
    cluster_nodes: usize,
    parallelism_factor: usize,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
}

/// Relativize one file list against its table root (single-list convenience over
/// [`relativize_shards_to_root`], preserving order and byte sizes).
fn relativize_files_to_root(files: Vec<FileEntry>, table_root: &str) -> Vec<FileEntry> {
    relativize_shards_to_root(vec![files], table_root)
        .pop()
        .unwrap_or_default()
}

/// Build one side's single-table sharded fan-out SQL (an outer ungrouped scalar
/// `LAKEHOUSE_SCAN` over the nested distributor, or a from-less scalar call on
/// literals for a single shard — no `SELECT * FROM (...)` wrapper, decision [5]),
/// emitting the columns the outer wrapper references for this side and pushing this
/// side's SIDE-LOCAL WHERE conjuncts down as a DataFusion filter. No join block, no
/// limit push. Used for BOTH sides of the unaccelerated fallback: the outer Exasol
/// query (see [`build_n_scan_join_sql`]) still applies the projection, conditions, and
/// the FULL `WHERE`, so `columns` (the side's narrowed `(UPPERCASE name, Exasol
/// type)` list, see [`referenced_side_columns`]) must expose every column any outer
/// clause references. `side_filter` (see [`side_local_filter`]) is rendered bare-name
/// via `render_df_filter_safe` so DataFusion row-group-prunes and row-filters this
/// leg before emitting, rather than shipping every row for Exasol to filter.
fn build_side_fan_out_sql(
    side: &ResolvedJoinSide,
    columns: &[(String, String)],
    side_filter: Option<&Json>,
    tuning: &JoinScanTuning,
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let proj_cols: Vec<ProjectionItem> = columns
        .iter()
        .map(|(name, _)| ProjectionItem::Column(name.clone()))
        .collect();
    let proj_types: Vec<String> = columns.iter().map(|(_, ty)| ty.clone()).collect();

    let g = shard_count(
        tuning.cluster_nodes,
        tuning.parallelism_factor,
        side.files.len(),
    );
    let shards = crate::adapter::sharding::partition_files_by_bytes(side.files.clone(), g);
    let shards = relativize_shards_to_root(shards, &side.table_root);

    let spec = ScanSpec {
        table_root: side.table_root.clone(),
        files: vec![],
        projection: proj_cols.clone(),
        // Render BARE (strip Exasol's `tableAlias`): the fan-out is a single-table
        // scan whose relation exposes bare uppercase column names, so an
        // alias-qualified reference would not resolve — exactly the single-table
        // scan path's contract. The outer wrapper's WHERE re-qualifies separately.
        filter: side_filter
            .map(strip_table_alias)
            .and_then(|f| render_df_filter_safe(&f)),
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: proj_types.clone(),
        logical_schema: side.logical_schema.clone(),
        name_mapping: side.name_mapping.clone(),
        join: None,
        storage: side.effective_storage.clone(),
        df_target_partitions: tuning.df_target_partitions,
        df_batch_size: tuning.df_batch_size,
        df_threads_per_udf: tuning.df_threads_per_udf,
        memory_pool_fraction: tuning.memory_pool_fraction,
        instance_overhead_mb: tuning.instance_overhead_mb,
        s3_max_connections: tuning.s3_max_connections,
    };
    build_scan_driving_sql(
        &spec,
        &shards,
        &proj_cols,
        &proj_types,
        None,
        &[],
        &[],
        udf_name,
        merge_udf_name,
        distribute_udf_name,
    )
}

/// Build the broadcast fan-out scan-driving SQL (task 3.4).
///
/// The fact (larger) side is sharded into G byte-balanced work units exactly as the
/// single-table path does; the dimension (smaller) side's FULL file list, table
/// root, logical schema, join type, and rendered condition ride ONCE in the
/// shard-invariant common blob's join block ([`JoinSpec`]). Every shard invocation
/// therefore re-scans the same dimension side and joins it against its fact-file
/// subset node-locally, with no cross-shard exchange. Reuses [`build_scan_driving_sql`]
/// unchanged — the join block travels transparently inside the common blob.
///
/// One `StorageProps` serves both registered tables inside the single DataFusion
/// session; the fact side's effective storage is used. When vended credentials are
/// disabled (the common MinIO case) both sides' effective storage is identical, so
/// this is exact; with per-prefix vended STS creds both tables must be readable with
/// the fact side's grant (both live under one warehouse for the broadcast target).
fn build_broadcast_join_sql(
    sides: &JoinSides,
    rendered: &RenderedJoinPushdown,
    tuning: &JoinScanTuning,
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let fact = &sides.fact;
    let dimension = &sides.dimension;

    let g = shard_count(
        tuning.cluster_nodes,
        tuning.parallelism_factor,
        fact.files.len(),
    );
    let shards = crate::adapter::sharding::partition_files_by_bytes(fact.files.clone(), g);
    let shards = relativize_shards_to_root(shards, &fact.table_root);

    let join = JoinSpec {
        table_root: dimension.table_root.clone(),
        files: relativize_files_to_root(dimension.files.clone(), &dimension.table_root),
        logical_schema: dimension.logical_schema.clone(),
        name_mapping: dimension.name_mapping.clone(),
        join_type: JoinType::Inner,
        condition: rendered.condition.clone(),
    };

    let spec = ScanSpec {
        table_root: fact.table_root.clone(),
        files: vec![],
        projection: rendered.projection.clone(),
        filter: rendered.filter.clone(),
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: rendered.projection_types.clone(),
        logical_schema: fact.logical_schema.clone(),
        name_mapping: fact.name_mapping.clone(),
        join: Some(join),
        storage: fact.effective_storage.clone(),
        df_target_partitions: tuning.df_target_partitions,
        df_batch_size: tuning.df_batch_size,
        df_threads_per_udf: tuning.df_threads_per_udf,
        memory_pool_fraction: tuning.memory_pool_fraction,
        instance_overhead_mb: tuning.instance_overhead_mb,
        s3_max_connections: tuning.s3_max_connections,
    };

    build_scan_driving_sql(
        &spec,
        &shards,
        &rendered.projection,
        &rendered.projection_types,
        None,
        &[],
        &[],
        udf_name,
        merge_udf_name,
        distribute_udf_name,
    )
}

/// The N-scan wrapper's `GROUP BY` clause (without the keyword), table-qualified.
/// `None` when the request carries no non-empty `groupBy`. A group key that cannot
/// be rendered is a last-resort hard error (no native re-plan).
fn qualified_join_group_by(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    let keys = match pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        Some(keys) => keys,
        None => return Ok(None),
    };
    let mut parts = Vec::with_capacity(keys.len());
    for key in keys {
        parts.push(render_expression_qualified(key, alias_of).ok_or_else(|| {
            UdfError::User(
                "join pushdown declined: a GROUP BY key could not be rendered for the qualified \
                 N-scan join; this is a hard error, not a native re-plan"
                    .into(),
            )
        })?);
    }
    Ok(Some(parts.join(", ")))
}

/// The N-scan wrapper's `HAVING` clause (without the keyword), table-qualified.
/// `None` when the request carries no `having`. An unrenderable HAVING is a
/// last-resort hard error (dropping it would return wrong rows; no native re-plan).
fn qualified_join_having(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    match pushdown_req.get("having").filter(|h| !h.is_null()) {
        Some(having) => Ok(Some(
            render_expression_qualified(having, alias_of).ok_or_else(|| {
                UdfError::User(
                    "join pushdown declined: HAVING could not be rendered for the qualified \
                     N-scan join; this is a hard error, not a native re-plan"
                        .into(),
                )
            })?,
        )),
        None => Ok(None),
    }
}

/// The N-scan wrapper's `ORDER BY` clause (without the keyword), table-qualified.
/// `None` when the request carries no non-empty `orderBy`. Only bare-column sort
/// keys are advertised (`ORDER_BY_COLUMN`); an element that is not a renderable bare
/// column is a last-resort hard error (dropping it would return an unordered
/// result Exasol delegated and no longer re-sorts; no native re-plan).
fn qualified_join_order_by(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    let elements = match pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        Some(elements) => elements,
        None => return Ok(None),
    };
    let decline = || {
        UdfError::User(
            "join pushdown declined: an ORDER BY key could not be rendered for the qualified \
             N-scan join; this is a hard error, not a native re-plan"
                .into(),
        )
    };
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        let key = parse_sort_key_element(element).ok_or_else(decline)?;
        let expr = element.get("expression").ok_or_else(decline)?;
        let rendered = render_expression_qualified(expr, alias_of).ok_or_else(decline)?;
        parts.push(key.render_ordered(&rendered));
    }
    Ok(Some(parts.join(", ")))
}

/// The full base row as `(ProjectionItem::Column, Exasol type)` lists, positionally
/// aligned. Used by the grouped qualified-wrapper fallback so its inner sharded raw
/// scan exposes every column the outer grouped select list / GROUP BY / HAVING /
/// ORDER BY can reference.
pub(super) fn full_row_projection(
    all_cols: &[(String, String)],
) -> (Vec<ProjectionItem>, Vec<String>) {
    (
        all_cols
            .iter()
            .map(|(name, _)| ProjectionItem::Column(name.clone()))
            .collect(),
        all_cols.iter().map(|(_, ty)| ty.clone()).collect(),
    )
}

/// Build the qualified single-table wrapper for a GROUP BY request that could not be
/// decomposed into the partial/merge plan (an undecomposable scalar-over-aggregate
/// item, a non-numeric aggregate with no HAVING, or any other non-pushable grouped
/// shape). This is the join N-scan fallback at N = 1: one aliased raw fan-out
/// subquery, no cross-join and no join condition, with the exact grouped select list,
/// GROUP BY, HAVING, ORDER BY, and LIMIT rendered as ordinary Exasol SQL over it so
/// Exasol's core engine computes the aggregate over the returned rows.
///
/// Reuses the join path's qualified renderers verbatim: the single table is aliased
/// `LHS_T0`, every column reference is table-qualified against that alias, and
/// aggregates are spliced verbatim by the `vs-expression` translator (Exasol
/// aggregates over materialized rows, not over merged partials). The per-shard scan
/// stays LIMIT-free and sort-free (`fan_out_spec` carries no limit/order_by); the
/// group keys, HAVING, ORDER BY, and LIMIT live only in the outer wrapper. The WHERE
/// filter is applied inside the scan (via `fan_out_spec.filter`), so no outer WHERE
/// is needed — mirroring the grouped push-down path. The result column count and
/// per-column types match Exasol's positional `selectListDataTypes` validation, so
/// this never emits the `04000`-triggering bare row scan.
pub(super) fn build_grouped_qualified_fallback_sql<E: Clone + Into<FileEntry>>(
    request: &Json,
    pushdown_req: &Json,
    fan_out_spec: &ScanSpec,
    shards: &[Vec<E>],
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    const ALIAS: &str = "LHS_T0";

    // Alias EVERY involved table name to the single subquery alias, so a column
    // node's `tableName` (or a stale request `tableAlias`) resolves to `"LHS_T0"`.
    let alias_of: HashMap<String, String> = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .map(|tables| {
            tables
                .iter()
                .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
                .map(|name| (name.to_ascii_uppercase(), ALIAS.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let aliases = vec![ALIAS.to_string()];

    // The scan exposes the full base row; reconstruct the `(name, type)` universe
    // from the fan-out spec so the no-select-list fallback (unusual for a grouped
    // request) still resolves types from the one side.
    let all_cols: Vec<(String, String)> = fan_out_spec
        .projection
        .iter()
        .zip(fan_out_spec.emit_exa_types.iter())
        .filter_map(|(item, ty)| match item {
            ProjectionItem::Column(name) => Some((name.clone(), ty.clone())),
            ProjectionItem::Expr { .. } => None,
        })
        .collect();
    let cols_per_side = vec![all_cols];

    let select_items = n_scan_join_select_items(pushdown_req, &alias_of, &aliases, &cols_per_side)?;
    let group_by = qualified_join_group_by(pushdown_req, &alias_of)?;
    let having = qualified_join_having(pushdown_req, &alias_of)?;
    let order_by = qualified_join_order_by(pushdown_req, &alias_of)?;
    let limit = extract_limit(pushdown_req);

    // One aliased raw sharded fan-out. LIMIT-free / sort-free / no aggregates — the
    // fan-out spec already guarantees this.
    let proj_cols = fan_out_spec.projection.clone();
    let proj_types = fan_out_spec.emit_exa_types.clone();
    let fan_out = build_scan_driving_sql(
        fan_out_spec,
        shards,
        &proj_cols,
        &proj_types,
        None,
        &[],
        &[],
        udf_name,
        merge_udf_name,
        distribute_udf_name,
    );

    let select = if select_items.is_empty() {
        "*".to_string()
    } else {
        select_items
            .iter()
            .map(projection_item_select_sql)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut sql = format!("SELECT {select} FROM ({fan_out}) AS {}", quote_ident(ALIAS));
    if let Some(clause) = group_by {
        sql.push_str(&format!(" GROUP BY {clause}"));
    }
    if let Some(clause) = having {
        sql.push_str(&format!(" HAVING {clause}"));
    }
    if let Some(clause) = order_by {
        sql.push_str(&format!(" ORDER BY {clause}"));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    // ---------------------------------------------------------------------------
    // Join detection (task 3.1): `detect_join` shape classification.
    // ---------------------------------------------------------------------------

    /// Build a two-table-join pushdown request. `from_extra` is spliced into the
    /// `from` object (e.g. to swap `join_type`, drop a field, or corrupt a side),
    /// and `condition` becomes the join's `condition` node.
    fn join_request(from_extra: Json, condition: Json) -> Json {
        let mut from = serde_json::json!({
            "type": "join",
            "join_type": "inner",
            "left": {"name": "CUSTOMER", "type": "table"},
            "right": {"name": "ORDERS", "type": "table"},
        });
        if let Json::Object(extra) = from_extra {
            from.as_object_mut().unwrap().extend(extra);
        }
        from["condition"] = condition;

        serde_json::json!({
            "involvedTables": [
                {
                    "name": "CUSTOMER",
                    "columns": [
                        {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                        {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
                    ],
                },
                {
                    "name": "ORDERS",
                    "columns": [
                        {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                        {"name": "O_ORDERDATE", "dataType": {"type": "date"}},
                    ],
                },
            ],
            "pushdownRequest": {
                "type": "select",
                "from": from,
                "selectList": [
                    {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                    {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                ],
            },
            "schemaMetadataInfo": {
                "properties": {},
                "adapterNotes": serde_json::json!({
                    "TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}
                }).to_string(),
            },
        })
    }

    /// The standard equi-join condition: `CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY`.
    fn equi_condition() -> Json {
        serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
        })
    }

    /// A genuine two-table inner equi-join is detected as the unified `Join` shape,
    /// with both leaves' original-cased Iceberg identifiers recovered from `TABLE_MAP`
    /// (the two-table case is simply N = 2).
    #[test]
    fn genuine_inner_equi_join_is_detected_with_both_idents() {
        let request = join_request(Json::Null, equi_condition());
        let pushdown_req = pd(&request);

        let shape = detect_join(&request, &pushdown_req).expect("TABLE_MAP has both tables");
        match shape {
            JoinShape::Join(join) => {
                assert_eq!(join.tables.len(), 2);
                assert_eq!(join.tables[0].table_name, "CUSTOMER");
                assert_eq!(join.tables[1].table_name, "ORDERS");
                assert_eq!(join.tables[0].iceberg_ident, "lh.customer");
                assert_eq!(join.tables[1].iceberg_ident, "lh.orders");
                assert_eq!(join.conditions, vec![equi_condition()]);
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// A plain single-table pushdown request (today's normal case, no `from` field
    /// at all) is `NotAJoin` and completely unaffected by the detector.
    #[test]
    fn plain_single_table_request_is_not_a_join() {
        let request = nq4_request();
        let shape = detect_join(&request, &pd(&request)).expect("not a join, no TABLE_MAP lookup");
        assert_eq!(shape, JoinShape::NotAJoin);
    }

    /// A `from` clause that is a plain table reference (`type: "table"`) is also
    /// `NotAJoin` — the single-table shape some requests carry explicitly.
    #[test]
    fn from_table_node_is_not_a_join() {
        let mut request = nq4_request();
        request["pushdownRequest"]["from"] =
            serde_json::json!({"name": "LINEITEM", "type": "table"});
        let shape = detect_join(&request, &pd(&request)).expect("not a join");
        assert_eq!(shape, JoinShape::NotAJoin);
    }

    /// Left/right/full outer joins are declined as `Ineligible(NotInnerJoinType)`,
    /// never `Eligible` — the broadcast contract advertises only `JOIN_TYPE_INNER`.
    #[test]
    fn outer_join_is_ineligible() {
        for outer in ["left_outer", "right_outer", "full_outer"] {
            let request = join_request(serde_json::json!({"join_type": outer}), equi_condition());
            let shape = detect_join(&request, &pd(&request)).expect("shape decline, no Err");
            assert_eq!(
                shape,
                JoinShape::Ineligible(IneligibleJoinReason::NotInnerJoinType),
                "join_type '{outer}' must be ineligible, not broadcast-eligible"
            );
        }
    }

    /// A non-equi two-table inner join (e.g. `<`) is NOT declined — it is served by
    /// the unified fallback, so it yields the `Join` shape carrying both tables and
    /// the (non-equi) condition. Only broadcast (an inner optimization) is gated on
    /// equi; the N-scan fallback renders any inner-join condition into its WHERE.
    #[test]
    fn non_equi_two_table_join_is_served_by_unified_fallback() {
        let condition = serde_json::json!({
            "type": "predicate_less",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
        });
        let request = join_request(Json::Null, condition.clone());
        match detect_join(&request, &pd(&request)).expect("served, not declined") {
            JoinShape::Join(join) => {
                assert_eq!(join.tables.len(), 2);
                assert_eq!(join.conditions, vec![condition]);
            }
            other => panic!("expected Join (unified fallback), got {other:?}"),
        }
    }

    /// A three-table inner-join pushdown request: `(CUSTOMER ⋈ ORDERS) ⋈ LINEITEM`,
    /// all three in `TABLE_MAP`. Leaves in stable tree order CUSTOMER, ORDERS,
    /// LINEITEM; two join conditions (`C_CUSTKEY=O_CUSTKEY`, `O_ORDERKEY=L_ORDERKEY`).
    fn three_table_join_request() -> Json {
        serde_json::json!({
            "involvedTables": [
                {"name": "CUSTOMER", "columns": [
                    {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "ORDERS", "columns": [
                    {"name": "O_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "LINEITEM", "columns": [
                    {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "L_QUANTITY", "dataType": {"type": "decimal", "precision": 15, "scale": 2}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"name": "CUSTOMER", "type": "table"},
                        "right": {"name": "ORDERS", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}}},
                    "right": {"name": "LINEITEM", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "O_ORDERKEY", "tableName": "ORDERS"},
                        "right": {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"}}},
                "selectList": [
                    {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                    {"type": "column", "name": "L_QUANTITY", "tableName": "LINEITEM"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders", "LINEITEM": "lh.lineitem"}})
                    .to_string()},
        })
    }

    /// A three-table all-inner nested join is classified as the unified `Join` shape
    /// (never an error, never Ineligible): the three leaves in stable tree order and
    /// the two collected join conditions, each leaf's Iceberg ident recovered from
    /// `TABLE_MAP` (pushdown-planning-join "A three-or-more-table inner join falls
    /// back to an N-scan unaccelerated wrapper").
    #[test]
    fn three_table_inner_join_is_unified_join() {
        let request = three_table_join_request();
        let shape = detect_join(&request, &pd(&request)).expect("all leaves are in TABLE_MAP");
        match shape {
            JoinShape::Join(join) => {
                let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
                assert_eq!(names, ["CUSTOMER", "ORDERS", "LINEITEM"]);
                let idents: Vec<&str> = join
                    .tables
                    .iter()
                    .map(|t| t.iceberg_ident.as_str())
                    .collect();
                assert_eq!(idents, ["lh.customer", "lh.orders", "lh.lineitem"]);
                assert_eq!(join.conditions.len(), 2, "N-1 conditions for N=3 tables");
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// A non-inner join node ANYWHERE in the tree (here the nested left node is a
    /// left outer join) declines as `Ineligible(NotInnerJoinType)` — a cross-join +
    /// conjunctive WHERE cannot reproduce outer-join semantics.
    #[test]
    fn non_inner_node_in_join_tree_is_ineligible() {
        let mut request = three_table_join_request();
        request["pushdownRequest"]["from"]["left"]["join_type"] = serde_json::json!("left_outer");
        let shape = detect_join(&request, &pd(&request)).expect("shape decline, no Err");
        assert_eq!(
            shape,
            JoinShape::Ineligible(IneligibleJoinReason::NotInnerJoinType)
        );
    }

    /// A leaf of a multi-table tree absent from `TABLE_MAP` is a hard `Err` (stale
    /// virtual schema), identical to the two-table path — never a silent decline.
    #[test]
    fn multi_table_leaf_absent_from_table_map_is_err() {
        let mut request = three_table_join_request();
        request["schemaMetadataInfo"]["adapterNotes"] = Json::String(
            serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}})
                .to_string(),
        );
        let err = detect_join(&request, &pd(&request))
            .expect_err("LINEITEM is absent from TABLE_MAP: must be Err, not a decline");
        assert!(
            err.to_string().contains("LINEITEM"),
            "error must name the unmapped table: {err}"
        );
    }

    /// The Q1-shape three-table inner-join pushdown request:
    /// `(SUPPLIER ⋈ NATION) ⋈ REGION`, all three in `TABLE_MAP`. Leaves in stable
    /// tree order SUPPLIER, NATION, REGION; two join conditions
    /// (`S_NATIONKEY=N_NATIONKEY`, `N_REGIONKEY=R_REGIONKEY`).
    fn q1_join_request() -> Json {
        serde_json::json!({
            "involvedTables": [
                {"name": "SUPPLIER", "columns": [
                    {"name": "S_SUPPKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "S_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "S_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "NATION", "columns": [
                    {"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "N_REGIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "REGION", "columns": [
                    {"name": "R_REGIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "R_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"name": "SUPPLIER", "type": "table"},
                        "right": {"name": "NATION", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "S_NATIONKEY", "tableName": "SUPPLIER"},
                            "right": {"type": "column", "name": "N_NATIONKEY", "tableName": "NATION"}}},
                    "right": {"name": "REGION", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "N_REGIONKEY", "tableName": "NATION"},
                        "right": {"type": "column", "name": "R_REGIONKEY", "tableName": "REGION"}}},
                "selectList": [
                    {"type": "column", "name": "S_NAME", "tableName": "SUPPLIER"},
                    {"type": "column", "name": "R_NAME", "tableName": "REGION"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"SUPPLIER": "lh.supplier", "NATION": "lh.nation", "REGION": "lh.region"}})
                    .to_string()},
        })
    }

    /// The NQ3-shape four-table inner-join pushdown request:
    /// `((PART ⋈ PARTSUPP) ⋈ SUPPLIER) ⋈ NATION`, all four in `TABLE_MAP`. Leaves in
    /// stable tree order PART, PARTSUPP, SUPPLIER, NATION; three join conditions.
    fn nq3_join_request() -> Json {
        serde_json::json!({
            "involvedTables": [
                {"name": "PART", "columns": [
                    {"name": "P_PARTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "P_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "PARTSUPP", "columns": [
                    {"name": "PS_PARTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "PS_SUPPKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "PS_AVAILQTY", "dataType": {"type": "decimal", "precision": 15, "scale": 0}}]},
                {"name": "SUPPLIER", "columns": [
                    {"name": "S_SUPPKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "S_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "NATION", "columns": [
                    {"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "N_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"type": "join", "join_type": "inner",
                            "left": {"name": "PART", "type": "table"},
                            "right": {"name": "PARTSUPP", "type": "table"},
                            "condition": {"type": "predicate_equal",
                                "left": {"type": "column", "name": "P_PARTKEY", "tableName": "PART"},
                                "right": {"type": "column", "name": "PS_PARTKEY", "tableName": "PARTSUPP"}}},
                        "right": {"name": "SUPPLIER", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "PS_SUPPKEY", "tableName": "PARTSUPP"},
                            "right": {"type": "column", "name": "S_SUPPKEY", "tableName": "SUPPLIER"}}},
                    "right": {"name": "NATION", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "S_NATIONKEY", "tableName": "SUPPLIER"},
                        "right": {"type": "column", "name": "N_NATIONKEY", "tableName": "NATION"}}},
                "selectList": [
                    {"type": "column", "name": "P_NAME", "tableName": "PART"},
                    {"type": "column", "name": "PS_AVAILQTY", "tableName": "PARTSUPP"},
                    {"type": "column", "name": "N_NAME", "tableName": "NATION"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP": {
                    "PART": "lh.part", "PARTSUPP": "lh.partsupp",
                    "SUPPLIER": "lh.supplier", "NATION": "lh.nation"}})
                    .to_string()},
        })
    }

    /// A four-table all-inner nested join (NQ3 shape: `part⋈partsupp⋈supplier⋈nation`)
    /// is the unified `Join` shape with all four leaves (stable tree order) and the
    /// three collected join conditions — the detector generalizes past N=3, never
    /// capping at three tables.
    #[test]
    fn four_table_inner_join_is_unified_join() {
        let request = nq3_join_request();
        let shape = detect_join(&request, &pd(&request)).expect("all leaves are in TABLE_MAP");
        match shape {
            JoinShape::Join(join) => {
                let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
                assert_eq!(names, ["PART", "PARTSUPP", "SUPPLIER", "NATION"]);
                let idents: Vec<&str> = join
                    .tables
                    .iter()
                    .map(|t| t.iceberg_ident.as_str())
                    .collect();
                assert_eq!(
                    idents,
                    ["lh.part", "lh.partsupp", "lh.supplier", "lh.nation"]
                );
                assert_eq!(join.conditions.len(), 3, "N-1 conditions for N=4 tables");
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// `detect_join` is driven by the `from` TREE, not the `involvedTables` count:
    /// a two-table `from` yields the unified `Join` shape with exactly those two
    /// tables even when `involvedTables` lists more (the old `TooManyTables`
    /// defensive belt is gone — the tree is authoritative).
    #[test]
    fn detect_join_follows_from_tree_not_involved_tables_count() {
        let mut request = join_request(Json::Null, equi_condition());
        request["involvedTables"].as_array_mut().unwrap().push(serde_json::json!({
            "name": "NATION",
            "columns": [{"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}],
        }));
        match detect_join(&request, &pd(&request)).expect("tree-driven, no decline") {
            JoinShape::Join(join) => {
                let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
                assert_eq!(names, ["CUSTOMER", "ORDERS"], "only the from-tree leaves");
            }
            other => panic!("expected Join over the two from-tree tables, got {other:?}"),
        }
    }

    /// An otherwise-eligible join whose virtual table name is absent from
    /// `TABLE_MAP` is a hard `Err` (stale virtual schema), not a decline — the
    /// same treatment the single-table path gives an unmapped involved table.
    #[test]
    fn join_with_unmapped_table_is_an_error() {
        let mut request = join_request(Json::Null, equi_condition());
        request["schemaMetadataInfo"]["adapterNotes"] =
            Json::String(serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer"}}).to_string());
        let err = detect_join(&request, &pd(&request))
            .expect_err("ORDERS is absent from TABLE_MAP: must be Err, not a decline");
        assert!(
            err.to_string().contains("ORDERS"),
            "error must name the unmapped table: {err}"
        );
    }

    // ---------------------------------------------------------------------------
    // Join rendering (task 3.3): disjoint-column guard + condition/filter/projection
    // rendering via the reused vs-expression translator.
    // ---------------------------------------------------------------------------

    /// Recover the [`DetectedJoin`] a request classifies to (the tests below all
    /// operate on the standard two-table CUSTOMER⋈ORDERS shape from `join_request`).
    fn detected_join(request: &Json) -> DetectedJoin {
        match detect_join(request, &pd(request)).expect("detected join shape") {
            JoinShape::Join(join) => join,
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// Two tables whose column names are genuinely disjoint (TPC-H `C_*` vs `O_*`)
    /// pass the guard, so bare column names resolve unambiguously.
    #[test]
    fn disjoint_schema_guard_passes_for_disjoint_column_names() {
        let request = join_request(Json::Null, equi_condition());
        let left = involved_table_columns(&request, "CUSTOMER");
        let right = involved_table_columns(&request, "ORDERS");
        assert!(
            disjoint_schema_guard(&left, &right),
            "C_* and O_* column sets are disjoint and must pass the guard"
        );
    }

    /// ANY overlapping column name fails the guard, and the failure is surfaced as
    /// a clean decline (`Ok(None)`) — the caller falls through to the unaccelerated
    /// path — never as an error.
    #[test]
    fn overlapping_column_name_fails_guard_and_declines_without_error() {
        let mut request = join_request(Json::Null, equi_condition());
        // Give BOTH sides a column with the same name.
        for table_idx in [0, 1] {
            request["involvedTables"][table_idx]["columns"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "name": "SHARED_KEY",
                    "dataType": {"type": "varchar", "size": 10}
                }));
        }

        let left = involved_table_columns(&request, "CUSTOMER");
        let right = involved_table_columns(&request, "ORDERS");
        assert!(
            !disjoint_schema_guard(&left, &right),
            "a shared column name must fail the disjoint guard"
        );

        // The whole rendering entry point declines cleanly, not with an Err.
        let detected = detected_join(&request);
        let outcome = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("a guard failure is a decline, not an error");
        assert!(
            outcome.is_none(),
            "a column-name collision must decline to the unaccelerated path"
        );
    }

    /// A simple equi-condition renders to the correct DataFusion SQL boolean
    /// expression via the reused translator, and is threaded verbatim into the
    /// rendered join's `condition` (→ `JoinSpec::condition`).
    #[test]
    fn join_condition_renders_via_translator() {
        assert_eq!(
            render_join_condition(&equi_condition()).as_deref(),
            Some(r#"("C_CUSTKEY" = "O_CUSTKEY")"#),
            "the equi-condition must render to a bare-name DataFusion boolean expr"
        );

        let request = join_request(Json::Null, equi_condition());
        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("disjoint, renderable join")
            .expect("a disjoint join must render, not decline");
        assert_eq!(rendered.condition, r#"("C_CUSTKEY" = "O_CUSTKEY")"#);
    }

    /// A WHERE filter referencing columns from BOTH sides renders correctly against
    /// the combined schema (bare names, disjoint → unambiguous).
    #[test]
    fn join_where_filter_spanning_both_sides_renders() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["filter"] = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 "right": {"type": "literal_string", "value": "1995-01-01"}},
            ],
        });

        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("disjoint, renderable join")
            .expect("must render");
        let filter = rendered
            .filter
            .expect("a cross-side WHERE filter must render");
        assert!(
            filter.contains(r#""C_NAME""#),
            "the left-side column must appear in the rendered filter: {filter}"
        );
        assert!(
            filter.contains(r#""O_ORDERDATE""#),
            "the right-side column must appear in the rendered filter: {filter}"
        );
        assert!(
            filter.contains("AND"),
            "the conjunction of both sides must render: {filter}"
        );
    }

    /// The cross-table projection attributes each projected column to its OWNING
    /// side's Exasol type: `C_NAME` from CUSTOMER (`VARCHAR(100)`), `O_ORDERDATE`
    /// from ORDERS (`DATE`).
    #[test]
    fn join_projection_emits_attribute_each_side_owning_type() {
        let request = join_request(Json::Null, equi_condition());
        let detected = detected_join(&request);
        let (projection, types) =
            extract_join_projection(&request, &pd(&request), &detected).expect("projectable");

        assert_eq!(
            projection,
            vec![
                ProjectionItem::Column("C_NAME".into()),
                ProjectionItem::Column("O_ORDERDATE".into()),
            ],
            "projection spans both tables in select-list order"
        );
        assert_eq!(
            types,
            vec!["VARCHAR(100)".to_string(), "DATE".to_string()],
            "each column's EMITS type comes from the side that owns it"
        );
    }

    // -----------------------------------------------------------------------
    // Join SQL-shape and decline routing (tasks 3.4 / 3.5)
    // -----------------------------------------------------------------------

    /// pushdown-planning-join "A join outside the broadcast contract is declined
    /// safely". Two independent facets are asserted together because they are the
    /// two ways a join leaves the broadcast contract:
    ///
    /// 1. A shape `detect_join` classifies `Ineligible` (a non-inner join node in the
    ///    tree, or a malformed shape) cannot be rendered at all — so it MUST map to a
    ///    `User` decline error, NEVER fall through to the single-table path (which
    ///    would scan only the first involved table and silently drop the join).
    ///    Spanning more than two tables, non-equi, and overlapping column names are
    ///    NOT Ineligible — they are served by the unified fallback.
    /// 2. An otherwise-eligible join whose two tables share a column name fails the
    ///    disjoint-column guard, so `render_broadcast_join` declines with `Ok(None)`.
    ///    The `vs-expression` translator emits only bare column names, so a two-scan
    ///    wrapper would carry an ambiguous `ON`/`WHERE`/`SELECT` — hence the router
    ///    treats `None` as "fallback cannot be built" and errors rather than emit a
    ///    wrong plan.
    #[test]
    fn join_outside_contract_declined_safely() {
        // Facet 1: every ineligible reason declines to a HARD error — a
        // client-facing F-UDF-CL-RUST-9001, NEVER a native re-plan. The message must
        // say so plainly (contains "declined"/"cannot") and MUST NOT claim a retry.
        for reason in [
            IneligibleJoinReason::NotInnerJoinType,
            IneligibleJoinReason::UnsupportedShape,
        ] {
            let err = ineligible_join_decline(reason);
            match err {
                UdfError::User(msg) => {
                    assert!(
                        msg.contains("join pushdown declined") && msg.contains("cannot"),
                        "ineligible reason {reason:?} must be a plain hard-error decline: {msg}"
                    );
                    assert!(
                        !msg.contains("retry"),
                        "ineligible reason {reason:?} must NOT claim a native retry: {msg}"
                    );
                }
                other => panic!("ineligible join must be a User decline, got {other:?}"),
            }
        }

        // An outer join reaches the decline path as Ineligible, never Join.
        let outer = join_request(
            serde_json::json!({"join_type": "left_outer"}),
            equi_condition(),
        );
        assert!(
            matches!(
                detect_join(&outer, &pd(&outer)),
                Ok(JoinShape::Ineligible(
                    IneligibleJoinReason::NotInnerJoinType
                ))
            ),
            "an outer join must classify Ineligible so the decline path is taken"
        );

        // Facet 2: overlapping column names → render declines with Ok(None).
        let mut request = join_request(Json::Null, equi_condition());
        for table_idx in [0, 1] {
            request["involvedTables"][table_idx]["columns"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "name": "SHARED_COL",
                    "dataType": {"type": "varchar", "size": 10}
                }));
        }
        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("guard failure is a decline, not an error");
        assert!(
            rendered.is_none(),
            "overlapping column names must decline broadcast rendering (Ok(None))"
        );
    }

    /// The unified fallback (N = 2): each side scanned through its own sharded
    /// fan-out, joined by an `INNER JOIN … ON` chain (the join condition on the join
    /// point), projecting the qualified select list. The single ORDERS-side-local
    /// filter is pushed into the ORDERS leg, so the outer WHERE has no residual. The
    /// two-table case uses the SAME `LHS_T*` renderer as N ≥ 3.
    #[test]
    fn two_table_join_falls_back_to_unified_n_scan_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["filter"] = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
            "right": {"type": "literal_string", "value": "1995-01-01"}
        });
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the two-table unified fallback must build");

        for alias in ["LHS_T0", "LHS_T1"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "both side fan-outs must appear as aliased derived-table subqueries: {sql}"
            );
        }
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the equi-condition must attach table-qualified as the join point's ON clause: {sql}"
        );
        assert!(
            sql.contains(r#"SELECT "LHS_T0"."C_NAME", "LHS_T1"."O_ORDERDATE" FROM"#),
            "the cross-table projection must drive the outer SELECT in order: {sql}"
        );
        // The lone ORDERS-side-local filter is pushed into the ORDERS leg, so no
        // residual conjunct remains and there is no outer WHERE.
        assert!(
            sql.contains("'1995-01-01'"),
            "the ORDERS-side-local filter must be pushed into that leg's fan-out: {sql}"
        );
        assert!(
            !sql.contains(" WHERE "),
            "every side-local filter is pushed into its leg, so no residual outer WHERE: {sql}"
        );
        // The unified fallback is an INNER JOIN chain, never a broadcast join block.
        assert!(sql.contains("INNER JOIN"), "{sql}");
        assert!(
            !sql.contains("\"join\":{"),
            "the fallback must not embed a broadcast join block: {sql}"
        );
    }

    // -----------------------------------------------------------------------
    // Qualified two-scan fallback (fix: qualified rendering independent of the
    // disjoint-column guard, and aggregate-over-join routed through two-scan)
    // -----------------------------------------------------------------------

    fn two_scan_tuning() -> JoinScanTuning {
        JoinScanTuning {
            cluster_nodes: 1,
            parallelism_factor: 1,
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 0,
            s3_max_connections: 1,
        }
    }

    /// A join whose two tables share a column name (`ID`) fails the disjoint guard
    /// (so the broadcast path declines), but the unified N-scan fallback still builds
    /// a correct, UNAMBIGUOUS wrapper (N = 2): the condition and projection reference
    /// `"LHS_T0"."ID"` / `"LHS_T1"."ID"`, never a bare ambiguous `"ID"`. This is the
    /// `EVENTS ⋈ LABELS ON a.id = b.id` regression.
    #[test]
    fn colliding_columns_render_qualified_unified_wrapper_without_error() {
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "EVENTS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "SCORE", "dataType": {"type": "double"}}]},
                {"name": "LABELS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "LABEL", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"name": "EVENTS", "type": "table"},
                    "right": {"name": "LABELS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                        "right": {"type": "column", "name": "ID", "tableName": "LABELS"}}},
                "selectList": [
                    {"type": "column", "name": "ID", "tableName": "EVENTS"},
                    {"type": "column", "name": "LABEL", "tableName": "LABELS"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP": {"EVENTS": "lh.events", "LABELS": "lh.labels"}})
                    .to_string()},
        });

        // Precondition: the shared ID column fails the disjoint guard, so broadcast
        // rendering declines (Ok(None)) — the very reason the OLD code errored.
        let left = involved_table_columns(&request, "EVENTS");
        let right = involved_table_columns(&request, "LABELS");
        assert!(!disjoint_schema_guard(&left, &right));
        let detected = detected_join(&request);
        assert!(
            render_broadcast_join(&request, &pd(&request), &detected)
                .unwrap()
                .is_none()
        );

        let sides = vec![
            resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
            resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the qualified unified fallback must build despite the column-name collision");

        assert!(
            sql.contains(r#"("LHS_T0"."ID" = "LHS_T1"."ID")"#),
            "the equi-condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T0"."ID""#) && sql.contains(r#""LHS_T1"."LABEL""#),
            "the projection must be table-qualified per owning side: {sql}"
        );
        assert!(sql.contains("INNER JOIN"), "{sql}");
    }

    /// The N-scan (N≥3) builder produces an `INNER JOIN … ON` chain — N distinct
    /// `LHS_T*` fan-out aliases, every one of the N-1 join conditions rendered
    /// table-qualified and greedily attached to its join point, and the select list
    /// qualified to its owning side — never an `Err` for an all-inner tree over
    /// resolvable tables (pushdown-planning-join "A three-or-more-table inner join
    /// falls back to an N-scan unaccelerated wrapper").
    #[test]
    fn build_n_scan_join_sql_produces_qualified_n_scan_wrapper() {
        let request = three_table_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
            resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("an all-inner N-scan wrapper must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."O_ORDERKEY" = "LHS_T2"."L_ORDERKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
        assert_eq!(
            sql.matches("INNER JOIN").count(),
            2,
            "conditions must attach across a two-hop INNER JOIN … ON chain: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T0"."C_NAME""#) && sql.contains(r#""LHS_T2"."L_QUANTITY""#),
            "the select list must be qualified to each column's owning side: {sql}"
        );
    }

    /// The N-scan builder also handles the Q1 shape (`supplier⋈nation⋈region`): three
    /// distinct `LHS_T*` fan-out aliases and both join conditions rendered
    /// table-qualified, never an `Err`.
    #[test]
    fn build_n_scan_join_sql_for_q1_shape_supplier_nation_region() {
        let request = q1_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("SUPPLIER", vec![("s3://w/s-0.parquet", 10)]),
            resolved_side("NATION", vec![("s3://w/n-0.parquet", 5)]),
            resolved_side("REGION", vec![("s3://w/r-0.parquet", 2)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the Q1-shape (supplier⋈nation⋈region) must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."S_NATIONKEY" = "LHS_T1"."N_NATIONKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."N_REGIONKEY" = "LHS_T2"."R_REGIONKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
    }

    /// The N-scan builder also handles the NQ3 shape
    /// (`part⋈partsupp⋈supplier⋈nation`, N=4): four distinct `LHS_T*` fan-out
    /// aliases and all three join conditions rendered table-qualified, never an
    /// `Err` — the builder generalizes past N=3.
    #[test]
    fn build_n_scan_join_sql_for_nq3_shape_part_partsupp_supplier_nation() {
        let request = nq3_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("PART", vec![("s3://w/p-0.parquet", 10)]),
            resolved_side("PARTSUPP", vec![("s3://w/ps-0.parquet", 40)]),
            resolved_side("SUPPLIER", vec![("s3://w/s-0.parquet", 5)]),
            resolved_side("NATION", vec![("s3://w/n-0.parquet", 3)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the NQ3-shape (part⋈partsupp⋈supplier⋈nation) must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2", "LHS_T3"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."P_PARTKEY" = "LHS_T1"."PS_PARTKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."PS_SUPPKEY" = "LHS_T2"."S_SUPPKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T2"."S_NATIONKEY" = "LHS_T3"."N_NATIONKEY""#),
            "third join condition must be table-qualified: {sql}"
        );
    }

    /// Three tables that ALL share a column name (`ID`) — the N-table analog of
    /// `colliding_columns_render_qualified_two_scan_without_error` — still build a
    /// correct, unambiguous N-scan wrapper: every `ID` reference (both join
    /// conditions and the select list) is table-qualified, never bare.
    #[test]
    fn build_n_scan_join_sql_renders_qualified_when_three_tables_share_column_name() {
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "EVENTS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "SCORE", "dataType": {"type": "double"}}]},
                {"name": "LABELS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "LABEL", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "TAGS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "TAG_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"name": "EVENTS", "type": "table"},
                        "right": {"name": "LABELS", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                            "right": {"type": "column", "name": "ID", "tableName": "LABELS"}}},
                    "right": {"name": "TAGS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "ID", "tableName": "LABELS"},
                        "right": {"type": "column", "name": "ID", "tableName": "TAGS"}}},
                "selectList": [
                    {"type": "column", "name": "ID", "tableName": "EVENTS"},
                    {"type": "column", "name": "LABEL", "tableName": "LABELS"},
                    {"type": "column", "name": "TAG_NAME", "tableName": "TAGS"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"EVENTS": "lh.events", "LABELS": "lh.labels", "TAGS": "lh.tags"}})
                    .to_string()},
        });
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
            resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
            resolved_side("TAGS", vec![("s3://w/t-0.parquet", 10)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("three tables sharing an ID column must still build, never Err");

        assert!(
            sql.contains(r#""LHS_T0"."ID" = "LHS_T1"."ID""#),
            "first condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."ID" = "LHS_T2"."ID""#),
            "second condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        // The outer wrapper's own SELECT list (as opposed to each independently
        // scanned, unambiguous per-side fan-out's inner projection) must qualify
        // every shared `ID` reference — never a bare, cross-side-ambiguous `"ID"`.
        assert!(
            sql.starts_with(r#"SELECT "LHS_T0"."ID", "LHS_T1"."LABEL", "LHS_T2"."TAG_NAME" FROM "#),
            "the outer SELECT list must qualify the shared ID column, never bare: {sql}"
        );
    }

    /// Group D (task 4.1): the two-table above-broadcast-threshold fallback renders
    /// its FROM as a left-to-right `INNER JOIN … ON` chain (not a comma cross-join +
    /// flat WHERE). The single equi-condition attaches as the join point's `ON`
    /// clause, table-qualified, at the point that brings the second leg into scope.
    #[test]
    fn above_threshold_join_falls_back_inner_join_on() {
        let request = join_request(Json::Null, equi_condition());
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the above-threshold two-table fallback must build");

        assert!(
            sql.contains("INNER JOIN"),
            "the fallback FROM must be an INNER JOIN chain, not a comma cross-join: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T0" INNER JOIN"#),
            "the first leg must be the left side of the INNER JOIN chain: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the equi-condition must attach table-qualified as the join point's ON clause: {sql}"
        );
        assert!(
            !sql.contains(r#"AS "LHS_T0", "#),
            "the legacy comma cross-join between legs must be gone: {sql}"
        );
    }

    /// Group D (task 4.1): a three-table inner join renders a two-hop
    /// `INNER JOIN … ON` chain, each condition greedily attached at the earliest
    /// join point where all its tables are in scope (by table-name set). No residual
    /// filter → no outer WHERE.
    #[test]
    fn three_table_join_inner_join_on_chain() {
        let request = three_table_join_request();
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
            resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the three-table inner-join chain must build");

        assert_eq!(
            sql.matches("INNER JOIN").count(),
            2,
            "N=3 tables → a two-hop INNER JOIN chain: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the first condition attaches at the join point bringing LHS_T1 into scope: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T2" ON (("LHS_T1"."O_ORDERKEY" = "LHS_T2"."L_ORDERKEY"))"#),
            "the second condition attaches at the join point bringing LHS_T2 into scope: {sql}"
        );
        assert!(
            !sql.contains(" WHERE "),
            "every condition lives in an ON clause and there is no residual filter, so no \
             outer WHERE: {sql}"
        );
    }

    /// Group D (tasks 4.1 + 4.2): greedy-attach by table-name set AND the WHERE split.
    /// A star shape `(N1 ⋈ (N2 ⋈ FACT))` where BOTH conditions reference FACT (the
    /// deepest leaf, `LHS_T2`): both attach at the last join point, so the middle
    /// join point (bringing `LHS_T2`'s sibling `LHS_T1` into scope) has no
    /// newly-resolvable condition and renders `ON 1=1`. A CUSTOMER-side-local WHERE
    /// conjunct is pushed into that leg's fan-out (never re-applied in the outer
    /// WHERE); only the cross-table residual conjunct survives in the outer WHERE.
    #[test]
    fn join_conditions_greedy_attach_and_side_local_pushdown() {
        let cond_n2_fact = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "N2_KEY", "tableName": "N2"},
            "right": {"type": "column", "name": "F_N2KEY", "tableName": "FACT"}});
        let cond_n1_fact = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "N1_KEY", "tableName": "N1"},
            "right": {"type": "column", "name": "F_N1KEY", "tableName": "FACT"}});
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "N1", "columns": [
                    {"name": "N1_KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "N1_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "N2", "columns": [
                    {"name": "N2_KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "FACT", "columns": [
                    {"name": "F_N1KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "F_N2KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "F_VALUE", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"name": "N1", "type": "table"},
                    "right": {"type": "join", "join_type": "inner",
                        "left": {"name": "N2", "type": "table"},
                        "right": {"name": "FACT", "type": "table"},
                        "condition": cond_n2_fact},
                    "condition": cond_n1_fact},
                "selectList": [
                    {"type": "column", "name": "N1_NAME", "tableName": "N1"},
                    {"type": "column", "name": "F_VALUE", "tableName": "FACT"}],
                "filter": {"type": "predicate_and", "expressions": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "N1_NAME", "tableName": "N1"},
                     "right": {"type": "literal_string", "value": "ACME"}},
                    {"type": "predicate_greater",
                     "left": {"type": "column", "name": "F_VALUE", "tableName": "FACT"},
                     "right": {"type": "column", "name": "N1_KEY", "tableName": "N1"}}]},
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"N1": "lh.n1", "N2": "lh.n2", "FACT": "lh.fact"}})
                    .to_string()},
        });
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("N1", vec![("s3://w/n1-0.parquet", 10)]),
            resolved_side("N2", vec![("s3://w/n2-0.parquet", 10)]),
            resolved_side("FACT", vec![("s3://w/f-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the star-shape greedy-attach fallback must build");

        // The middle join point brings N2 (LHS_T1) into scope but neither condition is
        // resolvable there (both also need FACT / LHS_T2) → ON 1=1.
        assert!(
            sql.contains(r#"AS "LHS_T1" ON 1=1"#),
            "a join point with no newly-resolvable condition must render ON 1=1: {sql}"
        );
        // Both conditions greedily attach at the last join point (LHS_T2), AND-conjoined.
        assert!(
            sql.contains(r#"AS "LHS_T2" ON (("LHS_T1"."N2_KEY" = "LHS_T2"."F_N2KEY")) AND (("LHS_T0"."N1_KEY" = "LHS_T2"."F_N1KEY"))"#),
            "both FACT-touching conditions must attach greedily at the final join point: {sql}"
        );

        // Task 4.2: the N1-side-local conjunct is pushed into N1's fan-out leg…
        assert!(
            sql.contains("'ACME'"),
            "the side-local conjunct must be pushed into its leg's fan-out: {sql}"
        );
        // …and NOT re-applied in the outer WHERE, which keeps only the cross-table residual.
        let where_clause = &sql[sql
            .find(" WHERE ")
            .expect("the cross-table residual must remain in an outer WHERE")..];
        assert!(
            !where_clause.contains("ACME"),
            "the side-local conjunct must NOT be duplicated in the outer WHERE: {sql}"
        );
        assert!(
            where_clause.contains(r#""LHS_T2"."F_VALUE""#)
                && where_clause.contains(r#""LHS_T0"."N1_KEY""#),
            "the cross-table residual conjunct must render qualified in the outer WHERE: {sql}"
        );
    }

    /// An aggregate over a join (`COUNT(*), MIN(o.O_ORDERDATE)`) routes through the
    /// unified N-scan wrapper and lets Exasol evaluate the aggregate over the
    /// materialized join — a two-column result (`COUNT(*)`,
    /// `MIN("LHS_T1"."O_ORDERDATE")`), not the full-row projection the old code
    /// emitted (which produced the "expected 2 columns but pushdown has 5" failure).
    #[test]
    fn aggregate_over_join_renders_exasol_aggregate_over_unified_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["selectList"] = serde_json::json!([
            {"type": "function_aggregate", "name": "COUNT", "arguments": []},
            {"type": "function_aggregate", "name": "MIN", "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}]},
        ]);

        assert!(
            join_requires_exasol_postprocessing(&pd(&request)),
            "an aggregate select list must force the Exasol-executed fallback path"
        );

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("aggregate-over-join must build the unified wrapper");

        assert!(sql.contains("COUNT(*)"), "COUNT(*) must be rendered: {sql}");
        assert!(
            sql.contains(r#"MIN("LHS_T1"."O_ORDERDATE")"#),
            "MIN must qualify its argument to the owning side: {sql}"
        );
        assert!(
            sql.starts_with(r#"SELECT COUNT(*), MIN("LHS_T1"."O_ORDERDATE") FROM"#),
            "the outer SELECT must be exactly the two aggregate columns: {sql}"
        );
        assert!(
            sql.contains("INNER JOIN") && !sql.contains("\"join\":{"),
            "aggregate-over-join is an INNER JOIN chain fallback, never a broadcast block: {sql}"
        );
    }

    /// A three-side `alias_of` map ({CUSTOMER→LHS_T0, ORDERS→LHS_T1,
    /// LINEITEM→LHS_T2}) for the seam-unification tests, matching the `LHS_T*` scheme
    /// [`build_n_scan_alias_map`] produces from resolved sides.
    fn seam_alias_of() -> HashMap<String, String> {
        HashMap::from([
            ("CUSTOMER".to_string(), "LHS_T0".to_string()),
            ("ORDERS".to_string(), "LHS_T1".to_string()),
            ("LINEITEM".to_string(), "LHS_T2".to_string()),
        ])
    }

    /// The finding-#1 seam: a select item that is a SCALAR FUNCTION WRAPPING
    /// AGGREGATES — the reported `ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1
    /// ELSE 0 END) / COUNT(*), 2)` — renders through `render_selectlist_item_qualified`
    /// (NOT `None`, no decline), with its nested aggregates spliced verbatim and its
    /// nested column argument table-qualified to the owning side. Before the vs-expression
    /// aggregate arm + seam unification this recursed into the translator's catch-all and
    /// returned `None`, declining the whole grouped-join pushdown at every arity.
    #[test]
    fn render_selectlist_item_qualified_renders_scalar_over_aggregate() {
        let alias_of = seam_alias_of();
        let sum_case = serde_json::json!({
            "type": "function_aggregate", "name": "SUM", "distinct": false,
            "arguments": [{
                "type": "function_scalar", "name": "CASE", "arguments": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"},
                     "right": {"type": "literal_string", "value": "R"}},
                    {"type": "literal_exactnumeric", "value": 1},
                    {"type": "literal_exactnumeric", "value": 0}]}]
        });
        let count_star = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
        });
        let item = serde_json::json!({
            "type": "function_scalar", "name": "ROUND", "arguments": [
                {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                    {"type": "function_scalar", "name": "MULT", "arguments": [
                        {"type": "literal_double", "value": 100.0},
                        sum_case]},
                    count_star]},
                {"type": "literal_exactnumeric", "value": 2}]
        });

        let sql = render_selectlist_item_qualified(&item, &alias_of)
            .expect("a scalar-over-aggregate item must render, never decline to None");
        assert!(
            sql.contains(r#"SUM(CASE WHEN ("LHS_T2"."L_RETURNFLAG" = 'R') THEN 1 ELSE 0 END)"#),
            "the nested SUM(CASE ...) must render with its column table-qualified: {sql}"
        );
        assert!(
            sql.contains("COUNT(*)"),
            "the nested COUNT(*) must render as the star case: {sql}"
        );
    }

    /// The finding-#1 byte-compatibility guard: a TOP-LEVEL bare aggregate renders
    /// through the unified `render_selectlist_item_qualified` byte-identically to the
    /// former dedicated `render_aggregate_qualified` — a single-arg aggregate as
    /// `NAME("ALIAS"."COL")`, `COUNT(*)` as `COUNT(*)`, and `DISTINCT` preserved. The
    /// exact expected strings are captured here so any future drift at the seam fails.
    #[test]
    fn render_selectlist_item_qualified_top_level_aggregate_byte_compatible() {
        let alias_of = seam_alias_of();

        let sum = serde_json::json!({
            "type": "function_aggregate", "name": "SUM", "distinct": false,
            "arguments": [{"type": "column", "name": "O_TOTALPRICE", "tableName": "ORDERS"}]
        });
        assert_eq!(
            render_selectlist_item_qualified(&sum, &alias_of).as_deref(),
            Some(r#"SUM("LHS_T1"."O_TOTALPRICE")"#)
        );

        let count_star = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
        });
        assert_eq!(
            render_selectlist_item_qualified(&count_star, &alias_of).as_deref(),
            Some("COUNT(*)")
        );

        let count_distinct = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "distinct": true,
            "arguments": [{"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"}]
        });
        assert_eq!(
            render_selectlist_item_qualified(&count_distinct, &alias_of).as_deref(),
            Some(r#"COUNT(DISTINCT "LHS_T0"."C_CUSTKEY")"#)
        );
    }

    /// A bare-column ORDER BY over a join is rendered table-qualified in the unified
    /// wrapper (with explicit direction + NULL placement), so Exasol — which has
    /// delegated the ordering — sorts on the unambiguous, owning-side column.
    #[test]
    fn order_by_over_join_renders_qualified_in_unified_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["orderBy"] = serde_json::json!([
            {"expression": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
             "isAscending": true, "nullsLast": false},
        ]);

        assert!(join_requires_exasol_postprocessing(&pd(&request)));

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("ordered unified wrapper must build");
        assert!(
            sql.contains(r#"ORDER BY "LHS_T1"."O_ORDERDATE" ASC NULLS FIRST"#),
            "ORDER BY must be table-qualified with explicit direction/nulls: {sql}"
        );
    }

    /// `join_requires_exasol_postprocessing` fires for every clause the broadcast
    /// in-UDF join cannot serve, and is false for a plain projection+filter join.
    #[test]
    fn post_processing_predicate_covers_every_forcing_clause() {
        let plain = join_request(Json::Null, equi_condition());
        assert!(!join_requires_exasol_postprocessing(&pd(&plain)));

        let mut limited = join_request(Json::Null, equi_condition());
        limited["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 10});
        assert!(join_requires_exasol_postprocessing(&pd(&limited)));

        let mut grouped = join_request(Json::Null, equi_condition());
        grouped["pushdownRequest"]["groupBy"] =
            serde_json::json!([{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}]);
        assert!(join_requires_exasol_postprocessing(&pd(&grouped)));

        let mut having = join_request(Json::Null, equi_condition());
        having["pushdownRequest"]["having"] =
            serde_json::json!({"type": "literal_bool", "value": true});
        assert!(join_requires_exasol_postprocessing(&pd(&having)));
    }

    // -----------------------------------------------------------------------
    // Per-side pruning (PR #70 review): side-local conjunct attribution,
    // projection narrowing, and per-side filter pushdown in the fallback path.
    // -----------------------------------------------------------------------

    /// A conjunct referencing only one side's columns is attributed to that side
    /// alone: the CUSTOMER-only conjunct threads to CUSTOMER, the ORDERS-only
    /// conjunct to ORDERS, and neither leaks to the other.
    #[test]
    fn side_local_filter_attributes_conjuncts_to_owning_side() {
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 "right": {"type": "literal_string", "value": "1995-01-01"}},
            ],
        });

        let cust = render_df_filter_safe(
            &side_local_filter(&filter, "CUSTOMER").expect("a CUSTOMER-local conjunct exists"),
        )
        .expect("renders");
        assert!(
            cust.contains("C_NAME") && !cust.contains("O_ORDERDATE"),
            "CUSTOMER side-local filter must carry only C_NAME: {cust}"
        );

        let ord = render_df_filter_safe(
            &side_local_filter(&filter, "ORDERS").expect("an ORDERS-local conjunct exists"),
        )
        .expect("renders");
        assert!(
            ord.contains("O_ORDERDATE") && !ord.contains("C_NAME"),
            "ORDERS side-local filter must carry only O_ORDERDATE: {ord}"
        );
    }

    /// A cross-table conjunct (references both sides) and an OR spanning both sides
    /// are withheld from BOTH sides' pruning — only the outer wrapper's WHERE
    /// applies them. A single-side-local conjunct alongside a cross-table one is
    /// still extracted for its side.
    #[test]
    fn side_local_filter_withholds_cross_table_and_or_conjuncts() {
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                // cross-table: references CUSTOMER and ORDERS
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                 "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}},
                // CUSTOMER-local
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
            ],
        });
        let cust = render_df_filter_safe(
            &side_local_filter(&filter, "CUSTOMER").expect("CUSTOMER-local conjunct present"),
        )
        .expect("renders");
        assert!(
            cust.contains("C_NAME") && !cust.contains("O_CUSTKEY"),
            "the cross-table conjunct must NOT be pushed to CUSTOMER: {cust}"
        );
        assert!(
            side_local_filter(&filter, "ORDERS").is_none(),
            "ORDERS is only referenced by the cross-table conjunct, so nothing is side-local to it"
        );

        // An OR spanning both sides is one opaque conjunct referencing both → withheld.
        let or_filter = serde_json::json!({
            "type": "predicate_or",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 "right": {"type": "literal_string", "value": "1995-01-01"}},
            ],
        });
        assert!(side_local_filter(&or_filter, "CUSTOMER").is_none());
        assert!(side_local_filter(&or_filter, "ORDERS").is_none());

        // An OR referencing only ONE side is side-local to it (still prunable).
        let one_side_or = serde_json::json!({
            "type": "predicate_or",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "GLOBEX"}},
            ],
        });
        assert!(
            side_local_filter(&one_side_or, "CUSTOMER").is_some(),
            "an OR over one side alone is side-local and prunable"
        );
        assert!(side_local_filter(&one_side_or, "ORDERS").is_none());
    }

    /// A filter that is a single (non-AND) conjunct is attributed to its owning side
    /// without a top-level AND wrapper.
    #[test]
    fn side_local_filter_handles_a_single_conjunct() {
        let single = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
            "right": {"type": "literal_string", "value": "ACME"}
        });
        assert!(side_local_filter(&single, "CUSTOMER").is_some());
        assert!(side_local_filter(&single, "ORDERS").is_none());
    }

    /// Attribution is by `tableName`, NOT by column name: with a column name shared
    /// across both tables (`ID`), a conjunct on `EVENTS.ID` is side-local to EVENTS
    /// only and is never applied to LABELS (which also has an `ID`). This is the
    /// shared-column-name safety the whole per-side pruning rests on.
    #[test]
    fn side_local_filter_attributes_shared_column_by_table_not_name() {
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                 "right": {"type": "literal_exactnumeric", "value": 5}},
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "LABEL", "tableName": "LABELS"},
                 "right": {"type": "literal_string", "value": "x"}},
            ],
        });

        let events = render_df_filter_safe(
            &side_local_filter(&filter, "EVENTS").expect("EVENTS.ID conjunct is side-local"),
        )
        .expect("renders");
        assert!(
            events.contains("ID") && events.contains('5'),
            "EVENTS side-local filter must carry the ID > 5 predicate: {events}"
        );

        let labels = render_df_filter_safe(
            &side_local_filter(&filter, "LABELS").expect("LABELS.LABEL conjunct is side-local"),
        )
        .expect("renders");
        assert!(
            labels.contains("LABEL") && !labels.contains('5'),
            "the EVENTS.ID predicate must NOT be applied to LABELS despite the shared name: {labels}"
        );
    }

    /// The fallback projection is narrowed to the columns the outer wrapper
    /// references for a side — SELECT list + join condition + WHERE — preserving
    /// the full-column order/type, and dropping columns referenced nowhere.
    #[test]
    fn referenced_side_columns_narrows_to_used_columns() {
        let pushdown_req = serde_json::json!({
            "selectList": [{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}],
            "filter": {"type": "predicate_equal",
                "left": {"type": "column", "name": "C_ADDRESS", "tableName": "CUSTOMER"},
                "right": {"type": "literal_string", "value": "z"}},
        });
        let condition = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}
        });
        let full = vec![
            ("C_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
            ("C_NAME".to_string(), "VARCHAR(100)".to_string()),
            ("C_ADDRESS".to_string(), "VARCHAR(100)".to_string()),
            ("C_PHONE".to_string(), "VARCHAR(20)".to_string()),
        ];
        let narrowed = referenced_side_columns(&pushdown_req, &condition, "CUSTOMER", &full);
        let names: Vec<&str> = narrowed.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec!["C_CUSTKEY", "C_NAME", "C_ADDRESS"],
            "narrows to condition + select + filter columns, in full-column order, dropping C_PHONE"
        );
        // The kept columns retain their full-column Exasol types.
        assert_eq!(
            narrowed[1],
            ("C_NAME".to_string(), "VARCHAR(100)".to_string())
        );
    }

    /// An absent (or empty) SELECT list means the wrapper projects every column via
    /// `SELECT *`, so no narrowing is applied — all columns are kept.
    #[test]
    fn referenced_side_columns_keeps_all_when_select_list_absent() {
        let condition = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}
        });
        let full = vec![
            ("C_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
            ("C_NAME".to_string(), "VARCHAR(100)".to_string()),
        ];
        let narrowed =
            referenced_side_columns(&serde_json::json!({}), &condition, "CUSTOMER", &full);
        assert_eq!(
            narrowed, full,
            "an absent select list ⇒ SELECT *, keep every column"
        );
    }

    /// A per-side fan-out pushes its side-local filter down as a DataFusion
    /// `ScanSpec.filter` (present in the common blob); absent when there is none.
    ///
    /// Regression (PR #70 e2e "No field named \"O\".\"O_ORDERDATE\""): Exasol sends
    /// each column with a `tableAlias` (the query's `FROM fact_orders o` alias). The
    /// fan-out is a SINGLE-TABLE scan over a relation with BARE uppercase columns, so
    /// its pushed filter MUST render bare — the alias must be stripped, or the
    /// alias-qualified reference fails to resolve against the fan-out.
    #[test]
    fn side_fan_out_pushes_bare_side_local_filter_into_common_blob() {
        let side = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]);
        let cols = vec![
            ("O_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
            ("O_ORDERDATE".to_string(), "DATE".to_string()),
        ];
        // Exactly the Exasol shape: BOTH tableName AND tableAlias present.
        let filter = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS", "tableAlias": "O"},
            "right": {"type": "literal_string", "value": "1995-01-01"}
        });

        let sql_with = build_side_fan_out_sql(
            &side,
            &cols,
            Some(&filter),
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        );
        let common = common_arg_literal(&sql_with);
        assert!(
            common.contains("\"filter\"") && common.contains("O_ORDERDATE"),
            "the side-local filter must be pushed into the fan-out common blob: {common}"
        );
        assert!(
            !common.contains(r#"\"O\".\"O_ORDERDATE\""#)
                && !common.contains(r#""O"."O_ORDERDATE""#),
            "the fan-out filter MUST be bare (alias stripped), never alias-qualified: {common}"
        );

        let sql_without = build_side_fan_out_sql(
            &side,
            &cols,
            None,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        );
        let common_none = common_arg_literal(&sql_without);
        assert!(
            !common_none.contains("\"filter\""),
            "no side-local filter ⇒ no filter field in the common blob: {common_none}"
        );
    }

    /// A multi-shard join leg routes through the new distributor + scalar scan
    /// primitive: the fan-out `GROUP BY shard_key` lives in the distributor and the
    /// outer scalar `SCAN` is ungrouped, with NO `SELECT * FROM (...)` materialization
    /// wrapper (decision [5]). The leg is a bare subquery the outer join wrapper reads.
    #[test]
    fn side_fan_out_routes_through_distributor_scalar_scan_no_wrapper() {
        let side = resolved_side(
            "ORDERS",
            vec![("s3://w/o-0.parquet", 100), ("s3://w/o-1.parquet", 100)],
        );
        let cols = vec![("O_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string())];
        // Force two shards: two nodes × factor 1 over two files.
        let tuning = JoinScanTuning {
            cluster_nodes: 2,
            parallelism_factor: 1,
            ..two_scan_tuning()
        };
        let sql =
            build_side_fan_out_sql(&side, &cols, None, &tuning, "SCAN", "MERGE", "DISTRIBUTE");

        assert!(
            !sql.contains("SELECT * FROM ("),
            "the leg must not use a SELECT * materialization wrapper: {sql}"
        );
        assert!(
            sql.starts_with("SELECT SCAN("),
            "the leg is the outer ungrouped scalar scan itself: {sql}"
        );
        assert!(
            sql.contains("DISTRIBUTE(files) FROM (VALUES")
                && sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
            "the leg's fan-out GROUP BY shard_key must live in the distributor: {sql}"
        );
    }

    /// The broadcast fact side routes through the same distributor + scalar scan
    /// primitive (task 3.4): a multi-file fact side fans out via the nested
    /// distributor under an outer ungrouped scalar `SCAN`, with no `SELECT * FROM
    /// (...)` wrapper; the dimension side rides once in the common blob's join block.
    #[test]
    fn broadcast_fact_side_uses_distributor_scalar_scan() {
        let fact = resolved_side(
            "LINEITEM",
            vec![("s3://w/l-0.parquet", 1000), ("s3://w/l-1.parquet", 1000)],
        );
        let dimension = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]);
        let sides = JoinSides {
            fact,
            dimension,
            broadcast_eligible: true,
        };
        let rendered = RenderedJoinPushdown {
            condition: r#""L_ORDERKEY" = "O_ORDERKEY""#.to_string(),
            filter: None,
            projection: vec![ProjectionItem::Column("L_ORDERKEY".to_string())],
            projection_types: vec!["DECIMAL(20,0)".to_string()],
        };
        let tuning = JoinScanTuning {
            cluster_nodes: 2,
            parallelism_factor: 1,
            ..two_scan_tuning()
        };
        let sql =
            build_broadcast_join_sql(&sides, &rendered, &tuning, "SCAN", "MERGE", "DISTRIBUTE");

        assert!(
            !sql.contains("SELECT * FROM ("),
            "the broadcast fact side must not use a SELECT * wrapper: {sql}"
        );
        assert!(
            sql.starts_with("SELECT SCAN("),
            "the fact side is the outer ungrouped scalar scan itself: {sql}"
        );
        assert!(
            sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
            "the fact side fans out via the nested shard_key distributor: {sql}"
        );
    }

    /// The broadcast path is UNCHANGED by the per-side pruning fix: `render_broadcast_join`
    /// still renders `rendered.filter` exactly as before, PRESERVING Exasol's native
    /// `tableAlias` qualifier (the in-UDF `build_join_sql` join resolves it). This is
    /// the mechanical guard the reviewer asked for — the two-scan fan-out's bare
    /// stripping must NOT leak into, nor alter, the broadcast rendering.
    #[test]
    fn render_broadcast_join_preserves_native_table_alias_unchanged() {
        let mut request = join_request(Json::Null, equi_condition());
        // Give every join column node Exasol's native tableAlias, as the live cluster does.
        request["pushdownRequest"]["filter"] = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS", "tableAlias": "O"},
            "right": {"type": "literal_string", "value": "1995-01-01"}
        });
        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("renders")
            .expect("disjoint join renders");
        let filter = rendered.filter.expect("filter renders");
        assert!(
            filter.contains(r#""O"."O_ORDERDATE""#),
            "broadcast rendering must preserve Exasol's native tableAlias (unchanged): {filter}"
        );
    }

    /// End-to-end fallback wiring: the unified wrapper prunes each leg (side-local
    /// filter pushed into BOTH fan-out common blobs) AND narrows each leg's
    /// projection (an involved column referenced nowhere in the wrapper is dropped).
    /// Here BOTH filter conjuncts are side-local (one per leg), so — under the task
    /// 4.2 split — the outer WHERE has no residual conjunct and is omitted entirely;
    /// the join condition attaches to the INNER JOIN's ON clause instead.
    #[test]
    fn unified_join_prunes_and_narrows_each_leg() {
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "CUSTOMER", "columns": [
                    {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
                    {"name": "C_ADDRESS", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "ORDERS", "columns": [
                    {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "O_ORDERDATE", "dataType": {"type": "date"}},
                    {"name": "O_TOTALPRICE", "dataType": {"type": "decimal", "precision": 20, "scale": 2}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"name": "CUSTOMER", "type": "table"},
                    "right": {"name": "ORDERS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                        "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}}},
                "selectList": [
                    {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                    {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}],
                "filter": {"type": "predicate_and", "expressions": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                     "right": {"type": "literal_string", "value": "ACME"}},
                    {"type": "predicate_greater",
                     "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                     "right": {"type": "literal_string", "value": "1995-01-01"}}]},
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}})
                    .to_string()},
        });

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("unified wrapper must build");

        // Finding 3: columns referenced nowhere in the wrapper are dropped from the legs.
        assert!(
            !sql.contains("C_ADDRESS"),
            "an unreferenced CUSTOMER column must be narrowed out of the fan-out: {sql}"
        );
        assert!(
            !sql.contains("O_TOTALPRICE"),
            "an unreferenced ORDERS column must be narrowed out of the fan-out: {sql}"
        );

        // Finding 2: each leg gets its own side-local filter pushed into its common blob.
        assert_eq!(
            sql.matches("\"filter\"").count(),
            2,
            "both fan-out legs must carry a side-local ScanSpec.filter: {sql}"
        );

        // Both side-local conjuncts are pushed into their legs' common blobs; the
        // outer WHERE keeps only cross-table residual, of which there is none here.
        assert!(
            sql.contains("'ACME'") && sql.contains("'1995-01-01'"),
            "each leg's side-local conjunct must be pushed into its fan-out: {sql}"
        );
        assert!(
            !sql.contains(" WHERE "),
            "no cross-table residual conjunct remains, so the outer WHERE is omitted: {sql}"
        );
        // The join condition attaches to the INNER JOIN chain's ON clause.
        assert!(
            sql.contains(r#"ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the equi-condition attaches to the join point's ON clause: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Join side selection + broadcast threshold: `select_broadcast_sides`.
    // The pure core of the two-table broadcast role/threshold decision — exercised
    // without a live Iceberg catalog. `plan_join` resolves each side via
    // `resolve_one_join_side` and delegates here, so this covers the decision.
    // ---------------------------------------------------------------------------

    /// The default `JOIN_BROADCAST_MAX_BYTES` (128 MiB).
    const BROADCAST_MAX: u64 = 134_217_728;

    /// Build a resolved join side with a given `(path, byte_size)` file list.
    /// Storage/schema/root are populated so the tests can assert the full resolved
    /// payload rides along with the selected role; only the byte totals drive
    /// selection.
    fn resolved_side(table_name: &str, files: Vec<(&str, u64)>) -> ResolvedJoinSide {
        let lower = table_name.to_lowercase();
        ResolvedJoinSide::new(
            table_name.to_string(),
            format!("lh.{lower}"),
            format!("s3://warehouse/lh/{lower}"),
            files
                .into_iter()
                .map(|(p, s)| FileEntry::new(p, s))
                .collect(),
            vec![LogicalField {
                field_id: 1,
                name: format!("{table_name}_KEY"),
                arrow_type: "int64".to_string(),
                nullable: false,
                initial_default: None,
            }],
            Vec::new(),
            sample_storage(),
        )
    }

    /// `total_bytes` is the saturating sum of every file's `file_size_in_bytes`
    /// (the Iceberg-manifest size — no Parquet read).
    #[test]
    fn resolved_side_sums_file_bytes_saturating() {
        assert_eq!(
            resolved_side("ORDERS", vec![("a", 100), ("b", 250), ("c", 4)]).total_bytes,
            354
        );
        // Empty side ⇒ zero bytes.
        assert_eq!(resolved_side("EMPTY", vec![]).total_bytes, 0);
        // A byte total that would overflow u64 saturates to u64::MAX (treated as
        // "far over any threshold"), never wraps.
        assert_eq!(
            resolved_side("HUGE", vec![("x", u64::MAX), ("y", 1)]).total_bytes,
            u64::MAX
        );
    }

    /// The smaller side by bytes is the dimension; the larger is the fact, and the
    /// full resolved payload (files, schema, root, storage, idents) rides along
    /// with each role for tasks 3.3/3.4. Here the LEFT argument is smaller.
    #[test]
    fn dimension_is_left_when_left_side_is_smaller() {
        let customer = resolved_side("CUSTOMER", vec![("c1", 1_000)]);
        let orders = resolved_side("ORDERS", vec![("o1", 50_000), ("o2", 50_000)]);
        let sides = select_broadcast_sides(customer, orders, BROADCAST_MAX);

        assert_eq!(sides.dimension.table_name, "CUSTOMER");
        assert_eq!(sides.fact.table_name, "ORDERS");
        assert_eq!(sides.dimension.total_bytes, 1_000);
        assert_eq!(sides.fact.total_bytes, 100_000);
        assert!(
            sides.broadcast_eligible,
            "1000 bytes is well under the 128 MiB threshold"
        );
        // Resolved payload travels with the role.
        assert_eq!(sides.dimension.iceberg_ident, "lh.customer");
        assert_eq!(sides.fact.iceberg_ident, "lh.orders");
        assert_eq!(sides.dimension.files, vec![FileEntry::new("c1", 1_000)]);
        assert_eq!(sides.dimension.table_root, "s3://warehouse/lh/customer");
        assert_eq!(sides.dimension.logical_schema.len(), 1);
        assert_eq!(sides.dimension.effective_storage, sample_storage());
    }

    /// Reversing the FROM-clause order (larger side first) still selects the
    /// smaller side as the dimension — selection is by byte size, not position.
    #[test]
    fn dimension_is_right_when_right_side_is_smaller() {
        let orders = resolved_side("ORDERS", vec![("o1", 50_000), ("o2", 50_000)]);
        let customer = resolved_side("CUSTOMER", vec![("c1", 1_000)]);
        let sides = select_broadcast_sides(orders, customer, BROADCAST_MAX);

        assert_eq!(sides.dimension.table_name, "CUSTOMER");
        assert_eq!(sides.fact.table_name, "ORDERS");
        assert_eq!(sides.dimension.total_bytes, 1_000);
        assert!(sides.broadcast_eligible);
    }

    /// The dimension (smaller) side exceeding the threshold is reported as NOT
    /// broadcast-eligible — cleanly via the flag, never an error — so the caller
    /// builds the deterministic unaccelerated two-scan fallback (decision-log [2]).
    #[test]
    fn dimension_over_threshold_is_not_broadcast_eligible() {
        let part = resolved_side("PART", vec![("p1", 200)]);
        let lineitem = resolved_side("LINEITEM", vec![("l1", 900)]);
        // Threshold 100 is below even the smaller side's 200 bytes.
        let sides = select_broadcast_sides(part, lineitem, 100);

        assert_eq!(
            sides.dimension.table_name, "PART",
            "PART (200 bytes) is the smaller side"
        );
        assert_eq!(sides.fact.table_name, "LINEITEM");
        assert!(
            !sides.broadcast_eligible,
            "dimension total 200 > threshold 100: not broadcast-eligible"
        );
    }

    /// A dimension exactly AT the threshold is eligible (inclusive `<=`); one byte
    /// over is not — the boundary the byte-size decision hinges on.
    #[test]
    fn threshold_boundary_is_inclusive() {
        let at = select_broadcast_sides(
            resolved_side("DIM", vec![("d", 100)]),
            resolved_side("FACT", vec![("f", 10_000)]),
            100,
        );
        assert!(
            at.broadcast_eligible,
            "dimension == threshold must be eligible"
        );

        let over = select_broadcast_sides(
            resolved_side("DIM", vec![("d", 101)]),
            resolved_side("FACT", vec![("f", 10_000)]),
            100,
        );
        assert!(
            !over.broadcast_eligible,
            "dimension == threshold + 1 must not be eligible"
        );
    }

    /// An empty side (zero files ⇒ zero bytes) is the trivially broadcast-eligible
    /// dimension, and selection stays deterministic (documented empty-side edge).
    #[test]
    fn empty_side_is_the_eligible_dimension() {
        let empty = resolved_side("EMPTYDIM", vec![]);
        let fact = resolved_side("FACT", vec![("f", 5_000)]);
        let sides = select_broadcast_sides(empty, fact, BROADCAST_MAX);

        assert_eq!(sides.dimension.table_name, "EMPTYDIM");
        assert_eq!(sides.dimension.total_bytes, 0);
        assert!(sides.dimension.files.is_empty());
        assert!(sides.broadcast_eligible);
    }

    /// On an exact byte-size tie (e.g. a self-join, both sides the same table) the
    /// FIRST argument is the dimension — deterministic, documented tie-break.
    #[test]
    fn equal_size_tie_breaks_to_first_argument() {
        let a = resolved_side("SELF_A", vec![("s", 4_242)]);
        let b = resolved_side("SELF_B", vec![("s", 4_242)]);
        let sides = select_broadcast_sides(a, b, BROADCAST_MAX);

        assert_eq!(sides.dimension.table_name, "SELF_A");
        assert_eq!(sides.fact.table_name, "SELF_B");
        assert_eq!(sides.dimension.total_bytes, sides.fact.total_bytes);
    }

    /// A non-empty schema quote-qualifies the UDF name; an empty string or `None`
    /// (the handshake's own no-schema case) falls back to the bare, unqualified
    /// name with no new conditional.
    #[test]
    fn qualify_udf_uses_schema_and_falls_back_when_empty() {
        assert_eq!(qualify_udf(Some("schema"), "UDF"), "\"schema\".UDF");
        assert_eq!(qualify_udf(Some(""), "UDF"), "UDF");
        assert_eq!(qualify_udf(None, "UDF"), "UDF");
    }
}
