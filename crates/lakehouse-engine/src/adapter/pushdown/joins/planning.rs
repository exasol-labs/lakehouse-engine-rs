use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{CatalogProps, FileEntry, LogicalField, NameMappingEntry, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::super::file_resolution::resolve_file_list;
use super::super::support::{column_types, extract_limit, order_by_present};
use lakehouse_catalog::CatalogSession;

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
    pub effective_storage: StorageBackend,
    /// Sum of every file's `file_size_in_bytes` — the broadcast-threshold metric.
    pub total_bytes: u64,
}

impl ResolvedJoinSide {
    /// Assemble a resolved side, computing `total_bytes` from the file list with a
    /// saturating sum (a byte total that overflows `u64` is clamped to `u64::MAX`,
    /// which is correctly treated as "far over any broadcast threshold").
    pub(super) fn new(
        table_name: String,
        iceberg_ident: String,
        table_root: String,
        files: Vec<FileEntry>,
        logical_schema: Vec<LogicalField>,
        name_mapping: Vec<NameMappingEntry>,
        effective_storage: StorageBackend,
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
pub(super) async fn resolve_one_join_side(
    table_name: &str,
    iceberg_ident: &str,
    session: &CatalogSession,
    storage: &StorageBackend,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
    filter_json: Option<&Json>,
) -> Result<ResolvedJoinSide, UdfError> {
    let side_catalog = CatalogProps {
        table: iceberg_ident.to_string(),
        ..catalog.clone()
    };
    let (files, effective_storage, logical_schema, table_root, name_mapping) =
        resolve_file_list(session, &side_catalog, storage, creds, filter_json).await?;
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
/// premise — `resolve_table_schema` Unicode-uppercases every name it declares, so no
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

/// Whether a join pushdown request carries work Exasol must execute over the
/// materialized two-scan join rather than inside the broadcast in-UDF join: an
/// aggregate (single-group or grouped), a GROUP BY, an ORDER BY, a LIMIT, or a
/// HAVING. The broadcast path renders only projection + filter + join condition, so
/// any of these routes the join to the qualified two-scan fallback (which renders
/// them as ordinary Exasol SQL over the join), reproducing pre-`JOIN`-capability
/// behavior exactly.
pub(super) fn join_requires_exasol_postprocessing(pushdown_req: &Json) -> bool {
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

#[cfg(test)]
mod tests {
    use super::super::tests::{
        equi_condition, join_request, nq3_join_request, resolved_side, three_table_join_request,
    };
    use super::*;
    use crate::adapter::pushdown::test_support::*;

    // ---------------------------------------------------------------------------
    // Join detection: `detect_join` shape classification.
    // ---------------------------------------------------------------------------

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
    // Join side selection + broadcast threshold: `select_broadcast_sides`.
    // The pure core of the two-table broadcast role/threshold decision — exercised
    // without a live Iceberg catalog. `plan_join` resolves each side via
    // `resolve_one_join_side` and delegates here, so this covers the decision.
    // ---------------------------------------------------------------------------

    /// The default `JOIN_BROADCAST_MAX_BYTES` (128 MiB).
    const BROADCAST_MAX: u64 = 134_217_728;

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
    /// with each role. Here the LEFT argument is smaller.
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
    /// builds the deterministic unaccelerated two-scan fallback.
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
}
