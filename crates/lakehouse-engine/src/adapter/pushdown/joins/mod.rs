use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{CatalogProps, StorageProps};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::file_resolution::empty_result_sql;
use super::support::{DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, project_columns, quote_ident};

mod planning;
mod rendering;
mod sql_builders;

pub(crate) use planning::{
    DetectedJoin, IneligibleJoinReason, JoinLeaf, JoinShape, JoinSides, ResolvedJoinSide,
    detect_join,
};
pub(crate) use sql_builders::{RenderedJoinPushdown, render_broadcast_join};

pub(super) use sql_builders::{
    build_qualified_single_table_fallback_sql, referenced_column_projection,
};

use planning::{
    involved_table_columns, join_requires_exasol_postprocessing, resolve_one_join_side,
    select_broadcast_sides,
};
use rendering::side_local_filter;
use sql_builders::{JoinScanTuning, build_broadcast_join_sql, build_n_scan_join_sql};

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
        &distribute_udf_name,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::pushdown::test_support::*;
    use crate::scan::spec::{FileEntry, LogicalField};

    // Shared join-test fixtures at the joins-module root — the join analog of the
    // pushdown-wide `test_support` fixtures. Each concern submodule's test module
    // reaches them via `super::super::tests::{...}` across the added nesting level, so
    // there is a single copy rather than one duplicate per submodule.

    /// Build a two-table-join pushdown request. `from_extra` is spliced into the
    /// `from` object (e.g. to swap `join_type`, drop a field, or corrupt a side),
    /// and `condition` becomes the join's `condition` node.
    pub(super) fn join_request(from_extra: Json, condition: Json) -> Json {
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
    pub(super) fn equi_condition() -> Json {
        serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
        })
    }

    /// A three-table inner-join pushdown request: `(CUSTOMER ⋈ ORDERS) ⋈ LINEITEM`,
    /// all three in `TABLE_MAP`. Leaves in stable tree order CUSTOMER, ORDERS,
    /// LINEITEM; two join conditions (`C_CUSTKEY=O_CUSTKEY`, `O_ORDERKEY=L_ORDERKEY`).
    pub(super) fn three_table_join_request() -> Json {
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

    /// The NQ3-shape four-table inner-join pushdown request:
    /// `((PART ⋈ PARTSUPP) ⋈ SUPPLIER) ⋈ NATION`, all four in `TABLE_MAP`. Leaves in
    /// stable tree order PART, PARTSUPP, SUPPLIER, NATION; three join conditions.
    pub(super) fn nq3_join_request() -> Json {
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

    /// Recover the [`DetectedJoin`] a request classifies to (the tests below all
    /// operate on the standard two-table CUSTOMER⋈ORDERS shape from `join_request`).
    pub(super) fn detected_join(request: &Json) -> DetectedJoin {
        match detect_join(request, &pd(request)).expect("detected join shape") {
            JoinShape::Join(join) => join,
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// Build a resolved join side with a given `(path, byte_size)` file list.
    /// Storage/schema/root are populated so the tests can assert the full resolved
    /// payload rides along with the selected role; only the byte totals drive
    /// selection.
    pub(super) fn resolved_side(table_name: &str, files: Vec<(&str, u64)>) -> ResolvedJoinSide {
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

    pub(super) fn two_scan_tuning() -> JoinScanTuning {
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

    /// A non-empty schema quote-qualifies the UDF name; an empty string or `None`
    /// (the handshake's own no-schema case) falls back to the bare, unqualified
    /// name with no new conditional.
    #[test]
    fn qualify_udf_uses_schema_and_falls_back_when_empty() {
        assert_eq!(qualify_udf(Some("schema"), "UDF"), "\"schema\".UDF");
        assert_eq!(qualify_udf(Some(""), "UDF"), "UDF");
        assert_eq!(qualify_udf(None, "UDF"), "UDF");
    }

    #[test]
    fn golden_ineligible_decline_message_unchanged() {
        let not_inner = match ineligible_join_decline(IneligibleJoinReason::NotInnerJoinType) {
            UdfError::User(msg) => msg,
            other => panic!("expected User decline, got {other:?}"),
        };
        let unsupported = match ineligible_join_decline(IneligibleJoinReason::UnsupportedShape) {
            UdfError::User(msg) => msg,
            other => panic!("expected User decline, got {other:?}"),
        };
        assert_eq!(
            not_inner,
            "join pushdown declined: the join is not an inner join; the adapter cannot render this join shape, so this is a hard error, not a native re-plan"
        );
        assert_eq!(
            unsupported,
            "join pushdown declined: the join `from` clause has an unsupported shape; the adapter cannot render this join shape, so this is a hard error, not a native re-plan"
        );
    }
}
