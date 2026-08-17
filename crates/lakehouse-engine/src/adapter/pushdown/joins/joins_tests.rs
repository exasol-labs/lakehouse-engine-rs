use super::*;
use crate::adapter::pushdown::ResolvedScan;
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
        ResolvedScan {
            files: files
                .into_iter()
                .map(|(p, s)| FileEntry::new(p, s))
                .collect(),
            effective_storage: sample_storage(),
            logical_schema: vec![LogicalField {
                field_id: Some(1),
                name: format!("{table_name}_KEY"),
                arrow_type: "int64".to_string(),
                nullable: false,
                initial_default: None,
                physical_name: None,
            }],
            table_root: format!("s3://warehouse/lh/{lower}"),
            name_mapping: Vec::new(),
            partition_columns: Vec::new(),
            refused_columns: Vec::new(),
        },
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

fn delta_join_request(select_list: Json) -> Json {
    delta_join_request_over(
        serde_json::json!([
            {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
        ]),
        serde_json::json!([
            {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            {"name": "BINARY_COL", "dataType": {"type": "varchar", "size": 2000000}},
        ]),
        select_list,
    )
}

/// The same two-Delta-table inner equi-join over caller-declared column lists, so a
/// test can declare the SAME column name on both legs.
fn delta_join_request_over(
    customer_columns: Json,
    orders_columns: Json,
    select_list: Json,
) -> Json {
    serde_json::json!({
        "involvedTables": [
            {"name": "CUSTOMER", "columns": customer_columns},
            {"name": "ORDERS", "columns": orders_columns},
        ],
        "pushdownRequest": {
            "type": "select",
            "from": {
                "type": "join",
                "join_type": "inner",
                "left": {"name": "CUSTOMER", "type": "table"},
                "right": {"name": "ORDERS", "type": "table"},
                "condition": {
                    "type": "predicate_equal",
                    "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                    "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
                },
            },
            "selectList": select_list,
        },
        "schemaMetadataInfo": {
            "properties": {},
            "adapterNotes": serde_json::json!({
                "TABLE_MAP": {"CUSTOMER": "cat.sch.customer", "ORDERS": "cat.sch.orders"}
            }).to_string(),
        },
    })
}

/// Scenario: A refused column refuses only the requests that read or emit it
///
/// The JOIN half: a refused column reached through a join leg is refused by the same
/// rule as on the single-table path, per resolved side and ahead of the empty-side
/// early return — both legs here resolve with NO active file, so a gate placed after
/// that return would answer the refused request with an empty result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_delta_column_reached_through_a_join_leg_is_refused() {
    let catalog = unity_delta_catalog().await;
    let storage = two_delta_legs_one_refusing_binary_col().await;
    let mappable = serde_json::json!([
        {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
        {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
    ]);
    let reaching_refused = serde_json::json!([
        {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
        {"type": "column", "name": "BINARY_COL", "tableName": "ORDERS"},
    ]);

    delta_pushdown(
        &delta_join_request(mappable),
        &catalog.uri,
        storage.clone(),
        "cat.sch.orders",
    )
    .await
    .expect("a join naming only mappable columns must plan");

    let error = delta_pushdown(
        &delta_join_request(reaching_refused),
        &catalog.uri,
        storage,
        "cat.sch.orders",
    )
    .await
    .expect_err("a join leg emitting the refused column must be refused, never answered empty");

    assert_refuses_binary_col(error);
}

/// Scenario: A refused column refuses only the requests that read or emit it
///
/// A refusal belongs to the table that raised it. Both legs here declare a
/// `BINARY_COL`, but only `ORDERS`' is a Delta `binary`; `CUSTOMER`'s is a `string`
/// the reader maps. A select list naming only `CUSTOMER.BINARY_COL` therefore reads
/// nothing `ORDERS` refused, and a gate matching a request-global touched set
/// against every side's refused list would refuse it on the strength of the name
/// alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_column_on_one_join_side_does_not_refuse_a_same_named_mappable_column_on_the_other()
 {
    let catalog = unity_delta_catalog().await;
    let storage = delta_object_endpoint(vec![
        (
            delta_commit_zero_key("customer"),
            fileless_delta_commit(
                "customer",
                &[
                    ("c_custkey", "long"),
                    ("c_name", "string"),
                    ("binary_col", "string"),
                ],
            ),
        ),
        (
            delta_commit_zero_key("orders"),
            fileless_delta_commit("orders", &[("o_custkey", "long"), ("binary_col", "binary")]),
        ),
    ])
    .await;
    let request = delta_join_request_over(
        serde_json::json!([
            {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
            {"name": "BINARY_COL", "dataType": {"type": "varchar", "size": 2000000}},
        ]),
        serde_json::json!([
            {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            {"name": "BINARY_COL", "dataType": {"type": "varchar", "size": 2000000}},
        ]),
        serde_json::json!([
            {"type": "column", "name": "BINARY_COL", "tableName": "CUSTOMER"},
        ]),
    );

    delta_pushdown(&request, &catalog.uri, storage, "cat.sch.orders")
        .await
        .expect("a select list naming only the mappable side's column must plan");
}

/// Scenario: A refused column refuses only the requests that read or emit it
///
/// The fail-safe half of per-side attribution: an unqualified `BINARY_COL` names no
/// side, so it is charged to BOTH and the leg that refused it refuses the query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unqualified_column_reference_is_charged_to_every_join_side() {
    let catalog = unity_delta_catalog().await;
    let storage = two_delta_legs_one_refusing_binary_col().await;
    let request = delta_join_request(serde_json::json!([
        {"type": "column", "name": "BINARY_COL"},
    ]));

    let error = delta_pushdown(&request, &catalog.uri, storage, "cat.sch.orders")
        .await
        .expect_err("a column reference naming no side must be charged to every side");

    assert_refuses_binary_col(error);
}

/// Scenario: A refused column refuses only the requests that read or emit it
///
/// A `SELECT *` join names no column anywhere yet emits every column each side
/// declares, so the side carrying the refused column must be charged its own
/// declared row rather than admitted for lack of a `column` node naming it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_select_star_join_is_refused_by_the_side_declaring_the_refused_column() {
    let catalog = unity_delta_catalog().await;
    let storage = two_delta_legs_one_refusing_binary_col().await;
    let request = delta_join_request(Json::Null);

    let error = delta_pushdown(&request, &catalog.uri, storage, "cat.sch.orders")
        .await
        .expect_err("SELECT * emits ORDERS.BINARY_COL, which ORDERS refused");

    assert_refuses_binary_col(error);
}

/// `CUSTOMER` (all mappable) joined to `ORDERS`, whose `binary_col` the Delta reader
/// refuses. Neither leg carries an active file.
async fn two_delta_legs_one_refusing_binary_col() -> StorageBackend {
    delta_object_endpoint(vec![
        (
            delta_commit_zero_key("customer"),
            fileless_delta_commit("customer", &[("c_custkey", "long"), ("c_name", "string")]),
        ),
        (
            delta_commit_zero_key("orders"),
            fileless_delta_commit("orders", &[("o_custkey", "long"), ("binary_col", "binary")]),
        ),
    ])
    .await
}

fn assert_refuses_binary_col(error: UdfError) {
    let message = match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    };
    assert!(
        message.contains("binary_col") && message.contains("#350"),
        "the refusal must be the gate's own message, naming the column and its reason: \
         {message}"
    );
}
