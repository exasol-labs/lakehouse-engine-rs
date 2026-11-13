use super::attribution::JoinLegs;
use super::*;
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::pushdown::ResolvedScan;
use crate::adapter::pushdown::test_support::*;
use crate::scan::spec::{FileEntry, LogicalField, ProjectionItem};
use crate::types::exasol_type::ExasolType;
use futures::FutureExt;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

// Shared join fixtures, reached by each submodule's tests via `super::super::tests`.

/// `from_extra` is spliced into the `from` object.
pub(super) fn join_request(from_extra: Json, condition: Json) -> Json {
    built_join(
        customer_orders_request(from_extra, condition)
            .column(table_col("CUSTOMER", "C_NAME"))
            .column(table_col("ORDERS", "O_ORDERDATE")),
    )
}

/// [`join_request`]'s two tables joined on `condition`, before its select list.
pub(super) fn customer_orders_request(from_extra: Json, condition: Json) -> PushdownRequestBuilder {
    let mut from = serde_json::json!({
        "type": "join",
        "join_type": "inner",
        "left": table_ref("CUSTOMER"),
        "right": table_ref("ORDERS"),
    });
    if let Json::Object(extra) = from_extra {
        from.as_object_mut().unwrap().extend(extra);
    }
    from["condition"] = condition;

    pushdown_request(
        "CUSTOMER",
        &[
            ("C_CUSTKEY", ExasolType::decimal(20, 0)),
            ("C_NAME", ExasolType::varchar(100)),
        ],
    )
    .table(
        "ORDERS",
        &[
            ("O_CUSTKEY", ExasolType::decimal(20, 0)),
            ("O_ORDERDATE", ExasolType::Date),
        ],
    )
    .field("type", serde_json::json!("select"))
    .field("from", from)
    .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
}

/// `join` over [`customer_orders_request`]'s tables, built with each table's identifier note.
pub(super) fn built_join(join: PushdownRequestBuilder) -> Json {
    with_identifier_notes(
        join.build().0,
        &[("CUSTOMER", "lh.customer"), ("ORDERS", "lh.orders")],
    )
}

pub(super) fn equi_condition() -> Json {
    predicate_equal(
        table_col("CUSTOMER", "C_CUSTKEY"),
        table_col("ORDERS", "O_CUSTKEY"),
    )
}

/// `(CUSTOMER ⋈ ORDERS) ⋈ LINEITEM`.
pub(super) fn three_table_join_request() -> Json {
    with_identifier_notes(
        pushdown_request(
            "CUSTOMER",
            &[
                ("C_CUSTKEY", ExasolType::decimal(20, 0)),
                ("C_NAME", ExasolType::varchar(100)),
            ],
        )
        .table(
            "ORDERS",
            &[
                ("O_ORDERKEY", ExasolType::decimal(20, 0)),
                ("O_CUSTKEY", ExasolType::decimal(20, 0)),
            ],
        )
        .table(
            "LINEITEM",
            &[
                ("L_ORDERKEY", ExasolType::decimal(20, 0)),
                ("L_QUANTITY", ExasolType::decimal(15, 2)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .field(
            "from",
            inner_join(
                customer_orders_join(),
                table_ref("LINEITEM"),
                predicate_equal(
                    table_col("ORDERS", "O_ORDERKEY"),
                    table_col("LINEITEM", "L_ORDERKEY"),
                ),
            ),
        )
        .column(table_col("CUSTOMER", "C_NAME"))
        .column(table_col("LINEITEM", "L_QUANTITY"))
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
        .build()
        .0,
        &[
            ("CUSTOMER", "lh.customer"),
            ("ORDERS", "lh.orders"),
            ("LINEITEM", "lh.lineitem"),
        ],
    )
}

/// NQ3 shape: `((PART ⋈ PARTSUPP) ⋈ SUPPLIER) ⋈ NATION`.
pub(super) fn nq3_join_request() -> Json {
    with_identifier_notes(
        pushdown_request(
            "PART",
            &[
                ("P_PARTKEY", ExasolType::decimal(20, 0)),
                ("P_NAME", ExasolType::varchar(100)),
            ],
        )
        .table(
            "PARTSUPP",
            &[
                ("PS_PARTKEY", ExasolType::decimal(20, 0)),
                ("PS_SUPPKEY", ExasolType::decimal(20, 0)),
                ("PS_AVAILQTY", ExasolType::decimal(15, 0)),
            ],
        )
        .table(
            "SUPPLIER",
            &[
                ("S_SUPPKEY", ExasolType::decimal(20, 0)),
                ("S_NATIONKEY", ExasolType::decimal(20, 0)),
            ],
        )
        .table(
            "NATION",
            &[
                ("N_NATIONKEY", ExasolType::decimal(20, 0)),
                ("N_NAME", ExasolType::varchar(100)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .field(
            "from",
            inner_join(
                inner_join(
                    inner_join(
                        table_ref("PART"),
                        table_ref("PARTSUPP"),
                        predicate_equal(
                            table_col("PART", "P_PARTKEY"),
                            table_col("PARTSUPP", "PS_PARTKEY"),
                        ),
                    ),
                    table_ref("SUPPLIER"),
                    predicate_equal(
                        table_col("PARTSUPP", "PS_SUPPKEY"),
                        table_col("SUPPLIER", "S_SUPPKEY"),
                    ),
                ),
                table_ref("NATION"),
                predicate_equal(
                    table_col("SUPPLIER", "S_NATIONKEY"),
                    table_col("NATION", "N_NATIONKEY"),
                ),
            ),
        )
        .column(table_col("PART", "P_NAME"))
        .column(table_col("PARTSUPP", "PS_AVAILQTY"))
        .column(table_col("NATION", "N_NAME"))
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
        .build()
        .0,
        &[
            ("PART", "lh.part"),
            ("PARTSUPP", "lh.partsupp"),
            ("SUPPLIER", "lh.supplier"),
            ("NATION", "lh.nation"),
        ],
    )
}

/// `None` omits both the leaf's `alias` and the columns' `tableAlias`, as Exasol does for an
/// unaliased occurrence (mirrors the live `EXPLAIN VIRTUAL` capture for #361).
pub(super) fn self_join_request(leg_aliases: &[Option<&str>]) -> Json {
    fn order_key(alias: Option<&str>) -> Json {
        table_column("FACT_ORDERS", alias, "O_ORDERKEY")
    }

    let mut from = table_ref_as("FACT_ORDERS", leg_aliases[0]);
    for (left_leg, right_leg) in leg_aliases.iter().zip(leg_aliases.iter().skip(1)) {
        from = inner_join(
            from,
            table_ref_as("FACT_ORDERS", *right_leg),
            predicate_equal(order_key(*left_leg), order_key(*right_leg)),
        );
    }

    with_identifier_notes(
        pushdown_request(
            "FACT_ORDERS",
            &[
                ("O_ORDERKEY", ExasolType::decimal(20, 0)),
                ("O_CUSTKEY", ExasolType::decimal(20, 0)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .field("from", from)
        .column(order_key(leg_aliases[0]))
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
        .build()
        .0,
        &[("FACT_ORDERS", "lh.fact_orders")],
    )
}

pub(super) fn detected_join(request: &Json) -> DetectedJoin {
    match detect_join(request, &pd(request)).expect("detected join shape") {
        JoinShape::Join(join) => join,
        other => panic!("expected Join, got {other:?}"),
    }
}

/// The legs production binds for `request`: its join's legs, or its one table's.
pub(super) fn request_legs(request: &Json) -> JoinLegs {
    match detect_join(request, &pd(request)).expect("a plannable request") {
        JoinShape::Join(join) => join.legs(),
        JoinShape::NotAJoin => JoinLegs::for_single_scan(request),
    }
}

/// The select list projected as `render_broadcast_join` projects it: every column qualified
/// through the leg binding and typed by its own leg's universe, widened row included.
pub(super) fn broadcast_projection(request: &Json) -> (Vec<ProjectionItem>, Vec<ExasolType>, bool) {
    let join = detected_join(request);
    let legs = join.legs();
    let qualified = legs.qualify(&pd(request));
    let sides = request_sides(request, &join);
    let leg_types = ColumnTypes::per_relation(
        sides
            .iter()
            .enumerate()
            .map(|(leg, side)| (legs.leg_alias(leg), &side.column_types)),
    );
    let (items, types, widened) =
        project_split(&parsed_pushdown(request).with_json(qualified), &leg_types)
            .expect("projectable");
    (items, types, widened)
}

/// Derived through [`DetectedJoin::legs`], the only multi-leg constructor; conditions play
/// no part in leg identity.
pub(super) fn legs_from_leaves(leaves: Vec<JoinLeaf>) -> JoinLegs {
    DetectedJoin {
        tables: leaves,
        conditions: Vec::new(),
    }
    .legs()
}

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
                nested: None,
                physical_name: None,
            }],
            table_root: format!("s3://warehouse/lh/{lower}"),
            name_mapping: Vec::new(),
            partition_columns: Vec::new(),
            refused_columns: Vec::new(),
        },
        column_types(NO_COLUMNS),
    )
}

pub(super) fn two_scan_tuning() -> JoinScanRequestConfig<'static> {
    JoinScanRequestConfig {
        cluster_nodes: 1,
        parallelism_factor: 1,
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 0,
        s3_max_connections: 1,
        binary_as_string: false,
        connection: &crate::adapter::pushdown::test_support::TEST_CONNECTION,
    }
}

#[test]
fn qualify_udf_uses_schema_and_falls_back_when_empty() {
    assert_eq!(qualify_udf(Some("schema"), "UDF"), "\"schema\".UDF");
    assert_eq!(qualify_udf(Some(""), "UDF"), "UDF");
    assert_eq!(qualify_udf(None, "UDF"), "UDF");
}

const CUSTOMER_COLUMNS: &[(&str, &str)] = &[("c_custkey", "long"), ("c_name", "string")];

const ORDERS_COLUMNS: &[(&str, &str)] = &[("o_custkey", "long"), ("variant_col", "variant")];

/// The log declares the refused column a string: a `variant` log needs the `variantType` feature,
/// which the reader refuses as a whole table, while the notes alone decide the column's refusal.
const ORDERS_LOG_COLUMNS: &[(&str, &str)] = &[("o_custkey", "long"), ("variant_col", "string")];

/// `None` selects every column, as `SELECT *` does.
fn delta_join_request(select_list: Option<Vec<Json>>) -> Json {
    let request = delta_join_request_over(
        &[
            ("C_CUSTKEY", ExasolType::decimal(20, 0)),
            ("C_NAME", ExasolType::varchar(100)),
        ],
        &[
            ("O_CUSTKEY", ExasolType::decimal(20, 0)),
            ("VARIANT_COL", ExasolType::WIDEST_VARCHAR),
        ],
        select_list,
    );
    let request = with_delta_notes(request, "CUSTOMER", CUSTOMER_COLUMNS, &[]);
    with_delta_notes(request, "ORDERS", ORDERS_COLUMNS, &[])
}

/// Lets a test declare the same column name on both legs.
fn delta_join_request_over(
    customer_columns: &[(&str, ExasolType)],
    orders_columns: &[(&str, ExasolType)],
    select_list: Option<Vec<Json>>,
) -> Json {
    let request = pushdown_request("CUSTOMER", customer_columns)
        .table("ORDERS", orders_columns)
        .field("type", "select")
        .field("from", customer_orders_join())
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}));
    let request = select_list
        .into_iter()
        .flatten()
        .fold(request, PushdownRequestBuilder::column);
    with_identifier_notes(
        request.build().0,
        &[
            ("CUSTOMER", "cat.sch.customer"),
            ("ORDERS", "cat.sch.orders"),
        ],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_delta_column_reached_through_a_join_leg_is_refused() {
    let catalog = unity_delta_catalog().await;
    let storage = two_delta_legs_one_refusing_variant_col().await;
    let mappable = Some(vec![
        table_col("CUSTOMER", "C_NAME"),
        table_col("ORDERS", "O_CUSTKEY"),
    ]);
    let reaching_refused = Some(vec![
        table_col("CUSTOMER", "C_NAME"),
        table_col("ORDERS", "VARIANT_COL"),
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

    assert_refuses_variant_col(error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_column_on_one_join_side_does_not_refuse_a_same_named_mappable_column_on_the_other()
 {
    let catalog = unity_delta_catalog().await;
    let customer_columns = [
        ("c_custkey", "long"),
        ("c_name", "string"),
        ("variant_col", "string"),
    ];
    let storage = delta_object_endpoint(vec![
        (
            delta_commit_zero_key("customer"),
            fileless_delta_commit("customer", &customer_columns),
        ),
        (
            delta_commit_zero_key("orders"),
            fileless_delta_commit("orders", ORDERS_LOG_COLUMNS),
        ),
    ])
    .await;
    let request = delta_join_request_over(
        &[
            ("C_CUSTKEY", ExasolType::decimal(20, 0)),
            ("C_NAME", ExasolType::varchar(100)),
            ("VARIANT_COL", ExasolType::WIDEST_VARCHAR),
        ],
        &[
            ("O_CUSTKEY", ExasolType::decimal(20, 0)),
            ("VARIANT_COL", ExasolType::WIDEST_VARCHAR),
        ],
        Some(vec![table_col("CUSTOMER", "VARIANT_COL")]),
    );
    let request = with_delta_notes(request, "CUSTOMER", &customer_columns, &[]);
    let request = with_delta_notes(request, "ORDERS", ORDERS_COLUMNS, &[]);

    delta_pushdown(&request, &catalog.uri, storage, "cat.sch.orders")
        .await
        .expect("a select list naming only the mappable side's column must plan");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_select_star_join_is_refused_by_the_side_declaring_the_refused_column() {
    let catalog = unity_delta_catalog().await;
    let storage = two_delta_legs_one_refusing_variant_col().await;
    let request = delta_join_request(None);

    let error = delta_pushdown(&request, &catalog.uri, storage, "cat.sch.orders")
        .await
        .expect_err("SELECT * emits ORDERS.VARIANT_COL, which ORDERS refused");

    assert_refuses_variant_col(error);
}

/// Neither leg carries an active file.
async fn two_delta_legs_one_refusing_variant_col() -> StorageBackend {
    delta_object_endpoint(vec![
        (
            delta_commit_zero_key("customer"),
            fileless_delta_commit("customer", CUSTOMER_COLUMNS),
        ),
        (
            delta_commit_zero_key("orders"),
            fileless_delta_commit("orders", ORDERS_LOG_COLUMNS),
        ),
    ])
    .await
}

/// Two `add` files, so a local partition equality has one file to keep and one to prune.
fn two_file_delta_commit(
    id: &str,
    columns: &[(&str, &str)],
    partition_column: &str,
    values: [&str; 2],
) -> String {
    let fields: Vec<Json> = columns
        .iter()
        .map(|(name, delta_type)| {
            serde_json::json!({"name": name, "type": delta_type, "nullable": true, "metadata": {}})
        })
        .collect();
    let protocol = serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
    let metadata = serde_json::json!({"metaData": {
        "id": id,
        "format": {"provider": "parquet", "options": {}},
        "schemaString": serde_json::json!({"type": "struct", "fields": fields}).to_string(),
        "partitionColumns": [partition_column],
        "configuration": {},
        "createdTime": 1,
    }});
    let adds: Vec<Json> = values
        .iter()
        .map(|value| {
            let mut partition_values = serde_json::Map::new();
            partition_values.insert(
                partition_column.to_string(),
                Json::String(value.to_string()),
            );
            serde_json::json!({"add": {
                "path": format!("{partition_column}={value}/part-0.parquet"),
                "partitionValues": Json::Object(partition_values),
                "size": 100,
                "modificationTime": 1,
                "dataChange": true,
            }})
        })
        .collect();
    format!("{protocol}\n{metadata}\n{}\n{}\n", adds[0], adds[1])
}

const REGION_CUSTOMER_COLUMNS: &[(&str, &str)] = &[("c_custkey", "long"), ("c_region", "string")];

const STATUS_ORDERS_COLUMNS: &[(&str, &str)] = &[("o_custkey", "long"), ("o_status", "string")];

async fn two_delta_legs_each_pruned_by_its_own_local_filter() -> StorageBackend {
    delta_object_endpoint(partitioned_delta_logs()).await
}

fn partitioned_delta_logs() -> Vec<(String, String)> {
    vec![
        (
            delta_commit_zero_key("customer"),
            two_file_delta_commit(
                "customer",
                REGION_CUSTOMER_COLUMNS,
                "c_region",
                ["us", "eu"],
            ),
        ),
        (
            delta_commit_zero_key("orders"),
            two_file_delta_commit(
                "orders",
                STATUS_ORDERS_COLUMNS,
                "o_status",
                ["open", "closed"],
            ),
        ),
    ]
}

/// A join selecting `C_REGION` and `O_STATUS` over the two partitioned legs, each leg's columns
/// carrying the notes CREATE records for it.
fn partitioned_legs_request() -> Json {
    let request = delta_join_request_over(
        &[
            ("C_CUSTKEY", ExasolType::decimal(20, 0)),
            ("C_REGION", ExasolType::varchar(100)),
        ],
        &[
            ("O_CUSTKEY", ExasolType::decimal(20, 0)),
            ("O_STATUS", ExasolType::varchar(100)),
        ],
        Some(vec![
            table_col("CUSTOMER", "C_REGION"),
            table_col("ORDERS", "O_STATUS"),
        ]),
    );
    let request = with_delta_notes(request, "CUSTOMER", REGION_CUSTOMER_COLUMNS, &["c_region"]);
    with_delta_notes(request, "ORDERS", STATUS_ORDERS_COLUMNS, &["o_status"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_delta_join_leg_prunes_by_its_own_side_local_predicate() {
    let catalog = unity_delta_catalog().await;
    let storage = two_delta_legs_each_pruned_by_its_own_local_filter().await;
    let mut request = partitioned_legs_request();
    request["pushdownRequest"]["filter"] = and([
        string_equal("CUSTOMER", "C_REGION", "us"),
        string_equal("ORDERS", "O_STATUS", "open"),
    ]);

    let result = delta_pushdown(&request, &catalog.uri, storage, "cat.sch.orders")
        .await
        .expect("a broadcast-eligible join with per-leg local predicates must plan");

    let sql = result["sql"]
        .as_str()
        .expect("a planned pushdown carries sql");

    assert!(
        sql.contains("c_region=us/part-0.parquet"),
        "CUSTOMER's own local filter must keep its matching file: {sql}"
    );
    assert!(
        !sql.contains("c_region=eu/part-0.parquet"),
        "CUSTOMER's own local filter must prune its non-matching file: {sql}"
    );
    assert!(
        sql.contains("o_status=open/part-0.parquet"),
        "ORDERS' own local filter must keep its matching file: {sql}"
    );
    assert!(
        !sql.contains("o_status=closed/part-0.parquet"),
        "ORDERS' own local filter must prune its non-matching file, unaffected by CUSTOMER's filter: {sql}"
    );
}

/// A fake server that holds the first request for each key `held` derives from its target until
/// two keys' first requests are in flight, so legs that run one after the other never get an
/// answer. Returns the server's address.
async fn two_leg_rendezvous(
    held: fn(&str) -> Option<String>,
    respond: impl Fn(&str) -> (u16, &'static str, String) + Send + Sync + 'static,
) -> String {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first_reads = Arc::new(Mutex::new(HashSet::new()));
    let respond = Arc::new(respond);
    let (address, _) = spawn_fake_http_server(move |head| {
        let (barrier, first_reads, respond) = (
            Arc::clone(&barrier),
            Arc::clone(&first_reads),
            Arc::clone(&respond),
        );
        let target = request_target(&head).to_string();
        async move {
            let first_read = held(&target)
                .is_some_and(|key| first_reads.lock().expect("first reads").insert(key));
            if first_read {
                barrier.wait().await;
            }
            respond(&target)
        }
        .boxed()
    })
    .await;
    address
}

/// The table an Iceberg REST `loadTable` of `target` names.
fn loaded_table(target: &str) -> Option<String> {
    (!target.starts_with("/v1/config")).then(|| target.rsplit('/').next().unwrap_or("t").into())
}

/// The table whose `_delta_log` a GET or a listing of `target` reads.
fn delta_log_table(target: &str) -> Option<String> {
    let decoded = percent_encoding::percent_decode_str(target).decode_utf8_lossy();
    let (before, _) = decoded.split_once("/_delta_log")?;
    before.rsplit(['/', '=']).next().map(str::to_string)
}

/// Both formats run on the production adapter's current-thread runtime, where a leg that holds
/// the thread would starve the other.
/// Scenario: A request's table resolutions run concurrently rather than one after another
#[tokio::test(flavor = "current_thread")]
async fn join_legs_resolve_concurrently() {
    let iceberg_catalog = two_leg_rendezvous(loaded_table, |target| {
        let body = match loaded_table(target) {
            Some(table) => snapshotless_load_table_body(&format!("s3://bucket/{table}")),
            None => "{}".to_string(),
        };
        (200, "application/json", body)
    })
    .await;
    let unity_catalog = unity_delta_catalog().await;
    let objects = partitioned_delta_logs();
    let delta_store = two_leg_rendezvous(delta_log_table, move |target| {
        object_response("bucket", target, &objects)
    })
    .await;
    let iceberg_request = with_iceberg_notes(join_request(Json::Null, equi_condition()));
    let delta_request = partitioned_legs_request();

    let cases = [
        (
            "Iceberg loadTable",
            pushdown_over(
                &iceberg_request,
                CatalogKind::IcebergRest,
                &iceberg_catalog,
                sample_storage(),
                "lh.customer",
                1024,
            )
            .boxed(),
        ),
        (
            "Delta log replay",
            delta_pushdown(
                &delta_request,
                &unity_catalog.uri,
                endpoint_storage(&delta_store),
                "cat.sch.orders",
            )
            .boxed(),
        ),
    ];

    for (legs, plan) in cases {
        let planned = tokio::time::timeout(std::time::Duration::from_secs(20), plan)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{legs}: both legs must be in flight at once; a leg that holds the runtime \
                     thread, or starts after the other ends, never releases the rendezvous"
                )
            })
            .unwrap_or_else(|error| panic!("{legs}: the join plans: {error}"));
        assert_eq!(
            planned["type"], "pushdown",
            "{legs}: the overlap must leave the planned response shape untouched: {planned:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolved_join_sides_stay_in_leg_index_order() {
    let catalog = unity_delta_catalog().await;
    let storage = two_delta_legs_each_pruned_by_its_own_local_filter().await;
    let request = partitioned_legs_request();

    // A zero broadcast threshold forces the N-scan fallback, which indexes sides by leg.
    let result = pushdown_over(
        &request,
        CatalogKind::UnityCatalogNative,
        &catalog.uri,
        storage,
        "cat.sch.orders",
        0,
    )
    .await
    .expect("two non-empty Delta legs plan the N-scan fallback");
    let sql = result["sql"]
        .as_str()
        .expect("a planned pushdown carries sql");

    let customer = sql
        .find("c_region=")
        .expect("the left leg's own files must be rendered");
    let orders = sql
        .find("o_status=")
        .expect("the right leg's own files must be rendered");
    assert!(
        customer < orders,
        "the resolved sides must stay in leg-index order, left leaf first: {sql}"
    );
}
