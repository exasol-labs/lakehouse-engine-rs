//! `CATALOG_KIND = 'DIRECT_STORAGE'`: a plain directory of Parquet files on MinIO,
//! queried with no catalog service. FAILS (never skips) when the stack is unreachable.
#![cfg(feature = "exasol-e2e")]

mod common;

use common::e2e_harness::{
    VsProps, create_schema_and_scripts, create_virtual_schema_with_password, exa_conn,
    explain_virtual_sql, install_slc, local_stack_storage, parse_int, parse_numeric,
    try_create_virtual_schema_with_password, upload_so, value_to_string,
};
use common::exasol_ws::ExaConn;
use common::raw_parquet::write_parquet_fixture;
use common::stack::{
    CatalogConnectionPassword, minio_url_internal, wait_for_exasol, wait_for_minio,
};

use lakehouse_engine::scan::spec::StorageBackend;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, ListBuilder, MapArray, StringArray, StringBuilder, StructArray,
    TimestampMicrosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value as Json;

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

const BASE_DIRECT: &str = "s3://warehouse/direct/";
const BASE_INCOMPATIBLE: &str = "s3://warehouse/direct_incompatible/";
const BASE_DISCOVERY: &str = "s3://warehouse/direct_discovery/";
const BASE_COLLISION_MISSING: &str = "s3://warehouse/direct_hive_collision_missing/";

const VS_DIRECT: &str = "DIRECT_LAKEHOUSE";
const VS_DIRECT_NARROW: &str = "DIRECT_LAKEHOUSE_NARROW";
const VS_HIVE_OFF: &str = "DIRECT_HIVE_OFF";
const VS_INCOMPATIBLE: &str = "DIRECT_INCOMPATIBLE_VS";
const VS_DISCOVERY: &str = "DIRECT_DISCOVERY_VS";
const VS_DISCOVERY_NS: &str = "DIRECT_DISCOVERY_NS_VS";
const VS_DISCOVERY_EMPTY_NS: &str = "DIRECT_DISCOVERY_EMPTY_NS_VS";
const VS_COLLISION_MISSING: &str = "DIRECT_HIVE_COLLISION_MISSING";
const VS_COLLISION_MISSING_HIVE_OFF: &str = "DIRECT_HIVE_COLLISION_MISSING_OFF";

const CONN_DIRECT: &str = "DIRECT_STORAGE_CREDS";
const CONN_INCOMPATIBLE: &str = "DIRECT_STORAGE_INCOMPATIBLE_CREDS";
const CONN_DISCOVERY: &str = "DIRECT_STORAGE_DISCOVERY_CREDS";
const CONN_COLLISION_MISSING: &str = "DIRECT_STORAGE_COLLISION_MISSING_CREDS";

/// No `warehouse`: the direct-storage kind rejects that field.
fn direct_storage_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        endpoint: minio_url_internal(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        path_style: true,
        ..Default::default()
    }
}

fn direct_vs<'a>(vs_name: &'a str, conn_name: &'a str) -> VsProps<'a> {
    VsProps::new(vs_name, "")
        .with_catalog_kind("DIRECT_STORAGE")
        .with_catalog_conn_name(conn_name)
}

fn try_create_direct_vs(conn: &mut ExaConn, vs_name: &str, conn_name: &str, base: &str) -> Json {
    try_create_virtual_schema_with_password(
        conn,
        &direct_vs(vs_name, conn_name),
        base,
        &direct_storage_password(),
    )
}

fn rejection_message<'a>(resp: &'a Json, what: &str) -> &'a str {
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "{what} must fail CREATE VIRTUAL SCHEMA: {resp}"
    );
    resp["exception"]["text"].as_str().unwrap_or("")
}

fn assert_mentions(msg: &str, needles: &[&str]) {
    for needle in needles {
        assert!(msg.contains(needle), "error must mention {needle:?}: {msg}");
    }
}

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();

        write_all_fixtures();

        install_slc();
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);

        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DIRECT, "").with_catalog_kind("DIRECT_STORAGE"),
            BASE_DIRECT,
            &direct_storage_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &direct_vs(VS_DIRECT_NARROW, CONN_DIRECT).with_merge_schema("FALSE"),
            BASE_DIRECT,
            &direct_storage_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &direct_vs(VS_HIVE_OFF, CONN_DIRECT).with_hive_partitioning("FALSE"),
            BASE_DIRECT,
            &direct_storage_password(),
        );

        create_virtual_schema_with_password(
            &mut conn,
            &direct_vs(VS_DISCOVERY, CONN_DISCOVERY),
            BASE_DISCOVERY,
            &direct_storage_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DISCOVERY_NS, "sub")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DISCOVERY),
            BASE_DISCOVERY,
            &direct_storage_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DISCOVERY_EMPTY_NS, "nonexistent_ns")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DISCOVERY),
            BASE_DISCOVERY,
            &direct_storage_password(),
        );
    });
}

fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("EVENT_ID", DataType::Int64, false),
        Field::new("NAME", DataType::Utf8, true),
        Field::new("EVENT_DATE", DataType::Date32, true),
        Field::new(
            "EVENT_TS",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("AMOUNT", DataType::Decimal128(10, 2), true),
        Field::new("IS_ACTIVE", DataType::Boolean, true),
        Field::new("SCORE", DataType::Float64, true),
    ]))
}

/// Indexed by `EVENT_ID - 1`.
const EVENT_NAMES: [&str; 5] = ["alice", "bob", "carol", "dave", "erin"];
const EVENT_ACTIVE: [bool; 5] = [true, false, true, false, true];
const EVENT_SCORES: [f64; 5] = [1.1, 2.2, 3.3, 4.4, 5.5];

fn events_batch(ids: &[i64]) -> RecordBatch {
    let idx = |id: i64| (id - 1) as usize;
    let names: Vec<&str> = ids.iter().map(|&id| EVENT_NAMES[idx(id)]).collect();
    let active: Vec<bool> = ids.iter().map(|&id| EVENT_ACTIVE[idx(id)]).collect();
    let scores: Vec<f64> = ids.iter().map(|&id| EVENT_SCORES[idx(id)]).collect();
    let dates: Vec<i32> = ids.iter().map(|&id| 19000 + id as i32).collect();
    let ts: Vec<i64> = ids
        .iter()
        .map(|&id| 1_700_000_000_000_000 + id * 1_000_000)
        .collect();
    let amounts: Vec<i128> = ids.iter().map(|&id| (id * 1050) as i128).collect();

    RecordBatch::try_new(
        events_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names)),
            Arc::new(Date32Array::from(dates)),
            Arc::new(TimestampMicrosecondArray::from(ts)),
            Arc::new(
                Decimal128Array::from(amounts)
                    .with_precision_and_scale(10, 2)
                    .expect("events AMOUNT precision/scale is valid"),
            ),
            Arc::new(BooleanArray::from(active)),
            Arc::new(Float64Array::from(scores)),
        ],
    )
    .expect("events batch construction is infallible")
}

fn nested_batch(id: i64, value: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("VALUE", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .expect("nested batch construction is infallible")
}

fn widened_narrow_file() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("QTY", DataType::Int32, true),
        Field::new("PRICE", DataType::Float32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(Float32Array::from(vec![1.5f32, 2.5])),
        ],
    )
    .expect("widened narrow-file batch construction is infallible")
}

/// `QTY = 5_000_000_000` exceeds `i32`, so it errors only under the narrow
/// `MERGE_SCHEMA = 'FALSE'` declaration.
fn widened_wide_file() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("QTY", DataType::Int64, true),
        Field::new("PRICE", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![3i64, 4])),
            Arc::new(Int64Array::from(vec![5_000_000_000i64, 40])),
            Arc::new(Float64Array::from(vec![3.5f64, 4.5])),
        ],
    )
    .expect("widened wide-file batch construction is infallible")
}

fn missing_col_file1() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("A", DataType::Int64, true),
        Field::new("B", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .expect("missing_col file1 batch construction is infallible")
}

fn missing_col_file2() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("A", DataType::Int64, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![3i64]))])
        .expect("missing_col file2 batch construction is infallible")
}

fn incompatible_file1() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("X", DataType::Utf8, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))])
        .expect("incompatible file1 batch construction is infallible")
}

fn incompatible_file2() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("X", DataType::Int64, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42i64]))])
        .expect("incompatible file2 batch construction is infallible")
}

fn event_labels_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("EVENT_ID", DataType::Int64, false),
        Field::new("LABEL", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 3, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("event_labels batch construction is infallible")
}

fn id_row<const N: usize>(id: i64, extra: [(Field, ArrayRef); N]) -> RecordBatch {
    let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = std::iter::once((
        Field::new("ID", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![id])) as ArrayRef,
    ))
    .chain(extra)
    .unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("one-row fixture batch construction is infallible")
}

fn discovery_id_batch(id: i64) -> RecordBatch {
    id_row(id, [])
}

fn tags_column() -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    builder.values().append_option(Some("x"));
    builder.values().append_option(Some("y"));
    builder.append(true);
    builder.append_null();
    Arc::new(builder.finish())
}

fn address_column() -> ArrayRef {
    let street: ArrayRef = Arc::new(StringArray::from(vec![Some("Main St"), None]));
    let city: ArrayRef = Arc::new(StringArray::from(vec![Some("Town"), None]));
    Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("STREET", DataType::Utf8, true),
                Field::new("CITY", DataType::Utf8, true),
            ]),
            vec![street, city],
            Some(NullBuffer::from(vec![true, false])),
        )
        .expect("ADDRESS struct column construction is infallible"),
    )
}

fn attrs_column() -> ArrayRef {
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let values: ArrayRef = Arc::new(StringArray::from(vec!["1", "2"]));
    let entries = StructArray::try_new(
        Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ]),
        vec![keys, values],
        None,
    )
    .expect("ATTRS entries struct construction is infallible");
    let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    Arc::new(
        MapArray::try_new(
            entries_field,
            OffsetBuffer::new(vec![0i32, 2, 2].into()),
            entries,
            Some(NullBuffer::from(vec![true, false])),
            false,
        )
        .expect("ATTRS map column construction is infallible"),
    )
}

fn complex_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new(
            "TAGS",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "ADDRESS",
            DataType::Struct(Fields::from(vec![
                Field::new("STREET", DataType::Utf8, true),
                Field::new("CITY", DataType::Utf8, true),
            ])),
            true,
        ),
        Field::new(
            "ATTRS",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            tags_column(),
            address_column(),
            attrs_column(),
        ],
    )
    .expect("complex batch construction is infallible")
}

fn sales_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("AMOUNT", DataType::Float64, true),
    ]));
    let amounts: Vec<f64> = ids.iter().map(|&id| id as f64 * 10.5).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .expect("sales batch construction is infallible")
}

fn discounted_sales_batch(ids: &[i64]) -> RecordBatch {
    let sales = sales_batch(ids);
    let mut fields = sales.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("DISCOUNT", DataType::Float64, true)));
    let mut columns = sales.columns().to_vec();
    let discounts: Vec<f64> = ids.iter().map(|&id| id as f64 / 10.0).collect();
    columns.push(Arc::new(Float64Array::from(discounts)));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("discounted sales batch construction is infallible")
}

/// Byte order ranks these `B < a < é`, unlike a case-insensitive or locale order,
/// so a range predicate tells the orders apart.
const REGION_VALUES: [&str; 3] = ["B", "a", "é"];

/// `SECOND(TS, 3)` is 5.25, so `SECOND(TS, 3) > 1` holds on every row.
const REGION_TS_MICROS: i64 = 1_767_225_605_250_000;

fn regions_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("TS", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(TimestampMicrosecondArray::from(vec![REGION_TS_MICROS])),
        ],
    )
    .expect("regions batch construction is infallible")
}

fn stored_k_batch(id: i64, stored_k: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("K", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![stored_k])),
        ],
    )
    .expect("stored-K batch construction is infallible")
}

fn put_raw_bytes(uri: &str, bytes: Vec<u8>) {
    let without_scheme = uri.strip_prefix("s3://").expect("uri must be s3://...");
    let (bucket, key) = without_scheme
        .split_once('/')
        .expect("uri must have a <bucket>/<key> form");
    let StorageBackend::S3(storage) = local_stack_storage() else {
        panic!("local_stack_storage() must be S3")
    };
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_endpoint(&storage.endpoint)
        .with_allow_http(storage.allow_http)
        .with_virtual_hosted_style_request(!storage.path_style)
        .build()
        .unwrap_or_else(|e| panic!("configure MinIO object store for {uri}: {e}"));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        store
            .put(&ObjectStorePath::from(key), PutPayload::from(bytes))
            .await
            .unwrap_or_else(|e| panic!("PUT {uri}: {e}"));
    });
}

fn write_delta_caveat_fixture() {
    let base = format!("{BASE_DIRECT}delta_caveat");
    write_parquet_fixture(&format!("{base}/file1.parquet"), discovery_id_batch(10));
    write_parquet_fixture(&format!("{base}/file2.parquet"), discovery_id_batch(20));

    let version0 = concat!(
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        "\n",
        r#"{"metaData":{"id":"direct-caveat","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"ID\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":0}}"#,
        "\n",
        r#"{"add":{"path":"file1.parquet","partitionValues":{},"size":256,"modificationTime":0,"dataChange":true}}"#,
        "\n",
        r#"{"add":{"path":"file2.parquet","partitionValues":{},"size":256,"modificationTime":0,"dataChange":true}}"#,
    );
    let version1 = r#"{"remove":{"path":"file2.parquet","deletionTimestamp":0,"dataChange":true}}"#;

    put_raw_bytes(
        &format!("{base}/_delta_log/00000000000000000000.json"),
        version0.as_bytes().to_vec(),
    );
    put_raw_bytes(
        &format!("{base}/_delta_log/00000000000000000001.json"),
        version1.as_bytes().to_vec(),
    );
}

fn write_all_fixtures() {
    write_parquet_fixture(
        &format!("{BASE_DIRECT}events/file1.parquet"),
        events_batch(&[1, 2, 3]),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}events/file2.parquet"),
        events_batch(&[4, 5]),
    );

    write_parquet_fixture(
        &format!("{BASE_DIRECT}nested/A/p1.parquet"),
        nested_batch(1, "from-a"),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}nested/B/p2.parquet"),
        nested_batch(2, "from-b"),
    );

    // The narrow file must sort first: MERGE_SCHEMA = 'FALSE' samples the first-listed file.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}widened/file1.parquet"),
        widened_narrow_file(),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}widened/file2.parquet"),
        widened_wide_file(),
    );

    write_parquet_fixture(
        &format!("{BASE_DIRECT}missing_col/file1.parquet"),
        missing_col_file1(),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}missing_col/file2.parquet"),
        missing_col_file2(),
    );

    write_parquet_fixture(
        &format!("{BASE_DIRECT}complex/file1.parquet"),
        complex_batch(),
    );

    write_parquet_fixture(
        &format!("{BASE_DIRECT}event_labels/file1.parquet"),
        event_labels_batch(),
    );

    write_delta_caveat_fixture();

    write_parquet_fixture(
        &format!("{BASE_DIRECT}sales/year=2026/month=09/p1.parquet"),
        discounted_sales_batch(&[1, 2]),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}sales/year=2025/month=__HIVE_DEFAULT_PARTITION__/p2.parquet"),
        sales_batch(&[3]),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}sales/year=2024/month=01/p3.parquet"),
        sales_batch(&[4]),
    );

    write_parquet_fixture(
        &format!("{BASE_DIRECT}encoded/region=a%2Fb/p.parquet"),
        discovery_id_batch(1),
    );

    // The plain directory must list first so MERGE_SCHEMA = 'FALSE' samples it.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}mixed/A/p.parquet"),
        discovery_id_batch(1),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}mixed/year=2026/p.parquet"),
        discovery_id_batch(2),
    );

    for (id, region) in (1..).zip(REGION_VALUES) {
        write_parquet_fixture(
            &format!("{BASE_DIRECT}regions/region={region}/p.parquet"),
            regions_batch(id),
        );
    }

    write_parquet_fixture(
        &format!("{BASE_DIRECT}loose.parquet"),
        discovery_id_batch(999),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}empty/_SUCCESS"),
        discovery_id_batch(0),
    );

    // Isolated root (as is BASE_COLLISION_MISSING): it fails enumeration, so it must
    // not share a base path with any passing scenario.
    write_parquet_fixture(
        &format!("{BASE_INCOMPATIBLE}incompatible/file1.parquet"),
        incompatible_file1(),
    );
    write_parquet_fixture(
        &format!("{BASE_INCOMPATIBLE}incompatible/file2.parquet"),
        incompatible_file2(),
    );

    write_parquet_fixture(
        &format!("{BASE_DIRECT}collision_override/k=1/p.parquet"),
        stored_k_batch(1, 99),
    );

    write_parquet_fixture(
        &format!("{BASE_COLLISION_MISSING}collision_missing_segment/k=1/p1.parquet"),
        stored_k_batch(1, 99),
    );
    write_parquet_fixture(
        &format!("{BASE_COLLISION_MISSING}collision_missing_segment/p2.parquet"),
        stored_k_batch(2, 42),
    );

    write_parquet_fixture(
        &format!("{BASE_DISCOVERY}orders/file.parquet"),
        discovery_id_batch(100),
    );
    write_parquet_fixture(
        &format!("{BASE_DISCOVERY}deep/y=2026/p.parquet"),
        discovery_id_batch(200),
    );
    write_parquet_fixture(
        &format!("{BASE_DISCOVERY}sub/orders_eu/file.parquet"),
        discovery_id_batch(300),
    );
    write_parquet_fixture(
        &format!("{BASE_DISCOVERY}empty/_SUCCESS"),
        discovery_id_batch(0),
    );
    write_parquet_fixture(
        &format!("{BASE_DISCOVERY}hidden_only/_staging/p.parquet"),
        discovery_id_batch(0),
    );
    write_parquet_fixture(
        &format!("{BASE_DISCOVERY}notes.parquet"),
        discovery_id_batch(0),
    );
}

fn declared_type(conn: &mut ExaConn, vs_name: &str, table: &str, column: &str) -> String {
    let ty = conn.query_columns(&format!(
        "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS \
         WHERE COLUMN_SCHEMA='{vs_name}' AND COLUMN_TABLE='{table}' AND COLUMN_NAME='{column}'"
    ))[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("{vs_name}.{table}.{column} has no declared type"))
        .to_string();
    ty.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Prefix match tolerates Exasol's `COLUMN_TYPE` rendering (charset suffix,
/// `DOUBLE PRECISION` -> `DOUBLE`, substituted `TIMESTAMP` precision).
fn assert_declared_type(
    conn: &mut ExaConn,
    vs_name: &str,
    table: &str,
    column: &str,
    expected: &str,
) {
    let actual = declared_type(conn, vs_name, table, column);
    let expected_stripped: String = expected.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(
        actual.starts_with(&expected_stripped),
        "{vs_name}.{table}.{column}: expected Exasol type starting {expected}, got {actual}"
    );
}

fn string_column<C: FromIterator<String>>(conn: &mut ExaConn, sql: &str) -> C {
    conn.query_columns(sql)[0]
        .iter()
        .map(value_to_string)
        .collect()
}

fn served_tables(conn: &mut ExaConn, vs_name: &str) -> Vec<String> {
    string_column(
        conn,
        &format!(
            "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA='{vs_name}' ORDER BY TABLE_NAME"
        ),
    )
}

fn declared_columns(conn: &mut ExaConn, vs_name: &str, table: &str) -> Vec<String> {
    string_column(
        conn,
        &format!(
            "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS \
             WHERE COLUMN_SCHEMA='{vs_name}' AND COLUMN_TABLE='{table}' \
             ORDER BY COLUMN_ORDINAL_POSITION"
        ),
    )
}

/// Scenario: the raw Parquet fixture is physically the types it declares
#[test]
fn raw_parquet_fixtures_are_physically_the_types_they_declare() {
    setup();

    let StorageBackend::S3(storage) = local_stack_storage() else {
        panic!("local_stack_storage() must be S3")
    };
    let store = AmazonS3Builder::new()
        .with_bucket_name("warehouse")
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_endpoint(&storage.endpoint)
        .with_allow_http(storage.allow_http)
        .with_virtual_hosted_style_request(!storage.path_style)
        .build()
        .expect("configure MinIO object store for the fixture-shape guard");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let bytes = rt.block_on(async {
        store
            .get(&ObjectStorePath::from("direct/events/file1.parquet"))
            .await
            .expect("GET direct/events/file1.parquet")
            .bytes()
            .await
            .expect("read direct/events/file1.parquet bytes")
    });
    let reader = SerializedFileReader::new(bytes).expect("open committed raw-Parquet fixture file");
    let schema_descr = reader.metadata().file_metadata().schema_descr();

    let physical = |name: &str| {
        schema_descr
            .columns()
            .iter()
            .find(|c| c.name().eq_ignore_ascii_case(name))
            .unwrap_or_else(|| panic!("fixture must carry column {name}"))
            .physical_type()
    };

    assert_eq!(physical("EVENT_ID").to_string(), "INT64");
    assert_eq!(physical("NAME").to_string(), "BYTE_ARRAY");
    assert_eq!(physical("EVENT_DATE").to_string(), "INT32");
    assert_eq!(physical("EVENT_TS").to_string(), "INT64");
    assert_eq!(physical("IS_ACTIVE").to_string(), "BOOLEAN");
    assert_eq!(physical("SCORE").to_string(), "DOUBLE");
}

/// Scenario: `events/` declares the Arrow-to-Exasol mapping and returns both files' rows
#[test]
fn events_directory_declares_and_returns_mixed_types_across_both_files() {
    setup();
    let mut conn = exa_conn();

    assert_declared_type(&mut conn, VS_DIRECT, "EVENTS", "EVENT_ID", "DECIMAL(20,0)");
    assert_declared_type(&mut conn, VS_DIRECT, "EVENTS", "NAME", "VARCHAR(2000000)");
    assert_declared_type(&mut conn, VS_DIRECT, "EVENTS", "EVENT_DATE", "DATE");
    assert_declared_type(&mut conn, VS_DIRECT, "EVENTS", "EVENT_TS", "TIMESTAMP");
    assert_declared_type(&mut conn, VS_DIRECT, "EVENTS", "AMOUNT", "DECIMAL(10,2)");
    assert_declared_type(&mut conn, VS_DIRECT, "EVENTS", "IS_ACTIVE", "BOOLEAN");
    assert_declared_type(&mut conn, VS_DIRECT, "EVENTS", "SCORE", "DOUBLE");

    let cols = conn.query_columns(&format!(
        "SELECT EVENT_ID, NAME, IS_ACTIVE, SCORE FROM {} ORDER BY EVENT_ID",
        vs_table(VS_DIRECT, "EVENTS")
    ));
    assert_eq!(
        cols[0].len(),
        5,
        "events/ must return the union of both files' rows"
    );
    for row in 0..5 {
        let id = parse_int(&cols[0][row]);
        assert_eq!(id, (row as i64) + 1);
        assert_eq!(value_to_string(&cols[1][row]), EVENT_NAMES[row]);
        assert_eq!(cols[2][row].as_bool(), Some(EVENT_ACTIVE[row]));
        assert!((parse_numeric(&cols[3][row]) - EVENT_SCORES[row]).abs() < 1e-9);
    }

    let non_null_count = conn.query_columns(&format!(
        "SELECT COUNT(*) FROM {} WHERE EVENT_DATE IS NOT NULL AND EVENT_TS IS NOT NULL AND AMOUNT IS NOT NULL",
        vs_table(VS_DIRECT, "EVENTS")
    ));
    assert_eq!(parse_int(&non_null_count[0][0]), 5);
}

/// Scenario: nested subdirectories union into one table with zero partition columns
#[test]
fn nested_directory_unions_subdirectory_files_with_zero_partition_columns() {
    setup();
    let mut conn = exa_conn();

    let column_count = conn.query_columns(&format!(
        "SELECT COUNT(*) FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA='{VS_DIRECT}' AND COLUMN_TABLE='NESTED'"
    ));
    assert_eq!(
        parse_int(&column_count[0][0]),
        2,
        "NESTED must declare exactly ID and VALUE"
    );

    let cols = conn.query_columns(&format!(
        "SELECT ID, \"VALUE\" FROM {} ORDER BY ID",
        vs_table(VS_DIRECT, "NESTED")
    ));
    assert_eq!(cols[0].len(), 2);
    assert_eq!(parse_int(&cols[0][0]), 1);
    assert_eq!(value_to_string(&cols[1][0]), "from-a");
    assert_eq!(parse_int(&cols[0][1]), 2);
    assert_eq!(value_to_string(&cols[1][1]), "from-b");
}

/// Scenario: struct, list, and map columns declare `VARCHAR(2000000)` and render as JSON
#[test]
fn complex_directory_declares_varchar_and_returns_parseable_json() {
    setup();
    let mut conn = exa_conn();

    for column in ["TAGS", "ADDRESS", "ATTRS"] {
        let ty = declared_type(&mut conn, VS_DIRECT, "COMPLEX", column);
        assert!(
            ty.starts_with("VARCHAR(2000000)"),
            "COMPLEX.{column} must declare VARCHAR(2000000), got {ty}"
        );
    }

    let cols = conn.query_columns(&format!(
        "SELECT ID, TAGS, ADDRESS, ATTRS FROM {} ORDER BY ID",
        vs_table(VS_DIRECT, "COMPLEX")
    ));
    assert_eq!(cols[0].len(), 2);

    let tags0: serde_json::Value = serde_json::from_str(
        cols[1][0]
            .as_str()
            .expect("TAGS row 0 must be a JSON string"),
    )
    .expect("TAGS row 0 must parse as JSON");
    assert_eq!(tags0, serde_json::json!(["x", "y"]));

    let address0: serde_json::Value = serde_json::from_str(
        cols[2][0]
            .as_str()
            .expect("ADDRESS row 0 must be a JSON string"),
    )
    .expect("ADDRESS row 0 must parse as JSON");
    assert_eq!(
        address0,
        serde_json::json!({"STREET": "Main St", "CITY": "Town"})
    );

    let attrs0: serde_json::Value = serde_json::from_str(
        cols[3][0]
            .as_str()
            .expect("ATTRS row 0 must be a JSON string"),
    )
    .expect("ATTRS row 0 must parse as JSON");
    assert_eq!(attrs0, serde_json::json!({"a": "1", "b": "2"}));

    assert!(cols[1][1].is_null(), "TAGS row 1 must be SQL NULL");
    assert!(cols[2][1].is_null(), "ADDRESS row 1 must be SQL NULL");
    assert!(cols[3][1].is_null(), "ATTRS row 1 must be SQL NULL");
}

/// Scenario: a loose file and a data-file-less directory serve no table
#[test]
fn loose_file_and_empty_directory_serve_no_table() {
    setup();
    let mut conn = exa_conn();
    let tables = served_tables(&mut conn, VS_DIRECT);
    assert!(!tables.contains(&"LOOSE".to_string()));
    assert!(!tables.contains(&"EMPTY".to_string()));
    assert!(tables.contains(&"EVENTS".to_string()));
}

/// Scenario: widened columns declare the wider type and every row reads back
#[test]
fn widened_columns_declare_the_wider_type_and_every_row_reads_back() {
    setup();
    let mut conn = exa_conn();

    assert_eq!(
        declared_type(&mut conn, VS_DIRECT, "WIDENED", "QTY"),
        "DECIMAL(20,0)"
    );

    let cols = conn.query_columns(&format!(
        "SELECT ID, QTY, PRICE FROM {} ORDER BY ID",
        vs_table(VS_DIRECT, "WIDENED")
    ));
    assert_eq!(
        cols[0].len(),
        4,
        "widened/ must return the rows of both files"
    );
    let expected_qty = [10i64, 20, 5_000_000_000, 40];
    let expected_price = [1.5f64, 2.5, 3.5, 4.5];
    for row in 0..4 {
        assert_eq!(parse_int(&cols[1][row]), expected_qty[row]);
        assert!((parse_numeric(&cols[2][row]) - expected_price[row]).abs() < 1e-6);
    }
}

/// Scenario: a column missing from one file is declared and reads NULL there
#[test]
fn missing_column_declares_the_union_and_nulls_the_absent_column() {
    setup();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT A, B FROM {} ORDER BY A",
        vs_table(VS_DIRECT, "MISSING_COL")
    ));
    assert_eq!(cols[0].len(), 3);
    assert_eq!(value_to_string(&cols[1][0]), "x");
    assert_eq!(value_to_string(&cols[1][1]), "y");
    assert!(
        cols[1][2].is_null(),
        "the row from the B-less file must read NULL for B"
    );
    assert_eq!(parse_int(&cols[0][2]), 3);
}

/// Scenario: an incompatible column pair fails CREATE VIRTUAL SCHEMA naming the column and both files
#[test]
fn incompatible_pair_fails_create_and_refresh_naming_column_and_files() {
    setup();
    let mut conn = exa_conn();

    let resp = try_create_direct_vs(
        &mut conn,
        VS_INCOMPATIBLE,
        CONN_INCOMPATIBLE,
        BASE_INCOMPATIBLE,
    );
    let msg = rejection_message(&resp, "the incompatible pair");
    assert_mentions(msg, &["X", "file1.parquet", "file2.parquet"]);
    assert!(
        !msg.contains("minioadmin"),
        "error must not leak credential values: {msg}"
    );
}

/// Scenario: `MERGE_SCHEMA = 'FALSE'` declares the sampled narrow type and a fitting projection reads all rows
#[test]
fn merge_schema_false_declares_the_narrow_sampled_type_and_reads_the_fitting_projection() {
    setup();
    let mut conn = exa_conn();

    assert_eq!(
        declared_type(&mut conn, VS_DIRECT_NARROW, "WIDENED", "QTY"),
        "DECIMAL(10,0)",
        "MERGE_SCHEMA = 'FALSE' must declare the narrow (Int32) sampled type"
    );

    let cols = conn.query_columns(&format!(
        "SELECT ID, PRICE FROM {} ORDER BY ID",
        vs_table(VS_DIRECT_NARROW, "WIDENED")
    ));
    assert_eq!(
        cols[0].len(),
        4,
        "a fitting projection must still return both files' rows"
    );
    let expected_price = [1.5f64, 2.5, 3.5, 4.5];
    for row in 0..4 {
        assert!((parse_numeric(&cols[1][row]) - expected_price[row]).abs() < 1e-6);
    }
}

/// Scenario: a value outside the stale narrow declaration surfaces a clean error
#[test]
fn stale_declaration_decides_the_emitted_width() {
    setup();
    let mut conn = exa_conn();

    let resp = conn.try_execute(&format!(
        "SELECT QTY FROM {}",
        vs_table(VS_DIRECT_NARROW, "WIDENED")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "a QTY value outside the narrow declared width must surface a clean error: {resp}"
    );
}

/// Scenario: a Delta directory read as raw Parquet deliberately returns tombstoned rows
#[test]
fn delta_directory_read_as_raw_parquet_returns_tombstoned_rows() {
    setup();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT ID FROM {} ORDER BY ID",
        vs_table(VS_DIRECT, "DELTA_CAVEAT")
    ));
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(
        ids,
        vec![10, 20],
        "both file1 (current) and file2 (tombstoned by the transaction log) must be \
         returned, since a raw-Parquet read applies no remove action: {ids:?}"
    );
}

/// Scenario: only first-level directories with a data file become tables and `NAMESPACE` narrows to a subtree
#[test]
fn discovery_scopes_to_first_level_directories_and_namespace_narrows_to_a_subtree() {
    setup();
    let mut conn = exa_conn();

    let tables = served_tables(&mut conn, VS_DISCOVERY);
    assert!(tables.contains(&"ORDERS".to_string()));
    assert!(tables.contains(&"DEEP".to_string()));
    assert!(!tables.contains(&"EMPTY".to_string()));
    assert!(!tables.contains(&"HIDDEN_ONLY".to_string()));
    assert!(
        !tables.contains(&"ORDERS_EU".to_string()),
        "sub/ is a table whose files live below it, not a namespace: {tables:?}"
    );

    let deep_cols = conn.query_columns(&format!(
        "SELECT ID, Y FROM {} ",
        vs_table(VS_DISCOVERY, "DEEP")
    ));
    assert_eq!(parse_int(&deep_cols[0][0]), 200);
    assert_eq!(value_to_string(&deep_cols[1][0]), "2026");
    assert_eq!(
        declared_columns(&mut conn, VS_DISCOVERY, "DEEP"),
        ["ID", "Y"],
        "a y=2026 path segment must become the partition column Y, after the Parquet column"
    );

    let ns_tables = served_tables(&mut conn, VS_DISCOVERY_NS);
    assert_eq!(ns_tables, vec!["ORDERS_EU".to_string()]);
    let ns_cols = conn.query_columns(&format!(
        "SELECT ID FROM {}",
        vs_table(VS_DISCOVERY_NS, "ORDERS_EU")
    ));
    assert_eq!(parse_int(&ns_cols[0][0]), 300);

    let empty_ns_tables = served_tables(&mut conn, VS_DISCOVERY_EMPTY_NS);
    assert!(
        empty_ns_tables.is_empty(),
        "a NAMESPACE naming an empty path must create with no tables"
    );
}

/// Scenario: malformed CONNECTIONs are rejected at CREATE VIRTUAL SCHEMA without leaking credentials
#[test]
fn malformed_connections_are_rejected_at_create_virtual_schema() {
    setup();
    let mut conn = exa_conn();

    let s3_secret = "SUPER_SECRET_S3_KEY";
    let azure_secret = "SUPER_SECRET_AZURE_KEY";

    let cases: Vec<(&str, String, CatalogConnectionPassword)> = vec![
        (
            "REJECTS_WAREHOUSE",
            BASE_DISCOVERY.to_string(),
            CatalogConnectionPassword {
                warehouse: "s3://warehouse/".to_string(),
                secret_key: s3_secret.to_string(),
                ..direct_storage_password()
            },
        ),
        (
            "REJECTS_TOKEN",
            BASE_DISCOVERY.to_string(),
            CatalogConnectionPassword {
                token: Some("bogus-token".to_string()),
                secret_key: s3_secret.to_string(),
                ..direct_storage_password()
            },
        ),
        (
            "REJECTS_PLAINTEXT_ABFS",
            "abfs://container@account.dfs.core.windows.net/lake".to_string(),
            CatalogConnectionPassword {
                account_name: Some("myaccount".to_string()),
                account_key: Some(azure_secret.to_string()),
                ..Default::default()
            },
        ),
        (
            "REJECTS_EMPTY_ADDRESS",
            String::new(),
            CatalogConnectionPassword {
                secret_key: s3_secret.to_string(),
                ..direct_storage_password()
            },
        ),
        (
            "REJECTS_AZURE_CREDS_ON_S3_SCHEME",
            "s3://warehouse/direct/".to_string(),
            CatalogConnectionPassword {
                account_name: Some("myaccount".to_string()),
                account_key: Some(azure_secret.to_string()),
                ..Default::default()
            },
        ),
    ];

    for (index, (label, address, password)) in cases.into_iter().enumerate() {
        let conn_name = format!("DIRECT_STORAGE_BAD_CREDS_{index}");
        let resp = try_create_virtual_schema_with_password(
            &mut conn,
            &direct_vs(label, &conn_name),
            &address,
            &password,
        );
        let msg = rejection_message(&resp, label);
        assert!(
            !msg.contains(s3_secret),
            "{label} must not leak the S3 secret: {msg}"
        );
        assert!(
            !msg.contains(azure_secret),
            "{label} must not leak the Azure key: {msg}"
        );
        if label == "REJECTS_WAREHOUSE" {
            assert_mentions(msg, &["warehouse"]);
        }
        if label == "REJECTS_TOKEN" {
            assert_mentions(msg, &["token"]);
        }
        if label == "REJECTS_PLAINTEXT_ABFS" {
            assert_mentions(msg, &["abfss"]);
        }
        if label == "REJECTS_EMPTY_ADDRESS" {
            assert!(
                msg.contains("storage base path") || msg.contains("address"),
                "{label}: {msg}"
            );
        }
        if label == "REJECTS_AZURE_CREDS_ON_S3_SCHEME" {
            assert_mentions(msg, &["s3"]);
        }
    }

    let resp = try_create_virtual_schema_with_password(
        &mut conn,
        &direct_vs(
            "REJECTS_MERGE_SCHEMA",
            "DIRECT_STORAGE_BAD_MERGE_SCHEMA_CREDS",
        )
        .with_merge_schema("NOTABOOL"),
        BASE_DISCOVERY,
        &direct_storage_password(),
    );
    rejection_message(&resp, "an unparseable MERGE_SCHEMA value");
}

/// Scenario: projection, filter, and `LIMIT` reach the scan spec
#[test]
fn projection_filter_and_limit_reach_the_scan() {
    setup();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT EVENT_ID, NAME FROM {} WHERE EVENT_ID > 2 ORDER BY EVENT_ID LIMIT 2",
        vs_table(VS_DIRECT, "EVENTS")
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols[0].len(), 2);
    assert_eq!(parse_int(&cols[0][0]), 3);
    assert_eq!(value_to_string(&cols[1][0]), "carol");
    assert_eq!(parse_int(&cols[0][1]), 4);
    assert_eq!(value_to_string(&cols[1][1]), "dave");

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.to_uppercase().contains("EVENT_ID"),
        "pushed spec must carry the projected column: {pushed_sql}"
    );
    assert!(
        pushed_sql.contains("\"limit\":2"),
        "pushed spec must carry the limit: {pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("\"filter\":null"),
        "pushed spec must carry the predicate: {pushed_sql}"
    );
}

/// Scenario: single-group and `GROUP BY` aggregates match the unpushed answer
#[test]
fn group_by_aggregate_matches_the_unpushed_answer() {
    setup();
    let mut conn = exa_conn();

    let table = vs_table(VS_DIRECT, "EVENTS");
    let single_group = conn.query_columns(&format!("SELECT COUNT(*), SUM(EVENT_ID) FROM {table}"));
    assert_eq!(parse_int(&single_group[0][0]), 5);
    assert_eq!(parse_int(&single_group[1][0]), 15);

    let raw = conn.query_columns(&format!("SELECT EVENT_ID, IS_ACTIVE FROM {table}"));
    let mut expected: std::collections::HashMap<bool, (i64, i64)> =
        std::collections::HashMap::new();
    for (id_cell, active_cell) in raw[0].iter().zip(raw[1].iter()) {
        let id = parse_int(id_cell);
        let active = active_cell.as_bool().expect("IS_ACTIVE must be boolean");
        let entry = expected.entry(active).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += id;
    }

    let grouped = conn.query_columns(&format!(
        "SELECT IS_ACTIVE, COUNT(*), SUM(EVENT_ID) FROM {table} GROUP BY IS_ACTIVE"
    ));
    assert_eq!(grouped[0].len(), expected.len());
    for ((active_cell, count_cell), sum_cell) in grouped[0]
        .iter()
        .zip(grouped[1].iter())
        .zip(grouped[2].iter())
    {
        let active = active_cell.as_bool().expect("IS_ACTIVE must be boolean");
        let count = parse_int(count_cell);
        let sum = parse_int(sum_cell);
        let (expected_count, expected_sum) = expected
            .get(&active)
            .unwrap_or_else(|| panic!("unexpected IS_ACTIVE group {active}"));
        assert_eq!(count, *expected_count);
        assert_eq!(sum, *expected_sum);
    }
}

/// Scenario: a two-table inner equi-join matches the unpushed answer in one pushdown request
#[test]
fn two_table_join_matches_the_unpushed_answer_in_one_request() {
    setup();
    let mut conn = exa_conn();

    let events_table = vs_table(VS_DIRECT, "EVENTS");
    let labels_table = vs_table(VS_DIRECT, "EVENT_LABELS");

    let events = conn.query_columns(&format!("SELECT EVENT_ID, NAME FROM {events_table}"));
    let labels = conn.query_columns(&format!("SELECT EVENT_ID, LABEL FROM {labels_table}"));
    let names: std::collections::HashMap<i64, String> = events[0]
        .iter()
        .zip(events[1].iter())
        .map(|(id, name)| (parse_int(id), value_to_string(name)))
        .collect();
    let mut expected: Vec<(i64, String, String)> = labels[0]
        .iter()
        .zip(labels[1].iter())
        .map(|(id, label)| {
            let id = parse_int(id);
            let name = names
                .get(&id)
                .unwrap_or_else(|| panic!("EVENT_LABELS EVENT_ID {id} has no matching event"))
                .clone();
            (id, name, value_to_string(label))
        })
        .collect();
    expected.sort();

    let join_sql = format!(
        "SELECT e.EVENT_ID, e.NAME, l.LABEL FROM {events_table} e \
         JOIN {labels_table} l ON e.EVENT_ID = l.EVENT_ID"
    );
    let cols = conn.query_columns(&join_sql);
    let mut actual: Vec<(i64, String, String)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .zip(cols[2].iter())
        .map(|((id, name), label)| (parse_int(id), value_to_string(name), value_to_string(label)))
        .collect();
    actual.sort();
    assert_eq!(actual, expected);

    let pushed_sql = explain_virtual_sql(&mut conn, &join_sql).to_lowercase();
    assert!(
        pushed_sql.contains("events"),
        "join pushdown request must name EVENTS: {pushed_sql}"
    );
    assert!(
        pushed_sql.contains("event_labels"),
        "join pushdown request must name EVENT_LABELS: {pushed_sql}"
    );
}

const SALES_2026_FILE: &str = "year=2026/month=09/p1.parquet";

fn nullable_string(value: &Json) -> Option<String> {
    (!value.is_null()).then(|| value_to_string(value))
}

fn int_column(cells: &[Json]) -> Vec<i64> {
    let mut ids: Vec<i64> = cells.iter().map(parse_int).collect();
    ids.sort();
    ids
}

/// Scenario: hive segments declare trailing `VARCHAR` partition columns with decoded values
#[test]
fn hive_segments_declare_varchar_partition_columns_with_decoded_values() {
    setup();
    let mut conn = exa_conn();

    assert_eq!(
        declared_columns(&mut conn, VS_DIRECT, "SALES"),
        ["ID", "AMOUNT", "DISCOUNT", "YEAR", "MONTH"],
        "partition columns must follow the folded Parquet columns"
    );
    for partition_column in ["YEAR", "MONTH"] {
        assert_declared_type(
            &mut conn,
            VS_DIRECT,
            "SALES",
            partition_column,
            "VARCHAR(2000000)",
        );
    }

    let cols = conn.query_columns(&format!(
        "SELECT ID, \"YEAR\", \"MONTH\" FROM {} ORDER BY ID",
        vs_table(VS_DIRECT, "SALES")
    ));
    let rows: Vec<(i64, Option<String>, Option<String>)> = (0..cols[0].len())
        .map(|row| {
            (
                parse_int(&cols[0][row]),
                nullable_string(&cols[1][row]),
                nullable_string(&cols[2][row]),
            )
        })
        .collect();
    let some = |value: &str| Some(value.to_string());
    assert_eq!(
        rows,
        vec![
            (1, some("2026"), some("09")),
            (2, some("2026"), some("09")),
            (3, some("2025"), None),
            (4, some("2024"), some("01")),
        ],
        "each row must carry its own file's partition values"
    );

    assert_declared_type(
        &mut conn,
        VS_DIRECT,
        "ENCODED",
        "REGION",
        "VARCHAR(2000000)",
    );
    let regions: Vec<String> = string_column(
        &mut conn,
        &format!("SELECT REGION FROM {}", vs_table(VS_DIRECT, "ENCODED")),
    );
    assert_eq!(regions, ["a/b"], "a partition value must percent-decode");
}

/// Scenario: a mixed layout unions partition keys and nulls the missing key
#[test]
fn mixed_layout_unions_partition_keys_and_nulls_the_missing_key() {
    setup();
    let mut conn = exa_conn();

    assert_eq!(
        declared_columns(&mut conn, VS_DIRECT, "MIXED"),
        ["ID", "YEAR"]
    );
    let cols = conn.query_columns(&format!(
        "SELECT ID, \"YEAR\" FROM {} ORDER BY ID",
        vs_table(VS_DIRECT, "MIXED")
    ));
    let rows: Vec<(i64, Option<String>)> = (0..cols[0].len())
        .map(|row| (parse_int(&cols[0][row]), nullable_string(&cols[1][row])))
        .collect();
    assert_eq!(
        rows,
        vec![(1, None), (2, Some("2026".to_string()))],
        "the file lacking the year= segment must read NULL for YEAR"
    );

    assert_eq!(
        declared_columns(&mut conn, VS_DIRECT_NARROW, "MIXED"),
        ["ID"],
        "MERGE_SCHEMA = 'FALSE' must declare the sampled file's keys alone"
    );
    let narrow = conn.query_columns(&format!(
        "SELECT ID FROM {}",
        vs_table(VS_DIRECT_NARROW, "MIXED")
    ));
    assert_eq!(
        int_column(&narrow[0]),
        [1, 2],
        "an ignored key must neither fail the query nor drop a row"
    );
}

/// Scenario: a partition key colliding with a Parquet column overrides it
#[test]
fn partition_key_colliding_with_a_parquet_column_overrides_it() {
    setup();
    let mut conn = exa_conn();

    assert_eq!(
        declared_columns(&mut conn, VS_DIRECT, "COLLISION_OVERRIDE"),
        ["ID", "K"],
        "K must be declared exactly once, as the partition column"
    );
    assert_declared_type(
        &mut conn,
        VS_DIRECT,
        "COLLISION_OVERRIDE",
        "K",
        "VARCHAR(2000000)",
    );
    let values: Vec<String> = string_column(
        &mut conn,
        &format!(
            "SELECT K FROM {}",
            vs_table(VS_DIRECT, "COLLISION_OVERRIDE")
        ),
    );
    assert_eq!(
        values,
        ["1"],
        "K must read the k=1 directory value, never the stored 99"
    );

    let stored = conn.query_columns(&format!(
        "SELECT K FROM {}",
        vs_table(VS_HIVE_OFF, "COLLISION_OVERRIDE")
    ));
    assert_eq!(
        int_column(&stored[0]),
        [99],
        "with hive partitioning off, K must read the file's own stored value"
    );
}

/// Scenario: a colliding column in a file with no matching segment fails CREATE VIRTUAL SCHEMA
#[test]
fn partition_key_collision_with_a_missing_segment_fails_the_refresh() {
    setup();
    let mut conn = exa_conn();

    let resp = try_create_direct_vs(
        &mut conn,
        VS_COLLISION_MISSING,
        CONN_COLLISION_MISSING,
        BASE_COLLISION_MISSING,
    );
    let msg = rejection_message(&resp, "a K-storing file with no k= segment");
    assert_mentions(
        msg,
        &[
            "column 'K'",
            "partition key 'k='",
            "collision_missing_segment/p2.parquet",
        ],
    );
    assert!(
        !msg.contains("minioadmin"),
        "error must not leak credential values: {msg}"
    );

    create_virtual_schema_with_password(
        &mut conn,
        &direct_vs(VS_COLLISION_MISSING_HIVE_OFF, CONN_COLLISION_MISSING)
            .with_hive_partitioning("FALSE"),
        BASE_COLLISION_MISSING,
        &direct_storage_password(),
    );
    assert_eq!(
        served_tables(&mut conn, VS_COLLISION_MISSING_HIVE_OFF),
        ["COLLISION_MISSING_SEGMENT"]
    );
    let stored = conn.query_columns(&format!(
        "SELECT K FROM {}",
        vs_table(VS_COLLISION_MISSING_HIVE_OFF, "COLLISION_MISSING_SEGMENT")
    ));
    assert_eq!(
        int_column(&stored[0]),
        [42, 99],
        "with hive partitioning off, every file must read its own stored K"
    );
}

/// Scenario: `HIVE_PARTITIONING = 'FALSE'` declares no partition columns and prunes no file
#[test]
fn hive_partitioning_false_declares_no_partition_columns() {
    setup();
    let mut conn = exa_conn();

    let expected: [(&str, &[&str]); 4] = [
        ("SALES", &["ID", "AMOUNT", "DISCOUNT"]),
        ("ENCODED", &["ID"]),
        ("MIXED", &["ID"]),
        ("REGIONS", &["ID", "TS"]),
    ];
    for (table, columns) in expected {
        assert_eq!(
            declared_columns(&mut conn, VS_HIVE_OFF, table),
            columns,
            "{table} must declare no partition column"
        );
    }
    let count = conn.query_columns(&format!(
        "SELECT COUNT(*) FROM {}",
        vs_table(VS_HIVE_OFF, "SALES")
    ));
    assert_eq!(parse_int(&count[0][0]), 4, "no SALES file may be pruned");
}

/// Scenario: a partition filter prunes the resolved file list
#[test]
fn partition_filter_prunes_the_resolved_file_list() {
    setup();
    let mut conn = exa_conn();

    for predicate in ["\"YEAR\" = '2026'", "\"YEAR\" > '2025'"] {
        let sql = format!(
            "SELECT ID FROM {} WHERE {predicate}",
            vs_table(VS_DIRECT, "SALES")
        );
        let pushed = explain_virtual_sql(&mut conn, &sql);
        assert!(
            pushed.contains(SALES_2026_FILE),
            "{predicate}: the pushed scan must name the year=2026 file: {pushed}"
        );
        for pruned in ["year=2025/", "year=2024/"] {
            assert!(
                !pushed.contains(pruned),
                "{predicate}: the pushed scan must not name a {pruned} file: {pushed}"
            );
        }

        let rows = conn.query_columns(&sql);
        assert_eq!(int_column(&rows[0]), [1, 2], "{predicate}");
    }
}

/// Scenario: a column only pruned files carry reads NULL
#[test]
fn a_column_only_pruned_files_carry_reads_null() {
    setup();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ID, DISCOUNT FROM {} WHERE \"YEAR\" = '2025'",
        vs_table(VS_DIRECT, "SALES")
    );
    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed.contains("year=2025/") && !pushed.contains("year=2026/"),
        "the scan must keep only the DISCOUNT-less year=2025 file: {pushed}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(int_column(&cols[0]), [3]);
    assert!(
        cols[1][0].is_null(),
        "DISCOUNT must read NULL from a file lacking it"
    );
}

fn natively_selected_regions(conn: &mut ExaConn, predicate: &str) -> BTreeSet<String> {
    let values = REGION_VALUES
        .iter()
        .map(|region| format!("SELECT CAST('{region}' AS VARCHAR(2000000)) AS R"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    string_column(conn, &format!("SELECT R FROM ({values}) WHERE {predicate}"))
}

fn regions_named_by(pushed_sql: &str) -> BTreeSet<String> {
    pushed_sql
        .split("region=")
        .skip(1)
        .filter_map(|rest| rest.split_once("/p.parquet"))
        .map(|(encoded, _)| {
            percent_encoding::percent_decode_str(encoded)
                .decode_utf8()
                .unwrap_or_else(|e| panic!("REGIONS path segment {encoded} is not UTF-8: {e}"))
                .into_owned()
        })
        .collect()
}

fn returned_regions(conn: &mut ExaConn, predicate: &str) -> BTreeSet<String> {
    string_column(
        conn,
        &format!(
            "SELECT REGION FROM {} WHERE {predicate}",
            vs_table(VS_DIRECT, "REGIONS")
        ),
    )
}

/// Scenario: range pruning matches Exasol's native `VARCHAR` ordering
#[test]
fn range_pruning_matches_exasols_native_varchar_ordering() {
    setup();
    let mut conn = exa_conn();

    let templates = [
        "{column} > 'Z'",
        "{column} < 'z'",
        "{column} BETWEEN 'B' AND 'a'",
    ];
    let natives: Vec<BTreeSet<String>> = templates
        .iter()
        .map(|template| natively_selected_regions(&mut conn, &template.replace("{column}", "R")))
        .collect();
    for (template, native) in templates.iter().zip(&natives) {
        let predicate = template.replace("{column}", "REGION");

        let pushed = explain_virtual_sql(
            &mut conn,
            &format!(
                "SELECT ID FROM {} WHERE {predicate}",
                vs_table(VS_DIRECT, "REGIONS")
            ),
        );
        assert_eq!(
            &regions_named_by(&pushed),
            native,
            "{predicate}: the pushed scan's files must match Exasol's native selection: {pushed}"
        );
        assert_eq!(
            &returned_regions(&mut conn, &predicate),
            native,
            "{predicate}: the returned rows must match Exasol's native selection"
        );
    }

    let native = &natives[0];
    let predicate = "REGION > 'Z' AND SECOND(TS, 3) > 1";
    let pushed = explain_virtual_sql(
        &mut conn,
        &format!(
            "SELECT ID FROM {} WHERE {predicate}",
            vs_table(VS_DIRECT, "REGIONS")
        ),
    );
    assert_eq!(
        &regions_named_by(&pushed),
        native,
        "{predicate}: pruning must still run on the REGION conjunct: {pushed}"
    );
    assert_eq!(
        &returned_regions(&mut conn, predicate),
        native,
        "{predicate}: the declined conjunct holds on every row, so the row set must not change"
    );
}

/// Scenario: a partition predicate no file satisfies returns zero rows without error
#[test]
fn zero_matching_files_prune_to_zero_rows_without_error() {
    setup();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ID FROM {} WHERE \"YEAR\" = '2099'",
        vs_table(VS_DIRECT, "SALES")
    );
    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        !pushed.contains("year="),
        "the pushed scan must name no SALES file: {pushed}"
    );

    assert_eq!(
        conn.query_row_count(&sql),
        0,
        "a query keeping no file must return zero rows"
    );
}
