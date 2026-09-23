//! End-to-end coverage for `CATALOG_KIND = 'DIRECT_STORAGE'`: a plain directory of
//! Parquet files on MinIO, queried through the Virtual Schema with no catalog service
//! involved. Per project rules this suite FAILS (never skips) when the stack is
//! unreachable.
#![cfg(feature = "exasol-e2e")]

mod common;

use common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCHEMA_NAME, VsProps, create_schema_and_scripts,
    create_virtual_schema_with_password, exa_conn, explain_virtual_sql, install_slc,
    local_stack_creds, local_stack_s3_store, local_stack_storage, parse_int, parse_numeric,
    upload_so, value_to_string,
};
use common::exasol_ws::ExaConn;
use common::raw_parquet::write_parquet_fixture;
use common::stack::{
    CatalogConnectionPassword, build_create_connection_sql, minio_url_internal, wait_for_exasol,
    wait_for_minio,
};

use lakehouse_engine::adapter::parquet_directory::{DirectoryOptions, MergeMode};
use lakehouse_engine::adapter::pushdown::{
    ConnectionStorage, ResolvedScan, ScanSource, format_reader,
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
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::{Value as Json, json};

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

// ---------------------------------------------------------------------------
// Virtual Schemas / CONNECTIONs / base paths
// ---------------------------------------------------------------------------

const BASE_DIRECT: &str = "s3://warehouse/direct/";
const BASE_INCOMPATIBLE: &str = "s3://warehouse/direct_incompatible/";
const BASE_DISCOVERY: &str = "s3://warehouse/direct_discovery/";
const BASE_COLLISION: &str = "s3://warehouse/direct_hive_collision/";
const BASE_COLLISION_MISSING: &str = "s3://warehouse/direct_hive_collision_missing/";

const VS_DIRECT: &str = "DIRECT_LAKEHOUSE";
const VS_DIRECT_NARROW: &str = "DIRECT_LAKEHOUSE_NARROW";
const VS_HIVE_OFF: &str = "DIRECT_HIVE_OFF";
const VS_INCOMPATIBLE: &str = "DIRECT_INCOMPATIBLE_VS";
const VS_DISCOVERY: &str = "DIRECT_DISCOVERY_VS";
const VS_DISCOVERY_NS: &str = "DIRECT_DISCOVERY_NS_VS";
const VS_DISCOVERY_EMPTY_NS: &str = "DIRECT_DISCOVERY_EMPTY_NS_VS";
const VS_COLLISION: &str = "DIRECT_HIVE_COLLISION";
const VS_COLLISION_HIVE_OFF: &str = "DIRECT_HIVE_COLLISION_OFF";
const VS_COLLISION_MISSING: &str = "DIRECT_HIVE_COLLISION_MISSING";
const VS_COLLISION_MISSING_HIVE_OFF: &str = "DIRECT_HIVE_COLLISION_MISSING_OFF";

const CONN_DIRECT: &str = "DIRECT_STORAGE_CREDS";
const CONN_INCOMPATIBLE: &str = "DIRECT_STORAGE_INCOMPATIBLE_CREDS";
const CONN_DISCOVERY: &str = "DIRECT_STORAGE_DISCOVERY_CREDS";
const CONN_COLLISION: &str = "DIRECT_STORAGE_COLLISION_CREDS";
const CONN_COLLISION_MISSING: &str = "DIRECT_STORAGE_COLLISION_MISSING_CREDS";

/// `CatalogConnectionPassword` for a direct-storage CONNECTION: static S3 creds,
/// no `warehouse` (the direct-storage kind rejects that field).
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

// ---------------------------------------------------------------------------
// One-time setup: write every fixture, provision every Virtual Schema.
// ---------------------------------------------------------------------------

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
            &VsProps::new(VS_DIRECT_NARROW, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DIRECT)
                .with_merge_schema("FALSE"),
            BASE_DIRECT,
            &direct_storage_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_HIVE_OFF, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DIRECT)
                .with_hive_partitioning("FALSE"),
            BASE_DIRECT,
            &direct_storage_password(),
        );

        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DISCOVERY, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DISCOVERY),
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

// ---------------------------------------------------------------------------
// Fixture writers
// ---------------------------------------------------------------------------

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

/// Row order: id, name, active, score. Shared ground truth for every EVENTS
/// assertion in this file.
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

/// `QTY` (`5_000_000_000`) exceeds `i32` — errors only under a narrow
/// (`MERGE_SCHEMA = 'FALSE'`) width; the wide/default-merge declaration reads it unchanged.
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

fn discovery_id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))])
        .expect("discovery-fixture batch construction is infallible")
}

/// One `list<utf8>` cell (`TAGS`), populated for row 0, null for row 1.
fn tags_column() -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    builder.values().append_option(Some("x"));
    builder.values().append_option(Some("y"));
    builder.append(true);
    builder.append_null();
    Arc::new(builder.finish())
}

/// One `struct<STREET: utf8, CITY: utf8>` cell (`ADDRESS`), populated for row
/// 0, null for row 1.
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

/// One `map<utf8, utf8>` cell (`ATTRS`): row 0 carries two entries, row 1 is
/// null (zero entries at a null offset span).
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

/// `sales_batch` plus a `DISCOUNT` column, which only the `year=2026` file carries.
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

/// Byte order ranks these `B < a < é`, which neither a case-insensitive nor a locale order does,
/// so a range predicate over them tells the orders apart. Row `n` of the fixture is `ID = n + 1`.
const REGION_VALUES: [&str; 3] = ["B", "a", "é"];

/// 2026-01-01 00:00:05.25 UTC: its `SECOND(TS, 3)` is 5.25, so `SECOND(TS, 3) > 1` holds on every row.
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

/// A stored `K` column that a `k=` directory segment collides with once uppercased.
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

/// PUTs raw bytes at `uri` — for Delta transaction-log JSON, which isn't Parquet
/// and so falls outside `write_parquet_fixture`'s contract.
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
    // events/ — mixed-type column set, two files.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}events/file1.parquet"),
        events_batch(&[1, 2, 3]),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}events/file2.parquet"),
        events_batch(&[4, 5]),
    );

    // nested/ — one table, two subdirectory files, zero partition columns.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}nested/A/p1.parquet"),
        nested_batch(1, "from-a"),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}nested/B/p2.parquet"),
        nested_batch(2, "from-b"),
    );

    // widened/ — file1 (narrow, sorts first) then file2 (wide).
    write_parquet_fixture(
        &format!("{BASE_DIRECT}widened/file1.parquet"),
        widened_narrow_file(),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}widened/file2.parquet"),
        widened_wide_file(),
    );

    // missing_col/ — file1 carries A and B, file2 carries only A.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}missing_col/file1.parquet"),
        missing_col_file1(),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}missing_col/file2.parquet"),
        missing_col_file2(),
    );

    // complex/ — one struct, one list, one map column.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}complex/file1.parquet"),
        complex_batch(),
    );

    // event_labels/ — join partner for events/, EVENT_ID subset {1,3,5}.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}event_labels/file1.parquet"),
        event_labels_batch(),
    );

    // delta_caveat/ — a Delta table directory read as raw Parquet.
    write_delta_caveat_fixture();

    // sales/ — three year partitions with disjoint rows; only year=2026 carries DISCOUNT.
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

    // encoded/ — a percent-encoded partition value, keyed verbatim as Spark writes it.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}encoded/region=a%2Fb/p.parquet"),
        discovery_id_batch(1),
    );

    // mixed/ — one plain directory, listed first, and one key=value directory.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}mixed/A/p.parquet"),
        discovery_id_batch(1),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}mixed/year=2026/p.parquet"),
        discovery_id_batch(2),
    );

    // regions/ — one file per REGION value.
    for (id, region) in (1..).zip(REGION_VALUES) {
        write_parquet_fixture(
            &format!("{BASE_DIRECT}regions/region={region}/p.parquet"),
            regions_batch(id),
        );
    }

    // Loose file and empty-of-data-files directory under the base path: neither becomes a table.
    write_parquet_fixture(
        &format!("{BASE_DIRECT}loose.parquet"),
        discovery_id_batch(999),
    );
    write_parquet_fixture(
        &format!("{BASE_DIRECT}empty/_SUCCESS"),
        discovery_id_batch(0),
    );

    // direct_incompatible/incompatible/ — isolated root: this pair fails enumeration,
    // must not share a base path with any passing scenario.
    write_parquet_fixture(
        &format!("{BASE_INCOMPATIBLE}incompatible/file1.parquet"),
        incompatible_file1(),
    );
    write_parquet_fixture(
        &format!("{BASE_INCOMPATIBLE}incompatible/file2.parquet"),
        incompatible_file2(),
    );

    // direct_hive_collision/ — isolated root: the k=1 segment overrides the stored K = 99.
    write_parquet_fixture(
        &format!("{BASE_COLLISION}collision_override/k=1/p.parquet"),
        stored_k_batch(1, 99),
    );

    // direct_hive_collision_missing/ — isolated root: p2 stores K but carries no k= segment,
    // which fails enumeration, so it must not share a base path with any passing scenario.
    write_parquet_fixture(
        &format!("{BASE_COLLISION_MISSING}collision_missing_segment/k=1/p1.parquet"),
        stored_k_batch(1, 99),
    );
    write_parquet_fixture(
        &format!("{BASE_COLLISION_MISSING}collision_missing_segment/p2.parquet"),
        stored_k_batch(2, 42),
    );

    // direct_discovery/ — discovery + NAMESPACE fixtures.
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

// ---------------------------------------------------------------------------
// Small query helpers
// ---------------------------------------------------------------------------

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

/// Asserts `column`'s declared type starts with `expected`, tolerant of Exasol's
/// `COLUMN_TYPE` rendering quirks (whitespace, charset suffix, `DOUBLE PRECISION`
/// -> `DOUBLE`, substituted `TIMESTAMP` precision).
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

fn served_tables(conn: &mut ExaConn, vs_name: &str) -> Vec<String> {
    let cols = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA='{vs_name}' ORDER BY TABLE_NAME"
    ));
    cols[0].iter().map(value_to_string).collect()
}

/// `table`'s declared column names, in declaration order.
fn declared_columns(conn: &mut ExaConn, vs_name: &str, table: &str) -> Vec<String> {
    let cols = conn.query_columns(&format!(
        "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS \
         WHERE COLUMN_SCHEMA='{vs_name}' AND COLUMN_TABLE='{table}' \
         ORDER BY COLUMN_ORDINAL_POSITION"
    ));
    cols[0].iter().map(value_to_string).collect()
}

// ---------------------------------------------------------------------------
// Fixture-shape guard
// ---------------------------------------------------------------------------

/// Reads the fixture's own Parquet footer back from MinIO (bypassing Exasol) to
/// catch a silently normalized fixture before it lets a later read test pass vacuously.
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

// ---------------------------------------------------------------------------
// Mixed-type directory, widening, missing column, incompatible pair,
// MERGE_SCHEMA=FALSE, Delta-directory caveat
// ---------------------------------------------------------------------------

/// `events/` declares the Arrow-to-Exasol mapping for every carried type and
/// returns the union of its two files' rows.
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

/// `nested/A/` and `nested/B/` union into one `NESTED` table with zero partition
/// columns — an unlimited-depth directory contributes files, not columns.
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

/// `complex/`'s struct, list, and map columns declare `VARCHAR(2000000)` and
/// render as parseable JSON.
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

/// A loose file and a data-file-less directory under the base path serve no
/// table; `CREATE VIRTUAL SCHEMA` still succeeds.
#[test]
fn loose_file_and_empty_directory_serve_no_table() {
    setup();
    let mut conn = exa_conn();
    let tables = served_tables(&mut conn, VS_DIRECT);
    assert!(!tables.contains(&"LOOSE".to_string()));
    assert!(!tables.contains(&"EMPTY".to_string()));
    assert!(tables.contains(&"EVENTS".to_string()));
}

/// `widened/`'s columns declare the WIDER type folded across both files; every row
/// reads back unchanged — the narrow file's values widened, the wide file's as-is.
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

/// `missing_col/` declares the union of both files' columns; the row from
/// the file lacking `B` reads NULL for it, unchanged for `A`.
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

/// A column pair no supported widening covers fails `CREATE VIRTUAL SCHEMA`, naming
/// the column and both files; uses the isolated `direct_incompatible/` root so no
/// passing scenario shares its enumeration.
#[test]
fn incompatible_pair_fails_create_and_refresh_naming_column_and_files() {
    setup();
    let mut conn = exa_conn();

    let create_conn_sql = build_create_connection_sql(
        CONN_INCOMPATIBLE,
        BASE_INCOMPATIBLE,
        &direct_storage_password(),
    );
    conn.execute(&create_conn_sql);
    let _ = conn.try_execute(&format!(
        "DROP VIRTUAL SCHEMA IF EXISTS {VS_INCOMPATIBLE} CASCADE"
    ));

    let resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_INCOMPATIBLE}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CONN_INCOMPATIBLE}'
  CATALOG_KIND        = 'DIRECT_STORAGE'
  ALLOW_HTTP          = 'true'"#
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "the incompatible pair must fail CREATE VIRTUAL SCHEMA: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains("X"),
        "error must name the offending column X: {msg}"
    );
    assert!(
        msg.contains("file1.parquet") || msg.contains("incompatible/file1.parquet"),
        "error must name file1: {msg}"
    );
    assert!(
        msg.contains("file2.parquet") || msg.contains("incompatible/file2.parquet"),
        "error must name file2: {msg}"
    );
    assert!(
        !msg.contains("minioadmin"),
        "error must not leak credential values: {msg}"
    );
}

/// Under `MERGE_SCHEMA = 'FALSE'`, `WIDENED.QTY` declares the NARROW type sampled
/// from the first-listed file; a query never touching the out-of-range column
/// still returns every row of both files.
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

/// A wide-file `QTY` value that doesn't fit the narrow declared type surfaces a
/// clean error, not a silently wrong/NULL value — proving the stale (narrow)
/// declaration reaches the scan plan, not just the refresh path.
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

/// A Delta table directory read as raw Parquet returns every file's rows, including
/// the tombstoned one its transaction log removes — deliberate, not a defect.
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

// ---------------------------------------------------------------------------
// Discovery, NAMESPACE, CONNECTION rejection, pushdown parity
// ---------------------------------------------------------------------------

/// Only first-level directories holding a data file become tables; `NAMESPACE`
/// scopes discovery to a subtree of the CONNECTION address.
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

/// A CONNECTION the direct-storage kind cannot accept is rejected at `CREATE VIRTUAL
/// SCHEMA`, naming the offending field or scheme and never a credential value.
#[test]
fn malformed_connections_are_rejected_at_create_virtual_schema() {
    setup();
    let mut conn = exa_conn();

    let base = direct_storage_password();
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
        let vs_name = label;
        conn.execute(&build_create_connection_sql(
            &conn_name, &address, &password,
        ));
        let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs_name} CASCADE"));
        let resp = conn.try_execute(&format!(
            r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{conn_name}'
  CATALOG_KIND        = 'DIRECT_STORAGE'
  ALLOW_HTTP          = 'true'"#
        ));
        assert_eq!(
            resp["status"].as_str(),
            Some("error"),
            "{label} must fail CREATE VIRTUAL SCHEMA: {resp}"
        );
        let msg = resp["exception"]["text"].as_str().unwrap_or("");
        assert!(
            !msg.contains(s3_secret),
            "{label} must not leak the S3 secret: {msg}"
        );
        assert!(
            !msg.contains(azure_secret),
            "{label} must not leak the Azure key: {msg}"
        );
        if label == "REJECTS_WAREHOUSE" {
            assert!(msg.contains("warehouse"), "{label}: {msg}");
        }
        if label == "REJECTS_TOKEN" {
            assert!(msg.contains("token"), "{label}: {msg}");
        }
        if label == "REJECTS_PLAINTEXT_ABFS" {
            assert!(
                msg.contains("abfss"),
                "{label} must name abfss as the accepted spelling: {msg}"
            );
        }
        if label == "REJECTS_EMPTY_ADDRESS" {
            assert!(
                msg.contains("storage base path") || msg.contains("address"),
                "{label}: {msg}"
            );
        }
        if label == "REJECTS_AZURE_CREDS_ON_S3_SCHEME" {
            assert!(msg.contains("s3"), "{label}: {msg}");
        }
    }

    // An unparseable MERGE_SCHEMA value likewise fails at CREATE VIRTUAL SCHEMA.
    let conn_name = "DIRECT_STORAGE_BAD_MERGE_SCHEMA_CREDS";
    conn.execute(&build_create_connection_sql(
        conn_name,
        BASE_DISCOVERY,
        &base,
    ));
    let _ = conn.try_execute("DROP VIRTUAL SCHEMA IF EXISTS REJECTS_MERGE_SCHEMA CASCADE");
    let resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA REJECTS_MERGE_SCHEMA
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{conn_name}'
  CATALOG_KIND        = 'DIRECT_STORAGE'
  MERGE_SCHEMA        = 'NOTABOOL'
  ALLOW_HTTP          = 'true'"#
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "an unparseable MERGE_SCHEMA value must fail CREATE VIRTUAL SCHEMA: {resp}"
    );
}

/// Projection, filter, and `LIMIT` reach the scan spec: returned rows match, and the
/// pushed spec carries the projected columns, the predicate, and the limit.
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

/// A single-group aggregate and a `GROUP BY` aggregate over `events/` each
/// return the same answer the equivalent unpushed query returns.
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

/// An INNER equi-join between `EVENTS` and `EVENT_LABELS` on `EVENT_ID` returns
/// exactly the unpushed join's rows, and `EXPLAIN VIRTUAL` carries ONE pushdown
/// request naming both tables.
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

// ---------------------------------------------------------------------------
// Hive partitioning: declaration, collisions, pruning, VARCHAR ordering
// ---------------------------------------------------------------------------

const HIVE_UNION: DirectoryOptions = DirectoryOptions {
    merge_mode: MergeMode::FoldEveryFile,
    hive_partitioning: true,
};

const HIVE_OFF_UNION: DirectoryOptions = DirectoryOptions {
    merge_mode: MergeMode::FoldEveryFile,
    hive_partitioning: false,
};

const SALES_2026_FILE: &str = "year=2026/month=09/p1.parquet";

/// Resolves `table` under `BASE_DIRECT` through the format-reader seam a pushdown request plans
/// through, in-process against live MinIO, so a test sees exactly which files a filter keeps.
fn resolve_in_process(
    table: &str,
    options: DirectoryOptions,
    filter: Option<&Json>,
) -> ResolvedScan {
    let store: Arc<dyn ObjectStore> = Arc::new(local_stack_s3_store("warehouse"));
    let storage = local_stack_storage();
    let creds = local_stack_creds();
    let connection = ConnectionStorage {
        storage: &storage,
        creds: &creds,
        allow_http: true,
    };
    let table_root = format!("{BASE_DIRECT}{table}");
    let reader = format_reader(
        ScanSource::DirectParquet {
            store: &store,
            table_root: &table_root,
            options,
            declared_columns: &[],
        },
        &connection,
    )
    .unwrap_or_else(|e| panic!("format_reader({table}) must succeed: {e}"));
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for in-process scan resolution")
        .block_on(reader.resolve_scan(filter))
        .unwrap_or_else(|e| panic!("resolve_scan({table}, {filter:?}) must succeed: {e}"))
}

/// The table-relative paths of the files `filter` keeps in `table`.
fn kept_paths(table: &str, filter: Option<&Json>) -> BTreeSet<String> {
    resolve_in_process(table, HIVE_UNION, filter)
        .files
        .into_iter()
        .map(|file| file.path)
        .collect()
}

fn column(name: &str) -> Json {
    json!({"type": "column", "name": name})
}

fn string(value: &str) -> Json {
    json!({"type": "literal_string", "value": value})
}

fn equal(left: Json, right: Json) -> Json {
    json!({"type": "predicate_equal", "left": left, "right": right})
}

/// Exasol advertises no greater-than predicate: it sends `a > b` as `b < a`.
fn less(left: Json, right: Json) -> Json {
    json!({"type": "predicate_less", "left": left, "right": right})
}

fn between(expression: Json, low: Json, high: Json) -> Json {
    json!({"type": "predicate_between", "expression": expression, "left": low, "right": high})
}

fn nullable_string(value: &Json) -> Option<String> {
    (!value.is_null()).then(|| value_to_string(value))
}

fn int_column(cells: &[Json]) -> Vec<i64> {
    let mut ids: Vec<i64> = cells.iter().map(parse_int).collect();
    ids.sort();
    ids
}

/// `sales/`'s `year=`/`month=` segments declare `VARCHAR` partition columns after its Parquet
/// columns; `__HIVE_DEFAULT_PARTITION__` reads NULL and `encoded/`'s `a%2Fb` decodes to `a/b`.
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
    let encoded = conn.query_columns(&format!(
        "SELECT REGION FROM {}",
        vs_table(VS_DIRECT, "ENCODED")
    ));
    let regions: Vec<String> = encoded[0].iter().map(value_to_string).collect();
    assert_eq!(regions, ["a/b"], "a partition value must percent-decode");
}

/// `mixed/`'s partition columns are the union of its files' keys, so the plain-directory file
/// reads NULL for `YEAR`; under `MERGE_SCHEMA = 'FALSE'` the sampled, first-listed plain-directory
/// file declares no key, and the other file's key is ignored without failing the query.
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

/// `collision_override/k=1/`'s directory value overrides the file's own stored `K = 99`, which
/// only a `HIVE_PARTITIONING = 'FALSE'` schema over the same base reads.
#[test]
fn partition_key_colliding_with_a_parquet_column_overrides_it() {
    setup();
    let mut conn = exa_conn();

    create_virtual_schema_with_password(
        &mut conn,
        &VsProps::new(VS_COLLISION, "")
            .with_catalog_kind("DIRECT_STORAGE")
            .with_catalog_conn_name(CONN_COLLISION),
        BASE_COLLISION,
        &direct_storage_password(),
    );
    create_virtual_schema_with_password(
        &mut conn,
        &VsProps::new(VS_COLLISION_HIVE_OFF, "")
            .with_catalog_kind("DIRECT_STORAGE")
            .with_catalog_conn_name(CONN_COLLISION)
            .with_hive_partitioning("FALSE"),
        BASE_COLLISION,
        &direct_storage_password(),
    );

    assert_eq!(
        declared_columns(&mut conn, VS_COLLISION, "COLLISION_OVERRIDE"),
        ["ID", "K"],
        "K must be declared exactly once, as the partition column"
    );
    assert_declared_type(
        &mut conn,
        VS_COLLISION,
        "COLLISION_OVERRIDE",
        "K",
        "VARCHAR(2000000)",
    );
    let overridden = conn.query_columns(&format!(
        "SELECT K FROM {}",
        vs_table(VS_COLLISION, "COLLISION_OVERRIDE")
    ));
    let values: Vec<String> = overridden[0].iter().map(value_to_string).collect();
    assert_eq!(
        values,
        ["1"],
        "K must read the k=1 directory value, never the stored 99"
    );

    let stored = conn.query_columns(&format!(
        "SELECT K FROM {}",
        vs_table(VS_COLLISION_HIVE_OFF, "COLLISION_OVERRIDE")
    ));
    assert_eq!(
        int_column(&stored[0]),
        [99],
        "with hive partitioning off, K must read the file's own stored value"
    );
}

/// A file storing `K` under no `k=` segment, beside a file under one, fails `CREATE VIRTUAL
/// SCHEMA` naming the column, the key, and that file; with hive partitioning off no key exists to
/// collide, so the same base declares its table. Uses the isolated
/// `direct_hive_collision_missing/` root so no passing scenario shares its enumeration.
#[test]
fn partition_key_collision_with_a_missing_segment_fails_the_refresh() {
    setup();
    let mut conn = exa_conn();

    conn.execute(&build_create_connection_sql(
        CONN_COLLISION_MISSING,
        BASE_COLLISION_MISSING,
        &direct_storage_password(),
    ));
    let _ = conn.try_execute(&format!(
        "DROP VIRTUAL SCHEMA IF EXISTS {VS_COLLISION_MISSING} CASCADE"
    ));
    let resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_COLLISION_MISSING}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CONN_COLLISION_MISSING}'
  CATALOG_KIND        = 'DIRECT_STORAGE'
  ALLOW_HTTP          = 'true'"#
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "a K-storing file with no k= segment must fail CREATE VIRTUAL SCHEMA: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains("column 'K'"),
        "error must name the column K: {msg}"
    );
    assert!(
        msg.contains("partition key 'k='"),
        "error must name the key k: {msg}"
    );
    assert!(
        msg.contains("collision_missing_segment/p2.parquet"),
        "error must name the file lacking the segment: {msg}"
    );
    assert!(
        !msg.contains("minioadmin"),
        "error must not leak credential values: {msg}"
    );

    create_virtual_schema_with_password(
        &mut conn,
        &VsProps::new(VS_COLLISION_MISSING_HIVE_OFF, "")
            .with_catalog_kind("DIRECT_STORAGE")
            .with_catalog_conn_name(CONN_COLLISION_MISSING)
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

/// Under `HIVE_PARTITIONING = 'FALSE'` a `key=value` segment is a plain directory: no table
/// declares a partition column, every file entry carries an empty value map, and none is pruned.
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

    let resolved = resolve_in_process("sales", HIVE_OFF_UNION, None);
    assert!(resolved.partition_columns.is_empty());
    assert_eq!(resolved.files.len(), 3);
    assert!(
        resolved
            .files
            .iter()
            .all(|file| file.partition_values.is_empty()),
        "every file entry must carry an empty partition-value map: {:?}",
        resolved.files
    );
}

/// `YEAR = '2026'` and `YEAR > '2025'` each keep only the `year=2026` file, in-process and in the
/// scan Exasol is handed, and return that file's rows.
#[test]
fn partition_filter_prunes_the_resolved_file_list() {
    setup();
    let mut conn = exa_conn();

    let unfiltered = kept_paths("sales", None);
    assert_eq!(unfiltered.len(), 3, "unfiltered SALES: {unfiltered:?}");

    let cases = [
        ("\"YEAR\" = '2026'", equal(column("YEAR"), string("2026"))),
        ("\"YEAR\" > '2025'", less(string("2025"), column("YEAR"))),
    ];
    for (predicate, filter) in cases {
        let kept = kept_paths("sales", Some(&filter));
        assert!(
            kept.len() < unfiltered.len(),
            "{predicate} must prune: kept {kept:?} of {unfiltered:?}"
        );
        assert_eq!(
            kept,
            BTreeSet::from([SALES_2026_FILE.to_string()]),
            "{predicate} must keep only the year=2026 file"
        );

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

/// `DISCOUNT`, declared from the `year=2026` file's footer, still reads NULL when pruning keeps
/// only the `year=2025` file, whose footer lacks it.
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

/// The regions of `REGION_VALUES` Exasol's own `VARCHAR` comparison selects for `predicate`,
/// written over the column `R`.
fn natively_selected_regions(conn: &mut ExaConn, predicate: &str) -> BTreeSet<String> {
    let values = REGION_VALUES
        .iter()
        .map(|region| format!("SELECT CAST('{region}' AS VARCHAR(2000000)) AS R"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let cols = conn.query_columns(&format!("SELECT R FROM ({values}) WHERE {predicate}"));
    cols[0].iter().map(value_to_string).collect()
}

fn regions_of(kept_paths: &BTreeSet<String>) -> BTreeSet<String> {
    kept_paths
        .iter()
        .map(|path| {
            let encoded = path
                .strip_prefix("region=")
                .and_then(|rest| rest.strip_suffix("/p.parquet"))
                .unwrap_or_else(|| panic!("unexpected REGIONS file path {path}"));
            percent_encoding::percent_decode_str(encoded)
                .decode_utf8()
                .unwrap_or_else(|e| {
                    panic!("REGIONS file path segment {encoded} is not valid UTF-8: {e}")
                })
                .into_owned()
        })
        .collect()
}

fn regions_named_by(pushed_sql: &str) -> BTreeSet<String> {
    const PATH_UNSAFE: &percent_encoding::AsciiSet = &percent_encoding::AsciiSet::EMPTY
        .add(b'%')
        .add(b'#')
        .add(b'?');
    REGION_VALUES
        .iter()
        .filter(|region| {
            let encoded = percent_encoding::utf8_percent_encode(region, PATH_UNSAFE).to_string();
            pushed_sql.contains(&format!("region={encoded}/p.parquet"))
        })
        .map(|region| region.to_string())
        .collect()
}

fn returned_regions(conn: &mut ExaConn, predicate: &str) -> BTreeSet<String> {
    let cols = conn.query_columns(&format!(
        "SELECT REGION FROM {} WHERE {predicate}",
        vs_table(VS_DIRECT, "REGIONS")
    ));
    cols[0].iter().map(value_to_string).collect()
}

/// A range or `BETWEEN` predicate on `REGION` keeps exactly the files whose value Exasol's own
/// `VARCHAR` comparison selects, in-process, in the scan Exasol is handed, and in the rows
/// returned; a declined conjunct beside it changes none of the three.
#[test]
fn range_pruning_matches_exasols_native_varchar_ordering() {
    setup();
    let mut conn = exa_conn();

    let cases = [
        ("{column} > 'Z'", less(string("Z"), column("REGION"))),
        ("{column} < 'z'", less(column("REGION"), string("z"))),
        (
            "{column} BETWEEN 'B' AND 'a'",
            between(column("REGION"), string("B"), string("a")),
        ),
    ];
    for (template, filter) in cases {
        let native = natively_selected_regions(&mut conn, &template.replace("{column}", "R"));
        let predicate = template.replace("{column}", "REGION");

        assert_eq!(
            regions_of(&kept_paths("regions", Some(&filter))),
            native,
            "{predicate}: the in-process kept files must match Exasol's native selection"
        );
        let pushed = explain_virtual_sql(
            &mut conn,
            &format!(
                "SELECT ID FROM {} WHERE {predicate}",
                vs_table(VS_DIRECT, "REGIONS")
            ),
        );
        assert_eq!(
            regions_named_by(&pushed),
            native,
            "{predicate}: the pushed scan's files must match Exasol's native selection: {pushed}"
        );
        assert_eq!(
            returned_regions(&mut conn, &predicate),
            native,
            "{predicate}: the returned rows must match Exasol's native selection"
        );
    }

    let native = natively_selected_regions(&mut conn, "R > 'Z'");
    let predicate = "REGION > 'Z' AND SECOND(TS, 3) > 1";
    let pushed = explain_virtual_sql(
        &mut conn,
        &format!(
            "SELECT ID FROM {} WHERE {predicate}",
            vs_table(VS_DIRECT, "REGIONS")
        ),
    );
    assert_eq!(
        regions_named_by(&pushed),
        native,
        "{predicate}: pruning must still run on the REGION conjunct: {pushed}"
    );
    assert_eq!(
        returned_regions(&mut conn, predicate),
        native,
        "{predicate}: the declined conjunct holds on every row, so the row set must not change"
    );
}

/// A partition predicate no file satisfies resolves zero files and returns zero rows, not an error.
#[test]
fn zero_matching_files_prune_to_zero_rows_without_error() {
    setup();
    let mut conn = exa_conn();

    let kept = kept_paths("sales", Some(&equal(column("YEAR"), string("2099"))));
    assert!(kept.is_empty(), "YEAR = '2099' must keep no file: {kept:?}");

    let sql = format!(
        "SELECT ID FROM {} WHERE \"YEAR\" = '2099'",
        vs_table(VS_DIRECT, "SALES")
    );
    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        !pushed.contains("year="),
        "the pushed scan must name no SALES file: {pushed}"
    );

    let resp = conn.try_execute(&sql);
    assert_eq!(
        resp["status"].as_str(),
        Some("ok"),
        "a query keeping no file must not fail: {resp}"
    );
    assert_eq!(
        resp["responseData"]["results"][0]["resultSet"]["numRows"].as_i64(),
        Some(0),
        "a query keeping no file must return zero rows: {resp}"
    );
}
