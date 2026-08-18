//! End-to-end coverage for Iceberg type promotion
//! (`vs-adapter/iceberg-type-promotion`, `packaging/iceberg-type-promotion-fixture`).
//!
//! Two concerns share this binary, both driven against the local Apache Spark
//! Iceberg fixtures stack (`scripts/spark-fixtures/`, MinIO + Iceberg REST
//! catalog), following the same split `e2e_int96_timestamp_test.rs` uses for
//! the INT96 fixture:
//!
//! 1. A fixture-shape guard (task 6.3) — resolves the committed
//!    `e2e_lakehouse.iceberg_type_promotion` table's pre-promotion data file
//!    and asserts its `int_long` / `float_double` / `decimal_decimal` columns
//!    are still physically `INT32` / `FLOAT` / `INT64` (carrying the
//!    `DECIMAL(10,2)` logical annotation), so a silent Iceberg-side rewrite to
//!    the promoted types fails the suite loudly rather than letting the scan
//!    test below pass vacuously.
//! 2. A full-stack scan test (task 6.4) — drives the promoted table through
//!    the VS -> adapter -> scan-UDF stack and asserts both the pre- and
//!    post-promotion rows return at the promoted types, including the
//!    post-promotion `int_long` value outside the 32-bit range.
//!
//! There is no `date` -> `timestamp` promotion test here: Apache Iceberg Java
//! never implements that promotion at any version this stack can run, so no
//! fixture can carry it. Its refusal (`refuse_date_promotion`) is covered by
//! unit tests over a synthetic `TableMetadata` alone — see
//! `specs/_decision/074-add-type-relaxation.md`, "Iceberg `date` -> `timestamp` /
//! `timestamp_ns` is refused at plan time from the schema history".
//!
//! The `iceberg_type_promotion` table is authored ONCE, at stack bring-up, by
//! the `spark-iceberg-fixtures` one-shot Compose job (see
//! `scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql`, wired
//! into `run_fixtures.sh`) — this harness never seeds it. Ground truth lives
//! in `tests/common/type_promotion_fixtures.rs` and MUST stay in lockstep
//! with that SQL.
//!
//! Per project rules this test FAILS (never skips) when its stack is
//! unreachable: `setup()` calls `wait_for_minio`/`wait_for_iceberg_catalog`,
//! which panic (never return `Err`) on a dependency that never comes up.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::stack::{wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio};
use common::type_promotion_fixtures::{
    DECIMAL_DECIMAL_COLUMN, DECIMAL_DECIMAL_PRE_PROMOTION_PHYSICAL_TYPE, FLOAT_DOUBLE_COLUMN,
    FLOAT_DOUBLE_PRE_PROMOTION_PHYSICAL_TYPE, ICEBERG_TYPE_PROMOTION_TABLE, ID_COLUMN,
    INT_LONG_COLUMN, INT_LONG_PRE_PROMOTION_PHYSICAL_TYPE, NAMESPACE, POST_PROMOTION_ROWS,
    PRE_PROMOTION_ROWS, TypePromotionRow,
};

use lakehouse_engine::scan::spec::StorageBackend;

use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use parquet::basic::ConvertedType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnDescPtr;

use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// One-time setup (idempotent). Waits ONLY for the pieces the fixture-shape
// guard (task 6.3) actually reads — the MinIO object store and the Iceberg
// REST catalog. `setup_full_stack()` below layers Exasol + Virtual Schema
// provisioning on top of this base for the full-stack scan test (task 6.4).
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_minio();
        wait_for_iceberg_catalog();
    });
}

// ---------------------------------------------------------------------------
// Full-stack provisioning (task 6.4) — layered ON TOP of `setup()` above.
// ---------------------------------------------------------------------------

/// Shared VS used by every other E2E test binary too (idempotent recreation
/// with an identical body, so concurrent recreation across binaries is
/// harmless).
const VS_NAME: &str = "MY_LAKEHOUSE";

static FULL_STACK_SETUP_DONE: OnceLock<()> = OnceLock::new();

/// Provision the full VS -> adapter -> scan-UDF stack for the scan test (6.4).
fn setup_full_stack() {
    setup();
    FULL_STACK_SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        install_slc();
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, NAMESPACE));
    });
}

fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

// ---------------------------------------------------------------------------
// Fixture-shape guard (packaging/iceberg-type-promotion-fixture, task 6.3) —
// inspects the Spark-committed pre-promotion data file's Parquet footer
// directly, bypassing Exasol, to prove the committed fixture still carries
// the SOURCE physical encoding (not a silently normalised target-type
// rewrite that would let the read test in task 6.4 pass vacuously).
// ---------------------------------------------------------------------------

/// Read a data file's raw bytes from MinIO through the SAME `object_store` S3
/// client the scan UDF uses (the `StorageBackend::S3` arm of
/// `register_side_store` in `scan/object_store.rs`), so the fixture-shape
/// guard inspects the exact bytes the scan path would decode.
///
/// The Iceberg reader yields an absolute `s3://<bucket>/<key>` (or `s3a://…`)
/// URI; this splits off the bucket and reads the object by its key.
async fn fetch_object_bytes(uri: &str) -> bytes::Bytes {
    let without_scheme = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .unwrap_or_else(|| panic!("data file URI must be an s3/s3a URI, got: {uri}"));
    let (bucket, key) = without_scheme
        .split_once('/')
        .unwrap_or_else(|| panic!("data file URI must have a <bucket>/<key> form, got: {uri}"));

    let StorageBackend::S3(storage) = local_stack_storage() else {
        panic!("LocalStack fixture is S3-only")
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

    store
        .get(&ObjectStorePath::from(key))
        .await
        .unwrap_or_else(|e| panic!("GET {uri} from MinIO: {e}"))
        .bytes()
        .await
        .unwrap_or_else(|e| panic!("read bytes of {uri}: {e}"))
}

/// Find `column` in `schema`'s columns, panicking with the full column list if
/// it is absent.
fn find_column(schema: &parquet::schema::types::SchemaDescriptor, column: &str) -> ColumnDescPtr {
    schema
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case(column))
        .unwrap_or_else(|| {
            let names: Vec<&str> = schema.columns().iter().map(|c| c.name()).collect();
            panic!("committed Parquet file must have a '{column}' column, got columns {names:?}")
        })
        .clone()
}

/// The committed `iceberg_type_promotion` table resolves to exactly 2 data
/// files (one written BEFORE the three promotions, one written after), and
/// the one written before still carries the SOURCE physical Parquet encoding:
/// `int_long` is `INT32`, `float_double` is `FLOAT`, and `decimal_decimal` is
/// `INT64` carrying the `DECIMAL(10,2)` logical annotation (Iceberg encodes a
/// decimal of precision <= 18 as physical `INT64`, never
/// `FIXED_LEN_BYTE_ARRAY`).
///
/// `ALTER TABLE ... ALTER COLUMN ... TYPE` rewrites only the table's current
/// schema — the data file committed before it keeps its original physical
/// encoding. A silent Iceberg-side rewrite that normalised this file to the
/// promoted (target) types would let the full-stack scan test (task 6.4)
/// pass without ever exercising the narrow-to-wide cast; this test fails loud
/// instead, by asserting the physical encoding directly from the file's
/// Parquet footer rather than through a decoded scan result.
#[test]
fn e2e_type_promotion_pre_promotion_data_file_is_physically_narrow() {
    setup();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let files = rt.block_on(resolve_fixture_files(
        NAMESPACE,
        ICEBERG_TYPE_PROMOTION_TABLE,
    ));
    assert_eq!(
        files.len(),
        2,
        "{ICEBERG_TYPE_PROMOTION_TABLE} must resolve exactly 2 data files (one written before \
         the promotions, one after), got {}: {files:?}",
        files.len()
    );

    let readers: Vec<(String, SerializedFileReader<bytes::Bytes>)> = files
        .iter()
        .map(|file| {
            let bytes = rt.block_on(fetch_object_bytes(&file.path));
            let reader = SerializedFileReader::new(bytes)
                .unwrap_or_else(|e| panic!("open committed Parquet file {}: {e}", file.path));
            (file.path.clone(), reader)
        })
        .collect();

    let int_long_physical_types: Vec<(&String, parquet::basic::Type)> = readers
        .iter()
        .map(|(path, reader)| {
            let physical_type = find_column(
                reader.metadata().file_metadata().schema_descr(),
                INT_LONG_COLUMN,
            )
            .physical_type();
            (path, physical_type)
        })
        .collect();

    let pre_promotion_index = int_long_physical_types
        .iter()
        .position(|(_, physical_type)| *physical_type == parquet::basic::Type::INT32)
        .unwrap_or_else(|| {
            panic!(
                "exactly one of the 2 committed data files must carry '{INT_LONG_COLUMN}' as \
                 physical INT32 (the pre-promotion encoding); got physical types \
                 {int_long_physical_types:?}"
            )
        });
    let (pre_promotion_path, pre_promotion_reader) = &readers[pre_promotion_index];

    let schema_descr = pre_promotion_reader
        .metadata()
        .file_metadata()
        .schema_descr();

    let int_long_col = find_column(schema_descr, INT_LONG_COLUMN);
    assert_eq!(
        int_long_col.physical_type().to_string(),
        INT_LONG_PRE_PROMOTION_PHYSICAL_TYPE,
        "fixture guard: pre-promotion file {pre_promotion_path}'s '{INT_LONG_COLUMN}' column \
         must still be physically {INT_LONG_PRE_PROMOTION_PHYSICAL_TYPE}, got {:?}",
        int_long_col.physical_type()
    );

    let float_double_col = find_column(schema_descr, FLOAT_DOUBLE_COLUMN);
    assert_eq!(
        float_double_col.physical_type().to_string(),
        FLOAT_DOUBLE_PRE_PROMOTION_PHYSICAL_TYPE,
        "fixture guard: pre-promotion file {pre_promotion_path}'s '{FLOAT_DOUBLE_COLUMN}' \
         column must still be physically {FLOAT_DOUBLE_PRE_PROMOTION_PHYSICAL_TYPE}, got {:?}",
        float_double_col.physical_type()
    );

    let decimal_col = find_column(schema_descr, DECIMAL_DECIMAL_COLUMN);
    assert_eq!(
        decimal_col.physical_type().to_string(),
        DECIMAL_DECIMAL_PRE_PROMOTION_PHYSICAL_TYPE,
        "fixture guard: pre-promotion file {pre_promotion_path}'s '{DECIMAL_DECIMAL_COLUMN}' \
         column must still be physically {DECIMAL_DECIMAL_PRE_PROMOTION_PHYSICAL_TYPE} — Iceberg \
         encodes a decimal of precision <= 18 as physical INT64, never \
         FIXED_LEN_BYTE_ARRAY — got {:?}",
        decimal_col.physical_type()
    );
    assert_eq!(
        decimal_col.converted_type(),
        ConvertedType::DECIMAL,
        "fixture guard: pre-promotion file {pre_promotion_path}'s '{DECIMAL_DECIMAL_COLUMN}' \
         column must carry the DECIMAL converted-type annotation, got {:?}",
        decimal_col.converted_type()
    );
    assert_eq!(
        decimal_col.type_precision(),
        10,
        "fixture guard: pre-promotion file {pre_promotion_path}'s '{DECIMAL_DECIMAL_COLUMN}' \
         column must carry DECIMAL(10,2) precision 10, got {}",
        decimal_col.type_precision()
    );
    assert_eq!(
        decimal_col.type_scale(),
        2,
        "fixture guard: pre-promotion file {pre_promotion_path}'s '{DECIMAL_DECIMAL_COLUMN}' \
         column must carry DECIMAL(10,2) scale 2, got {}",
        decimal_col.type_scale()
    );
}

// ---------------------------------------------------------------------------
// Full-stack scan test (vs-adapter/iceberg-type-promotion, task 6.4) — drives
// the promoted table through the VS -> adapter -> scan-UDF stack and asserts
// every row returns at the table's CURRENT (promoted) types, across both the
// physically-narrow pre-promotion file and the physically-wide
// post-promotion file the fixture-shape guard above just proved exist.
// ---------------------------------------------------------------------------

/// A `SELECT` of `iceberg_type_promotion` through the Virtual Schema returns
/// all 4 rows from both physical data files at the table's current schema —
/// `int_long` cast from physical `int`/`long` up to logical `bigint`,
/// `float_double` cast from physical `float`/`double` up to logical
/// `double`, `decimal_decimal` cast from physical `decimal(10,2)`/`decimal(20,2)`
/// up to logical `decimal(20,2)` — including the post-promotion `int_long`
/// values that sit one step outside the 32-bit range a narrow-width read
/// could not have represented at all.
#[test]
fn iceberg_type_promotion_returns_both_layouts_at_the_promoted_types() {
    setup_full_stack();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT {ID_COLUMN}, {INT_LONG_COLUMN}, {FLOAT_DOUBLE_COLUMN}, {DECIMAL_DECIMAL_COLUMN} \
         FROM {} ORDER BY {ID_COLUMN}",
        vs_table(VS_NAME, ICEBERG_TYPE_PROMOTION_TABLE)
    );
    let cols = conn.query_columns(&sql);

    let expected_rows: Vec<&TypePromotionRow> = PRE_PROMOTION_ROWS
        .iter()
        .chain(POST_PROMOTION_ROWS.iter())
        .collect();

    assert_eq!(
        cols.len(),
        4,
        "SELECT {ID_COLUMN}, {INT_LONG_COLUMN}, {FLOAT_DOUBLE_COLUMN}, \
         {DECIMAL_DECIMAL_COLUMN} must return exactly 4 columns, got {}: {cols:?}",
        cols.len()
    );
    let ids = &cols[0];
    assert_eq!(
        ids.len(),
        expected_rows.len(),
        "{ICEBERG_TYPE_PROMOTION_TABLE} must return all {} rows across both the \
         pre- and post-promotion data files, got {}",
        expected_rows.len(),
        ids.len()
    );

    for (row_index, expected) in expected_rows.iter().enumerate() {
        let id = parse_int(&ids[row_index]);
        assert_eq!(
            id, expected.id,
            "row {row_index}: expected {ID_COLUMN} {}, got {id} (ORDER BY {ID_COLUMN} must \
             preserve insertion order across both data files)",
            expected.id
        );

        let int_long = parse_int(&cols[1][row_index]);
        assert_eq!(
            int_long, expected.int_long,
            "row {row_index} (id {}): expected {INT_LONG_COLUMN} = {}, got {int_long} — a \
             32-bit-width read would have failed or wrapped this value",
            expected.id, expected.int_long
        );

        let float_double = parse_numeric(&cols[2][row_index]);
        assert!(
            (float_double - expected.float_double).abs() < 1e-9,
            "row {row_index} (id {}): expected {FLOAT_DOUBLE_COLUMN} = {}, got {float_double}",
            expected.id,
            expected.float_double
        );

        let decimal_decimal = value_to_string(&cols[3][row_index]);
        assert_eq!(
            decimal_decimal, expected.decimal_decimal,
            "row {row_index} (id {}): expected {DECIMAL_DECIMAL_COLUMN} = {}, got \
             {decimal_decimal}",
            expected.id, expected.decimal_decimal
        );
    }
}
