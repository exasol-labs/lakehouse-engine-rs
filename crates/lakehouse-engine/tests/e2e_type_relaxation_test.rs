//! Iceberg type promotion E2E. No `date` -> `timestamp` case: Iceberg Java never implements that
//! promotion, so no fixture can carry it (unit-tested instead, ADR 074). The fixture is authored by
//! `scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql`; ground truth in
//! `tests/common/type_promotion_fixtures.rs` must stay in lockstep with it.
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

use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectStorePath;
use parquet::basic::ConvertedType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnDescPtr;

use std::sync::OnceLock;

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_minio();
        wait_for_iceberg_catalog();
    });
}

/// Shared with other E2E binaries; recreation uses an identical body, so it is harmless.
const VS_NAME: &str = "MY_LAKEHOUSE";

static FULL_STACK_SETUP_DONE: OnceLock<()> = OnceLock::new();

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

/// Reads through the same `object_store` S3 client the scan UDF uses, so the guard inspects the
/// exact bytes the scan would decode.
async fn fetch_object_bytes(uri: &str) -> bytes::Bytes {
    let (bucket, key) = split_s3_bucket_and_key(uri);
    let store = local_stack_s3_store(bucket);

    store
        .get(&ObjectStorePath::from(key))
        .await
        .unwrap_or_else(|e| panic!("GET {uri} from MinIO: {e}"))
        .bytes()
        .await
        .unwrap_or_else(|e| panic!("read bytes of {uri}: {e}"))
}

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

/// Scenario: the pre-promotion data file is still physically narrow
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

/// Scenario: rows from both physical layouts return at the promoted types
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
