//! Far-future INT96 timestamps decode without nanosecond overflow (#143).
//!
//! The physical-encoding guard exists because an INT64 column also decodes
//! `9999-12-31 23:59:59` without overflow, so the scan test alone would pass
//! vacuously on a silently degraded fixture.
//!
//! The fixture is authored once at stack bring-up by
//! `scripts/spark-fixtures/create_int96_timestamp_fixture.sql`;
//! `tests/common/int96_fixtures.rs` MUST stay in lockstep with that SQL.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::int96_fixtures::{
    INT96_TS_FAR_FUTURE_COLUMN, INT96_TS_FAR_FUTURE_EXPECTED_VALUE, INT96_TS_FAR_FUTURE_TABLE,
    NAMESPACE,
};
use common::stack::{wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio};

use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectStorePath;
use parquet::file::reader::{FileReader, SerializedFileReader};

use std::sync::OnceLock;

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_minio();
        wait_for_iceberg_catalog();
    });
}

/// Shared across E2E binaries; recreation is idempotent with an identical body.
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

/// Scenario: the committed fixture's timestamp column is physically INT96
#[test]
fn e2e_int96_fixture_present_and_int96_encoded() {
    setup();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let files = rt.block_on(resolve_fixture_files(NAMESPACE, INT96_TS_FAR_FUTURE_TABLE));
    // REPARTITION(1) in the fixture SQL forces a single data file.
    assert_eq!(
        files.len(),
        1,
        "{INT96_TS_FAR_FUTURE_TABLE} must resolve exactly 1 data file, got {}: {files:?}",
        files.len()
    );

    let data_uri = files[0].path.clone();
    let bytes = rt.block_on(fetch_object_bytes(&data_uri));

    let reader = SerializedFileReader::new(bytes)
        .unwrap_or_else(|e| panic!("open committed Parquet file {data_uri}: {e}"));
    let schema_descr = reader.metadata().file_metadata().schema_descr();

    let ts_col = schema_descr
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case(INT96_TS_FAR_FUTURE_COLUMN))
        .unwrap_or_else(|| {
            let names: Vec<&str> = schema_descr.columns().iter().map(|c| c.name()).collect();
            panic!(
                "committed Parquet file {data_uri} must have a '{INT96_TS_FAR_FUTURE_COLUMN}' \
                 column, got columns {names:?}"
            )
        });

    assert_eq!(
        ts_col.physical_type(),
        parquet::basic::Type::INT96,
        "fixture guard: the '{INT96_TS_FAR_FUTURE_COLUMN}' column of {data_uri} must be \
         physically INT96-encoded (a silent INT64 import decodes without overflow at nanosecond \
         too, so it would make the scan test pass vacuously — see issue #143), got {:?}",
        ts_col.physical_type()
    );
}

/// Scenario: a far-future INT96 timestamp scans without nanosecond overflow (#143)
#[test]
fn e2e_int96_far_future_timestamp_scans_without_overflow() {
    setup_full_stack();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT {INT96_TS_FAR_FUTURE_COLUMN} FROM {}",
        vs_table(VS_NAME, INT96_TS_FAR_FUTURE_TABLE)
    );

    let resp = conn.try_execute(&sql);
    assert_eq!(
        resp["status"].as_str(),
        Some("ok"),
        "scanning the far-future INT96 fixture must NOT fail with the issue #143 \
         nanosecond-overflow error — the coerce_int96=\"us\" fix must decode it at \
         microsecond resolution; got: {resp}"
    );

    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);
    assert_eq!(
        cols.len(),
        1,
        "SELECT {INT96_TS_FAR_FUTURE_COLUMN} must return exactly 1 column, got {}: {cols:?}",
        cols.len()
    );
    assert_eq!(
        cols[0].len(),
        1,
        "{INT96_TS_FAR_FUTURE_TABLE} holds exactly 1 row, got {}: {:?}",
        cols[0].len(),
        cols[0]
    );

    let ts = cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("timestamp value must be a string, got: {:?}", cols[0][0]));
    // Exasol appends fractional seconds; match the seconds-resolution prefix.
    assert!(
        ts.starts_with(INT96_TS_FAR_FUTURE_EXPECTED_VALUE),
        "scanned far-future INT96 timestamp must be {INT96_TS_FAR_FUTURE_EXPECTED_VALUE}, got {ts:?}"
    );
}
