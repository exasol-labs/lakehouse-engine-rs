//! De-risk direct-decode test for the INT96 timestamp overflow fix (issue #143,
//! plan `fix-int96-timestamp-overflow`, task 3.3).
//!
//! Proves the coercion fix against a committed, genuinely-INT96-encoded Parquet
//! asset — independent of the Iceberg catalog, the `add_files` import path, and
//! the Docker E2E stack. It runs under plain `cargo test` (no feature gate, no
//! live DB), so it de-risks the fix even if the E2E fixture route (task 3.2) or
//! the assumption that `add_files` preserves INT96 (task 2.3) were to regress.
//! Additive to the E2E test, not a replacement.
//!
//! The asset `tests/assets/int96_far_future.parquet` is a one-row Parquet file
//! with a single `required int96 ts` column encoding `9999-12-31 23:59:59` UTC.
//! Expressed as nanoseconds-since-epoch that instant is ~2.5e20, which overflows
//! i64 (max ~9.2e18) — the exact reason arrow-rs's DEFAULT INT96->Nanosecond
//! decode mishandles it (silently wrapping to a wrong ~1816 value, or erroring
//! per #143). Decoded through the shared `int96_coerced_parquet_format()` helper
//! (`coerce_int96="us"`, `coerce_int96_tz="UTC"`) it instead lands as a
//! microsecond-resolution UTC instant with the correct value.

use std::sync::Arc;

use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use lakehouse_engine::scan::int96_coerced_parquet_format;
use parquet::basic::Type as PhysicalType;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Absolute path to the committed genuine-INT96 asset.
const ASSET: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/assets/int96_far_future.parquet"
);

/// Expected decoded value, kept in lockstep with the E2E fixture ground truth
/// (`tests/common/int96_fixtures.rs::INT96_TS_FAR_FUTURE_EXPECTED_VALUE`, which
/// is feature-gated and so cannot be imported here).
const EXPECTED_VALUE: &str = "9999-12-31 23:59:59";

/// Microseconds-since-epoch for `9999-12-31 23:59:59` UTC (Unix seconds
/// 253402300799 × 1_000_000). The far-future value that overflows i64 at
/// nanosecond resolution but is representable at microsecond resolution.
const EXPECTED_MICROS: i64 = 253_402_300_799_000_000;

/// Read the asset's Parquet footer and return the physical type of column 0.
///
/// This test proves nothing about the overflow fix unless the asset is GENUINELY
/// INT96: an INT64-micros column decodes without overflow at nanosecond too, so
/// a silently-INT64 asset would make the decode assertions pass vacuously.
fn column0_physical_type() -> PhysicalType {
    let reader = SerializedFileReader::new(std::fs::File::open(ASSET).expect("open asset"))
        .expect("read parquet footer");
    reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .column(0)
        .physical_type()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn int96_far_future_parquet_decodes_at_microsecond_without_overflow() {
    // Guard: the committed asset must be physically INT96, or the test is vacuous.
    assert_eq!(
        column0_physical_type(),
        PhysicalType::INT96,
        "asset must be genuinely INT96-encoded"
    );

    let url = ListingTableUrl::parse(
        url::Url::from_file_path(ASSET)
            .expect("asset path is absolute")
            .as_str(),
    )
    .expect("listing url");

    let ctx = SessionContext::new();

    // The single source of truth for the INT96 coercion — the same helper the
    // scan's decode and schema-inference sites use.
    let options = ListingOptions::new(Arc::new(int96_coerced_parquet_format()))
        .with_file_extension(".parquet");

    // Schema inference under coercion must map INT96 to Timestamp(Microsecond, UTC)
    // — NOT the default Timestamp(Nanosecond), whose i64 range cannot hold this
    // instant.
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("infer schema");
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "coerced inference must yield Timestamp(Microsecond, UTC), not Nanosecond"
    );

    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    let table = ListingTable::try_new(config).expect("listing table");
    ctx.register_table("int96_asset", Arc::new(table))
        .expect("register table");

    // Decode must succeed with no nanosecond-overflow error.
    let batches = ctx
        .sql("SELECT ts FROM int96_asset")
        .await
        .expect("plan query")
        .collect()
        .await
        .expect("decode without nanosecond overflow");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "asset holds exactly one row");

    let col = batches[0].column(0);
    let ts = col
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("column decodes as a microsecond timestamp");

    assert_eq!(
        ts.value(0),
        EXPECTED_MICROS,
        "raw microseconds-since-epoch must be the far-future value"
    );
    let rendered = ts
        .value_as_datetime(0)
        .expect("valid datetime")
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    assert_eq!(
        rendered, EXPECTED_VALUE,
        "decoded value must render as the far-future timestamp"
    );
}
