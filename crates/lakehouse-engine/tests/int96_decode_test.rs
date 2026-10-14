//! Decodes a committed genuinely-INT96 Parquet asset (`9999-12-31 23:59:59` UTC, ~2.5e20 ns,
//! overflows i64 at nanosecond resolution) without the E2E stack (#143).

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

const ASSET: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/assets/int96_far_future.parquet"
);

/// Mirrors `int96_fixtures::INT96_TS_FAR_FUTURE_EXPECTED_VALUE`, which is feature-gated.
const EXPECTED_VALUE: &str = "9999-12-31 23:59:59";

const EXPECTED_MICROS: i64 = 253_402_300_799_000_000;

/// An INT64 asset would decode without overflow too, making the test vacuous.
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

    let options = ListingOptions::new(Arc::new(int96_coerced_parquet_format()))
        .with_file_extension(".parquet");

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
