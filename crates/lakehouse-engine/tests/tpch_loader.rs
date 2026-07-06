//! TPC-H data loader for the live benchmark (NOT part of `make test-e2e`).
//!
//! Generates the 8 TPC-H tables with the `tpchgen` **core** row generators and
//! builds workspace **arrow-58** `RecordBatch`es from the rows by hand (one column
//! builder per field), then writes them into the local Docker Iceberg REST catalog
//! (MinIO-backed), reusing the proven write path in `common::seed`
//! (`build_seed_catalog` + `create_and_append_files`). Run by `bench/run.sh` in
//! docker mode:
//!
//!   cargo test --features exasol-e2e --test tpch_loader -- --nocapture
//!
//! `tpchgen-arrow` is intentionally NOT used: it has no arrow-58 release
//! (2.0.2→arrow 57, 3.0.0→arrow 59), whereas iceberg 0.10's writer expects
//! arrow-58 batches. `tpchgen` core has zero dependencies (no arrow), so building
//! the columns by hand keeps the whole dev graph on a single arrow-58 tree.
//!
//! Idempotent: a table that already has data files is skipped. Scale factor via
//! `TPCH_SCALE` (default 0.3, ~300MB); target namespace via `ICEBERG_NAMESPACE`
//! (default `tpch`). Catalog/MinIO host-side URLs come from `common::stack`.
#![cfg(feature = "exasol-e2e")]

mod common;

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use common::{seed, stack};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use tpchgen::dates::TPCHDate;
use tpchgen::decimal::TPCHDecimal;
use tpchgen::generators::{
    Customer, CustomerGenerator, LineItem, LineItemGenerator, Nation, NationGenerator, Order,
    OrderGenerator, Part, PartGenerator, PartSupp, PartSuppGenerator, Region, RegionGenerator,
    Supplier, SupplierGenerator,
};

// TPC-H money/quantity columns are all Decimal(15,2), matching the fixed scale the
// generator emits. Kept as named constants so every column builder agrees.
const TPCH_DECIMAL_PRECISION: u8 = 15;
const TPCH_DECIMAL_SCALE: i8 = 2;

/// Arrow `Decimal128(15,2)` column from an iterator of `TPCHDecimal` (stored as an
/// i64 with 2 implied decimal places → i128 unscaled value, same as tpchgen-arrow).
fn decimal_col(values: impl Iterator<Item = TPCHDecimal>) -> Decimal128Array {
    Decimal128Array::from_iter_values(values.map(|v| v.into_inner() as i128))
        .with_precision_and_scale(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE)
        .expect("Decimal(15,2) is within Decimal128 range")
}

/// Arrow `Date32` column (days since the Unix epoch) from an iterator of `TPCHDate`.
fn date_col(values: impl Iterator<Item = TPCHDate>) -> Date32Array {
    Date32Array::from_iter_values(values.map(|d| d.to_unix_epoch()))
}

/// Arrow `Utf8` column from an iterator of any `Display` value. The generator's
/// string fields are a mix of `&str` and small display wrappers (part/brand/clerk
/// names, phone numbers, …); formatting each via `Display` reproduces exactly the
/// bytes tpchgen-arrow wrote, now as `Utf8` (the string type iceberg 0.10's writer
/// accepts, same as the other seed tables) instead of `Utf8View`.
fn text_col<T: std::fmt::Display>(values: impl Iterator<Item = T>) -> StringArray {
    StringArray::from_iter_values(values.map(|v| v.to_string()))
}

fn region_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, false),
    ]))
}

fn build_region_batch(rows: &[Region<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        region_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.r_regionkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.r_name))),
            Arc::new(text_col(rows.iter().map(|r| &r.r_comment))),
        ],
    )
    .expect("region RecordBatch construction is infallible")
}

fn nation_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, false),
    ]))
}

fn build_nation_batch(rows: &[Nation<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        nation_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.n_nationkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.n_name))),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.n_regionkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.n_comment))),
        ],
    )
    .expect("nation RecordBatch construction is infallible")
}

fn supplier_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new(
            "s_acctbal",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new("s_comment", DataType::Utf8, false),
    ]))
}

fn build_supplier_batch(rows: &[Supplier]) -> RecordBatch {
    RecordBatch::try_new(
        supplier_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.s_suppkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.s_name))),
            Arc::new(text_col(rows.iter().map(|r| &r.s_address))),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.s_nationkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.s_phone))),
            Arc::new(decimal_col(rows.iter().map(|r| r.s_acctbal))),
            Arc::new(text_col(rows.iter().map(|r| &r.s_comment))),
        ],
    )
    .expect("supplier RecordBatch construction is infallible")
}

fn customer_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new(
            "c_acctbal",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]))
}

fn build_customer_batch(rows: &[Customer<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        customer_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.c_custkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.c_name))),
            Arc::new(text_col(rows.iter().map(|r| &r.c_address))),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.c_nationkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.c_phone))),
            Arc::new(decimal_col(rows.iter().map(|r| r.c_acctbal))),
            Arc::new(text_col(rows.iter().map(|r| &r.c_mktsegment))),
            Arc::new(text_col(rows.iter().map(|r| &r.c_comment))),
        ],
    )
    .expect("customer RecordBatch construction is infallible")
}

fn part_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new(
            "p_retailprice",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new("p_comment", DataType::Utf8, false),
    ]))
}

fn build_part_batch(rows: &[Part<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        part_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.p_partkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.p_name))),
            Arc::new(text_col(rows.iter().map(|r| &r.p_mfgr))),
            Arc::new(text_col(rows.iter().map(|r| &r.p_brand))),
            Arc::new(text_col(rows.iter().map(|r| &r.p_type))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.p_size))),
            Arc::new(text_col(rows.iter().map(|r| &r.p_container))),
            Arc::new(decimal_col(rows.iter().map(|r| r.p_retailprice))),
            Arc::new(text_col(rows.iter().map(|r| &r.p_comment))),
        ],
    )
    .expect("part RecordBatch construction is infallible")
}

fn partsupp_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new(
            "ps_supplycost",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new("ps_comment", DataType::Utf8, false),
    ]))
}

fn build_partsupp_batch(rows: &[PartSupp<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        partsupp_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.ps_partkey),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.ps_suppkey),
            )),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.ps_availqty),
            )),
            Arc::new(decimal_col(rows.iter().map(|r| r.ps_supplycost))),
            Arc::new(text_col(rows.iter().map(|r| &r.ps_comment))),
        ],
    )
    .expect("partsupp RecordBatch construction is infallible")
}

fn order_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new(
            "o_totalprice",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]))
}

fn build_order_batch(rows: &[Order<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        order_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.o_orderkey),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.o_custkey),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.o_orderstatus))),
            Arc::new(decimal_col(rows.iter().map(|r| r.o_totalprice))),
            Arc::new(date_col(rows.iter().map(|r| r.o_orderdate))),
            Arc::new(text_col(rows.iter().map(|r| &r.o_orderpriority))),
            Arc::new(text_col(rows.iter().map(|r| &r.o_clerk))),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.o_shippriority),
            )),
            Arc::new(text_col(rows.iter().map(|r| &r.o_comment))),
        ],
    )
    .expect("order RecordBatch construction is infallible")
}

fn lineitem_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new(
            "l_quantity",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new(
            "l_extendedprice",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new(
            "l_discount",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new(
            "l_tax",
            DataType::Decimal128(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE),
            false,
        ),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ]))
}

fn build_lineitem_batch(rows: &[LineItem<'_>]) -> RecordBatch {
    // l_quantity is generated as a plain i64; tpchgen-arrow scales it to
    // Decimal(15,2) as `(q as i128) * 100`, so 17 → 17.00. Reproduce that exactly.
    let l_quantity =
        Decimal128Array::from_iter_values(rows.iter().map(|r| (r.l_quantity as i128) * 100))
            .with_precision_and_scale(TPCH_DECIMAL_PRECISION, TPCH_DECIMAL_SCALE)
            .expect("Decimal(15,2) is within Decimal128 range");

    RecordBatch::try_new(
        lineitem_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.l_orderkey),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.l_partkey),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.l_suppkey),
            )),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.l_linenumber),
            )),
            Arc::new(l_quantity),
            Arc::new(decimal_col(rows.iter().map(|r| r.l_extendedprice))),
            Arc::new(decimal_col(rows.iter().map(|r| r.l_discount))),
            Arc::new(decimal_col(rows.iter().map(|r| r.l_tax))),
            Arc::new(text_col(rows.iter().map(|r| &r.l_returnflag))),
            Arc::new(text_col(rows.iter().map(|r| &r.l_linestatus))),
            Arc::new(date_col(rows.iter().map(|r| r.l_shipdate))),
            Arc::new(date_col(rows.iter().map(|r| r.l_commitdate))),
            Arc::new(date_col(rows.iter().map(|r| r.l_receiptdate))),
            Arc::new(text_col(rows.iter().map(|r| &r.l_shipinstruct))),
            Arc::new(text_col(rows.iter().map(|r| &r.l_shipmode))),
            Arc::new(text_col(rows.iter().map(|r| &r.l_comment))),
        ],
    )
    .expect("lineitem RecordBatch construction is infallible")
}

/// Map an Arrow data type to the Iceberg primitive type (covers the TPC-H column
/// types: integer keys, strings, dates, decimals, doubles).
fn arrow_to_iceberg_type(dt: &DataType) -> Result<Type> {
    let p = match dt {
        DataType::Boolean => PrimitiveType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        // The loader builds string columns as Utf8 (the type iceberg's Parquet
        // writer accepts); LargeUtf8/Utf8View are accepted too and recorded as
        // Iceberg String.
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => PrimitiveType::String,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Decimal128(precision, scale) => PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        },
        other => anyhow::bail!("unsupported Arrow type for TPC-H loader: {other:?}"),
    };
    Ok(Type::Primitive(p))
}

/// Derive an Iceberg schema from an Arrow schema, assigning field IDs by position.
fn arrow_schema_to_iceberg(schema: &ArrowSchema) -> Result<IcebergSchema> {
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let id = (i + 1) as i32;
            let ty = arrow_to_iceberg_type(f.data_type())
                .with_context(|| format!("field '{}'", f.name()))?;
            let nf = if f.is_nullable() {
                NestedField::optional(id, f.name(), ty)
            } else {
                NestedField::required(id, f.name(), ty)
            };
            Ok::<_, anyhow::Error>(nf.into())
        })
        .collect::<Result<Vec<_>>>()?;
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .context("build TPC-H Iceberg schema")
}

#[tokio::test]
async fn load_tpch() -> Result<()> {
    let scale: f64 = std::env::var("TPCH_SCALE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.3);
    let namespace = std::env::var("ICEBERG_NAMESPACE").unwrap_or_else(|_| "tpch".to_string());
    // Number of Parquet files for the big tables (lineitem, orders). >1 makes the
    // adapter's GROUP BY shard_key fan-out (one shard per file) observable. Small
    // tables stay single-file. tpchgen's (scale, part, N) yields N disjoint parts.
    let files: i32 = std::env::var("TPCH_FILES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(4);
    const BATCH_SIZE: usize = 60_000;

    let catalog_url = stack::iceberg_catalog_url();
    let warehouse = "s3://warehouse/";
    let catalog = seed::build_seed_catalog(&catalog_url, warehouse, "tpch-loader").await?;
    println!(
        "Loading TPC-H (SF={scale}, big tables in {files} files) into '{namespace}' at {catalog_url}"
    );

    // Each TPC-H table: build N disjoint generator parts (one part per file),
    // batch each part's rows into arrow-58 RecordBatches by hand, derive the
    // Iceberg schema from the (static) Arrow schema, and create + append each part
    // as its own data file (idempotent). A macro because the 8 generator/row types
    // are distinct (no common trait object) and each has its own column builder.
    macro_rules! load_table {
        ($gen:ty, $build:path, $schema:path, $name:literal, $nfiles:expr) => {{
            let n: i32 = $nfiles;
            let parts: Vec<Vec<RecordBatch>> = (1..=n)
                .map(|part| {
                    let generator = <$gen>::new(scale, part, n);
                    let mut iter = generator.iter();
                    let mut batches: Vec<RecordBatch> = Vec::new();
                    loop {
                        let rows: Vec<_> = iter.by_ref().take(BATCH_SIZE).collect();
                        if rows.is_empty() {
                            break;
                        }
                        batches.push($build(&rows));
                    }
                    batches
                })
                .collect();
            let iceberg_schema = arrow_schema_to_iceberg(&$schema())
                .with_context(|| format!("derive Iceberg schema for {}", $name))?;
            let wrote =
                seed::create_and_append_files(&catalog, &namespace, $name, iceberg_schema, parts)
                    .await
                    .with_context(|| format!("load TPC-H table {}", $name))?;
            println!(
                "  {:<9} {} ({} file(s))",
                $name,
                if wrote {
                    "loaded"
                } else {
                    "skipped (already present)"
                },
                n
            );
        }};
    }

    load_table!(
        RegionGenerator,
        build_region_batch,
        region_schema,
        "region",
        1
    );
    load_table!(
        NationGenerator,
        build_nation_batch,
        nation_schema,
        "nation",
        1
    );
    load_table!(
        SupplierGenerator,
        build_supplier_batch,
        supplier_schema,
        "supplier",
        1
    );
    load_table!(
        CustomerGenerator,
        build_customer_batch,
        customer_schema,
        "customer",
        1
    );
    load_table!(PartGenerator, build_part_batch, part_schema, "part", 1);
    load_table!(
        PartSuppGenerator,
        build_partsupp_batch,
        partsupp_schema,
        "partsupp",
        1
    );
    load_table!(
        OrderGenerator,
        build_order_batch,
        order_schema,
        "orders",
        files
    );
    load_table!(
        LineItemGenerator,
        build_lineitem_batch,
        lineitem_schema,
        "lineitem",
        files
    );

    Ok(())
}
