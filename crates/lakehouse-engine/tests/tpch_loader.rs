//! TPC-H data loader for the live benchmark (NOT part of `make test-e2e`).
//!
//! Generates the 8 TPC-H tables with `tpchgen-arrow` and writes them into the
//! local Docker Iceberg REST catalog (MinIO-backed), reusing the proven write
//! path in `common::seed` (`build_seed_catalog` + `create_and_append`). Run by
//! `bench/run.sh` in docker mode:
//!
//!   cargo test --features exasol-e2e --test tpch_loader -- --nocapture
//!
//! Idempotent: a table that already has data files is skipped. Scale factor via
//! `TPCH_SCALE` (default 0.3, ~300MB); target namespace via `ICEBERG_NAMESPACE`
//! (default `tpch`). Catalog/MinIO host-side URLs come from `common::stack`.
#![cfg(feature = "exasol-e2e")]

mod common;

use anyhow::{Context, Result};
use common::{seed, stack};
use ice_arrow_schema::{DataType, Schema as ArrowSchema};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow,
    RecordBatchIterator, RegionArrow, SupplierArrow,
};

/// Map an Arrow data type to the Iceberg primitive type (covers the TPC-H column
/// types: integer keys, strings, dates, decimals, doubles).
fn arrow_to_iceberg_type(dt: &DataType) -> Result<Type> {
    let p = match dt {
        DataType::Boolean => PrimitiveType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        // tpchgen-arrow emits Utf8View for string columns; iceberg's Parquet
        // writer handles the view array, and Iceberg records it as String.
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

    // Each TPC-H table: build N disjoint generator parts (one Arrow iterator per
    // file), derive the Iceberg schema from the first part's Arrow schema, and
    // create + append each part as its own data file (idempotent). A macro because
    // the 8 generator/Arrow types are distinct (no common trait object).
    macro_rules! load_table {
        ($gen:ty, $arrow:ident, $name:literal, $nfiles:expr) => {{
            let n: i32 = $nfiles;
            let parts: Vec<$arrow> = (1..=n)
                .map(|part| $arrow::new(<$gen>::new(scale, part, n)).with_batch_size(BATCH_SIZE))
                .collect();
            let iceberg_schema = arrow_schema_to_iceberg(parts[0].schema())
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

    load_table!(RegionGenerator, RegionArrow, "region", 1);
    load_table!(NationGenerator, NationArrow, "nation", 1);
    load_table!(SupplierGenerator, SupplierArrow, "supplier", 1);
    load_table!(CustomerGenerator, CustomerArrow, "customer", 1);
    load_table!(PartGenerator, PartArrow, "part", 1);
    load_table!(PartSuppGenerator, PartSuppArrow, "partsupp", 1);
    load_table!(OrderGenerator, OrderArrow, "orders", files);
    load_table!(LineItemGenerator, LineItemArrow, "lineitem", files);

    Ok(())
}
