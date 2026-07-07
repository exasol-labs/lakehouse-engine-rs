//! Iceberg table seeder for lakehouse-engine E2E tests.
//!
//! Seeds a deterministic mixed-column table into the Iceberg REST catalog over MinIO.
//! Uses iceberg-rust 0.10.0-rc.2 + iceberg-catalog-rest 0.10.0-rc.2 (same as the
//! main crate). Arrow batches and parquet writer properties are built with the
//! workspace arrow/parquet 58 — the same single tree iceberg 0.10 links.
//!
//! Column mix exercises the full type-mapping table:
//!   id           INT64        → DECIMAL(20,0)
//!   name         UTF8         → VARCHAR(2000000)
//!   score        FLOAT64      → DOUBLE PRECISION
//!   event_date   DATE32       → DATE
//!   event_ts     TIMESTAMP(µs,None) → TIMESTAMP
//!
//! Complex columns (list/struct) are covered by unit tests; they are not written
//! here because iceberg-rust does not expose a struct/list writer.
//!
//! Table: e2e_lakehouse.events (namespace=e2e_lakehouse, table=events)
//! Rows: 20 deterministic rows so LIMIT 5 and WHERE score > 15.0 are both testable.
//!
//! Table: e2e_lakehouse.regions (namespace=e2e_lakehouse, table=regions)
//! Partitioned by `region` (identity transform), one data file per partition value.
//! Per-file id ranges are disjoint so both partition pruning and per-file min/max
//! range pruning are observable in E2E tests.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Date32Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use futures::TryStreamExt;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::spec::{
    DataFileFormat, Literal, NestedField, PrimitiveType, Schema as IcebergSchema, Struct,
    Transform, Type, UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::DefaultFileNameGenerator;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;

/// Namespace and table names for the E2E seed tables.
pub const E2E_NAMESPACE: &str = "e2e_lakehouse";
pub const E2E_TABLE: &str = "events";
/// Second E2E table: labels, with columns `id` and `label`.
pub const E2E_TABLE_2: &str = "labels";
/// Qualified table name for the events table (kept for any external reference).
pub const E2E_QUALIFIED_TABLE: &str = "e2e_lakehouse.events";

/// Total rows seeded into the events table.
pub const SEED_TOTAL_ROWS: usize = 20;
/// Rows with score > 15.0 (scores are 5.0 * (i+1) for i=0..19, so > 15.0 means i >= 3 → 17 rows).
pub const SEED_ROWS_SCORE_GT_15: usize = 17;
/// Total rows seeded into the labels table (one label per id in 1..=SEED_TOTAL_ROWS).
pub const SEED_LABELS_ROWS: usize = SEED_TOTAL_ROWS;

// ---------------------------------------------------------------------------
// Partitioned table (regions) — exported constants for E2E file-pruning tests
// ---------------------------------------------------------------------------

/// Table name for the partitioned seed table used in file-pruning E2E tests.
pub const E2E_PART_TABLE: &str = "regions";

/// Partition column name (identity transform, VARCHAR).
pub const PART_COL: &str = "region";

/// Partition value for file 1: "north".  Data file contains ids 1..=5.
pub const PART_VAL_NORTH: &str = "north";
/// Partition value for file 2: "central".  Data file contains ids 6..=10.
pub const PART_VAL_CENTRAL: &str = "central";
/// Partition value for file 3: "south".  Data file contains ids 11..=15.
pub const PART_VAL_SOUTH: &str = "south";

/// Ordered partition values — one data file is written per value.
pub const PART_VALUES: [&str; 3] = [PART_VAL_NORTH, PART_VAL_CENTRAL, PART_VAL_SOUTH];

/// Inclusive id range written into the "north" partition file.
pub const PART_NORTH_IDS: (usize, usize) = (1, 5);
/// Inclusive id range written into the "central" partition file.
pub const PART_CENTRAL_IDS: (usize, usize) = (6, 10);
/// Inclusive id range written into the "south" partition file.
pub const PART_SOUTH_IDS: (usize, usize) = (11, 15);

/// Rows per partition file (all partitions are equal-sized).
pub const PART_ROWS_PER_FILE: usize = 5;

/// Total rows across all partition files.
pub const PART_TOTAL_ROWS: usize = PART_ROWS_PER_FILE * PART_VALUES.len();

/// Date32 days-since-epoch for 2024-01-01.
const BASE_DATE: i32 = 19_723;
/// Microseconds since UNIX_EPOCH for 2024-01-01T00:00:00Z.
const BASE_TS_MICROS: i64 = 1_704_067_200_000_000;

/// Handle returned by the seed function.
pub struct SeedHandle {
    /// s3:// paths of the seeded data files (as seen from inside the catalog/Docker).
    pub data_file_paths: Vec<String>,
}

/// Seed all E2E tables (events, labels, regions, star schema) into the REST
/// catalog. Idempotent.
pub async fn seed_events(catalog_url: &str, warehouse: &str) -> Result<SeedHandle> {
    let events_handle = seed_events_table(catalog_url, warehouse).await?;
    seed_labels_table(catalog_url, warehouse).await?;
    seed_partitioned(catalog_url, warehouse).await?;
    seed_star_schema(catalog_url, warehouse).await?;
    seed_multi_table_join_extension(catalog_url, warehouse).await?;
    Ok(events_handle)
}

/// Build a REST catalog client for seed operations.
pub async fn build_seed_catalog(
    catalog_url: &str,
    warehouse: &str,
    label: &str,
) -> Result<impl Catalog> {
    let s3_endpoint = super::stack::minio_url();

    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_url.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse.to_string(),
    );
    props.insert(S3_ENDPOINT.to_string(), s3_endpoint);
    props.insert(S3_REGION.to_string(), "us-east-1".to_string());
    props.insert(S3_ACCESS_KEY_ID.to_string(), "minioadmin".to_string());
    props.insert(S3_SECRET_ACCESS_KEY.to_string(), "minioadmin".to_string());
    props.insert(S3_PATH_STYLE_ACCESS.to_string(), "true".to_string());

    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load(label, props)
        .await
        .context("connect to Iceberg REST catalog for seeding")
}

/// Create `namespace.table_name` (unpartitioned) with `iceberg_schema` if absent,
/// then append `batches` as a single Parquet data file via one fast-append.
///
/// Idempotent: returns `Ok(false)` without writing if the table already has data
/// files. Convenience wrapper over [`create_and_append_files`] for a single file.
pub async fn create_and_append(
    catalog: &impl Catalog,
    namespace: &str,
    table_name: &str,
    iceberg_schema: IcebergSchema,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<bool> {
    create_and_append_files(
        catalog,
        namespace,
        table_name,
        iceberg_schema,
        std::iter::once(batches),
    )
    .await
}

/// Create `namespace.table_name` (unpartitioned) with `iceberg_schema` if absent,
/// then write each element of `files` as its OWN Parquet data file (one fast-append
/// per file, reloading the table between appends). Writing multiple files makes the
/// adapter's `GROUP BY shard_key` fan-out observable (one shard per file).
///
/// Idempotent: returns `Ok(false)` without writing if the table already has data
/// files; `Ok(true)` when files were written. Reused by the TPC-H loader
/// (`tests/tpch_loader.rs`); the per-batch field-id overlay + write/commit is the
/// same pattern as the events/labels/regions seeds.
pub async fn create_and_append_files<F, B>(
    catalog: &impl Catalog,
    namespace: &str,
    table_name: &str,
    iceberg_schema: IcebergSchema,
    files: F,
) -> Result<bool>
where
    F: IntoIterator<Item = B>,
    B: IntoIterator<Item = RecordBatch>,
{
    let ns = NamespaceIdent::new(namespace.to_string());
    let ident = TableIdent::new(ns.clone(), table_name.to_string());

    // Short-circuit if already populated.
    if let Some(paths) = existing_data_file_paths(catalog, &ident).await?
        && !paths.is_empty()
    {
        return Ok(false);
    }

    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace")?
    {
        // Tolerate a concurrent create.
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let partition_spec = UnboundPartitionSpec::builder().with_spec_id(0).build();
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(iceberg_schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();
    let mut table = match catalog.create_table(&ns, creation).await {
        Ok(t) => t,
        Err(_) => catalog
            .load_table(&ident)
            .await
            .context("load existing table after create failed")?,
    };

    // Check again after load (race).
    if !collect_current_snapshot_paths(&table).await?.is_empty() {
        return Ok(false);
    }

    let mut wrote_any = false;
    for batches in files {
        write_one_file_append(catalog, &table, table_name, batches).await?;
        wrote_any = true;
        // Reload so the next append builds on the latest snapshot.
        table = catalog
            .load_table(&ident)
            .await
            .context("reload table between appends")?;
    }
    Ok(wrote_any)
}

/// Write `batches` as a single Parquet data file and fast-append it to `table`.
async fn write_one_file_append(
    catalog: &impl Catalog,
    table: &Table,
    table_name: &str,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<()> {
    let schema = table.metadata().current_schema().clone();
    let file_io = table.file_io().clone();
    let location_gen = FlatLocationGenerator {
        base: table.metadata().location().to_string(),
    };
    let file_name_gen = DefaultFileNameGenerator::new(
        table_name.to_string(),
        Some(uuid_suffix()),
        DataFileFormat::Parquet,
    );
    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        file_io,
        location_gen,
        file_name_gen,
    );
    let partition_key = iceberg::spec::PartitionKey::new(
        table.metadata().default_partition_spec().as_ref().clone(),
        schema.clone(),
        Struct::empty(),
    );
    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(Some(partition_key))
        .await
        .context("build data file writer")?;

    for batch in batches {
        let batch = overlay_iceberg_field_ids(&batch, &schema)?;
        writer.write(batch).await.context("write Arrow batch")?;
    }
    let data_files = writer.close().await.context("close data file writer")?;

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).context("apply fast-append action")?;
    tx.commit(catalog)
        .await
        .context("commit Iceberg snapshot")?;
    Ok(())
}

/// Seed only the events table into the REST catalog. Idempotent.
async fn seed_events_table(catalog_url: &str, warehouse: &str) -> Result<SeedHandle> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let table_ident = TableIdent::new(ns.clone(), E2E_TABLE.to_string());

    // Short-circuit if already seeded.
    if let Some(paths) = existing_data_file_paths(&catalog, &table_ident).await?
        && !paths.is_empty()
    {
        return Ok(SeedHandle {
            data_file_paths: paths,
        });
    }

    // Create namespace if missing.
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace")?
    {
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .context("create e2e_lakehouse namespace")?;
    }

    let iceberg_schema = events_iceberg_schema()?;
    // Unpartitioned: an empty partition spec. The data file then carries an
    // empty partition struct, which matches the (zero-field) spec — a void
    // field would require a one-value partition struct and fail the commit
    // ("Partition value is not compatible with partition type").
    let partition_spec = UnboundPartitionSpec::builder().with_spec_id(0).build();

    let creation = TableCreation::builder()
        .name(E2E_TABLE.to_string())
        .schema(iceberg_schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    let table = match catalog.create_table(&ns, creation).await {
        Ok(t) => t,
        Err(_) => catalog
            .load_table(&table_ident)
            .await
            .context("load existing events table after create failed")?,
    };

    // Check again after load (race).
    let existing = collect_current_snapshot_paths(&table).await?;
    if !existing.is_empty() {
        return Ok(SeedHandle {
            data_file_paths: existing,
        });
    }

    let paths = write_events_and_commit(&catalog, table).await?;
    Ok(SeedHandle {
        data_file_paths: paths,
    })
}

fn events_iceberg_schema() -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "score", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::required(4, "event_date", Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::required(5, "event_ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
        ])
        .build()
        .context("build events Iceberg schema")
}

/// Build a RecordBatch for the inclusive 1-indexed id range `first_id..=last_id`.
///
/// The per-row value formulas are identical regardless of how the rows are split
/// across batches: score = 5.0 * id, name = "event-NN", event_date = BASE_DATE +
/// (id-1), event_ts = BASE_TS_MICROS + (id-1) hours. Splitting the seed into two
/// id-ranges (1..=10 and 11..=20) therefore produces the SAME 20 rows, just
/// written across two parquet data files so the shard fan-out is observable.
fn make_events_batch(first_id: usize, last_id: usize) -> RecordBatch {
    let ids: Vec<i64> = (first_id as i64..=last_id as i64).collect();
    let names: Vec<String> = (first_id..=last_id)
        .map(|i| format!("event-{i:02}"))
        .collect();
    // score = 5.0 * id; scores > 15.0 → id >= 4 (1-indexed) → 17 rows have score > 15.0
    let scores: Vec<f64> = (first_id..=last_id).map(|i| 5.0 * i as f64).collect();
    let dates: Vec<i32> = (first_id..=last_id)
        .map(|i| BASE_DATE + (i as i32 - 1))
        .collect();
    // 1-hour spacing.
    let timestamps: Vec<i64> = (first_id..=last_id)
        .map(|i| BASE_TS_MICROS + (i as i64 - 1) * 3_600_000_000)
        .collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
        Field::new("event_date", DataType::Date32, false),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(scores)),
            Arc::new(Date32Array::from(dates)),
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
        ],
    )
    .expect("events RecordBatch construction is infallible")
}

/// Attach Iceberg PARQUET field-id metadata to each Arrow field by name-match.
fn overlay_iceberg_field_ids(
    batch: &RecordBatch,
    iceberg_schema: &IcebergSchema,
) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let fields: Vec<Field> = source_schema
        .fields()
        .iter()
        .map(|field| {
            let id = iceberg_schema
                .field_id_by_name(field.name())
                .with_context(|| {
                    format!(
                        "Iceberg schema missing field '{}' for field-id overlay",
                        field.name()
                    )
                })?;
            let mut meta = field.metadata().clone();
            meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
            Ok(field.as_ref().clone().with_metadata(meta))
        })
        .collect::<Result<_>>()?;

    let schema = Arc::new(ArrowSchema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec())
        .context("attach Iceberg field-id metadata")
}

#[derive(Clone)]
struct FlatLocationGenerator {
    base: String,
}

impl iceberg::writer::file_writer::location_generator::LocationGenerator for FlatLocationGenerator {
    fn generate_location(
        &self,
        _partition_key: Option<&iceberg::spec::PartitionKey>,
        file_name: &str,
    ) -> String {
        format!("{}/data/{}", self.base, file_name)
    }
}

/// Write the 20 seed rows across TWO parquet data files (id 1..=10 and 11..=20),
/// each committed as its own Iceberg fast-append. Two data files make the shard
/// fan-out observable: with file_count == 2 the adapter emits `GROUP BY shard_key`
/// (G = min(parallelism_factor, file_count, 300) = 2 > 1) instead of a single
/// invocation. The rows and all column values are identical to a single-file seed.
async fn write_events_and_commit<C: Catalog>(catalog: &C, table: Table) -> Result<Vec<String>> {
    let mid = SEED_TOTAL_ROWS / 2;
    let first_path = write_one_data_file(catalog, &table, 1, mid).await?;
    // Reload the table so the second append builds on the first snapshot.
    let table = catalog
        .load_table(table.identifier())
        .await
        .context("reload table between seed appends")?;
    let second_path = write_one_data_file(catalog, &table, mid + 1, SEED_TOTAL_ROWS).await?;

    Ok(vec![first_path, second_path])
}

/// Write rows for the inclusive id range as one parquet file and fast-append it.
/// Returns the committed data file path.
async fn write_one_data_file<C: Catalog>(
    catalog: &C,
    table: &Table,
    first_id: usize,
    last_id: usize,
) -> Result<String> {
    let iceberg_schema = table.metadata().current_schema().clone();
    let file_io = table.file_io().clone();
    let table_location = table.metadata().location().to_string();
    let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

    let location_gen = FlatLocationGenerator {
        base: table_location.clone(),
    };
    let file_name_gen = DefaultFileNameGenerator::new(
        "events".to_string(),
        Some(uuid_suffix()),
        DataFileFormat::Parquet,
    );

    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_schema.clone());
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        file_io.clone(),
        location_gen,
        file_name_gen,
    );

    // Unpartitioned: use a void partition key (empty Struct).
    let partition_key =
        iceberg::spec::PartitionKey::new(partition_spec, iceberg_schema.clone(), Struct::empty());

    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(Some(partition_key))
        .await
        .context("build data file writer")?;

    let batch = make_events_batch(first_id, last_id);
    let batch = overlay_iceberg_field_ids(&batch, &iceberg_schema)?;
    writer.write(batch).await.context("write Arrow batch")?;
    let data_files = writer.close().await.context("close data file writer")?;
    let paths: Vec<String> = data_files
        .iter()
        .map(|df| df.file_path().to_string())
        .collect();

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).context("apply fast-append action")?;
    tx.commit(catalog)
        .await
        .context("commit Iceberg snapshot")?;

    paths
        .into_iter()
        .next()
        .context("data file writer produced no file for the id range")
}

async fn existing_data_file_paths<C: Catalog>(
    catalog: &C,
    ident: &TableIdent,
) -> Result<Option<Vec<String>>> {
    if !catalog
        .table_exists(ident)
        .await
        .context("check table exists")?
    {
        return Ok(None);
    }
    let table = catalog.load_table(ident).await.context("load table")?;
    let paths = collect_current_snapshot_paths(&table).await?;
    Ok(Some(paths))
}

async fn collect_current_snapshot_paths(table: &Table) -> Result<Vec<String>> {
    if table.metadata().current_snapshot().is_none() {
        return Ok(Vec::new());
    }
    let scan = table
        .scan()
        .select_all()
        .build()
        .context("build scan for file enumeration")?;
    let tasks: Vec<_> = scan
        .plan_files()
        .await
        .context("plan files")?
        .try_collect()
        .await
        .context("collect file scan tasks")?;
    Ok(tasks
        .into_iter()
        .map(|t| t.data_file_path().to_string())
        .collect())
}

fn uuid_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = CTR.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{seq:x}")
}

// ---------------------------------------------------------------------------
// Labels table seeding
// ---------------------------------------------------------------------------

/// Seed the `labels` table (id INT64, label VARCHAR) into the `e2e_lakehouse`
/// namespace. The table contains one row per id in 1..=SEED_LABELS_ROWS with
/// `label = "label-NN"`, matching the events ids so an Exasol-side JOIN works.
async fn seed_labels_table(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-labels").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let table_ident = TableIdent::new(ns.clone(), E2E_TABLE_2.to_string());

    // Short-circuit if already seeded.
    if let Some(paths) = existing_data_file_paths(&catalog, &table_ident).await?
        && !paths.is_empty()
    {
        return Ok(());
    }

    // Namespace is created by seed_events_table; it must exist by now.
    let iceberg_schema = labels_iceberg_schema()?;
    let partition_spec = UnboundPartitionSpec::builder().with_spec_id(0).build();

    let creation = TableCreation::builder()
        .name(E2E_TABLE_2.to_string())
        .schema(iceberg_schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    let table = match catalog.create_table(&ns, creation).await {
        Ok(t) => t,
        Err(_) => catalog
            .load_table(&table_ident)
            .await
            .context("load existing labels table after create failed")?,
    };

    // Check again after load (race).
    let existing = collect_current_snapshot_paths(&table).await?;
    if !existing.is_empty() {
        return Ok(());
    }

    write_labels_and_commit(&catalog, table).await?;
    Ok(())
}

fn labels_iceberg_schema() -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "label", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .context("build labels Iceberg schema")
}

fn make_labels_batch(first_id: usize, last_id: usize) -> RecordBatch {
    let ids: Vec<i64> = (first_id as i64..=last_id as i64).collect();
    let labels: Vec<String> = (first_id..=last_id)
        .map(|i| format!("label-{i:02}"))
        .collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(labels)),
        ],
    )
    .expect("labels RecordBatch construction is infallible")
}

async fn write_labels_and_commit<C: Catalog>(catalog: &C, table: Table) -> Result<Vec<String>> {
    write_one_labels_data_file(catalog, &table, 1, SEED_LABELS_ROWS).await
}

async fn write_one_labels_data_file<C: Catalog>(
    catalog: &C,
    table: &Table,
    first_id: usize,
    last_id: usize,
) -> Result<Vec<String>> {
    let iceberg_schema = table.metadata().current_schema().clone();
    let file_io = table.file_io().clone();
    let table_location = table.metadata().location().to_string();
    let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

    let location_gen = FlatLocationGenerator {
        base: table_location.clone(),
    };
    let file_name_gen = DefaultFileNameGenerator::new(
        E2E_TABLE_2.to_string(),
        Some(uuid_suffix()),
        DataFileFormat::Parquet,
    );

    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_schema.clone());
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        file_io.clone(),
        location_gen,
        file_name_gen,
    );

    let partition_key =
        iceberg::spec::PartitionKey::new(partition_spec, iceberg_schema.clone(), Struct::empty());

    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(Some(partition_key))
        .await
        .context("build labels data file writer")?;

    let batch = make_labels_batch(first_id, last_id);
    let batch = overlay_iceberg_field_ids(&batch, &iceberg_schema)?;
    writer
        .write(batch)
        .await
        .context("write labels Arrow batch")?;
    let data_files = writer
        .close()
        .await
        .context("close labels data file writer")?;
    let paths: Vec<String> = data_files
        .iter()
        .map(|df| df.file_path().to_string())
        .collect();

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action
        .apply(tx)
        .context("apply labels fast-append action")?;
    tx.commit(catalog)
        .await
        .context("commit labels Iceberg snapshot")?;

    Ok(paths)
}

// ---------------------------------------------------------------------------
// Star-schema tables (dim_customer / fact_orders) for join-pushdown E2E tests
// ---------------------------------------------------------------------------
//
// Two tables with a genuine foreign-key relationship and DISJOINT column-name
// prefixes (C_* vs O_*, TPC-H customer/orders shape). The disjoint prefixes are
// what let the join-pushdown adapter's disjoint-column guard reuse the
// vs-expression translator and render a broadcast inner equi-join — the shared
// `id` on the events/labels tables would instead trip that guard.
//
// | table        | columns                          | rows | files |
// |--------------|----------------------------------|------|-------|
// | dim_customer | C_CUSTKEY, C_NAME                | 5    | 1     |
// | fact_orders  | O_ORDERKEY, O_CUSTKEY, O_ORDERDATE| 10  | 2     |
//
// Every order references a valid customer (O_CUSTKEY = ((O_ORDERKEY-1) % 5) + 1),
// so the inner join `fact_orders ⋈ dim_customer ON O_CUSTKEY = C_CUSTKEY` yields
// all FACT_ORDERS_ROWS rows. fact_orders is written across two files so the
// broadcast fan-out (fact side sharded) is observable; dim_customer's single,
// smaller file makes it the broadcast/dimension side under the default threshold.

/// Dimension table name for the join-pushdown E2E tests.
pub const E2E_DIM_TABLE: &str = "dim_customer";
/// Fact table name for the join-pushdown E2E tests.
pub const E2E_FACT_TABLE: &str = "fact_orders";
/// Rows in `dim_customer` (customer keys 1..=DIM_CUSTOMER_ROWS).
pub const DIM_CUSTOMER_ROWS: usize = 5;
/// Rows in `fact_orders` (order keys 1..=FACT_ORDERS_ROWS), across two files.
pub const FACT_ORDERS_ROWS: usize = 10;

/// The customer key an order references: `((order_key - 1) % DIM_CUSTOMER_ROWS) + 1`.
/// Every order therefore matches exactly one seeded customer.
pub fn order_custkey(order_key: usize) -> i64 {
    (((order_key - 1) % DIM_CUSTOMER_ROWS) + 1) as i64
}

/// Days-since-epoch for an order's `O_ORDERDATE`: `BASE_DATE + (order_key - 1)`,
/// i.e. 2024-01-01 for order 1, 2024-01-02 for order 2, and so on.
pub fn order_date_days(order_key: usize) -> i32 {
    BASE_DATE + (order_key as i32 - 1)
}

/// Seed the `dim_customer` and `fact_orders` star-schema tables into the
/// `e2e_lakehouse` namespace. Idempotent.
pub async fn seed_star_schema(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-star").await?;
    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for star schema")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let dim_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "C_CUSTKEY", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "C_NAME", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .context("build dim_customer Iceberg schema")?;
    create_and_append_files(
        &catalog,
        E2E_NAMESPACE,
        E2E_DIM_TABLE,
        dim_schema,
        vec![vec![make_customer_batch(1, DIM_CUSTOMER_ROWS)]],
    )
    .await
    .context("seed dim_customer table")?;

    let fact_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "O_ORDERKEY", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "O_CUSTKEY", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(3, "O_ORDERDATE", Type::Primitive(PrimitiveType::Date)).into(),
        ])
        .build()
        .context("build fact_orders Iceberg schema")?;
    let mid = FACT_ORDERS_ROWS / 2;
    create_and_append_files(
        &catalog,
        E2E_NAMESPACE,
        E2E_FACT_TABLE,
        fact_schema,
        vec![
            vec![make_orders_batch(1, mid)],
            vec![make_orders_batch(mid + 1, FACT_ORDERS_ROWS)],
        ],
    )
    .await
    .context("seed fact_orders table")?;
    Ok(())
}

fn make_customer_batch(first_key: usize, last_key: usize) -> RecordBatch {
    let keys: Vec<i64> = (first_key as i64..=last_key as i64).collect();
    let names: Vec<String> = (first_key..=last_key)
        .map(|k| format!("customer-{k:02}"))
        .collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("C_CUSTKEY", DataType::Int64, false),
        Field::new("C_NAME", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("dim_customer RecordBatch construction is infallible")
}

fn make_orders_batch(first_key: usize, last_key: usize) -> RecordBatch {
    let order_keys: Vec<i64> = (first_key as i64..=last_key as i64).collect();
    let cust_keys: Vec<i64> = (first_key..=last_key).map(order_custkey).collect();
    let dates: Vec<i32> = (first_key..=last_key).map(order_date_days).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("O_ORDERKEY", DataType::Int64, false),
        Field::new("O_CUSTKEY", DataType::Int64, false),
        Field::new("O_ORDERDATE", DataType::Date32, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(order_keys)),
            Arc::new(Int64Array::from(cust_keys)),
            Arc::new(Date32Array::from(dates)),
        ],
    )
    .expect("fact_orders RecordBatch construction is infallible")
}

// ---------------------------------------------------------------------------
// Star-schema extension (fact_lineitem / dim_supplier) for the N-scan
// unaccelerated fallback E2E tests (plan `fix-join-decline-hard-fail`, #76)
// ---------------------------------------------------------------------------
//
// `fact_lineitem` extends the `dim_customer ⋈ fact_orders` pair with a genuine
// foreign key (`L_ORDERKEY` → `O_ORDERKEY`), giving `dim_customer ⋈ fact_orders ⋈
// fact_lineitem` a real 3-table inner-join shape. `L_SUPPKEY` additionally
// foreign-keys `dim_supplier`, extending the SAME chain to a 4-table shape
// (`dim_customer ⋈ fact_orders ⋈ fact_lineitem ⋈ dim_supplier`) for the N=4 case
// — mirroring TPC-H's LINEITEM, which also carries both L_ORDERKEY and L_SUPPKEY,
// so these two additional tables are enough to E2E-cover both the 3-table
// (customer⋈orders⋈lineitem-shaped) and 4-table (NQ3-shaped) N-scan wrapper
// without seeding a wholly separate PART/PARTSUPP/SUPPLIER/NATION fixture set.
//
// | table         | columns                                          | rows | files |
// |---------------|---------------------------------------------------|------|-------|
// | fact_lineitem | L_ORDERKEY, L_LINENUMBER, L_SUPPKEY, L_QUANTITY    | 20   | 2     |
// | dim_supplier  | S_SUPPKEY, S_NAME                                  | 3    | 1     |
//
// Every line item references a valid order and a valid supplier, so the 3-table
// and 4-table inner joins both yield every seeded lineitem row.

/// Second fact table name for the multi-table (N≥3) join-pushdown E2E tests.
pub const E2E_LINEITEM_TABLE: &str = "fact_lineitem";
/// Second dimension table name for the multi-table (N=4) join-pushdown E2E tests.
pub const E2E_SUPPLIER_TABLE: &str = "dim_supplier";
/// Line items written per order (`L_LINENUMBER` cycles `1..=LINES_PER_ORDER`).
pub const LINES_PER_ORDER: usize = 2;
/// Total rows in `fact_lineitem` (`FACT_ORDERS_ROWS * LINES_PER_ORDER`).
pub const LINEITEM_ROWS: usize = FACT_ORDERS_ROWS * LINES_PER_ORDER;
/// Rows in `dim_supplier` (supplier keys 1..=SUPPLIER_ROWS).
pub const SUPPLIER_ROWS: usize = 3;

/// The supplier key a line item (by its order key) references:
/// `((order_key - 1) % SUPPLIER_ROWS) + 1`. Every line item therefore matches
/// exactly one seeded supplier.
pub fn line_suppkey(order_key: usize) -> i64 {
    (((order_key - 1) % SUPPLIER_ROWS) + 1) as i64
}

/// Seed `fact_lineitem` and `dim_supplier` into the `e2e_lakehouse` namespace,
/// extending the star schema to a 3-table and 4-table inner-join shape for the
/// N-scan unaccelerated fallback E2E tests. Idempotent.
pub async fn seed_multi_table_join_extension(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog =
        build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-multijoin").await?;
    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for multi-table join extension")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let supplier_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "S_SUPPKEY", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "S_NAME", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .context("build dim_supplier Iceberg schema")?;
    create_and_append_files(
        &catalog,
        E2E_NAMESPACE,
        E2E_SUPPLIER_TABLE,
        supplier_schema,
        vec![vec![make_supplier_batch(1, SUPPLIER_ROWS)]],
    )
    .await
    .context("seed dim_supplier table")?;

    let lineitem_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "L_ORDERKEY", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "L_LINENUMBER", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(3, "L_SUPPKEY", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(4, "L_QUANTITY", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .context("build fact_lineitem Iceberg schema")?;
    let mid = LINEITEM_ROWS / 2;
    create_and_append_files(
        &catalog,
        E2E_NAMESPACE,
        E2E_LINEITEM_TABLE,
        lineitem_schema,
        vec![
            vec![make_lineitem_batch(1, mid)],
            vec![make_lineitem_batch(mid + 1, LINEITEM_ROWS)],
        ],
    )
    .await
    .context("seed fact_lineitem table")?;
    Ok(())
}

fn make_supplier_batch(first_key: usize, last_key: usize) -> RecordBatch {
    let keys: Vec<i64> = (first_key as i64..=last_key as i64).collect();
    let names: Vec<String> = (first_key..=last_key)
        .map(|k| format!("supplier-{k:02}"))
        .collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("S_SUPPKEY", DataType::Int64, false),
        Field::new("S_NAME", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("dim_supplier RecordBatch construction is infallible")
}

/// Build a RecordBatch for the inclusive 1-indexed lineitem-row range
/// `first_row..=last_row`. Row `r`'s order key is `((r - 1) / LINES_PER_ORDER) + 1`
/// (`LINES_PER_ORDER` consecutive rows per order) and its line number cycles
/// `1..=LINES_PER_ORDER` within that order.
fn make_lineitem_batch(first_row: usize, last_row: usize) -> RecordBatch {
    let order_keys: Vec<i64> = (first_row..=last_row)
        .map(|r| (((r - 1) / LINES_PER_ORDER) + 1) as i64)
        .collect();
    let line_numbers: Vec<i64> = (first_row..=last_row)
        .map(|r| (((r - 1) % LINES_PER_ORDER) + 1) as i64)
        .collect();
    let supp_keys: Vec<i64> = order_keys
        .iter()
        .map(|&order_key| line_suppkey(order_key as usize))
        .collect();
    let quantities: Vec<i64> = (first_row..=last_row)
        .map(|r| (r % 10 + 1) as i64)
        .collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("L_ORDERKEY", DataType::Int64, false),
        Field::new("L_LINENUMBER", DataType::Int64, false),
        Field::new("L_SUPPKEY", DataType::Int64, false),
        Field::new("L_QUANTITY", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(order_keys)),
            Arc::new(Int64Array::from(line_numbers)),
            Arc::new(Int64Array::from(supp_keys)),
            Arc::new(Int64Array::from(quantities)),
        ],
    )
    .expect("fact_lineitem RecordBatch construction is infallible")
}

// ---------------------------------------------------------------------------
// Partitioned table (regions) seeding
// ---------------------------------------------------------------------------

/// Seed the `regions` table into the `e2e_lakehouse` namespace. Idempotent.
///
/// Schema: `id` INT64, `region` VARCHAR.
/// Partition spec: identity transform on `region` (field id 2, source id 2).
/// Data layout: one data file per partition value; id ranges are disjoint and
/// contiguous so per-file min/max bounds are tight:
///
/// | partition | ids     |
/// |-----------|---------|
/// | north     | 1 – 5   |
/// | central   | 6 – 10  |
/// | south     | 11 – 15 |
///
/// See `PART_NORTH_IDS`, `PART_CENTRAL_IDS`, `PART_SOUTH_IDS` for the
/// exact bounds the E2E assertions must reference.
pub async fn seed_partitioned(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-regions").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let table_ident = TableIdent::new(ns.clone(), E2E_PART_TABLE.to_string());

    // Short-circuit if already seeded.
    if let Some(paths) = existing_data_file_paths(&catalog, &table_ident).await?
        && !paths.is_empty()
    {
        return Ok(());
    }

    // Namespace is created by seed_events_table; it must exist by now.
    let iceberg_schema = regions_iceberg_schema()?;
    // Identity transform on the `region` field (source_id = 2, the field id of `region`).
    // The partition field needs an explicit field-id (Iceberg partition ids start at 1000);
    // an unbound spec serializes `field-id: null`, which the REST catalog rejects on create.
    let partition_field = UnboundPartitionField::builder()
        .source_id(2)
        .field_id(1000)
        .name(PART_COL.to_string())
        .transform(Transform::Identity)
        .build();
    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(1)
        .add_partition_fields([partition_field])
        .context("build regions partition spec")?
        .build();

    let creation = TableCreation::builder()
        .name(E2E_PART_TABLE.to_string())
        .schema(iceberg_schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    let table = match catalog.create_table(&ns, creation).await {
        Ok(t) => t,
        Err(_) => catalog
            .load_table(&table_ident)
            .await
            .context("load existing regions table after create failed")?,
    };

    // Check again after load (race).
    let existing = collect_current_snapshot_paths(&table).await?;
    if !existing.is_empty() {
        return Ok(());
    }

    write_regions_and_commit(&catalog, table).await?;
    Ok(())
}

fn regions_iceberg_schema() -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, PART_COL, Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .context("build regions Iceberg schema")
}

/// Build a RecordBatch for the given id range, all rows tagged with `region`.
fn make_regions_batch(first_id: usize, last_id: usize, region: &str) -> RecordBatch {
    let ids: Vec<i64> = (first_id as i64..=last_id as i64).collect();
    let regions: Vec<&str> = vec![region; last_id - first_id + 1];

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(PART_COL, DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
        ],
    )
    .expect("regions RecordBatch construction is infallible")
}

/// Write one file per partition, committing each as a separate fast-append.
async fn write_regions_and_commit<C: Catalog>(catalog: &C, table: Table) -> Result<()> {
    let ranges: [(&str, usize, usize); 3] = [
        (PART_VAL_NORTH, PART_NORTH_IDS.0, PART_NORTH_IDS.1),
        (PART_VAL_CENTRAL, PART_CENTRAL_IDS.0, PART_CENTRAL_IDS.1),
        (PART_VAL_SOUTH, PART_SOUTH_IDS.0, PART_SOUTH_IDS.1),
    ];

    let mut current_table = table;
    for (region, first_id, last_id) in ranges {
        write_one_partitioned_file(catalog, &current_table, first_id, last_id, region).await?;
        // Reload so the next append builds on the latest snapshot.
        current_table = catalog
            .load_table(current_table.identifier())
            .await
            .context("reload regions table between partition appends")?;
    }
    Ok(())
}

/// Write rows for one partition (identity-partitioned by `region`) as a single
/// Parquet file and fast-append it to the table.
async fn write_one_partitioned_file<C: Catalog>(
    catalog: &C,
    table: &Table,
    first_id: usize,
    last_id: usize,
    region: &str,
) -> Result<()> {
    let iceberg_schema = table.metadata().current_schema().clone();
    let file_io = table.file_io().clone();
    let table_location = table.metadata().location().to_string();
    let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

    let location_gen = FlatLocationGenerator {
        base: table_location.clone(),
    };
    let file_name_gen = DefaultFileNameGenerator::new(
        format!("{E2E_PART_TABLE}-{region}"),
        Some(uuid_suffix()),
        DataFileFormat::Parquet,
    );

    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_schema.clone());
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        file_io.clone(),
        location_gen,
        file_name_gen,
    );

    // Partition key carries the identity-transformed region value.
    let partition_data = Struct::from_iter([Some(Literal::string(region))]);
    let partition_key =
        iceberg::spec::PartitionKey::new(partition_spec, iceberg_schema.clone(), partition_data);

    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(Some(partition_key))
        .await
        .context("build regions data file writer")?;

    let batch = make_regions_batch(first_id, last_id, region);
    let batch = overlay_iceberg_field_ids(&batch, &iceberg_schema)?;
    writer
        .write(batch)
        .await
        .context("write regions Arrow batch")?;
    let data_files = writer
        .close()
        .await
        .context("close regions data file writer")?;

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action
        .apply(tx)
        .context("apply regions fast-append action")?;
    tx.commit(catalog)
        .await
        .context("commit regions Iceberg snapshot")?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Schema-evolution table (evo) — reproduces issue #26 (rename by field-id)
// ---------------------------------------------------------------------------
//
// iceberg-rust 0.9.1 exposes NO schema-evolution API (the `transaction` module
// has only append/snapshot/sort_order/location/properties/statistics/format
// actions, and `TableCommit`'s builder is `pub(crate)`). So the rename step is
// applied out-of-band via a raw Iceberg REST commit (`add-schema` +
// `set-current-schema`, keeping field-id 2) to the `iceberg-rest-fixture` — a
// full Java catalog, so it validates and applies the rename exactly as a real
// catalog would.

/// Table name for the column-rename schema-evolution repro (issue #26).
pub const E2E_EVO_TABLE: &str = "evo";
/// Inclusive id range written BEFORE the rename (physical parquet column `score`).
pub const EVO_PRE_RENAME_IDS: (i64, i64) = (1, 5);
/// Inclusive id range written AFTER the rename (physical parquet column `rating`).
pub const EVO_POST_RENAME_IDS: (i64, i64) = (6, 10);
/// Total rows a spec-compliant, field-id-based reader must return for `evo`.
pub const EVO_TOTAL_ROWS: usize = 10;
/// Column name for field-id 2 before the rename.
pub const EVO_OLD_COL: &str = "score";
/// Column name for field-id 2 after the rename.
pub const EVO_NEW_COL: &str = "rating";

/// Seed the `evo` table for schema-evolution testing: a column renamed in the
/// Iceberg catalog while pre-rename data files keep the old physical column name.
///
/// Sequence:
///   1. create `evo` (id BIGINT, `score` DOUBLE) — schema-id 0, field-id 2 = `score`
///   2. append file A: ids 1..=5, physical parquet column `score` (field-id 2)
///   3. REST commit: rename field-id 2 `score` → `rating` (field-id preserved)
///   4. append file B: ids 6..=10, physical parquet column `rating` (field-id 2)
///
/// A field-id-based reader binds both files to the current logical name `rating`
/// by field-id 2 and returns 10 rows with `rating = 10 * id`, no NULLs.
///
/// Not idempotent: drops and recreates `evo` so every run starts from a known
/// state.
pub async fn seed_renamed_column(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-evo").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let ident = TableIdent::new(ns.clone(), E2E_EVO_TABLE.to_string());

    // Namespace is created by seed_events_table; tolerate a concurrent create.
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for evo")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    // Drop-and-recreate for a deterministic starting state.
    if catalog
        .table_exists(&ident)
        .await
        .context("check evo table exists")?
    {
        catalog
            .drop_table(&ident)
            .await
            .context("drop existing evo table")?;
    }

    // 1. Create `evo` with the pre-rename schema (id BIGINT, score DOUBLE).
    let partition_spec = UnboundPartitionSpec::builder().with_spec_id(0).build();
    let creation = TableCreation::builder()
        .name(E2E_EVO_TABLE.to_string())
        .schema(evo_schema(0, EVO_OLD_COL)?)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();
    let table = catalog
        .create_table(&ns, creation)
        .await
        .context("create evo table")?;

    // 2. Append file A under the OLD column name (`score`).
    let (a0, a1) = EVO_PRE_RENAME_IDS;
    write_one_file_append(
        &catalog,
        &table,
        E2E_EVO_TABLE,
        [make_evo_batch(a0, a1, EVO_OLD_COL)],
    )
    .await
    .context("append evo file A (pre-rename)")?;

    // 3. Rename field-id 2 `score` → `rating` via a raw REST catalog commit.
    let table = catalog
        .load_table(&ident)
        .await
        .context("reload evo before rename")?;
    let current_schema_id = table.metadata().current_schema_id();
    rest_rename_column(
        catalog_url,
        E2E_NAMESPACE,
        E2E_EVO_TABLE,
        current_schema_id,
        evo_schema(current_schema_id + 1, EVO_NEW_COL)?,
    )
    .await
    .context("REST rename score -> rating")?;

    // 4. Append file B under the NEW column name (`rating`).
    let table = catalog
        .load_table(&ident)
        .await
        .context("reload evo after rename")?;
    assert_eq!(
        table
            .metadata()
            .current_schema()
            .field_by_id(2)
            .map(|f| f.name.as_str()),
        Some(EVO_NEW_COL),
        "REST rename did not take effect: field-id 2 is not '{EVO_NEW_COL}'"
    );
    let (b0, b1) = EVO_POST_RENAME_IDS;
    write_one_file_append(
        &catalog,
        &table,
        E2E_EVO_TABLE,
        [make_evo_batch(b0, b1, EVO_NEW_COL)],
    )
    .await
    .context("append evo file B (post-rename)")?;

    Ok(())
}

/// Two-field evo schema: field-id 1 = `id` (Long), field-id 2 = the column whose
/// name is passed as `col2` (Double). The field-id of the second column is fixed at
/// 2 regardless of its name — that stable id across the rename is the whole point
/// of the repro.
fn evo_schema(schema_id: i32, col2: &str) -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(schema_id)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, col2, Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
        .context("build evo Iceberg schema")
}

/// Build an evo RecordBatch for the inclusive id range with the second column
/// named `col2` (value = 10.0 * id, so post-rename values are distinct/checkable).
fn make_evo_batch(first_id: i64, last_id: i64, col2: &str) -> RecordBatch {
    let ids: Vec<i64> = (first_id..=last_id).collect();
    let vals: Vec<f64> = (first_id..=last_id).map(|i| 10.0 * i as f64).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(col2, DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(vals)),
        ],
    )
    .expect("evo RecordBatch construction is infallible")
}

// ---------------------------------------------------------------------------
// COUNT(DISTINCT) / expression-aggregate E2E probe tables
// ---------------------------------------------------------------------------
//
// Used only by `tests/e2e_count_distinct_test.rs`; not part of `seed_events`
// so the other E2E test binaries (which call `seed_events` in their own
// setup) do not pay the extra seeding cost.

/// Table name for the multi-shard `COUNT(DISTINCT)` + expression-aggregate probe.
pub const E2E_DISTINCT_TABLE: &str = "distinct_probe";
/// Nullable low-cardinality column on `distinct_probe`.
pub const DISTINCT_CATEGORY_COL: &str = "category";
/// Non-null low-cardinality column on `distinct_probe`, independent of `category`.
pub const DISTINCT_REGION_COL: &str = "region";
/// Variable-length string column on `distinct_probe` (length == id).
pub const DISTINCT_COMMENT_COL: &str = "comment";
/// Total rows seeded into `distinct_probe`, across TWO data files (ids 1..=10,
/// 11..=20) so `COUNT(DISTINCT)` pushdown must merge per-shard local sets.
pub const DISTINCT_PROBE_TOTAL_ROWS: usize = 20;
/// Distinct non-NULL `category` values across both shards: {"A","B","C"}.
/// "A" appears in BOTH shards (ids 3,6,9 in file 1; 12,15,18 in file 2),
/// proving the merge dedupes across shards rather than summing per-shard
/// counts. 7 rows have a NULL category (must not be counted).
pub const DISTINCT_CATEGORY_COUNT: i64 = 3;
/// Distinct `region` values (no NULLs): {"north","central","south","east"}.
pub const DISTINCT_REGION_COUNT: i64 = 4;
/// `SUM(LENGTH(comment))` over all 20 rows; comment length == id, so the sum
/// is 1 + 2 + ... + 20 = 210.
pub const DISTINCT_COMMENT_LENGTH_SUM: i64 = 210;

/// Table name for the single-shard high-cardinality `COUNT(DISTINCT)` safety-cap probe.
pub const E2E_HIGH_CARD_TABLE: &str = "high_card_probe";
/// High-cardinality column on `high_card_probe`.
pub const HIGH_CARD_COL: &str = "token";
/// Row count, written as a SINGLE data file (one shard), chosen so the
/// per-shard local distinct set deterministically exceeds
/// `MAX_DISTINCT_BYTES_PER_SHARD` (1 MiB in `scan/mod.rs`) well before
/// `MAX_DISTINCT_ELEMENTS_PER_SHARD` (100,000): 12,000 unique 100-byte
/// tokens serialize to > 1.2 MB of JSON, comfortably past the 1 MiB cap.
pub const HIGH_CARD_ROWS: usize = 12_000;

/// Seed the `distinct_probe` table (`id`, `category`, `region`, `comment`)
/// into the `e2e_lakehouse` namespace across TWO data files. Idempotent.
pub async fn seed_distinct_probe(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-distinct").await?;
    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for distinct_probe")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let iceberg_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(
                2,
                DISTINCT_CATEGORY_COL,
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(
                3,
                DISTINCT_REGION_COL,
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(
                4,
                DISTINCT_COMMENT_COL,
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
        ])
        .build()
        .context("build distinct_probe Iceberg schema")?;

    let file1 = vec![make_distinct_probe_batch(1, 10)];
    let file2 = vec![make_distinct_probe_batch(11, 20)];
    create_and_append_files(
        &catalog,
        E2E_NAMESPACE,
        E2E_DISTINCT_TABLE,
        iceberg_schema,
        vec![file1, file2],
    )
    .await
    .context("seed distinct_probe table")?;
    Ok(())
}

/// `category` for one id: non-NULL values are {"A","B"} on ids 1..=10 and
/// {"A","C"} on ids 11..=20, so "A" is the value shared across both shards.
fn category_for(id: i64) -> Option<String> {
    if id <= 10 {
        match id % 3 {
            1 => Some("B".to_string()),
            0 => Some("A".to_string()),
            _ => None,
        }
    } else {
        match id % 3 {
            0 => Some("A".to_string()),
            1 => Some("C".to_string()),
            _ => None,
        }
    }
}

/// `region` for one id: cycles through all 4 values every 4 ids, no NULLs.
fn region_for(id: i64) -> &'static str {
    match id % 4 {
        0 => "north",
        1 => "central",
        2 => "south",
        _ => "east",
    }
}

fn make_distinct_probe_batch(first_id: i64, last_id: i64) -> RecordBatch {
    let ids: Vec<i64> = (first_id..=last_id).collect();
    let categories: Vec<Option<String>> = ids.iter().map(|&id| category_for(id)).collect();
    let regions: Vec<&str> = ids.iter().map(|&id| region_for(id)).collect();
    // comment length == id, so SUM(LENGTH(comment)) is a non-trivial, easily
    // hand-checked value (1 + 2 + ... + 20).
    let comments: Vec<String> = ids.iter().map(|&id| "x".repeat(id as usize)).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(DISTINCT_CATEGORY_COL, DataType::Utf8, true),
        Field::new(DISTINCT_REGION_COL, DataType::Utf8, false),
        Field::new(DISTINCT_COMMENT_COL, DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(categories)),
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(comments)),
        ],
    )
    .expect("distinct_probe RecordBatch construction is infallible")
}

/// Seed the `high_card_probe` table (`id`, `token`) as a SINGLE data file (one
/// shard) with `HIGH_CARD_ROWS` unique 100-byte `token` values. Idempotent.
pub async fn seed_high_card_probe(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-highcard").await?;
    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for high_card_probe")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let iceberg_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, HIGH_CARD_COL, Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .context("build high_card_probe Iceberg schema")?;

    let batch = make_high_card_batch(HIGH_CARD_ROWS);
    create_and_append(
        &catalog,
        E2E_NAMESPACE,
        E2E_HIGH_CARD_TABLE,
        iceberg_schema,
        [batch],
    )
    .await
    .context("seed high_card_probe table")?;
    Ok(())
}

/// One data file's worth of unique, fixed-length (100-byte) `token` values,
/// zero-padded so every row's serialized JSON element contributes the same
/// byte count and the safety-cap trip point stays deterministic.
fn make_high_card_batch(rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (1..=rows as i64).collect();
    let tokens: Vec<String> = ids.iter().map(|&id| format!("{id:0>100}")).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(HIGH_CARD_COL, DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(tokens)),
        ],
    )
    .expect("high_card_probe RecordBatch construction is infallible")
}

/// Apply a column rename to an existing table via a raw Iceberg REST commit.
///
/// A rename is expressed as `add-schema` (a new schema whose renamed field keeps
/// its field-id) + `set-current-schema` (`schema-id: -1` = the just-added schema),
/// guarded by an `assert-current-schema-id` requirement. iceberg-rust 0.9.1 has
/// no public API to build a `TableCommit`, so we POST the commit body directly.
async fn rest_rename_column(
    catalog_url: &str,
    namespace: &str,
    table_name: &str,
    current_schema_id: i32,
    renamed_schema: IcebergSchema,
) -> Result<()> {
    let base = catalog_url.trim_end_matches('/');
    let client = reqwest::Client::new();

    // The catalog may advertise a routing `prefix` in /v1/config overrides.
    let prefix = client
        .get(format!("{base}/v1/config"))
        .send()
        .await
        .context("GET /v1/config")?
        .text()
        .await
        .ok()
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .and_then(|v| {
            v.get("overrides")
                .and_then(|o| o.get("prefix"))
                .and_then(|p| p.as_str())
                .map(str::to_string)
        })
        .filter(|p| !p.is_empty());

    let mut endpoint = format!("{base}/v1");
    if let Some(p) = &prefix {
        endpoint.push('/');
        endpoint.push_str(p);
    }
    endpoint.push_str(&format!("/namespaces/{namespace}/tables/{table_name}"));

    let requirements = vec![TableRequirement::CurrentSchemaIdMatch { current_schema_id }];
    let updates = vec![
        TableUpdate::AddSchema {
            schema: renamed_schema,
        },
        TableUpdate::SetCurrentSchema { schema_id: -1 },
    ];
    let body = serde_json::json!({
        "identifier": { "namespace": [namespace], "name": table_name },
        "requirements": serde_json::to_value(&requirements).context("serialize requirements")?,
        "updates": serde_json::to_value(&updates).context("serialize updates")?,
    });
    let body = serde_json::to_string(&body).context("serialize commit body")?;

    let resp = client
        .post(&endpoint)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .context("POST rename commit to REST catalog")?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("REST rename commit failed ({status}): {text}");
    }
    Ok(())
}
