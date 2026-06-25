//! Iceberg table seeder for lakehouse-engine E2E tests.
//!
//! Seeds a deterministic mixed-column table into the Iceberg REST catalog over MinIO.
//! Uses iceberg-rust 0.9.1 + iceberg-catalog-rest 0.9.1 (same as the main crate).
//!
//! Column mix exercises the full type-mapping table:
//!   id           INT64        → DECIMAL(20,0)
//!   name         UTF8         → VARCHAR(2000000)
//!   score        FLOAT64      → DOUBLE PRECISION
//!   event_date   DATE32       → DATE
//!   event_ts     TIMESTAMP(µs,None) → TIMESTAMP
//!
//! Complex columns (list/struct) are covered by unit tests; they are not written
//! here because iceberg-rust 0.9.1 does not expose a struct/list writer.
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
use futures::TryStreamExt;
use ice_arrow_array::{
    Date32Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use ice_arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use ice_parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use ice_parquet::file::properties::WriterProperties;
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
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;

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

/// Seed all E2E tables (events, labels, regions) into the REST catalog. Idempotent.
pub async fn seed_events(catalog_url: &str, warehouse: &str) -> Result<SeedHandle> {
    let events_handle = seed_events_table(catalog_url, warehouse).await?;
    seed_labels_table(catalog_url, warehouse).await?;
    seed_partitioned(catalog_url, warehouse).await?;
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
            configured_scheme: "s3".to_string(),
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
