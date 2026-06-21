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
#![cfg(feature = "exasol-e2e")]

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
    DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Struct, Type,
    UnboundPartitionSpec,
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

/// Namespace and table names for the E2E seed table.
pub const E2E_NAMESPACE: &str = "e2e_lakehouse";
pub const E2E_TABLE: &str = "events";
/// Qualified table name for VS properties.
pub const E2E_QUALIFIED_TABLE: &str = "e2e_lakehouse.events";

/// Total rows seeded. Enough that LIMIT 5 < total and WHERE score > 15.0 excludes some.
pub const SEED_TOTAL_ROWS: usize = 20;
/// Rows with score > 15.0 (scores are 5.0 * (i+1) for i=0..19, so > 15.0 means i >= 3 → 17 rows).
pub const SEED_ROWS_SCORE_GT_15: usize = 17;

/// Date32 days-since-epoch for 2024-01-01.
const BASE_DATE: i32 = 19_723;
/// Microseconds since UNIX_EPOCH for 2024-01-01T00:00:00Z.
const BASE_TS_MICROS: i64 = 1_704_067_200_000_000;

/// Handle returned by the seed function.
pub struct SeedHandle {
    /// s3:// paths of the seeded data files (as seen from inside the catalog/Docker).
    pub data_file_paths: Vec<String>,
}

/// Seed the E2E events table into the REST catalog. Idempotent.
pub async fn seed_events(catalog_url: &str, warehouse: &str) -> Result<SeedHandle> {
    // The seed runs on the host, so it reaches MinIO at the host-published
    // endpoint (localhost:<minio_port>), not the in-container `minio:9000`.
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

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        }))
        .load("lakehouse-e2e-seed", props)
        .await
        .context("connect to Iceberg REST catalog for seeding")?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let table_ident = TableIdent::new(ns.clone(), E2E_TABLE.to_string());

    // Short-circuit if already seeded.
    if let Some(paths) = existing_data_file_paths(&catalog, &table_ident).await? {
        if !paths.is_empty() {
            return Ok(SeedHandle {
                data_file_paths: paths,
            });
        }
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

fn make_events_batch() -> RecordBatch {
    let count = SEED_TOTAL_ROWS;
    let ids: Vec<i64> = (1..=count as i64).collect();
    let names: Vec<String> = (1..=count).map(|i| format!("event-{i:02}")).collect();
    // score = 5.0 * i; scores > 15.0 → i >= 4 (1-indexed) → 17 rows have score > 15.0
    let scores: Vec<f64> = (1..=count).map(|i| 5.0 * i as f64).collect();
    let dates: Vec<i32> = (0..count as i32).map(|i| BASE_DATE + i).collect();
    // 1-hour spacing.
    let timestamps: Vec<i64> = (0..count as i64)
        .map(|i| BASE_TS_MICROS + i * 3_600_000_000)
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

async fn write_events_and_commit<C: Catalog>(catalog: &C, table: Table) -> Result<Vec<String>> {
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

    let batch = make_events_batch();
    let batch = overlay_iceberg_field_ids(&batch, &iceberg_schema)?;
    writer.write(batch).await.context("write Arrow batch")?;
    let data_files = writer.close().await.context("close data file writer")?;
    let paths: Vec<String> = data_files
        .iter()
        .map(|df| df.file_path().to_string())
        .collect();

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).context("apply fast-append action")?;
    tx.commit(catalog)
        .await
        .context("commit Iceberg snapshot")?;

    Ok(paths)
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
