//! Iceberg table seeder for lakehouse-engine E2E tests (REST catalog over MinIO).
//!
//! Complex columns are writable: `seed_complex_types_probe` builds its Arrow batch from
//! `schema_to_arrow_schema` after `create_table`, so nested field-ids match the ones
//! the REST catalog assigned.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::json::ReaderBuilder;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{
    ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS,
    S3_REGION, S3_SECRET_ACCESS_KEY, StorageFactory,
};
use iceberg::spec::{
    DataFileFormat, FormatVersion, ListType, Literal, MapType, NestedField, PrimitiveType,
    Schema as IcebergSchema, Struct, StructType, Transform, Type, UnboundPartitionField,
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
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::{
    AwsCredential, CustomAwsCredentialLoader, OpenDalStorageFactory, ProvideCredential,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use reqsign_core::{Context as ReqsignContext, Result as ReqsignResult};
use serde_json::json;

pub const E2E_NAMESPACE: &str = "e2e_lakehouse";
pub const E2E_TABLE: &str = "events";
pub const E2E_TABLE_2: &str = "labels";
pub const E2E_QUALIFIED_TABLE: &str = "e2e_lakehouse.events";

pub const SEED_TOTAL_ROWS: usize = 20;
/// Scores are `5.0 * id`, so score > 15.0 holds for ids 4..=20.
pub const SEED_ROWS_SCORE_GT_15: usize = 17;
pub const SEED_LABELS_ROWS: usize = SEED_TOTAL_ROWS;

pub const E2E_PART_TABLE: &str = "regions";

pub const PART_COL: &str = "region";

pub const PART_VAL_NORTH: &str = "north";
pub const PART_VAL_CENTRAL: &str = "central";
pub const PART_VAL_SOUTH: &str = "south";

pub const PART_VALUES: [&str; 3] = [PART_VAL_NORTH, PART_VAL_CENTRAL, PART_VAL_SOUTH];

pub const PART_NORTH_IDS: (usize, usize) = (1, 5);
pub const PART_CENTRAL_IDS: (usize, usize) = (6, 10);
pub const PART_SOUTH_IDS: (usize, usize) = (11, 15);

pub const PART_ROWS_PER_FILE: usize = 5;

pub const PART_TOTAL_ROWS: usize = PART_ROWS_PER_FILE * PART_VALUES.len();

/// 2024-01-01 as Date32 days since epoch.
const BASE_DATE: i32 = 19_723;
/// 2024-01-01T00:00:00Z in microseconds since epoch.
const BASE_TS_MICROS: i64 = 1_704_067_200_000_000;

pub struct SeedHandle {
    pub data_file_paths: Vec<String>,
}

pub async fn seed_events(catalog_url: &str, warehouse: &str) -> Result<SeedHandle> {
    let events_handle = seed_events_table(catalog_url, warehouse).await?;
    seed_labels_table(catalog_url, warehouse).await?;
    seed_partitioned(catalog_url, warehouse).await?;
    seed_star_schema(catalog_url, warehouse).await?;
    seed_multi_table_join_extension(catalog_url, warehouse).await?;
    seed_char_pad_table(catalog_url, warehouse).await?;
    Ok(events_handle)
}

/// `Default` is the static MinIO baseline.
#[derive(Clone, Default)]
pub enum SeedStorage {
    #[default]
    Minio,
    /// Never the container-lifecycle service principal, or seeding would succeed without
    /// exercising the account-key path.
    Adls {
        account_name: String,
        account_key: String,
    },
}

/// A non-empty `token` is sent as a static bearer credential. `Minio` storage forces
/// static S3 credentials over whatever the catalog vends; `Adls` overrides nothing,
/// since a `sas-enabled: false` warehouse vends no credentials.
#[derive(Clone, Default)]
pub struct SeedCatalogAuth {
    pub token: Option<String>,
    pub storage: SeedStorage,
}

// `iceberg-catalog-rest` exports no constant for this key.
const REST_CATALOG_PROP_TOKEN: &str = "token";

fn seed_storage_config() -> (String, String, String, String, bool) {
    (
        super::stack::minio_url(),
        "us-east-1".to_string(),
        "minioadmin".to_string(),
        "minioadmin".to_string(),
        true,
    )
}

/// Always returns the static admin credentials, regardless of per-table vended ones;
/// see [`build_seed_catalog_with_auth`].
#[derive(Debug)]
struct StaticS3CredentialProvider {
    access_key_id: String,
    secret_access_key: String,
}

impl ProvideCredential for StaticS3CredentialProvider {
    type Credential = AwsCredential;

    async fn provide_credential(
        &self,
        _ctx: &ReqsignContext,
    ) -> ReqsignResult<Option<Self::Credential>> {
        Ok(Some(AwsCredential {
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            // A `None` `expires_in` marks the credential permanently valid.
            ..Default::default()
        }))
    }
}

fn seed_catalog_props(
    catalog_url: &str,
    warehouse: &str,
    auth: &SeedCatalogAuth,
) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_url.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse.to_string(),
    );

    match &auth.storage {
        SeedStorage::Minio => {
            let (endpoint, region, access_key, secret_key, path_style) = seed_storage_config();
            props.insert(S3_ENDPOINT.to_string(), endpoint);
            props.insert(S3_REGION.to_string(), region);
            props.insert(S3_ACCESS_KEY_ID.to_string(), access_key);
            props.insert(S3_SECRET_ACCESS_KEY.to_string(), secret_key);
            props.insert(S3_PATH_STYLE_ACCESS.to_string(), path_style.to_string());
        }
        SeedStorage::Adls {
            account_name,
            account_key,
        } => {
            props.insert(ADLS_ACCOUNT_NAME.to_string(), account_name.clone());
            props.insert(ADLS_ACCOUNT_KEY.to_string(), account_key.clone());
        }
    }

    if let Some(token) = auth.token.as_deref().filter(|v| !v.is_empty()) {
        props.insert(REST_CATALOG_PROP_TOKEN.to_string(), token.to_string());
    }

    props
}

pub async fn build_seed_catalog(
    catalog_url: &str,
    warehouse: &str,
    label: &str,
) -> Result<impl Catalog> {
    build_seed_catalog_with_auth(catalog_url, warehouse, label, SeedCatalogAuth::default()).await
}

pub async fn build_seed_catalog_with_auth(
    catalog_url: &str,
    warehouse: &str,
    label: &str,
    auth: SeedCatalogAuth,
) -> Result<impl Catalog> {
    let props = seed_catalog_props(catalog_url, warehouse, &auth);

    let storage_factory: Arc<dyn StorageFactory> = match &auth.storage {
        // Force static S3 credentials: iceberg-catalog-rest merges each table's vended config
        // over the static props, so writes to Lakekeeper's `sts-enabled` warehouse would sign
        // with the vended session token, which MinIO rejects (`InvalidTokenId`). A custom
        // credential loader replaces the config-derived credentials in opendal's S3 backend.
        SeedStorage::Minio => {
            let (_, _, access_key, secret_key, _) = seed_storage_config();
            Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: Some(CustomAwsCredentialLoader::new(
                    StaticS3CredentialProvider {
                        access_key_id: access_key,
                        secret_access_key: secret_key,
                    },
                )),
            })
        }
        // No override needed: Lakekeeper vends ADLS creds under `adls.sas-token.<host>`, but
        // `load_file_io` only reads the flat keys, so the account key is the only credential.
        SeedStorage::Adls { .. } => Arc::new(OpenDalStorageFactory::Azdls),
    };

    RestCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load(label, props)
        .await
        .context("connect to Iceberg REST catalog for seeding")
}

/// Returns `Ok(false)` without writing if the table already has data files.
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

/// Each file gets its own fast-append, making the `GROUP BY shard_key` fan-out
/// observable. Returns `Ok(false)` without writing if the table already has data files.
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
    create_and_append_files_with_properties(
        catalog,
        namespace,
        table_name,
        iceberg_schema,
        HashMap::new(),
        files,
    )
    .await
}

/// The REST catalog honours a format version only from the `format-version` property;
/// `TableCreation::format_version` is a no-op against it.
pub async fn create_and_append_files_with_properties<F, B>(
    catalog: &impl Catalog,
    namespace: &str,
    table_name: &str,
    iceberg_schema: IcebergSchema,
    properties: HashMap<String, String>,
    files: F,
) -> Result<bool>
where
    F: IntoIterator<Item = B>,
    B: IntoIterator<Item = RecordBatch>,
{
    let ns = NamespaceIdent::new(namespace.to_string());
    let ident = TableIdent::new(ns.clone(), table_name.to_string());

    // The Docker warehouse outlives test runs, so a populated table with a stale schema
    // is dropped and recreated rather than silently pinning old columns.
    if catalog
        .table_exists(&ident)
        .await
        .context("check table exists")?
    {
        let table = catalog.load_table(&ident).await.context("load table")?;
        let populated = !collect_current_snapshot_paths(&table).await?.is_empty();
        let schema_matches = schema_field_signature(table.metadata().current_schema())
            == schema_field_signature(&iceberg_schema);
        if populated && schema_matches {
            return Ok(false);
        }
        catalog
            .drop_table(&ident)
            .await
            .context("drop stale-schema table before reseed")?;
    }

    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let partition_spec = UnboundPartitionSpec::builder().with_spec_id(0).build();
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(iceberg_schema)
        .partition_spec(partition_spec)
        .properties(properties)
        .build();
    let mut table = match catalog.create_table(&ns, creation).await {
        Ok(t) => t,
        Err(create_error) => catalog.load_table(&ident).await.with_context(|| {
            format!("create table failed ({create_error}); loading it instead also failed")
        })?,
    };

    if !collect_current_snapshot_paths(&table).await?.is_empty() {
        return Ok(false);
    }

    let mut wrote_any = false;
    for batches in files {
        write_one_file_append(catalog, &table, table_name, batches).await?;
        wrote_any = true;
        table = catalog
            .load_table(&ident)
            .await
            .context("reload table between appends")?;
    }
    Ok(wrote_any)
}

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

async fn seed_events_table(catalog_url: &str, warehouse: &str) -> Result<SeedHandle> {
    seed_events_table_with_auth(catalog_url, warehouse, SeedCatalogAuth::default()).await
}

/// Lakekeeper routes by warehouse name from `GET /v1/config?warehouse=`, so only the
/// name is needed.
pub async fn seed_events_table_with_auth(
    catalog_url: &str,
    warehouse: &str,
    auth: SeedCatalogAuth,
) -> Result<SeedHandle> {
    let catalog =
        build_seed_catalog_with_auth(catalog_url, warehouse, "lakehouse-e2e-seed", auth).await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let table_ident = TableIdent::new(ns.clone(), E2E_TABLE.to_string());

    if let Some(paths) = existing_data_file_paths(&catalog, &table_ident).await?
        && !paths.is_empty()
    {
        return Ok(SeedHandle {
            data_file_paths: paths,
        });
    }

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
    // An empty spec, not a void field: a void field needs a one-value partition struct and
    // fails the commit ("Partition value is not compatible with partition type").
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

/// Row values depend only on the id, so any split across files yields the same rows.
fn make_events_batch(first_id: usize, last_id: usize) -> RecordBatch {
    let ids: Vec<i64> = (first_id as i64..=last_id as i64).collect();
    let names: Vec<String> = (first_id..=last_id)
        .map(|i| format!("event-{i:02}"))
        .collect();
    let scores: Vec<f64> = (first_id..=last_id).map(|i| 5.0 * i as f64).collect();
    let dates: Vec<i32> = (first_id..=last_id)
        .map(|i| BASE_DATE + (i as i32 - 1))
        .collect();
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

/// Two data files make the shard fan-out observable (`GROUP BY shard_key` with G = 2).
async fn write_events_and_commit<C: Catalog>(catalog: &C, table: Table) -> Result<Vec<String>> {
    let mid = SEED_TOTAL_ROWS / 2;
    let first_path = write_one_data_file(catalog, &table, 1, mid).await?;
    let table = catalog
        .load_table(table.identifier())
        .await
        .context("reload table between seed appends")?;
    let second_path = write_one_data_file(catalog, &table, mid + 1, SEED_TOTAL_ROWS).await?;

    Ok(vec![first_path, second_path])
}

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

/// Detects schema drift of a persisted table so it is reseeded, not reused.
fn schema_field_signature(schema: &IcebergSchema) -> Vec<(String, Type)> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.name.clone(), (*f.field_type).clone()))
        .collect()
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

/// Ids match the events table, so an Exasol-side JOIN works.
async fn seed_labels_table(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-labels").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let table_ident = TableIdent::new(ns.clone(), E2E_TABLE_2.to_string());

    if let Some(paths) = existing_data_file_paths(&catalog, &table_ident).await?
        && !paths.is_empty()
    {
        return Ok(());
    }

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

// Star schema for join pushdown. Disjoint column prefixes (C_* vs O_*) let the
// adapter's disjoint-column guard render a broadcast join; the shared `id` on
// events/labels would trip it. fact_orders spans two files so the fact-side fan-out
// is observable; dim_customer's single smaller file makes it the broadcast side.

pub const E2E_DIM_TABLE: &str = "dim_customer";
pub const E2E_FACT_TABLE: &str = "fact_orders";
pub const DIM_CUSTOMER_ROWS: usize = 5;
pub const FACT_ORDERS_ROWS: usize = 10;

pub fn order_custkey(order_key: usize) -> i64 {
    (((order_key - 1) % DIM_CUSTOMER_ROWS) + 1) as i64
}

pub fn order_date_days(order_key: usize) -> i32 {
    BASE_DATE + (order_key as i32 - 1)
}

/// DataFusion's full-scale text and Exasol's trimmed form differ in length (#223).
pub const O_TOTALPRICE_PS: (u8, i8) = (10, 2);

/// Every value ends in `"00"`, so the trimmed form is 3 characters shorter, and the
/// integer digit count grows with the key so a `LENGTH(...)` filter discriminates rows.
pub fn order_totalprice_unscaled(order_key: usize) -> i64 {
    const VALUES: [i64; FACT_ORDERS_ROWS] = [
        100, 800, 2700, 6400, 12500, 21600, 291200, 512000, 729000, 1000000,
    ];
    VALUES[order_key - 1]
}

pub async fn seed_star_schema_with_auth(
    catalog_url: &str,
    warehouse: &str,
    auth: SeedCatalogAuth,
) -> Result<()> {
    let catalog =
        build_seed_catalog_with_auth(catalog_url, warehouse, "lakehouse-e2e-seed-star", auth)
            .await?;
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

    let (tp_p, tp_s) = O_TOTALPRICE_PS;
    let fact_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "O_ORDERKEY", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "O_CUSTKEY", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(3, "O_ORDERDATE", Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::required(
                4,
                "O_TOTALPRICE",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: tp_p as u32,
                    scale: tp_s as u32,
                }),
            )
            .into(),
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

pub async fn seed_star_schema(catalog_url: &str, warehouse: &str) -> Result<()> {
    seed_star_schema_with_auth(catalog_url, warehouse, SeedCatalogAuth::default()).await
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
    let total_prices: Vec<i128> = (first_key..=last_key)
        .map(|k| order_totalprice_unscaled(k) as i128)
        .collect();
    let (tp_p, tp_s) = O_TOTALPRICE_PS;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("O_ORDERKEY", DataType::Int64, false),
        Field::new("O_CUSTKEY", DataType::Int64, false),
        Field::new("O_ORDERDATE", DataType::Date32, false),
        Field::new("O_TOTALPRICE", DataType::Decimal128(tp_p, tp_s), false),
    ]));

    let total_price_array = Decimal128Array::from(total_prices)
        .with_precision_and_scale(tp_p, tp_s)
        .expect("O_TOTALPRICE precision/scale is valid");

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(order_keys)),
            Arc::new(Int64Array::from(cust_keys)),
            Arc::new(Date32Array::from(dates)),
            Arc::new(total_price_array),
        ],
    )
    .expect("fact_orders RecordBatch construction is infallible")
}

// `fact_lineitem` (FK to orders and suppliers) and `dim_supplier` extend the star
// schema to 3- and 4-table join shapes for the N-scan fallback tests (#76).
// `L_RETURNFLAG` alternates R/N and `L_EXTENDEDPRICE` is numeric, for grouped
// scalar-over-aggregate join tests.

pub const E2E_LINEITEM_TABLE: &str = "fact_lineitem";
pub const E2E_SUPPLIER_TABLE: &str = "dim_supplier";
pub const LINES_PER_ORDER: usize = 2;
pub const LINEITEM_ROWS: usize = FACT_ORDERS_ROWS * LINES_PER_ORDER;
pub const SUPPLIER_ROWS: usize = 3;

pub fn line_suppkey(order_key: usize) -> i64 {
    (((order_key - 1) % SUPPLIER_ROWS) + 1) as i64
}

pub fn line_returnflag(row: usize) -> &'static str {
    if row % 2 == 1 { "R" } else { "N" }
}

/// Deterministic per row so the expected `AVG` can be computed independently in Rust.
pub fn line_extendedprice(row: usize) -> f64 {
    1000.0 + (row as f64) * 10.0
}

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
            NestedField::required(5, "L_RETURNFLAG", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(6, "L_EXTENDEDPRICE", Type::Primitive(PrimitiveType::Double))
                .into(),
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
    let return_flags: Vec<&'static str> = (first_row..=last_row).map(line_returnflag).collect();
    let extended_prices: Vec<f64> = (first_row..=last_row).map(line_extendedprice).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("L_ORDERKEY", DataType::Int64, false),
        Field::new("L_LINENUMBER", DataType::Int64, false),
        Field::new("L_SUPPKEY", DataType::Int64, false),
        Field::new("L_QUANTITY", DataType::Int64, false),
        Field::new("L_RETURNFLAG", DataType::Utf8, false),
        Field::new("L_EXTENDEDPRICE", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(order_keys)),
            Arc::new(Int64Array::from(line_numbers)),
            Arc::new(Int64Array::from(supp_keys)),
            Arc::new(Int64Array::from(quantities)),
            Arc::new(StringArray::from(return_flags)),
            Arc::new(Float64Array::from(extended_prices)),
        ],
    )
    .expect("fact_lineitem RecordBatch construction is infallible")
}

/// One data file per partition value with disjoint, contiguous id ranges, so per-file
/// min/max bounds are tight.
pub async fn seed_partitioned(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-regions").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let table_ident = TableIdent::new(ns.clone(), E2E_PART_TABLE.to_string());

    if let Some(paths) = existing_data_file_paths(&catalog, &table_ident).await?
        && !paths.is_empty()
    {
        return Ok(());
    }

    let iceberg_schema = regions_iceberg_schema()?;
    // The partition field needs an explicit field-id (partition ids start at 1000): an
    // unbound spec serializes `field-id: null`, which the REST catalog rejects.
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

async fn write_regions_and_commit<C: Catalog>(catalog: &C, table: Table) -> Result<()> {
    let ranges: [(&str, usize, usize); 3] = [
        (PART_VAL_NORTH, PART_NORTH_IDS.0, PART_NORTH_IDS.1),
        (PART_VAL_CENTRAL, PART_CENTRAL_IDS.0, PART_CENTRAL_IDS.1),
        (PART_VAL_SOUTH, PART_SOUTH_IDS.0, PART_SOUTH_IDS.1),
    ];

    let mut current_table = table;
    for (region, first_id, last_id) in ranges {
        write_one_partitioned_file(catalog, &current_table, first_id, last_id, region).await?;
        current_table = catalog
            .load_table(current_table.identifier())
            .await
            .context("reload regions table between partition appends")?;
    }
    Ok(())
}

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

// iceberg-rust 0.10 has no schema-evolution API, so the rename (#26) is applied via a
// raw REST `add-schema` + `set-current-schema` commit to the Java REST fixture.

pub const E2E_EVO_TABLE: &str = "evo";
/// Written before the rename (physical parquet column `score`).
pub const EVO_PRE_RENAME_IDS: (i64, i64) = (1, 5);
/// Written after the rename (physical parquet column `rating`).
pub const EVO_POST_RENAME_IDS: (i64, i64) = (6, 10);
pub const EVO_TOTAL_ROWS: usize = 10;
pub const EVO_OLD_COL: &str = "score";
pub const EVO_NEW_COL: &str = "rating";

/// A field-id reader must bind pre- and post-rename files to `rating` by field-id 2.
/// Not idempotent: drops and recreates `evo` so every run starts from a known state.
pub async fn seed_renamed_column(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-evo").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let ident = TableIdent::new(ns.clone(), E2E_EVO_TABLE.to_string());

    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for evo")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

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

    let (a0, a1) = EVO_PRE_RENAME_IDS;
    write_one_file_append(
        &catalog,
        &table,
        E2E_EVO_TABLE,
        [make_evo_batch(a0, a1, EVO_OLD_COL)],
    )
    .await
    .context("append evo file A (pre-rename)")?;

    // Rename field-id 2 `score` → `rating` via a raw REST catalog commit.
    let table = catalog
        .load_table(&ident)
        .await
        .context("reload evo before rename")?;
    let current_schema_id = table.metadata().current_schema_id();
    rest_replace_current_schema(
        catalog_url,
        E2E_NAMESPACE,
        E2E_EVO_TABLE,
        current_schema_id,
        evo_schema(current_schema_id + 1, EVO_NEW_COL)?,
    )
    .await
    .context("REST rename score -> rating")?;

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

/// Field-id 2 stays fixed regardless of `col2`: that stable id is the point of the repro.
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

// Iceberg projection rule (3): a field added after a data file was written reads as
// absent from that file and must return its `initial-default` (#27). ns-precision
// timestamps are not expressible in this catalog and are covered by unit tests only.

pub const EVO_INITDEF_TABLE: &str = "initdef";
pub const EVO_INITDEF_PRE_ADD_IDS: (i64, i64) = (1, 3);
pub const EVO_INITDEF_POST_ADD_IDS: (i64, i64) = (4, 6);
pub const EVO_INITDEF_TOTAL_ROWS: usize = 6;

/// Added REQUIRED-with-default: an absent required field returns its default, not an error.
pub const EVO_INITDEF_COL_BOOL: &str = "c_bool";
pub const EVO_INITDEF_COL_INT: &str = "c_int";
pub const EVO_INITDEF_COL_LONG: &str = "c_long";
pub const EVO_INITDEF_COL_FLOAT: &str = "c_float";
pub const EVO_INITDEF_COL_DOUBLE: &str = "c_double";
pub const EVO_INITDEF_COL_STRING: &str = "c_string";
pub const EVO_INITDEF_COL_DATE: &str = "c_date";
pub const EVO_INITDEF_COL_TS: &str = "c_ts";
pub const EVO_INITDEF_COL_DECIMAL: &str = "c_decimal";
pub const EVO_INITDEF_COL_TSTZ: &str = "c_tstz";

/// Greater than `i32::MAX`, so a Long/Int mix-up is caught.
const INITDEF_LONG_DEFAULT: i64 = 4_200_000_000;
const INITDEF_DECIMAL_DEFAULT_UNSCALED: i128 = 12_345;
const INITDEF_DECIMAL_REAL_UNSCALED: i128 = 67_890;
const INITDEF_REAL_DATE_DAYS: i32 = BASE_DATE + 182;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
/// Noon, not midnight, so a session-timezone offset cannot roll the asserted date.
const INITDEF_DEFAULT_TS_MICROS: i64 = BASE_TS_MICROS + 12 * MICROS_PER_HOUR;
const INITDEF_REAL_TS_MICROS: i64 = BASE_TS_MICROS + 182 * MICROS_PER_DAY + 12 * MICROS_PER_HOUR;
/// Deliberately the same UTC instant as `c_ts`'s default.
const INITDEF_DEFAULT_TSTZ_MICROS: i64 = BASE_TS_MICROS + 12 * MICROS_PER_HOUR;
const INITDEF_REAL_TSTZ_MICROS: i64 = BASE_TS_MICROS + 182 * MICROS_PER_DAY + 12 * MICROS_PER_HOUR;

pub enum ExpectedValue {
    Bool(bool),
    Num(f64),
    Text(&'static str),
    /// Matched by the leading `YYYY-MM-DD`, robust to fractional-second and session-timezone
    /// rendering differences.
    DatePrefix(&'static str),
}

impl ExpectedValue {
    pub fn matches(&self, actual: &serde_json::Value) -> bool {
        match self {
            ExpectedValue::Bool(b) => {
                actual.as_bool() == Some(*b)
                    || actual.as_i64().map(|i| (i != 0) == *b).unwrap_or(false)
                    || actual
                        .as_str()
                        .map(|s| (s.eq_ignore_ascii_case("true") || s == "1") == *b)
                        .unwrap_or(false)
            }
            ExpectedValue::Num(n) => actual
                .as_f64()
                .or_else(|| actual.as_str().and_then(|s| s.parse::<f64>().ok()))
                .map(|g| (g - n).abs() < 1e-6)
                .unwrap_or(false),
            ExpectedValue::Text(t) => actual.as_str() == Some(*t),
            ExpectedValue::DatePrefix(p) => {
                actual.as_str().map(|s| s.starts_with(p)).unwrap_or(false)
            }
        }
    }
}

pub struct InitDefColumn {
    pub name: &'static str,
    pub required: bool,
    pub default: ExpectedValue,
    pub real: ExpectedValue,
}

/// `c_bool` is REQUIRED-with-default; the rest are NULLABLE-with-default.
pub fn initdef_columns() -> Vec<InitDefColumn> {
    vec![
        InitDefColumn {
            name: EVO_INITDEF_COL_BOOL,
            required: true,
            default: ExpectedValue::Bool(true),
            real: ExpectedValue::Bool(false),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_INT,
            required: false,
            default: ExpectedValue::Num(42.0),
            real: ExpectedValue::Num(7.0),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_LONG,
            required: false,
            default: ExpectedValue::Num(INITDEF_LONG_DEFAULT as f64),
            real: ExpectedValue::Num(99.0),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_FLOAT,
            required: false,
            default: ExpectedValue::Num(1.5),
            real: ExpectedValue::Num(2.5),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_DOUBLE,
            required: false,
            default: ExpectedValue::Num(2.5),
            real: ExpectedValue::Num(9.75),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_STRING,
            required: false,
            default: ExpectedValue::Text("dflt"),
            real: ExpectedValue::Text("realv"),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_DATE,
            required: false,
            default: ExpectedValue::DatePrefix("2024-01-01"),
            real: ExpectedValue::DatePrefix("2024-07-01"),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_TS,
            required: false,
            default: ExpectedValue::DatePrefix("2024-01-01"),
            real: ExpectedValue::DatePrefix("2024-07-01"),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_DECIMAL,
            required: false,
            default: ExpectedValue::Num(123.45),
            real: ExpectedValue::Num(678.90),
        },
        InitDefColumn {
            name: EVO_INITDEF_COL_TSTZ,
            required: false,
            default: ExpectedValue::DatePrefix("2024-01-01"),
            real: ExpectedValue::DatePrefix("2024-07-01"),
        },
    ]
}

/// Not idempotent: drops and recreates `initdef` so every run starts clean.
pub async fn seed_added_columns_initial_default(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-initdef").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let ident = TableIdent::new(ns.clone(), EVO_INITDEF_TABLE.to_string());

    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for initdef")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    if catalog
        .table_exists(&ident)
        .await
        .context("check initdef table exists")?
    {
        catalog
            .drop_table(&ident)
            .await
            .context("drop existing initdef table")?;
    }

    // Iceberg requires v3 for non-null `initial-default` values. The REST catalog takes the
    // format version only from the `format-version` property, so it is set there and
    // asserted below.
    let partition_spec = UnboundPartitionSpec::builder().with_spec_id(0).build();
    let creation = TableCreation::builder()
        .name(EVO_INITDEF_TABLE.to_string())
        .schema(initdef_pre_add_schema()?)
        .partition_spec(partition_spec)
        .properties(HashMap::from([(
            "format-version".to_string(),
            "3".to_string(),
        )]))
        .format_version(FormatVersion::V3)
        .build();
    let table = catalog
        .create_table(&ns, creation)
        .await
        .context("create initdef table")?;
    assert_eq!(
        table.metadata().format_version(),
        FormatVersion::V3,
        "initdef table was not created at format-version 3 (non-null initial-default \
         requires v3); the REST catalog ignored the format-version request"
    );

    let (a0, a1) = EVO_INITDEF_PRE_ADD_IDS;
    write_one_file_append(
        &catalog,
        &table,
        EVO_INITDEF_TABLE,
        [make_initdef_id_only_batch(a0, a1)],
    )
    .await
    .context("append initdef file A (pre-add)")?;

    let table = catalog
        .load_table(&ident)
        .await
        .context("reload initdef before add-columns")?;
    let current_schema_id = table.metadata().current_schema_id();
    rest_replace_current_schema(
        catalog_url,
        E2E_NAMESPACE,
        EVO_INITDEF_TABLE,
        current_schema_id,
        initdef_post_add_schema(current_schema_id + 1)?,
    )
    .await
    .context("REST add-columns commit for initdef")?;

    let table = catalog
        .load_table(&ident)
        .await
        .context("reload initdef after add-columns")?;
    assert_eq!(
        table
            .metadata()
            .current_schema()
            .field_by_id(2)
            .map(|f| f.name.as_str()),
        Some(EVO_INITDEF_COL_BOOL),
        "REST add-columns did not take effect: field-id 2 is not '{EVO_INITDEF_COL_BOOL}'"
    );
    let (b0, b1) = EVO_INITDEF_POST_ADD_IDS;
    write_one_file_append(
        &catalog,
        &table,
        EVO_INITDEF_TABLE,
        [make_initdef_full_batch(b0, b1)],
    )
    .await
    .context("append initdef file B (post-add)")?;

    Ok(())
}

fn initdef_pre_add_schema() -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .context("build initdef pre-add Iceberg schema")
}

/// `c_bool` is REQUIRED-with-default; the rest are NULLABLE-with-default.
fn initdef_post_add_schema(schema_id: i32) -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(schema_id)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(
                2,
                EVO_INITDEF_COL_BOOL,
                Type::Primitive(PrimitiveType::Boolean),
            )
            .with_initial_default(Literal::bool(true))
            .into(),
            NestedField::optional(3, EVO_INITDEF_COL_INT, Type::Primitive(PrimitiveType::Int))
                .with_initial_default(Literal::int(42))
                .into(),
            NestedField::optional(
                4,
                EVO_INITDEF_COL_LONG,
                Type::Primitive(PrimitiveType::Long),
            )
            .with_initial_default(Literal::long(INITDEF_LONG_DEFAULT))
            .into(),
            NestedField::optional(
                5,
                EVO_INITDEF_COL_FLOAT,
                Type::Primitive(PrimitiveType::Float),
            )
            .with_initial_default(Literal::float(1.5f32))
            .into(),
            NestedField::optional(
                6,
                EVO_INITDEF_COL_DOUBLE,
                Type::Primitive(PrimitiveType::Double),
            )
            .with_initial_default(Literal::double(2.5f64))
            .into(),
            NestedField::optional(
                7,
                EVO_INITDEF_COL_STRING,
                Type::Primitive(PrimitiveType::String),
            )
            .with_initial_default(Literal::string("dflt"))
            .into(),
            NestedField::optional(
                8,
                EVO_INITDEF_COL_DATE,
                Type::Primitive(PrimitiveType::Date),
            )
            .with_initial_default(Literal::date(BASE_DATE))
            .into(),
            NestedField::optional(
                9,
                EVO_INITDEF_COL_TS,
                Type::Primitive(PrimitiveType::Timestamp),
            )
            .with_initial_default(Literal::timestamp(INITDEF_DEFAULT_TS_MICROS))
            .into(),
            NestedField::optional(
                10,
                EVO_INITDEF_COL_DECIMAL,
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            )
            .with_initial_default(Literal::decimal(INITDEF_DECIMAL_DEFAULT_UNSCALED))
            .into(),
            NestedField::optional(
                11,
                EVO_INITDEF_COL_TSTZ,
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .with_initial_default(Literal::timestamptz(INITDEF_DEFAULT_TSTZ_MICROS))
            .into(),
        ])
        .build()
        .context("build initdef post-add Iceberg schema")
}

fn make_initdef_id_only_batch(first_id: i64, last_id: i64) -> RecordBatch {
    let ids: Vec<i64> = (first_id..=last_id).collect();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))])
        .expect("initdef id-only RecordBatch construction is infallible")
}

/// Arrow types and nullability must mirror the post-add Iceberg schema exactly, or the
/// Iceberg parquet writer rejects the batch.
fn make_initdef_full_batch(first_id: i64, last_id: i64) -> RecordBatch {
    let ids: Vec<i64> = (first_id..=last_id).collect();
    let n = ids.len();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(EVO_INITDEF_COL_BOOL, DataType::Boolean, false),
        Field::new(EVO_INITDEF_COL_INT, DataType::Int32, true),
        Field::new(EVO_INITDEF_COL_LONG, DataType::Int64, true),
        Field::new(EVO_INITDEF_COL_FLOAT, DataType::Float32, true),
        Field::new(EVO_INITDEF_COL_DOUBLE, DataType::Float64, true),
        Field::new(EVO_INITDEF_COL_STRING, DataType::Utf8, true),
        Field::new(EVO_INITDEF_COL_DATE, DataType::Date32, true),
        Field::new(
            EVO_INITDEF_COL_TS,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(EVO_INITDEF_COL_DECIMAL, DataType::Decimal128(9, 2), true),
        // Must be "+00:00" (iceberg-rust's `UTC_TIME_ZONE`), not "UTC": the parquet writer
        // validates the batch by strict DataType equality against the Iceberg schema.
        Field::new(
            EVO_INITDEF_COL_TSTZ,
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        ),
    ]));

    let decimals = Decimal128Array::from(vec![INITDEF_DECIMAL_REAL_UNSCALED; n])
        .with_precision_and_scale(9, 2)
        .expect("initdef decimal precision/scale is valid");

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(BooleanArray::from(vec![false; n])),
            Arc::new(Int32Array::from(vec![7i32; n])),
            Arc::new(Int64Array::from(vec![99i64; n])),
            Arc::new(Float32Array::from(vec![2.5f32; n])),
            Arc::new(Float64Array::from(vec![9.75f64; n])),
            Arc::new(StringArray::from(vec!["realv"; n])),
            Arc::new(Date32Array::from(vec![INITDEF_REAL_DATE_DAYS; n])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                INITDEF_REAL_TS_MICROS;
                n
            ])),
            Arc::new(decimals),
            Arc::new(
                TimestampMicrosecondArray::from(vec![INITDEF_REAL_TSTZ_MICROS; n])
                    .with_timezone("+00:00"),
            ),
        ],
    )
    .expect("initdef full RecordBatch construction is infallible")
}

// Kept out of `seed_events` so other E2E binaries don't pay the extra seeding cost.

pub const E2E_DISTINCT_TABLE: &str = "distinct_probe";
pub const DISTINCT_CATEGORY_COL: &str = "category";
pub const DISTINCT_REGION_COL: &str = "region";
pub const DISTINCT_COMMENT_COL: &str = "comment";
/// Two data files, so `COUNT(DISTINCT)` pushdown must merge per-shard sets.
pub const DISTINCT_PROBE_TOTAL_ROWS: usize = 20;
/// "A" appears in both shards, proving the merge dedupes rather than sums. 7 rows have
/// a NULL category, which must not be counted.
pub const DISTINCT_CATEGORY_COUNT: i64 = 3;
pub const DISTINCT_REGION_COUNT: i64 = 4;
/// Comment length equals id, so the sum is 1 + 2 + ... + 20.
pub const DISTINCT_COMMENT_LENGTH_SUM: i64 = 210;

/// Single-shard high-cardinality `COUNT(DISTINCT)` regression probe (#146).
pub const E2E_HIGH_CARD_TABLE: &str = "high_card_probe";
pub const HIGH_CARD_COL: &str = "token";
/// One data file of unique 100-byte tokens: a ~3 MB shard-local distinct set, well
/// above the old per-shard budget from #146.
pub const HIGH_CARD_ROWS: usize = 30_000;

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

/// "A" is the value shared across both shards.
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

/// Zero-padded so the shard-local distinct set's byte size stays deterministic.
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

// Typed COUNT(DISTINCT) probe: one column per reachable Exasol type across two data
// files. Every column mixes in NULLs and repeats a value in both files, so a fan-out
// that summed per-shard counts would overcount. Expected counts are computed from the
// same arrays that build the batches.
//
// No CHAR column: no Iceberg/Arrow type maps to Exasol CHAR, so CHAR is covered only as
// a `CAST(... AS CHAR(n))` expression. `c_ts` values differ only in milliseconds,
// since a TIMESTAMP(3) column does not preserve sub-millisecond distinctions.

pub const E2E_TYPED_TABLE: &str = "typed_distinct_probe";

pub const TYPED_COL_DECIMAL_A: &str = "c_decimal_a";
pub const TYPED_COL_DECIMAL_B: &str = "c_decimal_b";
pub const TYPED_COL_DOUBLE: &str = "c_double";
/// Mixed case, so `UPPER(...)` folds some values together.
pub const TYPED_COL_VARCHAR: &str = "c_varchar";
pub const TYPED_COL_DATE: &str = "c_date";
/// Values differ only in the millisecond fraction.
pub const TYPED_COL_TS: &str = "c_ts";
pub const TYPED_COL_BOOL: &str = "c_bool";
pub const TYPED_COL_PRICE: &str = "c_price";
pub const TYPED_COL_QTY: &str = "c_qty";

pub const TYPED_TABLE_TOTAL_ROWS: usize = 12;
const TYPED_FILE_SPLIT: usize = 6;

const TYPED_DECIMAL_A_PS: (u8, i8) = (9, 2);
const TYPED_DECIMAL_B_PS: (u8, i8) = (20, 4);

/// Single source of truth for both the Arrow batches and the expected distinct counts.
/// Decimals are unscaled (`c_decimal_a` scale 2, `c_decimal_b` scale 4).
struct TypedProbe {
    ids: Vec<i64>,
    decimal_a: Vec<Option<i128>>,
    decimal_b: Vec<Option<i128>>,
    double: Vec<Option<f64>>,
    varchar: Vec<Option<&'static str>>,
    date_days: Vec<Option<i32>>,
    ts_micros: Vec<Option<i64>>,
    boolean: Vec<Option<bool>>,
    price: Vec<Option<f64>>,
    qty: Vec<Option<i64>>,
}

fn typed_probe() -> TypedProbe {
    let ts = |ms: i64| BASE_TS_MICROS + ms * 1_000;
    let day = |off: i32| BASE_DATE + off;
    TypedProbe {
        ids: (1..=TYPED_TABLE_TOTAL_ROWS as i64).collect(),
        // scale 2 unscaled: 10.50, 20.25, NULL, 30.00, 10.50, 40.99 | 10.50, 50.00, 20.25, NULL, 60.00, 30.00
        decimal_a: vec![
            Some(1050),
            Some(2025),
            None,
            Some(3000),
            Some(1050),
            Some(4099),
            Some(1050),
            Some(5000),
            Some(2025),
            None,
            Some(6000),
            Some(3000),
        ],
        // scale 4 unscaled: 100000.0001, 200000.0002, NULL, 300000.0003, 100000.0001, 400000.0004 | ...
        decimal_b: vec![
            Some(1_000_000_001),
            Some(2_000_000_002),
            None,
            Some(3_000_000_003),
            Some(1_000_000_001),
            Some(4_000_000_004),
            Some(1_000_000_001),
            Some(5_000_000_005),
            Some(2_000_000_002),
            None,
            Some(6_000_000_006),
            Some(3_000_000_003),
        ],
        double: vec![
            Some(0.5),
            Some(1.5),
            None,
            Some(2.5),
            Some(0.5),
            Some(3.5),
            Some(0.5),
            Some(4.5),
            Some(1.5),
            None,
            Some(5.5),
            Some(2.5),
        ],
        // Raw distinct = 8, UPPER-folded distinct = 5; "aa"/"AA"/"Aa" and "bb"/"BB" fold
        // across the shard boundary.
        varchar: vec![
            Some("aa"),
            Some("AA"),
            None,
            Some("bb"),
            Some("aa"),
            Some("cc"),
            Some("Aa"),
            Some("dd"),
            Some("BB"),
            None,
            Some("ee"),
            Some("cc"),
        ],
        date_days: vec![
            Some(day(0)),
            Some(day(1)),
            None,
            Some(day(2)),
            Some(day(0)),
            Some(day(3)),
            Some(day(0)),
            Some(day(4)),
            Some(day(1)),
            None,
            Some(day(5)),
            Some(day(2)),
        ],
        ts_micros: vec![
            Some(ts(100)),
            Some(ts(200)),
            None,
            Some(ts(300)),
            Some(ts(100)),
            Some(ts(400)),
            Some(ts(100)),
            Some(ts(500)),
            Some(ts(200)),
            None,
            Some(ts(600)),
            Some(ts(300)),
        ],
        boolean: vec![
            Some(true),
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true),
        ],
        // c_price * c_qty products: 6,6,NULL,4,6,10 | 12,12,6,8,NULL,20 → distinct = 6.
        price: vec![
            Some(2.0),
            Some(3.0),
            None,
            Some(4.0),
            Some(2.0),
            Some(5.0),
            Some(2.0),
            Some(3.0),
            Some(6.0),
            Some(4.0),
            None,
            Some(5.0),
        ],
        qty: vec![
            Some(3),
            Some(2),
            Some(5),
            Some(1),
            Some(3),
            Some(2),
            Some(6),
            Some(4),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
        ],
    }
}

/// Bitwise equality matches SQL `DISTINCT` here because all seeded values are positive
/// and finite.
fn distinct_f64(values: impl Iterator<Item = Option<f64>>) -> i64 {
    let set: std::collections::HashSet<u64> = values.flatten().map(f64::to_bits).collect();
    set.len() as i64
}

fn distinct_hashable<T: std::hash::Hash + Eq>(values: impl Iterator<Item = Option<T>>) -> i64 {
    let set: std::collections::HashSet<T> = values.flatten().collect();
    set.len() as i64
}

pub fn typed_decimal_a_distinct() -> i64 {
    distinct_hashable(typed_probe().decimal_a.into_iter())
}
pub fn typed_decimal_b_distinct() -> i64 {
    distinct_hashable(typed_probe().decimal_b.into_iter())
}
pub fn typed_double_distinct() -> i64 {
    distinct_f64(typed_probe().double.into_iter())
}
pub fn typed_varchar_distinct() -> i64 {
    distinct_hashable(typed_probe().varchar.into_iter())
}
pub fn typed_varchar_upper_distinct() -> i64 {
    distinct_hashable(
        typed_probe()
            .varchar
            .into_iter()
            .map(|v| v.map(str::to_uppercase)),
    )
}
pub fn typed_date_distinct() -> i64 {
    distinct_hashable(typed_probe().date_days.into_iter())
}
pub fn typed_ts_distinct() -> i64 {
    distinct_hashable(typed_probe().ts_micros.into_iter())
}
pub fn typed_bool_distinct() -> i64 {
    distinct_hashable(typed_probe().boolean.into_iter())
}
/// Equals the raw distinct count: seeded values have no trailing spaces, so CHAR
/// padding is injective over them.
pub fn typed_varchar_char_distinct() -> i64 {
    typed_varchar_distinct()
}
pub fn typed_product_distinct() -> i64 {
    let probe = typed_probe();
    distinct_f64(
        probe
            .price
            .into_iter()
            .zip(probe.qty)
            .map(|(p, q)| match (p, q) {
                (Some(p), Some(q)) => Some(p * q as f64),
                _ => None,
            }),
    )
}
pub fn typed_ts_case_distinct() -> i64 {
    let probe = typed_probe();
    distinct_hashable(
        probe
            .ts_micros
            .into_iter()
            .zip(probe.boolean)
            .map(|(ts, b)| if b == Some(true) { ts } else { None }),
    )
}

/// `STDDEV` is `STDDEV_SAMP`. Oracle computed independently of the pushdown path.
fn avg_and_stddev_samp(values: impl Iterator<Item = Option<f64>>) -> (f64, f64) {
    let xs: Vec<f64> = values.flatten().collect();
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let variance = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    (mean, variance.sqrt())
}

/// Exercises the partial-aggregate mismatch over a non-`DOUBLE` column (#399).
pub fn typed_id_avg_stddev() -> (f64, f64) {
    avg_and_stddev_samp(typed_probe().ids.into_iter().map(|id| Some(id as f64)))
}

pub fn typed_decimal_a_avg_stddev() -> (f64, f64) {
    let (_, scale) = TYPED_DECIMAL_A_PS;
    let divisor = 10f64.powi(scale as i32);
    avg_and_stddev_samp(
        typed_probe()
            .decimal_a
            .into_iter()
            .map(|v| v.map(|unscaled| unscaled as f64 / divisor)),
    )
}

pub fn typed_decimal_b_avg_stddev() -> (f64, f64) {
    let (_, scale) = TYPED_DECIMAL_B_PS;
    let divisor = 10f64.powi(scale as i32);
    avg_and_stddev_samp(
        typed_probe()
            .decimal_b
            .into_iter()
            .map(|v| v.map(|unscaled| unscaled as f64 / divisor)),
    )
}

pub async fn seed_typed_distinct_probe(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-typed").await?;
    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for typed_distinct_probe")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let (da_p, da_s) = TYPED_DECIMAL_A_PS;
    let (db_p, db_s) = TYPED_DECIMAL_B_PS;
    let iceberg_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(
                2,
                TYPED_COL_DECIMAL_A,
                Type::Primitive(PrimitiveType::Decimal {
                    precision: da_p as u32,
                    scale: da_s as u32,
                }),
            )
            .into(),
            NestedField::optional(
                3,
                TYPED_COL_DECIMAL_B,
                Type::Primitive(PrimitiveType::Decimal {
                    precision: db_p as u32,
                    scale: db_s as u32,
                }),
            )
            .into(),
            NestedField::optional(4, TYPED_COL_DOUBLE, Type::Primitive(PrimitiveType::Double))
                .into(),
            NestedField::optional(5, TYPED_COL_VARCHAR, Type::Primitive(PrimitiveType::String))
                .into(),
            NestedField::optional(6, TYPED_COL_DATE, Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::optional(7, TYPED_COL_TS, Type::Primitive(PrimitiveType::Timestamp))
                .into(),
            NestedField::optional(8, TYPED_COL_BOOL, Type::Primitive(PrimitiveType::Boolean))
                .into(),
            NestedField::optional(9, TYPED_COL_PRICE, Type::Primitive(PrimitiveType::Double))
                .into(),
            NestedField::optional(10, TYPED_COL_QTY, Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .context("build typed_distinct_probe Iceberg schema")?;

    let probe = typed_probe();
    let file1 = vec![make_typed_probe_batch(&probe, 0, TYPED_FILE_SPLIT)];
    let file2 = vec![make_typed_probe_batch(
        &probe,
        TYPED_FILE_SPLIT,
        TYPED_TABLE_TOTAL_ROWS,
    )];
    create_and_append_files(
        &catalog,
        E2E_NAMESPACE,
        E2E_TYPED_TABLE,
        iceberg_schema,
        vec![file1, file2],
    )
    .await
    .context("seed typed_distinct_probe table")?;
    Ok(())
}

/// Arrow types must mirror the Iceberg schema exactly or the parquet writer rejects the batch.
fn make_typed_probe_batch(probe: &TypedProbe, start: usize, end: usize) -> RecordBatch {
    let (da_p, da_s) = TYPED_DECIMAL_A_PS;
    let (db_p, db_s) = TYPED_DECIMAL_B_PS;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(TYPED_COL_DECIMAL_A, DataType::Decimal128(da_p, da_s), true),
        Field::new(TYPED_COL_DECIMAL_B, DataType::Decimal128(db_p, db_s), true),
        Field::new(TYPED_COL_DOUBLE, DataType::Float64, true),
        Field::new(TYPED_COL_VARCHAR, DataType::Utf8, true),
        Field::new(TYPED_COL_DATE, DataType::Date32, true),
        Field::new(
            TYPED_COL_TS,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(TYPED_COL_BOOL, DataType::Boolean, true),
        Field::new(TYPED_COL_PRICE, DataType::Float64, true),
        Field::new(TYPED_COL_QTY, DataType::Int64, true),
    ]));

    let decimal_a = Decimal128Array::from(probe.decimal_a[start..end].to_vec())
        .with_precision_and_scale(da_p, da_s)
        .expect("c_decimal_a precision/scale is valid");
    let decimal_b = Decimal128Array::from(probe.decimal_b[start..end].to_vec())
        .with_precision_and_scale(db_p, db_s)
        .expect("c_decimal_b precision/scale is valid");

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(probe.ids[start..end].to_vec())),
            Arc::new(decimal_a),
            Arc::new(decimal_b),
            Arc::new(Float64Array::from(probe.double[start..end].to_vec())),
            Arc::new(StringArray::from(probe.varchar[start..end].to_vec())),
            Arc::new(Date32Array::from(probe.date_days[start..end].to_vec())),
            Arc::new(TimestampMicrosecondArray::from(
                probe.ts_micros[start..end].to_vec(),
            )),
            Arc::new(BooleanArray::from(probe.boolean[start..end].to_vec())),
            Arc::new(Float64Array::from(probe.price[start..end].to_vec())),
            Arc::new(Int64Array::from(probe.qty[start..end].to_vec())),
        ],
    )
    .expect("typed_distinct_probe RecordBatch construction is infallible")
}

/// `add-schema` + `set-current-schema` (`schema-id: -1` = the just-added schema), guarded
/// by `assert-current-schema-id`. POSTed directly because iceberg-rust exposes no public
/// API to build a `TableCommit`.
pub async fn rest_replace_current_schema(
    catalog_url: &str,
    namespace: &str,
    table_name: &str,
    current_schema_id: i32,
    new_schema: IcebergSchema,
) -> Result<()> {
    let base = catalog_url.trim_end_matches('/');
    let client = reqwest::Client::new();

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
        TableUpdate::AddSchema { schema: new_schema },
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
        .context("POST schema-replace commit to REST catalog")?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("REST schema-replace commit failed ({status}): {text}");
    }
    Ok(())
}

// CHAR-padding probe (#192). `events.name` values are all 8 characters, so this table
// carries a trailing-space pair (a correct CHAR(n) pad merges them) and a 25-character
// value (a `CHAR(20)` cast must still raise Exasol's 22001 truncation error). Kept out
// of `events`/`labels`/`regions`, whose row counts existing tests assert.

pub const E2E_CHAR_PAD_TABLE: &str = "char_pad_probe";
pub const CHAR_PAD_COL: &str = "val";
/// Once padded to a common `CHAR(n)` width it equals `CHAR_PAD_SHORT_TRAILING_SPACE`,
/// so a correct pad merges the two into one group.
pub const CHAR_PAD_SHORT: &str = "ab";
/// Trailing spaces present in the source data, distinct from the pad the fix adds.
pub const CHAR_PAD_SHORT_TRAILING_SPACE: &str = "ab   ";
pub const CHAR_PAD_OTHER: &str = "cd";
/// 25 characters: over-length for `CHAR(20)`, but fits `CHAR(30)`.
pub const CHAR_PAD_OVER_LENGTH: &str = "over-length-value-abcdefg";
pub const CHAR_PAD_TOTAL_ROWS: usize = 4;

pub async fn seed_char_pad_table(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-char-pad").await?;
    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for CHAR-padding probe")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let iceberg_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, CHAR_PAD_COL, Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .context("build CHAR-padding probe Iceberg schema")?;

    create_and_append_files(
        &catalog,
        E2E_NAMESPACE,
        E2E_CHAR_PAD_TABLE,
        iceberg_schema,
        vec![vec![make_char_pad_batch()]],
    )
    .await
    .context("seed CHAR-padding probe table")?;
    Ok(())
}

fn make_char_pad_batch() -> RecordBatch {
    let values = StringArray::from(vec![
        CHAR_PAD_SHORT,
        CHAR_PAD_SHORT_TRAILING_SPACE,
        CHAR_PAD_OTHER,
        CHAR_PAD_OVER_LENGTH,
    ]);
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        CHAR_PAD_COL,
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(values)])
        .expect("CHAR-padding probe RecordBatch construction is infallible")
}

/// Its own namespace, so this table never enters another suite's table enumeration.
pub const E2E_NONASCII_NAMESPACE: &str = "e2e_nonascii";
/// Both the table name and the column name under test.
pub const E2E_NONASCII_TABLE: &str = "straße";
pub const NONASCII_COL: &str = E2E_NONASCII_TABLE;

/// Prefixed so a `LIKE` predicate selects a proper subset.
pub const NONASCII_VALUES: [&str; 4] = ["alpha-1", "alpha-2", "beta-1", "beta-2"];
pub const NONASCII_TOTAL_ROWS: i64 = NONASCII_VALUES.len() as i64;
pub const NONASCII_LIKE_PATTERN: &str = "alpha%";
pub const NONASCII_LIKE_MATCH_COUNT: i64 = 2;

pub async fn seed_non_ascii_identifier(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog = build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-nonascii").await?;
    let ns = NamespaceIdent::new(E2E_NONASCII_NAMESPACE.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for straße table")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let iceberg_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, NONASCII_COL, Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .context("build straße Iceberg schema")?;

    create_and_append(
        &catalog,
        E2E_NONASCII_NAMESPACE,
        E2E_NONASCII_TABLE,
        iceberg_schema,
        vec![make_non_ascii_identifier_batch()],
    )
    .await
    .context("seed straße table")?;
    Ok(())
}

fn make_non_ascii_identifier_batch() -> RecordBatch {
    let ids: Vec<i64> = (1..=NONASCII_VALUES.len() as i64).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(NONASCII_COL, DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(NONASCII_VALUES.to_vec())),
        ],
    )
    .expect("straße RecordBatch construction is infallible")
}

pub const E2E_COMPLEX_TABLE: &str = "complex_probe";

pub const COMPLEX_ROW_POPULATED: i64 = 1;
/// Every nested column is SQL NULL as a whole cell, not a member.
pub const COMPLEX_ROW_NULL: i64 = 2;
/// List/map columns are empty collections and `addr` has a NULL `city` member.
pub const COMPLEX_ROW_EMPTY: i64 = 3;
/// A second, distinct populated value per column, so predicates, grouping, ordering,
/// and `COUNT(DISTINCT)` have more than one non-null value to discriminate.
pub const COMPLEX_ROW_ALT: i64 = 4;
pub const COMPLEX_TOTAL_ROWS: usize = 4;

/// `iceberg-rest-fixture` assigns fresh field-ids on `create_table`, and
/// `overlay_iceberg_field_ids` repairs only top-level ids, so the batch is built from
/// the created table's schema to get the nested field-ids right.
pub async fn seed_complex_types_probe(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog =
        build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-complex-types").await?;

    let ns = NamespaceIdent::new(E2E_NAMESPACE.to_string());
    let table_ident = TableIdent::new(ns.clone(), E2E_COMPLEX_TABLE.to_string());

    if let Some(paths) = existing_data_file_paths(&catalog, &table_ident).await?
        && !paths.is_empty()
    {
        return Ok(());
    }

    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let iceberg_schema = complex_types_iceberg_schema()?;
    let partition_spec = UnboundPartitionSpec::builder().with_spec_id(0).build();
    let creation = TableCreation::builder()
        .name(E2E_COMPLEX_TABLE.to_string())
        .schema(iceberg_schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    let table = match catalog.create_table(&ns, creation).await {
        Ok(t) => t,
        Err(_) => catalog
            .load_table(&table_ident)
            .await
            .context("load existing complex-types table after create failed")?,
    };

    let existing = collect_current_snapshot_paths(&table).await?;
    if !existing.is_empty() {
        return Ok(());
    }

    write_complex_types_and_commit(&catalog, table).await
}

/// As authored; `create_table` assigns its own field-ids.
fn complex_types_iceberg_schema() -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(
                2,
                "tags",
                Type::List(ListType::new(
                    NestedField::list_element(3, Type::Primitive(PrimitiveType::String), false)
                        .into(),
                )),
            )
            .into(),
            NestedField::optional(
                4,
                "nums",
                Type::List(ListType::new(
                    NestedField::list_element(5, Type::Primitive(PrimitiveType::Int), false).into(),
                )),
            )
            .into(),
            NestedField::optional(
                6,
                "addr",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(7, "street", Type::Primitive(PrimitiveType::String))
                        .into(),
                    NestedField::optional(8, "city", Type::Primitive(PrimitiveType::String)).into(),
                ])),
            )
            .into(),
            NestedField::optional(
                9,
                "attrs",
                Type::Map(MapType::optional(
                    10,
                    Type::Primitive(PrimitiveType::String),
                    11,
                    Type::Primitive(PrimitiveType::String),
                )),
            )
            .into(),
            NestedField::optional(
                12,
                "int_map",
                Type::Map(MapType::optional(
                    13,
                    Type::Primitive(PrimitiveType::Int),
                    14,
                    Type::Primitive(PrimitiveType::String),
                )),
            )
            .into(),
            NestedField::optional(
                15,
                "items",
                Type::List(ListType::new(
                    NestedField::list_element(
                        16,
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(17, "a", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                        false,
                    )
                    .into(),
                )),
            )
            .into(),
        ])
        .build()
        .context("build complex-types Iceberg schema")
}

async fn write_complex_types_and_commit<C: Catalog>(catalog: &C, table: Table) -> Result<()> {
    let iceberg_schema = table.metadata().current_schema().clone();
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(&iceberg_schema)
            .context("derive Arrow schema for complex-types batch")?,
    );
    let batch = complex_types_batch(arrow_schema)?;

    let file_io = table.file_io().clone();
    let table_location = table.metadata().location().to_string();
    let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

    let location_gen = FlatLocationGenerator {
        base: table_location,
    };
    let file_name_gen = DefaultFileNameGenerator::new(
        E2E_COMPLEX_TABLE.to_string(),
        Some(uuid_suffix()),
        DataFileFormat::Parquet,
    );
    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_schema.clone());
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        file_io,
        location_gen,
        file_name_gen,
    );
    let partition_key =
        iceberg::spec::PartitionKey::new(partition_spec, iceberg_schema.clone(), Struct::empty());

    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(Some(partition_key))
        .await
        .context("build complex-types data file writer")?;
    writer
        .write(batch)
        .await
        .context("write complex-types Arrow batch")?;
    let data_files = writer
        .close()
        .await
        .context("close complex-types data file writer")?;

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action
        .apply(tx)
        .context("apply complex-types fast-append action")?;
    tx.commit(catalog)
        .await
        .context("commit complex-types Iceberg snapshot")?;
    Ok(())
}

/// Decoded straight into the created table's Arrow schema, so nested fields already
/// carry the field-ids Iceberg assigned.
fn complex_types_batch(schema: Arc<ArrowSchema>) -> Result<RecordBatch> {
    let rows = vec![
        json!({
            "id": COMPLEX_ROW_POPULATED,
            "tags": ["hello", "world"],
            "nums": [1, 2, 3],
            "addr": {"street": "Main St", "city": "Berlin"},
            "attrs": {"a": "1", "b": "2"},
            "int_map": {"1": "one", "2": "two"},
            "items": [{"a": 1}, {"a": 2}],
        }),
        json!({
            "id": COMPLEX_ROW_NULL,
            "tags": null,
            "nums": null,
            "addr": null,
            "attrs": null,
            "int_map": null,
            "items": null,
        }),
        json!({
            "id": COMPLEX_ROW_EMPTY,
            "tags": [],
            "nums": [],
            "addr": {"street": "Empty Ave", "city": null},
            "attrs": {},
            "int_map": {},
            "items": [],
        }),
        json!({
            "id": COMPLEX_ROW_ALT,
            "tags": ["foo", "bar", "baz"],
            "nums": [9, 8],
            "addr": {"street": "Second St", "city": "Paris"},
            "attrs": {"x": "9"},
            "int_map": {"3": "three"},
            "items": [{"a": 3}],
        }),
    ];

    let mut decoder = ReaderBuilder::new(schema)
        .build_decoder()
        .context("build complex-types JSON decoder")?;
    decoder
        .serialize(&rows)
        .context("serialize complex-types rows")?;
    decoder
        .flush()
        .context("flush complex-types JSON decoder")?
        .context("complex-types JSON decoder produced no batch")
}

pub const E2E_COMPLEX_JOIN_TABLE: &str = "complex_join_probe";

pub const COMPLEX_JOIN_POPULATED_DOC: &str = r#"["hello","world"]"#;
pub const COMPLEX_JOIN_ALT_DOC: &str = r#"["foo","bar","baz"]"#;
/// Matches no `complex_probe` row, so a join over the nested column must discriminate.
pub const COMPLEX_JOIN_ORPHAN_DOC: &str = r#"["never","matched"]"#;

pub async fn seed_complex_types_join_probe(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog =
        build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-complex-join").await?;

    let iceberg_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "tag_doc", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "label", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .context("build complex_join_probe Iceberg schema")?;

    create_and_append_files(
        &catalog,
        E2E_NAMESPACE,
        E2E_COMPLEX_JOIN_TABLE,
        iceberg_schema,
        vec![vec![make_complex_join_probe_batch()]],
    )
    .await
    .context("seed complex_join_probe table")?;
    Ok(())
}

fn make_complex_join_probe_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("tag_doc", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                COMPLEX_JOIN_POPULATED_DOC,
                COMPLEX_JOIN_ALT_DOC,
                COMPLEX_JOIN_ORPHAN_DOC,
            ])),
            Arc::new(StringArray::from(vec!["POPULAR", "ALT", "ORPHAN"])),
        ],
    )
    .expect("complex_join_probe RecordBatch construction is infallible")
}

/// Format version admitting `timestamp_ns`.
const ICEBERG_FORMAT_VERSION_PROPERTY: &str = "format-version";
const ICEBERG_FORMAT_VERSION_3: &str = "3";

/// Its own namespace, so this table never enters another suite's table enumeration.
pub const E2E_TSPRECISION_NAMESPACE: &str = "e2e_tsprecision";
pub const E2E_TSPRECISION_TABLE: &str = "ts_precision_probe";
pub const TSPRECISION_COL_TS: &str = "ts";
pub const TSPRECISION_COL_TSTZ: &str = "tstz";
pub const TSPRECISION_COL_TS_NS: &str = "ts_ns";

/// Two pairs share a millisecond prefix: four distinct values at `TIMESTAMP(6)`, two at
/// `TIMESTAMP(3)`.
pub const TSPRECISION_MICROS: [i64; 4] = [
    BASE_TS_MICROS + 1,
    BASE_TS_MICROS + 2,
    BASE_TS_MICROS + 123_456,
    BASE_TS_MICROS + 123_457,
];

/// Two values differing only below the microsecond: `COUNT(DISTINCT)` is 2 at
/// `TIMESTAMP(9)` and 1 at every coarser width.
pub const TSPRECISION_NANOS: [i64; 4] = [
    BASE_TS_MICROS * 1_000 + 1,
    BASE_TS_MICROS * 1_000 + 2,
    BASE_TS_MICROS * 1_000 + 1,
    BASE_TS_MICROS * 1_000 + 2,
];

pub async fn seed_timestamp_precision_probe(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog =
        build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-tsprecision").await?;
    let ns = NamespaceIdent::new(E2E_TSPRECISION_NAMESPACE.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("check namespace for timestamp precision probe table")?
    {
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;
    }

    let iceberg_schema = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(
                2,
                TSPRECISION_COL_TS,
                Type::Primitive(PrimitiveType::Timestamp),
            )
            .into(),
            NestedField::required(
                3,
                TSPRECISION_COL_TSTZ,
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .into(),
            NestedField::required(
                4,
                TSPRECISION_COL_TS_NS,
                Type::Primitive(PrimitiveType::TimestampNs),
            )
            .into(),
        ])
        .build()
        .context("build timestamp precision probe Iceberg schema")?;

    create_and_append_files_with_properties(
        &catalog,
        E2E_TSPRECISION_NAMESPACE,
        E2E_TSPRECISION_TABLE,
        iceberg_schema,
        HashMap::from([(
            ICEBERG_FORMAT_VERSION_PROPERTY.to_string(),
            ICEBERG_FORMAT_VERSION_3.to_string(),
        )]),
        std::iter::once(vec![make_timestamp_precision_probe_batch()]),
    )
    .await
    .context("seed timestamp precision probe table")?;
    Ok(())
}

fn make_timestamp_precision_probe_batch() -> RecordBatch {
    let ids: Vec<i64> = (1..=TSPRECISION_MICROS.len() as i64).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            TSPRECISION_COL_TS,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            TSPRECISION_COL_TSTZ,
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ),
        Field::new(
            TSPRECISION_COL_TS_NS,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMicrosecondArray::from(TSPRECISION_MICROS.to_vec())),
            Arc::new(
                TimestampMicrosecondArray::from(TSPRECISION_MICROS.to_vec())
                    .with_timezone("+00:00"),
            ),
            Arc::new(TimestampNanosecondArray::from(TSPRECISION_NANOS.to_vec())),
        ],
    )
    .expect("timestamp precision probe RecordBatch construction is infallible")
}

#[cfg(test)]
mod seed_catalog_props_tests {
    use super::*;
    use iceberg::io::{
        S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
    };
    use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};

    fn get<'a>(props: &'a std::collections::HashMap<String, String>, key: &str) -> Option<&'a str> {
        props.get(key).map(String::as_str)
    }

    #[test]
    fn default_auth_uses_static_minio_and_injects_no_catalog_auth() {
        let props = seed_catalog_props("http://lk:8181/catalog", "wh", &SeedCatalogAuth::default());

        assert_eq!(
            get(&props, REST_CATALOG_PROP_URI),
            Some("http://lk:8181/catalog")
        );
        assert_eq!(get(&props, REST_CATALOG_PROP_WAREHOUSE), Some("wh"));
        assert_eq!(get(&props, S3_ACCESS_KEY_ID), Some("minioadmin"));
        assert_eq!(get(&props, S3_SECRET_ACCESS_KEY), Some("minioadmin"));
        assert_eq!(get(&props, S3_REGION), Some("us-east-1"));
        assert_eq!(get(&props, S3_PATH_STYLE_ACCESS), Some("true"));
        assert!(
            !props[S3_ENDPOINT].is_empty(),
            "S3 endpoint must default to the host MinIO URL"
        );
        assert!(get(&props, "credential").is_none());
        assert!(get(&props, "oauth2-server-uri").is_none());
        assert!(get(&props, "scope").is_none());
        assert!(get(&props, "token").is_none());
        assert!(get(&props, ADLS_ACCOUNT_NAME).is_none());
        assert!(get(&props, ADLS_ACCOUNT_KEY).is_none());
    }

    #[test]
    fn adls_storage_carries_the_account_key_and_no_s3_property() {
        let auth = SeedCatalogAuth {
            token: Some("bearer-xyz".to_string()),
            storage: SeedStorage::Adls {
                account_name: "lhrsstatic".to_string(),
                account_key: "a2V5".to_string(),
            },
        };
        let props = seed_catalog_props("http://lk:8181/catalog", "wh-azure", &auth);

        assert_eq!(get(&props, ADLS_ACCOUNT_NAME), Some("lhrsstatic"));
        assert_eq!(get(&props, ADLS_ACCOUNT_KEY), Some("a2V5"));
        assert_eq!(get(&props, REST_CATALOG_PROP_WAREHOUSE), Some("wh-azure"));
        // Catalog auth is orthogonal to storage: an ADLS seed still needs its bearer token.
        assert_eq!(get(&props, "token"), Some("bearer-xyz"));

        // `azdls_config_parse` silently discards `s3.*` properties, so a stray one would leak
        // MinIO admin credentials into an Azure run invisibly.
        for s3_prop in [
            S3_ENDPOINT,
            S3_REGION,
            S3_ACCESS_KEY_ID,
            S3_SECRET_ACCESS_KEY,
            S3_PATH_STYLE_ACCESS,
        ] {
            assert!(
                get(&props, s3_prop).is_none(),
                "an ADLS seed must carry no {s3_prop}"
            );
        }
    }

    #[test]
    fn static_bearer_token_is_injected_when_no_client_credentials() {
        let auth = SeedCatalogAuth {
            token: Some("bearer-xyz".to_string()),
            ..Default::default()
        };
        let props = seed_catalog_props("http://lk:8181/catalog", "wh", &auth);

        assert_eq!(get(&props, "token"), Some("bearer-xyz"));
        assert!(get(&props, "credential").is_none());
    }

    #[test]
    fn events_seed_shape_is_identical_for_lakekeeper_and_baseline() {
        // Guards the row count and score formula the Lakekeeper scan assertions rely on.
        let mid = SEED_TOTAL_ROWS / 2;
        let file1 = make_events_batch(1, mid);
        let file2 = make_events_batch(mid + 1, SEED_TOTAL_ROWS);
        assert_eq!(file1.num_rows() + file2.num_rows(), SEED_TOTAL_ROWS);

        let score_col = file1
            .schema()
            .index_of("score")
            .expect("events has a score column");
        let count_gt_15: usize = [&file1, &file2]
            .iter()
            .map(|batch| {
                batch
                    .column(score_col)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("score column is Float64")
                    .iter()
                    .flatten()
                    .filter(|&score| score > 15.0)
                    .count()
            })
            .sum();
        assert_eq!(count_gt_15, SEED_ROWS_SCORE_GT_15);
    }
}
