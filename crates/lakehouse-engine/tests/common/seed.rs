//! Iceberg table seeder for lakehouse-engine E2E tests.
//!
//! Seeds a deterministic mixed-column table into the Iceberg REST catalog over MinIO.
//! Uses iceberg-rust 0.10.0 + iceberg-catalog-rest 0.10.0 (same as the
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
//! Complex columns (list/struct/map) ARE writable with iceberg-rust 0.10: `seed_complex_types_probe`
//! builds its Arrow batch from `iceberg::arrow::schema_to_arrow_schema` after `create_table`, so the
//! batch's nested field-ids match the ones the REST catalog actually assigned.
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
    BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
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
    seed_char_pad_table(catalog_url, warehouse).await?;
    Ok(events_handle)
}

/// Which object store a seed catalog writes its data files through.
///
/// `Default` is the static MinIO baseline every pre-Azure suite seeds against, so
/// `SeedCatalogAuth::default()` keeps reproducing the original behavior exactly.
#[derive(Clone, Default)]
pub enum SeedStorage {
    /// Static MinIO admin credentials — the baseline for the REST-fixture and
    /// Lakekeeper MinIO suites.
    #[default]
    Minio,
    /// ADLS Gen2 under a storage-account key — the Azure suite's path under
    /// test. The container-lifecycle service principal must never appear here,
    /// or seeding would succeed without exercising the account-key path.
    Adls {
        account_name: String,
        account_key: String,
    },
}

/// Optional authentication and storage selection for a seed catalog. `Default`
/// reproduces the unauthenticated, static-MinIO-credential baseline that
/// [`build_seed_catalog`] shipped before Lakekeeper support.
///
/// A non-empty `token` is sent to the REST catalog as a static bearer
/// credential. This is the only catalog-auth mode the Lakekeeper E2E suite uses:
/// its setup obtains a bearer token from Keycloak's client-credentials grant and
/// passes it here. (The suite does NOT drive the REST client's own OAuth2
/// client-credentials flow for seeding.)
///
/// `storage` selects the object store. [`SeedStorage::Minio`] forces static S3
/// credentials, overriding whatever the catalog vends (see
/// [`build_seed_catalog_with_auth`]); [`SeedStorage::Adls`] carries its account
/// key in the properties and overrides nothing — a `sas-enabled: false` ADLS
/// warehouse vends no credentials to override.
#[derive(Clone, Default)]
pub struct SeedCatalogAuth {
    pub token: Option<String>,
    pub storage: SeedStorage,
}

// REST-catalog auth property key (literal string, fixed by `iceberg-catalog-rest`;
// the crate exports no constant for it). It flows through
// `RestCatalogBuilder::load`, which copies every prop except `uri`/`warehouse`.
const REST_CATALOG_PROP_TOKEN: &str = "token";

/// Resolve the static S3 storage config `(endpoint, region, access_key,
/// secret_key, path_style)` for a seed catalog: always the static MinIO baseline
/// (`minioadmin`/`minioadmin`, host MinIO URL, `us-east-1`, path-style). Shared by
/// [`seed_catalog_props`] (props map) and [`build_seed_catalog_with_auth`] (the
/// forced static-credential loader) so both derive the same credentials from one
/// place.
fn seed_storage_config() -> (String, String, String, String, bool) {
    (
        super::stack::minio_url(),
        "us-east-1".to_string(),
        "minioadmin".to_string(),
        "minioadmin".to_string(),
        true,
    )
}

/// A [`ProvideCredential`] that always returns fixed static S3 credentials with no
/// session token.
///
/// Installed on the seed catalog's S3 storage factory so seeding signs every
/// object-store request with the static admin credentials, regardless of any
/// per-table credentials the catalog vends. See [`build_seed_catalog_with_auth`]
/// for why this is required against Lakekeeper's `sts-enabled` warehouse.
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
            // No session token: this is a long-lived root credential, not STS.
            // A `None` `expires_in` marks the credential permanently valid
            // (reqsign `SigningCredential::is_valid`).
            ..Default::default()
        }))
    }
}

/// Build the REST-catalog property map for a seed catalog from `auth`.
///
/// Pure (no I/O) so the credential/storage wiring is unit-testable without a
/// live catalog. Storage properties follow `auth.storage`; catalog auth is a static
/// bearer `token` when supplied (non-empty), otherwise none. The two storage arms
/// are mutually exclusive: an ADLS seed carries no S3 property at all, so a MinIO
/// credential can never travel with an Azure warehouse.
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

/// Build an unauthenticated REST catalog client for seed operations, using the
/// static MinIO credentials that back the baseline `iceberg-rest-fixture`.
///
/// Thin wrapper over [`build_seed_catalog_with_auth`] with no catalog auth —
/// every existing seed call site uses this unchanged.
pub async fn build_seed_catalog(
    catalog_url: &str,
    warehouse: &str,
    label: &str,
) -> Result<impl Catalog> {
    build_seed_catalog_with_auth(catalog_url, warehouse, label, SeedCatalogAuth::default()).await
}

/// Build a REST catalog client for seed operations with explicit `auth`.
///
/// Extends [`build_seed_catalog`] so seeding can target an OAuth2-secured
/// Lakekeeper warehouse via a static bearer catalog-auth token, over either
/// storage backend. `SeedCatalogAuth::default()` reproduces the unauthenticated
/// static-MinIO baseline exactly.
pub async fn build_seed_catalog_with_auth(
    catalog_url: &str,
    warehouse: &str,
    label: &str,
    auth: SeedCatalogAuth,
) -> Result<impl Catalog> {
    let props = seed_catalog_props(catalog_url, warehouse, &auth);

    let storage_factory: Arc<dyn StorageFactory> = match &auth.storage {
        // Force the STATIC S3 credentials for ALL storage I/O, overriding whatever
        // the catalog vends per-table. Lakekeeper's `sts-enabled` (vended) warehouse
        // returns short-lived STS session-token credentials in each table's
        // `loadTable`/`config` response, and iceberg-catalog-rest merges that config
        // OVER the static builder props (`RestCatalog::load_file_io`: `props.extend(
        // config)`), so the seed WRITE path would otherwise sign with the vended
        // session token — which MinIO rejects with `InvalidTokenId`. Installing a
        // `CustomAwsCredentialLoader` makes opendal's S3 backend use this
        // credential-provider chain in place of the config-derived credentials (a
        // user-supplied chain REPLACES the default/static provider), so seeding
        // always writes with the static admin credentials regardless of the
        // warehouse's vended-creds flag. The static warehouse never vends creds, so
        // its seeding behavior is unchanged (the loader returns the same
        // `minioadmin` credentials the props already carried). This is a
        // seed-harness-only override; the adapter's own read path handles vended
        // credentials correctly.
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
        // No override needed: Lakekeeper vends ADLS creds under the host-suffixed
        // key `adls.sas-token.<host>`, but iceberg-rust's `load_file_io` only reads
        // the flat `adls.sas-token`/`adls.account-key` property, so the account-key
        // `seed_catalog_props` set is the only credential in play regardless of
        // `sas-enabled`.
        SeedStorage::Adls { .. } => Arc::new(OpenDalStorageFactory::Azdls),
    };

    RestCatalogBuilder::default()
        .with_storage_factory(storage_factory)
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

    // Short-circuit only if the table is already populated AND its persisted
    // schema still matches the seed's expected schema. The Docker warehouse
    // outlives individual test runs, so a table seeded by an earlier revision
    // can carry a stale schema (missing columns a later seed added). Reuse it
    // only when the schema still matches; otherwise drop it so it is recreated
    // with the expected schema rather than silently pinning the old columns.
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
    seed_events_table_with_auth(catalog_url, warehouse, SeedCatalogAuth::default()).await
}

/// Seed the baseline `events` table into `warehouse` through a REST catalog built
/// with `auth`, then return the committed data-file paths.
///
/// This is the authenticated seeding entry point for the Lakekeeper E2E suite: it
/// creates the `e2e_lakehouse` namespace (if absent) and appends the SAME 20-row,
/// two-file events data (`SEED_TOTAL_ROWS`, `SEED_ROWS_SCORE_GT_15`) the
/// unauthenticated baseline produces, so the Lakekeeper scan tests can assert
/// against identical known values. Group C's setup calls this once per warehouse
/// (static + vended), passing a static bearer token in `auth`; Lakekeeper
/// negotiates the per-warehouse routing prefix from `GET /v1/config?warehouse=`, so
/// only the warehouse NAME is needed here. Idempotent per warehouse. With
/// `SeedCatalogAuth::default()` this is exactly the original baseline seed.
pub async fn seed_events_table_with_auth(
    catalog_url: &str,
    warehouse: &str,
    auth: SeedCatalogAuth,
) -> Result<SeedHandle> {
    let catalog =
        build_seed_catalog_with_auth(catalog_url, warehouse, "lakehouse-e2e-seed", auth).await?;

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

/// The ordered `(name, type)` signature of an Iceberg schema's top-level fields.
/// Detects when a persisted table's schema has drifted from the seed's expected
/// schema (e.g. a seeded table gained columns in a later revision), so a stale
/// table on the persistent Docker warehouse is dropped and reseeded instead of
/// silently pinning the old columns.
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

/// Precision/scale of `fact_orders.O_TOTALPRICE`, the scale > 0 DECIMAL column whose
/// stringified length differs between DataFusion's full-scale text and Exasol's
/// trimmed form (#223 slice 2).
pub const O_TOTALPRICE_PS: (u8, i8) = (10, 2);

/// The unscaled `O_TOTALPRICE` value (scale [`O_TOTALPRICE_PS`].1 = 2) for a
/// given order key, chosen so the untrimmed fixed-scale string and Exasol's
/// own trimmed string diverge in length: every value ends in `"00"` (the
/// trimmed form is always exactly 3 characters shorter, e.g. `"2912.00"` ->
/// `"2912"`), and the integer part's digit count grows with the order key so a
/// `LENGTH(...)` WHERE filter genuinely discriminates rows instead of
/// splitting the whole table on one side.
pub fn order_totalprice_unscaled(order_key: usize) -> i64 {
    const VALUES: [i64; FACT_ORDERS_ROWS] = [
        100, 800, 2700, 6400, 12500, 21600, 291200, 512000, 729000, 1000000,
    ];
    VALUES[order_key - 1]
}

/// Seed the `dim_customer` and `fact_orders` star-schema tables into the
/// `e2e_lakehouse` namespace, including `fact_orders.O_TOTALPRICE` (a scale > 0
/// DECIMAL column, see [`O_TOTALPRICE_PS`]). Idempotent.
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

/// Seed the star-schema tables using the unauthenticated static-MinIO baseline.
///
/// Thin wrapper over [`seed_star_schema_with_auth`] with no catalog auth — the
/// existing call site uses this unchanged.
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
// | fact_lineitem | L_ORDERKEY, L_LINENUMBER, L_SUPPKEY, L_QUANTITY,   | 20   | 2     |
// |               | L_RETURNFLAG, L_EXTENDEDPRICE                      |      |       |
// | dim_supplier  | S_SUPPKEY, S_NAME                                  | 3    | 1     |
//
// Every line item references a valid order and a valid supplier, so the 3-table
// and 4-table inner joins both yield every seeded lineitem row.
//
// `L_RETURNFLAG` and `L_EXTENDEDPRICE` extend `fact_lineitem` (plan
// `fix-join-decline-hard-fail`, PR #78 review finding #4) for the
// scalar-over-aggregate grouped-join E2E tests: a low-cardinality discriminator
// column (`L_RETURNFLAG`, alternating `'R'`/`'N'`) plus a numeric column
// (`L_EXTENDEDPRICE`) alongside the existing `L_QUANTITY`, so `SUM`/`AVG`
// aggregates and a `CASE WHEN L_RETURNFLAG = 'R' ...` discriminator can be
// grouped and rendered through a scalar function (`ROUND`) wrapping aggregates
// in a joined, grouped select list.

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

/// The `L_RETURNFLAG` a line item (by its global row number `1..=LINEITEM_ROWS`)
/// carries: alternates `"R"`/`"N"` so a `GROUP BY L_RETURNFLAG` in the
/// scalar-over-aggregate join E2E tests always sees two non-empty groups. Row 1
/// is `"R"`.
pub fn line_returnflag(row: usize) -> &'static str {
    if row % 2 == 1 { "R" } else { "N" }
}

/// The `L_EXTENDEDPRICE` a line item (by its global row number
/// `1..=LINEITEM_ROWS`) carries: a distinct deterministic value per row
/// (`1000.0 + row * 10.0`) so `AVG(L_EXTENDEDPRICE)` in the
/// scalar-over-aggregate join E2E tests is computable independently in Rust
/// from the same formula used to seed it.
pub fn line_extendedprice(row: usize) -> f64 {
    1000.0 + (row as f64) * 10.0
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
// iceberg-rust 0.10.0 exposes NO schema-evolution API (the `transaction` module
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
    rest_replace_current_schema(
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
// All-types initial-default schema-evolution table (initdef) — issue #27
// ---------------------------------------------------------------------------
//
// Exercises Iceberg column-projection rule (3): a field added AFTER a data file
// was written reads as ABSENT from that file and MUST return the field's
// `initial-default`. One column per Iceberg-expressible primitive type is added
// via a single out-of-band REST `add-schema` + `set-current-schema` commit
// (`rest_replace_current_schema`), each carrying an `initial-default`. File A is
// written under the pre-add (id-only) schema; file B under the post-add schema
// with real written values. A field-id scan of both files in one shard must
// return each added column's `initial-default` for file-A rows and its real
// written value for file-B rows.
//
// ns-precision timestamps are NOT Iceberg-expressible in this catalog version;
// they are covered exhaustively by the unit round-trip test (plan task 3.3).
// This includes ns-precision timestamptz: its initial-default logic stays
// covered by the unit round-trip test only.

/// Table name for the all-types initial-default schema-evolution fixture (#27).
pub const EVO_INITDEF_TABLE: &str = "initdef";
/// Inclusive id range written BEFORE the columns were added (file A, id-only).
pub const EVO_INITDEF_PRE_ADD_IDS: (i64, i64) = (1, 3);
/// Inclusive id range written AFTER the columns were added (file B, all columns).
pub const EVO_INITDEF_POST_ADD_IDS: (i64, i64) = (4, 6);
/// Total rows a field-id scan must return across both files.
pub const EVO_INITDEF_TOTAL_ROWS: usize = 6;

/// Boolean column, added REQUIRED-with-default (exercises the required branch of
/// rule (3): an absent required field returns its default rather than erroring).
pub const EVO_INITDEF_COL_BOOL: &str = "c_bool";
/// Int (32-bit) column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_INT: &str = "c_int";
/// Long (64-bit) column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_LONG: &str = "c_long";
/// Float (32-bit) column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_FLOAT: &str = "c_float";
/// Double (64-bit) column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_DOUBLE: &str = "c_double";
/// String column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_STRING: &str = "c_string";
/// Date column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_DATE: &str = "c_date";
/// Timestamp (no zone) column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_TS: &str = "c_ts";
/// Decimal(9,2) column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_DECIMAL: &str = "c_decimal";
/// Timestamptz (micros, UTC) column, added NULLABLE-with-default.
pub const EVO_INITDEF_COL_TSTZ: &str = "c_tstz";

/// `c_long` initial-default (chosen > i32::MAX so a Long/Int mix-up is caught).
const INITDEF_LONG_DEFAULT: i64 = 4_200_000_000;
/// `c_decimal` initial-default unscaled mantissa: 12345 at scale 2 == 123.45.
const INITDEF_DECIMAL_DEFAULT_UNSCALED: i128 = 12_345;
/// `c_decimal` real written value unscaled mantissa: 67890 at scale 2 == 678.90.
const INITDEF_DECIMAL_REAL_UNSCALED: i128 = 67_890;
/// `c_date` real written value: 2024-07-01 (BASE_DATE + 182 days).
const INITDEF_REAL_DATE_DAYS: i32 = BASE_DATE + 182;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
/// `c_ts` initial-default: 2024-01-01T12:00:00Z. Noon (not midnight)
/// so a session-timezone offset cannot roll the calendar date the test asserts.
const INITDEF_DEFAULT_TS_MICROS: i64 = BASE_TS_MICROS + 12 * MICROS_PER_HOUR;
/// `c_ts` real written value: 2024-07-01T12:00:00Z.
const INITDEF_REAL_TS_MICROS: i64 = BASE_TS_MICROS + 182 * MICROS_PER_DAY + 12 * MICROS_PER_HOUR;
/// `c_tstz` initial-default: 2024-01-01T12:00:00Z (same UTC instant as `c_ts`'s
/// default — timestamptz stores a UTC instant, so reusing the calendar day is
/// deliberate, not an oversight).
const INITDEF_DEFAULT_TSTZ_MICROS: i64 = BASE_TS_MICROS + 12 * MICROS_PER_HOUR;
/// `c_tstz` real written value: 2024-07-01T12:00:00Z.
const INITDEF_REAL_TSTZ_MICROS: i64 = BASE_TS_MICROS + 182 * MICROS_PER_DAY + 12 * MICROS_PER_HOUR;

/// The value a field-id scan is expected to return for one added column, matched
/// tolerantly against the JSON the Exasol result set delivers.
pub enum ExpectedValue {
    /// Exasol BOOLEAN (JSON bool, or a `1`/`0` / `"true"` fallback form).
    Bool(bool),
    /// Any numeric Exasol type (DECIMAL/DOUBLE), compared as `f64` within 1e-6,
    /// accepting either the JSON-number or JSON-string encoding.
    Num(f64),
    /// Exasol VARCHAR, compared exactly.
    Text(&'static str),
    /// Exasol DATE / TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE, matched by the
    /// leading `YYYY-MM-DD` calendar-date substring (robust to fractional-second
    /// and session-timezone rendering differences).
    DatePrefix(&'static str),
}

impl ExpectedValue {
    /// True when `actual` (a value from the Exasol result set) matches self.
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

/// One added column in the initdef fixture: its name, whether it was added as
/// REQUIRED (vs nullable), and the value a field-id scan must return for pre-add
/// (file A → `default`) vs post-add (file B → `real`) rows.
pub struct InitDefColumn {
    pub name: &'static str,
    pub required: bool,
    pub default: ExpectedValue,
    pub real: ExpectedValue,
}

/// The added columns in field-id order (field-ids 2..=11), each paired with its
/// expected `initial-default` (file-A rows) and real written value (file-B rows).
/// `c_bool` is the REQUIRED-with-default column; the rest are NULLABLE-with-default.
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

/// Seed the `initdef` table for all-types initial-default schema evolution (#27).
///
/// Sequence (mirrors `seed_renamed_column`):
///   1. create `initdef` with only `id` (field-id 1) — the pre-add schema
///   2. append file A: ids `EVO_INITDEF_PRE_ADD_IDS`, physical parquet has `id` only
///   3. REST commit: add one column per primitive type (field-ids 2..=11), each
///      with an `initial-default`; `c_bool` REQUIRED, the rest NULLABLE
///   4. append file B: ids `EVO_INITDEF_POST_ADD_IDS`, all columns with real values
///
/// A field-id scan returns `EVO_INITDEF_TOTAL_ROWS` rows: file-A rows carry each
/// added column's `initial-default`; file-B rows carry the real written values.
///
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

    // 1. Create `initdef` with the pre-add (id-only) schema at format-version 3.
    // Iceberg requires v3 for non-null `initial-default` values (see the add-schema
    // commit in step 3); `Schema.checkCompatibility` rejects them on a v2 table.
    //
    // NOTE: against the Iceberg REST catalog, `TableCreation::format_version(V3)` is a
    // no-op (iceberg-rust 0.10.0 does not send it in a form the server honors); the
    // REST create-table protocol derives the format-version from the `format-version`
    // TABLE PROPERTY (iceberg-java `TableProperties.FORMAT_VERSION`). We set the property
    // so the server actually creates a v3 table, and assert the result below so this can
    // never silently regress to v2.
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

    // 2. Append file A under the pre-add schema (only `id` is present).
    let (a0, a1) = EVO_INITDEF_PRE_ADD_IDS;
    write_one_file_append(
        &catalog,
        &table,
        EVO_INITDEF_TABLE,
        [make_initdef_id_only_batch(a0, a1)],
    )
    .await
    .context("append initdef file A (pre-add)")?;

    // 3. Add all primitive-typed columns (field-ids 2..=11) via a raw REST commit.
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

    // 4. Append file B under the post-add schema (real values for every column).
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

/// Pre-add initdef schema: only field-id 1 = `id` (Long).
fn initdef_pre_add_schema() -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .context("build initdef pre-add Iceberg schema")
}

/// Post-add initdef schema: `id` plus one column per primitive type (field-ids
/// 2..=11), each carrying its `initial-default`. `c_bool` is REQUIRED-with-default
/// (rule (3)'s required branch); the rest are NULLABLE-with-default.
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

/// File A batch: only the `id` column (pre-add physical layout).
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

/// File B batch: `id` plus every added column carrying its real written value.
/// Arrow types and nullability mirror the post-add Iceberg schema exactly so the
/// Iceberg parquet writer accepts the batch (decimal is scale-2 Decimal128).
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
        // The timezone label MUST be "+00:00" (iceberg-rust's `UTC_TIME_ZONE`),
        // not "UTC": the Iceberg parquet writer derives its target Arrow schema
        // from the Iceberg schema and validates the input batch by strict
        // DataType equality, so a "UTC" label here is rejected as an incompatible
        // type. (The read/emit path is tz-label-agnostic; only this write path
        // demands the exact label.)
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

/// Table name for the single-shard high-cardinality `COUNT(DISTINCT)` regression
/// probe (issue #146).
pub const E2E_HIGH_CARD_TABLE: &str = "high_card_probe";
/// High-cardinality column on `high_card_probe`.
pub const HIGH_CARD_COL: &str = "token";
/// Row count, written as a SINGLE data file (one shard) of unique 100-byte
/// `token` values. Tens of thousands of distinct values — at ~100 bytes each the
/// shard-local distinct set is ~3 MB, several times the 1,048,576-byte per-shard
/// budget the old JSON-serialized distinct-set path enforced (issue #146). The
/// native-merge path has no such cap: each shard-local distinct value streams as
/// one row and Exasol's own `COUNT(DISTINCT "V")` counts the union, so the query
/// completes and returns the exact count (equal to `HIGH_CARD_ROWS`, as every
/// token is unique). Kept well below the 657k-row real-world repro scale — tens
/// of thousands is enough to prove the fix on a single shard.
pub const HIGH_CARD_ROWS: usize = 30_000;

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
/// zero-padded so every distinct value is the same width and the shard-local
/// distinct set's byte size stays deterministic.
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

// ---------------------------------------------------------------------------
// Typed COUNT(DISTINCT) probe table — bare-column type matrix + expression args
// ---------------------------------------------------------------------------
//
// Used only by `tests/e2e_count_distinct_test.rs`. A single table carrying one
// column per Iceberg/Arrow-reachable Exasol type, seeded across TWO data files
// (rows 1..=6, 7..=12) so `COUNT(DISTINCT)` pushdown must dedup across the shard
// boundary rather than sum per-shard counts. Every typed column mixes in NULLs
// (which must be excluded from the count) and repeats at least one non-NULL value
// in BOTH files (so a fan-out that merely summed per-shard distinct rows would
// overcount). The expected distinct counts are NOT hand-written constants: they
// are computed by the `typed_*_distinct` helpers below FROM THE SAME arrays that
// build the Arrow batches (`typed_probe`), so the fixture is its own single source
// of truth and a data edit can never silently disagree with an expected count.
//
// Type coverage (bare-column, Case 1 fan-out — reviewer's requested matrix):
//   c_decimal_a  Iceberg decimal(9,2)  -> Exasol DECIMAL(9,2)
//   c_decimal_b  Iceberg decimal(20,4) -> Exasol DECIMAL(20,4)  (varying prec/scale)
//   c_double     Iceberg double        -> Exasol DOUBLE PRECISION
//   c_varchar    Iceberg string        -> Exasol VARCHAR(2000000)
//   c_date       Iceberg date          -> Exasol DATE
//   c_ts         Iceberg timestamp     -> Exasol TIMESTAMP (millisecond fraction)
//   c_bool       Iceberg boolean       -> Exasol BOOLEAN
//
// CHAR is deliberately absent: no Iceberg/Arrow source type maps to Exasol CHAR
// (Iceberg `string` maps to VARCHAR per the type table in the crate root), so a
// bare-column CHAR virtual column is unreachable through this scan path. VARCHAR
// (c_varchar) is the closest bare-column string coverage; a CHAR-typed result is
// covered only as a wrapper-routed `CAST(... AS CHAR(n))` expression argument.
//
// The `c_ts` values all share the same whole second (2024-01-01 00:00:00) and
// differ ONLY in the millisecond component (.100 .. .600). Exasol's default
// TIMESTAMP precision is fsp=3 (milliseconds), which the VS declares by mapping
// Iceberg `timestamp` to a plain `TIMESTAMP` column, so these millisecond-distinct
// instants are preserved and counted distinct. Sub-millisecond (microsecond-only)
// distinctions are NOT preserved by an Exasol TIMESTAMP(3) column — a deliberate
// Exasol-type limitation, not a fan-out defect — so the fixture stays at
// millisecond resolution. `c_price`/`c_qty` back the numeric product expression,
// and `c_bool`+`c_ts` back the temporal `CASE` expression, of task 2.4.

/// Table name for the typed `COUNT(DISTINCT)` probe.
pub const E2E_TYPED_TABLE: &str = "typed_distinct_probe";

/// Bare `DECIMAL(9,2)` column.
pub const TYPED_COL_DECIMAL_A: &str = "c_decimal_a";
/// Bare `DECIMAL(20,4)` column (distinct precision/scale from `c_decimal_a`).
pub const TYPED_COL_DECIMAL_B: &str = "c_decimal_b";
/// Bare `DOUBLE PRECISION` column.
pub const TYPED_COL_DOUBLE: &str = "c_double";
/// Bare `VARCHAR` column (mixed case, so `UPPER(...)` folds some values together).
pub const TYPED_COL_VARCHAR: &str = "c_varchar";
/// Bare `DATE` column.
pub const TYPED_COL_DATE: &str = "c_date";
/// Bare `TIMESTAMP` column (values differ only in the millisecond fraction).
pub const TYPED_COL_TS: &str = "c_ts";
/// Bare `BOOLEAN` column.
pub const TYPED_COL_BOOL: &str = "c_bool";
/// `DOUBLE` operand of the numeric product expression (`c_price * c_qty`).
pub const TYPED_COL_PRICE: &str = "c_price";
/// `DECIMAL(20,0)` operand of the numeric product expression (`c_price * c_qty`).
pub const TYPED_COL_QTY: &str = "c_qty";

/// Rows seeded into `typed_distinct_probe`, across two data files.
pub const TYPED_TABLE_TOTAL_ROWS: usize = 12;
/// Row-index boundary between the two data files (file 1: rows 1..=6, file 2: 7..=12).
const TYPED_FILE_SPLIT: usize = 6;

/// Precision/scale of `c_decimal_a` (Exasol `DECIMAL(9,2)`).
const TYPED_DECIMAL_A_PS: (u8, i8) = (9, 2);
/// Precision/scale of `c_decimal_b` (Exasol `DECIMAL(20,4)`).
const TYPED_DECIMAL_B_PS: (u8, i8) = (20, 4);

/// The full 12-row column vectors for `typed_distinct_probe`, the SINGLE source of
/// truth for both the Arrow batches and the expected distinct counts.
///
/// Decimal columns hold unscaled `i128` values (`c_decimal_a` scale 2, `c_decimal_b`
/// scale 4); `date_days` is days-since-epoch; `ts_micros` is microseconds since the
/// UNIX epoch. `None` is a NULL cell.
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

/// Build the deterministic 12-row `typed_distinct_probe` data. See the module note
/// above for the cross-shard-duplicate + NULL design of each column.
fn typed_probe() -> TypedProbe {
    // Millisecond offsets within 2024-01-01 00:00:00 for `c_ts`.
    let ts = |ms: i64| BASE_TS_MICROS + ms * 1_000;
    // Day offsets from 2024-01-01 for `c_date`.
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
        // Mixed case: raw distinct = 8, UPPER-folded distinct = 5. "aa"/"AA"/"Aa"
        // and "bb"/"BB" fold across the shard boundary, so native UPPER dedup is
        // exercised cross-shard.
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

/// Count distinct non-`None` `f64` cells by exact bit pattern (all seeded values are
/// positive and finite, so bitwise equality matches SQL `DISTINCT` equality here).
fn distinct_f64(values: impl Iterator<Item = Option<f64>>) -> i64 {
    let set: std::collections::HashSet<u64> = values.flatten().map(f64::to_bits).collect();
    set.len() as i64
}

/// Count distinct non-`None` cells of a `Hash + Eq` column.
fn distinct_hashable<T: std::hash::Hash + Eq>(values: impl Iterator<Item = Option<T>>) -> i64 {
    let set: std::collections::HashSet<T> = values.flatten().collect();
    set.len() as i64
}

/// Distinct `c_decimal_a` count (bare `DECIMAL(9,2)`), NULLs excluded.
pub fn typed_decimal_a_distinct() -> i64 {
    distinct_hashable(typed_probe().decimal_a.into_iter())
}
/// Distinct `c_decimal_b` count (bare `DECIMAL(20,4)`), NULLs excluded.
pub fn typed_decimal_b_distinct() -> i64 {
    distinct_hashable(typed_probe().decimal_b.into_iter())
}
/// Distinct `c_double` count (bare `DOUBLE`), NULLs excluded.
pub fn typed_double_distinct() -> i64 {
    distinct_f64(typed_probe().double.into_iter())
}
/// Distinct `c_varchar` count (bare `VARCHAR`, raw values), NULLs excluded.
pub fn typed_varchar_distinct() -> i64 {
    distinct_hashable(typed_probe().varchar.into_iter())
}
/// Distinct `UPPER(c_varchar)` count (wrapper-routed string expression), NULLs
/// excluded — lower than the raw count because mixed-case values fold together.
pub fn typed_varchar_upper_distinct() -> i64 {
    distinct_hashable(
        typed_probe()
            .varchar
            .into_iter()
            .map(|v| v.map(str::to_uppercase)),
    )
}
/// Distinct `c_date` count (bare `DATE`), NULLs excluded.
pub fn typed_date_distinct() -> i64 {
    distinct_hashable(typed_probe().date_days.into_iter())
}
/// Distinct `c_ts` count (bare `TIMESTAMP`), NULLs excluded.
pub fn typed_ts_distinct() -> i64 {
    distinct_hashable(typed_probe().ts_micros.into_iter())
}
/// Distinct `c_bool` count (bare `BOOLEAN`), NULLs excluded — at most 2.
pub fn typed_bool_distinct() -> i64 {
    distinct_hashable(typed_probe().boolean.into_iter())
}
/// Distinct `CAST(c_varchar AS CHAR(n))` count (wrapper-routed CHAR expression):
/// equals the raw `c_varchar` distinct count because the seeded values have no
/// trailing spaces, so fixed-width padding is injective over them.
pub fn typed_varchar_char_distinct() -> i64 {
    typed_varchar_distinct()
}
/// Distinct `c_price * c_qty` count (wrapper-routed numeric expression), rows where
/// either operand is NULL excluded.
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
/// Distinct `CASE WHEN c_bool THEN c_ts ELSE NULL END` count (wrapper-routed
/// temporal expression): the distinct millisecond-resolution timestamps among rows
/// whose `c_bool` is true. Proves sub-second (millisecond) distinctions survive the
/// wrapper's native dedup across the shard boundary, with no string intermediate.
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

/// Seed the `typed_distinct_probe` table into the `e2e_lakehouse` namespace across
/// TWO data files (rows 1..=6, 7..=12). Idempotent.
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

/// Build the Arrow `RecordBatch` for `typed_distinct_probe` rows `[start, end)`,
/// preserving each column's per-row NULLs. Arrow types mirror the Iceberg schema
/// exactly so the parquet writer accepts the batch.
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

/// Replace a table's current schema via a raw Iceberg REST commit.
///
/// Expressed as `add-schema` (the new schema, field-ids preserved for stable
/// fields) + `set-current-schema` (`schema-id: -1` = the just-added schema),
/// guarded by an `assert-current-schema-id` requirement. iceberg-rust exposes no
/// public API to build a `TableCommit`, so we POST the commit body directly. This
/// is the generic schema-evolution primitive behind both the column-rename repro
/// (`seed_renamed_column`) and the add-columns initial-default fixture
/// (`seed_added_columns_initial_default`).
pub async fn rest_replace_current_schema(
    catalog_url: &str,
    namespace: &str,
    table_name: &str,
    current_schema_id: i32,
    new_schema: IcebergSchema,
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

// ---------------------------------------------------------------------------
// CHAR-padding probe table (fix-192-char-type-pushdown, Task 7)
// ---------------------------------------------------------------------------
//
// A minimal dedicated table for the CHAR-declared group-key padding E2E tests
// (Task 8). `events`'s own `name` values are all exactly 8 characters
// (`event-NN`), so they can never exhibit a padding/merge divergence — this
// table exists solely to carry a trailing-space pair (which a correct CHAR(n)
// pad must merge into one group) and an over-length value (which a correct pad
// must leave unmodified, so Exasol's own `CAST(... AS CHAR(n))` still raises
// its 22001 truncation error rather than silently truncating). Sized so ONE
// table serves both Task 8 queries: a `CHAR(30)` cast fits every seeded value
// (isolating the merge behavior), while a `CHAR(20)` cast makes exactly the
// 25-character row over-length (isolating the truncation behavior).
//
// Deliberately NOT added to `events`, `labels`, or `regions`: those tables'
// row counts (`SEED_TOTAL_ROWS`, `SEED_LABELS_ROWS`) and partition-pruning id
// ranges are asserted by existing tests and must not shift.

/// Table name for the CHAR-padding probe (Task 7, fix-192-char-type-pushdown).
pub const E2E_CHAR_PAD_TABLE: &str = "char_pad_probe";
/// The probe's sole string column.
pub const CHAR_PAD_COL: &str = "val";
/// Row 1: a short value. Once space-padded to a common `CHAR(n)` width, it is
/// identical to `CHAR_PAD_SHORT_TRAILING_SPACE` — a correct CHAR(n) group-key
/// pad must merge the two into ONE group, matching native Exasol.
pub const CHAR_PAD_SHORT: &str = "ab";
/// Row 2: `CHAR_PAD_SHORT` plus trailing spaces already present in the source
/// data (distinct from the width-normalizing pad the fix itself adds).
pub const CHAR_PAD_SHORT_TRAILING_SPACE: &str = "ab   ";
/// Row 3: a distinct short value forming its own singleton group.
pub const CHAR_PAD_OTHER: &str = "cd";
/// Row 4: a 25-character value — over-length for a `CHAR(20)` cast (making the
/// truncation-error scenario testable) but not for a `CHAR(30)` cast (which
/// fits every seeded value and isolates the merge scenario from truncation).
pub const CHAR_PAD_OVER_LENGTH: &str = "over-length-value-abcdefg";
/// Total rows in the CHAR-padding probe.
pub const CHAR_PAD_TOTAL_ROWS: usize = 4;

/// Seed the CHAR-padding probe table (Task 7, fix-192-char-type-pushdown): a
/// single `val` VARCHAR column carrying `CHAR_PAD_SHORT`,
/// `CHAR_PAD_SHORT_TRAILING_SPACE`, `CHAR_PAD_OTHER`, and
/// `CHAR_PAD_OVER_LENGTH`, in one data file. Idempotent (via
/// `create_and_append_files`).
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

/// Build the single-file, 4-row `char_pad_probe` batch. See the module note
/// above for why each value is present.
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

/// Namespace and table for the non-ASCII (`ß`) identifier E2E coverage
/// (`refactor-col-types-guard-dedup` task 7). Its OWN namespace, so this table
/// never enters any other suite's `createVirtualSchema` table enumeration —
/// every other E2E virtual schema is created over [`E2E_NAMESPACE`].
pub const E2E_NONASCII_NAMESPACE: &str = "e2e_nonascii";
/// Both the TABLE name and the COLUMN name under test are this same
/// non-ASCII identifier, so the table and the seeder cannot drift apart.
pub const E2E_NONASCII_TABLE: &str = "straße";
pub const NONASCII_COL: &str = E2E_NONASCII_TABLE;

/// Seeded values for the `straße` column, prefixed so a `LIKE` predicate
/// selects a proper subset ([`NONASCII_LIKE_PATTERN`] matches the first two).
pub const NONASCII_VALUES: [&str; 4] = ["alpha-1", "alpha-2", "beta-1", "beta-2"];
pub const NONASCII_TOTAL_ROWS: i64 = NONASCII_VALUES.len() as i64;
pub const NONASCII_LIKE_PATTERN: &str = "alpha%";
pub const NONASCII_LIKE_MATCH_COUNT: i64 = 2;

/// Seed the `straße` table (`id`, `straße`) into its own `e2e_nonascii`
/// namespace. Idempotent.
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

// ---------------------------------------------------------------------------
// Complex-types probe (list/struct/map) — nested JSON rendering E2E fixture
// ---------------------------------------------------------------------------

/// Table name for the nested-type JSON rendering E2E probe.
pub const E2E_COMPLEX_TABLE: &str = "complex_probe";

/// Row id whose every nested column is fully populated.
pub const COMPLEX_ROW_POPULATED: i64 = 1;
/// Row id whose every nested column is SQL NULL (the whole cell, not a member).
pub const COMPLEX_ROW_NULL: i64 = 2;
/// Row id whose list/map columns are empty collections and whose `addr` struct
/// carries one NULL member field (`city`).
pub const COMPLEX_ROW_EMPTY: i64 = 3;
/// Row id carrying a second, DISTINCT populated value per column, so a predicate,
/// `GROUP BY`, `ORDER BY`, or `COUNT(DISTINCT)` over a nested column has more than
/// one non-null value to discriminate.
pub const COMPLEX_ROW_ALT: i64 = 4;
/// Total rows in the complex-types probe table.
pub const COMPLEX_TOTAL_ROWS: usize = 4;

/// Seed the `complex_probe` table into the `e2e_lakehouse` namespace: one primitive
/// `id` control column plus a `list<string>`, a `list<int>`, a
/// `struct<street: string, city: string>`, a `map<string, string>`, a
/// `map<int, string>`, and a `list<struct<a: int>>` — every shape
/// `datafusion-scan/nested-json-rendering` renders.
///
/// `iceberg-rest-fixture` assigns FRESH field-ids on `create_table`, and
/// `overlay_iceberg_field_ids` repairs only TOP-LEVEL ids by name, so a batch built
/// from the schema as AUTHORED below fails nested field-id lookup. This seed
/// therefore builds its Arrow batch from `schema_to_arrow_schema(table.metadata()
/// .current_schema())` AFTER `create_table` returns, so every nested field-id in
/// the batch matches what Iceberg actually assigned. Idempotent.
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

    // Check again after load (race).
    let existing = collect_current_snapshot_paths(&table).await?;
    if !existing.is_empty() {
        return Ok(());
    }

    write_complex_types_and_commit(&catalog, table).await
}

/// The complex-types probe's Iceberg schema, as AUTHORED. `create_table` assigns
/// its own field-ids; `write_complex_types_and_commit` re-derives the Arrow schema
/// from the CREATED table rather than from this one.
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

/// Build the complex-types Arrow batch from the CREATED table's own schema, then
/// write and commit it as one Parquet data file.
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

/// Decode the complex-types probe rows from JSON straight into `schema` — the
/// Arrow schema `schema_to_arrow_schema` derived from the CREATED table, so every
/// nested field the decoder builds already carries the field-id Iceberg assigned.
///
/// Row layout: [`COMPLEX_ROW_POPULATED`] is fully populated; [`COMPLEX_ROW_NULL`]
/// is SQL NULL in every nested column; [`COMPLEX_ROW_EMPTY`] carries an empty list
/// and an empty map in every list/map column plus a NULL `city` member inside an
/// otherwise-populated `addr` struct; [`COMPLEX_ROW_ALT`] is a second, DISTINCT
/// populated row.
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

/// Table name for the nested-type probe's JOIN partner.
pub const E2E_COMPLEX_JOIN_TABLE: &str = "complex_join_probe";

/// The rendered `tags` document [`COMPLEX_ROW_POPULATED`] carries, held in the join
/// partner as a plain `string`.
pub const COMPLEX_JOIN_POPULATED_DOC: &str = r#"["hello","world"]"#;
/// The rendered `tags` document [`COMPLEX_ROW_ALT`] carries.
pub const COMPLEX_JOIN_ALT_DOC: &str = r#"["foo","bar","baz"]"#;
/// A document no `complex_probe` row renders, so a join over the nested column has
/// to DISCRIMINATE rather than pair every row with every row.
pub const COMPLEX_JOIN_ORPHAN_DOC: &str = r#"["never","matched"]"#;

/// Seed the `complex_join_probe` table (`tag_doc`, `label`) into the
/// `e2e_lakehouse` namespace: a SECOND, distinct table whose plain `string` column
/// holds the documents `complex_probe`'s `tags` column renders to, so a join
/// CONDITION over a nested column can be exercised without aliasing that table to
/// itself. Idempotent.
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

/// Namespace and table for the timestamp-precision E2E probe
/// (`add-timestamp-precision-versioning` task 6). Its OWN namespace, so this
/// table never enters any other suite's `createVirtualSchema` table
/// enumeration — every other E2E virtual schema is created over
/// [`E2E_NAMESPACE`]. Column names and the seeded microsecond values mirror
/// task 1's live-verification capture exactly (see decision-log.md's "Task 1
/// Live Captures" section) so tasks 7/9 can rely on this fixture
/// deterministically.
pub const E2E_TSPRECISION_NAMESPACE: &str = "e2e_tsprecision";
pub const E2E_TSPRECISION_TABLE: &str = "ts_precision_probe";
pub const TSPRECISION_COL_TS: &str = "ts";
pub const TSPRECISION_COL_TSTZ: &str = "tstz";

/// Microseconds since UNIX_EPOCH for `2024-01-01 00:00:00.000001`,
/// `.000002`, `.123456`, `.123457` — two pairs that collapse to the same
/// millisecond prefix under `TIMESTAMP(3)` truncation but stay four distinct
/// values at `TIMESTAMP(6)`.
pub const TSPRECISION_MICROS: [i64; 4] = [
    BASE_TS_MICROS + 1,
    BASE_TS_MICROS + 2,
    BASE_TS_MICROS + 123_456,
    BASE_TS_MICROS + 123_457,
];

/// Seed the timestamp-precision probe (`id`, `ts`, `tstz`) into its own
/// `e2e_tsprecision` namespace. Idempotent.
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
        ])
        .build()
        .context("build timestamp precision probe Iceberg schema")?;

    create_and_append(
        &catalog,
        E2E_TSPRECISION_NAMESPACE,
        E2E_TSPRECISION_TABLE,
        iceberg_schema,
        vec![make_timestamp_precision_probe_batch()],
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
        // No catalog auth in the baseline (matches today's build_seed_catalog).
        assert!(get(&props, "credential").is_none());
        assert!(get(&props, "oauth2-server-uri").is_none());
        assert!(get(&props, "scope").is_none());
        assert!(get(&props, "token").is_none());
        // The storage arms are mutually exclusive: no ADLS property rides along on
        // the MinIO baseline.
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
        // Catalog auth is orthogonal to storage: an ADLS seed still needs its
        // Lakekeeper bearer token.
        assert_eq!(get(&props, "token"), Some("bearer-xyz"));

        // `azdls_config_parse` discards `s3.*` properties silently, so a stray one
        // wouldn't fail the seed — it would just leak MinIO admin credentials into
        // an Azure run invisibly.
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
        // seed_events_table_with_auth writes the SAME two-file events data
        // regardless of `auth`, so the Lakekeeper scan tests can assert against
        // the documented constants. Guard the shape those assertions rely on: the
        // two files together hold SEED_TOTAL_ROWS rows, of which
        // SEED_ROWS_SCORE_GT_15 score > 15.0. A drift in make_events_batch (row
        // count or score formula) fails here rather than silently breaking the
        // Lakekeeper E2E assertions.
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
