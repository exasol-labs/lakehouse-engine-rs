//! AWS Glue E2E harness: every `GlueRun` owns one Glue database and one S3 prefix and deletes
//! both when it drops, so two concurrent runs never share a fixture (`specs/testing.md` § Per-run cloud resources).

use super::cloud_fixture::{
    derive_run_segment, per_run_segment, require_var, run_teardown_off_runtime,
};
use super::raw_parquet::{encode_parquet, put_object};
use super::seed::{
    all_types_ids, all_types_validity, binary_values, boolean_values, date_values,
    decimal_10_2_values, decimal_38_10_values, float32_values, int_list_values,
    int_string_struct_values, int8_values, int16_values, int32_values, non_utf8_parquet,
    string_int_map_values, text_values, timestamp_values, write_one_file_append,
};
use super::stack::CatalogConnectionPassword;

use anyhow::{Context, Result, bail};
use arrow::array::{
    ArrayRef, BinaryArray, Decimal128Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use aws_sdk_glue::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_glue::error::ProvideErrorMetadata;
use aws_sdk_glue::types::builders::TableInputBuilder;
use aws_sdk_glue::types::{
    Column, DatabaseInput, PartitionInput, SerDeInfo, StorageDescriptor, TableInput,
};
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt, stream, try_join};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_REGION,
    S3_SECRET_ACCESS_KEY,
};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_storage_opendal::OpenDalStorageFactory;
use lakehouse_catalog::{HIVE_DEFAULT_PARTITION, redact_error_text};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectStorePath;

use std::collections::HashMap;
use std::sync::Arc;

pub const ACCESS_KEY_ID_VAR: &str = "GLUE_ACCESS_KEY_ID";
pub const SECRET_ACCESS_KEY_VAR: &str = "GLUE_SECRET_ACCESS_KEY";
pub const REGION_VAR: &str = "GLUE_REGION";
pub const FIXTURE_BUCKET_VAR: &str = "GLUE_FIXTURE_BUCKET";
pub const ASSUME_ROLE_BASE_ACCESS_KEY_ID_VAR: &str = "GLUE_ASSUME_ROLE_BASE_ACCESS_KEY_ID";
pub const ASSUME_ROLE_BASE_SECRET_ACCESS_KEY_VAR: &str = "GLUE_ASSUME_ROLE_BASE_SECRET_ACCESS_KEY";
pub const ASSUME_ROLE_ARN_VAR: &str = "GLUE_ASSUME_ROLE_ARN";
pub const ASSUME_ROLE_EXTERNAL_ID_VAR: &str = "GLUE_ASSUME_ROLE_EXTERNAL_ID";

/// Glue's database-name length limit.
const MAX_DATABASE_NAME_LEN: usize = 255;

/// Also the pattern `glue-orphan-sweep.yml` deletes after 24 hours.
const DATABASE_PREFIX: &str = "lh_e2e_";

/// Also the prefix `glue-orphan-sweep.yml` deletes after 24 hours.
const OBJECT_PREFIX_ROOT: &str = "lh_e2e";

const CREDENTIALS_PROVIDER: &str = "glue-e2e-harness";
const ALREADY_EXISTS_CODE: &str = "AlreadyExistsException";
const NOT_FOUND_CODE: &str = "EntityNotFoundException";

/// Plain owned data so a clone can cross into the teardown thread. It derives no `Debug`, so
/// the secret access key can never be formatted into test output.
#[derive(Clone)]
pub struct GlueEnv {
    pub access_key_id: String,
    secret_access_key: String,
    pub region: String,
    pub fixture_bucket: String,
    /// Holds only `sts:AssumeRole` on `assume_role_arn`, so it reaches Glue and S3 only as the role.
    pub assume_role_base_access_key_id: String,
    assume_role_base_secret_access_key: String,
    pub assume_role_arn: String,
    assume_role_external_id: String,
}

impl GlueEnv {
    /// Reads all four variables up front, so a missing one fails the suite before any caller
    /// can create a cloud resource.
    pub fn from_environment() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Takes the lookup as a parameter so a probe can substitute sentinel values without
    /// mutating the shared process environment.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let read = |name: &str| require_var("glue-e2e", name, lookup(name).as_deref());
        Self {
            access_key_id: read(ACCESS_KEY_ID_VAR),
            secret_access_key: read(SECRET_ACCESS_KEY_VAR),
            region: read(REGION_VAR),
            fixture_bucket: read(FIXTURE_BUCKET_VAR),
            assume_role_base_access_key_id: read(ASSUME_ROLE_BASE_ACCESS_KEY_ID_VAR),
            assume_role_base_secret_access_key: read(ASSUME_ROLE_BASE_SECRET_ACCESS_KEY_VAR),
            assume_role_arn: read(ASSUME_ROLE_ARN_VAR),
            assume_role_external_id: read(ASSUME_ROLE_EXTERNAL_ID_VAR),
        }
    }

    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    /// The address a `GLUE` CONNECTION names; the adapter derives its signing region from it.
    pub fn glue_endpoint(&self) -> String {
        format!("https://glue.{}.amazonaws.com", self.region)
    }

    pub fn redact(&self, text: &str) -> String {
        redact_error_text(
            text,
            &[
                &self.secret_access_key,
                &self.assume_role_base_secret_access_key,
                &self.assume_role_external_id,
            ],
        )
    }

    /// Panics with the redacted error chain, which may echo a request carrying the secret.
    #[track_caller]
    pub fn expect<T>(&self, result: Result<T>, label: &str) -> T {
        match result {
            Ok(value) => value,
            Err(error) => panic!("{}", self.redact(&format!("{label}: {error:#}"))),
        }
    }

    /// Never cached on `GlueEnv`: an SDK client's connection pool runs on the runtime that
    /// created it, and `GlueRun`'s teardown runs on a runtime of its own.
    pub fn glue_client(&self) -> aws_sdk_glue::Client {
        let config = aws_sdk_glue::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                self.access_key_id.clone(),
                self.secret_access_key.clone(),
                None,
                None,
                CREDENTIALS_PROVIDER,
            ))
            .region(Region::new(self.region.clone()))
            .build();
        aws_sdk_glue::Client::from_conf(config)
    }

    pub fn fixture_store(&self) -> Result<AmazonS3> {
        AmazonS3Builder::new()
            .with_bucket_name(&self.fixture_bucket)
            .with_region(&self.region)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
            .build()
            .with_context(|| format!("build the S3 store for bucket {}", self.fixture_bucket))
    }

    pub async fn database_exists(&self, database: &str) -> Result<bool> {
        let found = tolerate_absent(
            self.glue_client()
                .get_database()
                .name(database)
                .send()
                .await,
            &format!("GetDatabase {database}"),
        )?;
        Ok(found.is_some())
    }

    pub async fn object_keys(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .objects_under(&self.fixture_store()?, prefix)
            .await?
            .into_iter()
            .map(|object| object.to_string())
            .collect())
    }

    /// The listed paths themselves, never keys rebuilt from their text: `Path::from` would
    /// percent-encode the `%` of a raw key such as `p_str=a b%2Fc`.
    async fn objects_under(&self, store: &AmazonS3, prefix: &str) -> Result<Vec<ObjectStorePath>> {
        let objects: Vec<_> = store
            .list(Some(&ObjectStorePath::from(prefix)))
            .try_collect()
            .await
            .with_context(|| format!("list s3://{}/{prefix}", self.fixture_bucket))?;
        Ok(objects.into_iter().map(|object| object.location).collect())
    }
}

/// `None` for Glue's not-found error. Unredacted, like every error here: each exits through
/// `GlueEnv::expect` or the teardown's leak report, which redact the whole chain.
fn tolerate_absent<T, E>(result: std::result::Result<T, E>, operation: &str) -> Result<Option<T>>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.code() == Some(NOT_FOUND_CODE) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("Glue {operation}")),
    }
}

/// Leaves room for `DATABASE_PREFIX`, which already ends in `_`.
const RUN_ID_LEN: usize = MAX_DATABASE_NAME_LEN - DATABASE_PREFIX.len();

/// Athena and Hive accept only lowercase letters, digits, and `_` in a database name, so the
/// user segment is folded into that alphabet within Glue's length limit.
fn per_run_id() -> String {
    per_run_segment('_', RUN_ID_LEN)
}

fn derive_run_id(user: &str, millis: u128) -> String {
    derive_run_segment(user, millis, '_', RUN_ID_LEN)
}

pub fn database_name(run_id: &str) -> String {
    format!("{DATABASE_PREFIX}{run_id}")
}

/// Hold it on the test's stack, never in a static: statics never drop, so the run would leak.
/// A killed process still orphans it, which `glue-orphan-sweep.yml` deletes after 24 hours.
pub struct GlueRun {
    env: GlueEnv,
    run_id: String,
}

impl GlueRun {
    /// An existing database fails the run rather than being adopted, since `Drop` would delete it.
    pub async fn create(env: &GlueEnv) -> Result<Self> {
        let run_id = per_run_id();
        let database = database_name(&run_id);
        let input = DatabaseInput::builder()
            .name(&database)
            .description("lakehouse-engine glue-e2e run fixture")
            .build()
            .context("describe the per-run Glue database")?;
        match env
            .glue_client()
            .create_database()
            .database_input(input)
            .send()
            .await
        {
            Ok(_) => Ok(Self {
                env: env.clone(),
                run_id,
            }),
            Err(error) if error.code() == Some(ALREADY_EXISTS_CODE) => bail!(
                "Glue database {database} already exists: the per-run millisecond suffix makes a \
                 name collision a defect, not a state to adopt"
            ),
            Err(error) => Err(error).with_context(|| format!("Glue CreateDatabase {database}")),
        }
    }

    pub fn env(&self) -> &GlueEnv {
        &self.env
    }

    pub fn database(&self) -> String {
        database_name(&self.run_id)
    }

    /// The object-key prefix every fixture object of this run lives under, with its trailing `/`.
    pub fn object_prefix(&self) -> String {
        format!("{OBJECT_PREFIX_ROOT}/{}/", self.run_id)
    }

    pub fn uri(&self, relative_key: &str) -> String {
        format!(
            "s3://{}/{}{relative_key}",
            self.env.fixture_bucket,
            self.object_prefix()
        )
    }

    /// The location of the run's table `name`, with its trailing `/`.
    pub fn table_location(&self, name: &str) -> String {
        self.uri(&format!("{name}/"))
    }
}

impl Drop for GlueRun {
    fn drop(&mut self) {
        let env = self.env.clone();
        let database = self.database();
        let prefix = self.object_prefix();
        let resources = format!(
            "Glue database {database} and S3 prefix s3://{}/{prefix}",
            env.fixture_bucket
        );
        let leaks = run_teardown_off_runtime("glue-run-teardown", async move {
            delete_run_resources(&env, &database, &prefix).await
        })
        .unwrap_or_else(|failure| vec![format!("{resources}: {failure}")]);
        for leak in leaks {
            eprintln!("LEAKED {}", self.env.redact(&leak));
        }
    }
}

/// Each resource is deleted even when the other's delete failed, so one failure never leaks both.
async fn delete_run_resources(env: &GlueEnv, database: &str, prefix: &str) -> Vec<String> {
    let mut leaks = Vec::new();
    if let Err(error) = delete_database(env, database).await {
        leaks.push(format!("Glue database {database}: {error:#}"));
    }
    if let Err(error) = delete_prefix(env, prefix).await {
        leaks.push(format!(
            "S3 prefix s3://{}/{prefix}: {error:#}",
            env.fixture_bucket
        ));
    }
    leaks
}

async fn delete_database(env: &GlueEnv, database: &str) -> Result<()> {
    let client = env.glue_client();
    let tables = database_table_names(&client, database).await?;
    let client = &client;
    try_join_all(tables.iter().map(|table| async move {
        tolerate_absent(
            client
                .delete_table()
                .database_name(database)
                .name(table)
                .send()
                .await,
            &format!("DeleteTable {database}.{table}"),
        )
    }))
    .await?;
    tolerate_absent(
        client.delete_database().name(database).send().await,
        &format!("DeleteDatabase {database}"),
    )?;
    Ok(())
}

/// A database already absent has no table left to delete.
async fn database_table_names(
    client: &aws_sdk_glue::Client,
    database: &str,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut token = None;
    loop {
        let Some(page) = tolerate_absent(
            client
                .get_tables()
                .database_name(database)
                .set_next_token(token.take())
                .send()
                .await,
            &format!("GetTables {database}"),
        )?
        else {
            return Ok(Vec::new());
        };
        names.extend(
            page.table_list()
                .iter()
                .map(|table| table.name().to_string()),
        );
        token = page
            .next_token()
            .filter(|next| !next.is_empty())
            .map(str::to_string);
        if token.is_none() || page.table_list().is_empty() {
            return Ok(names);
        }
    }
}

async fn delete_prefix(env: &GlueEnv, prefix: &str) -> Result<()> {
    let store = env.fixture_store()?;
    let objects = env.objects_under(&store, prefix).await?;
    store
        .delete_stream(stream::iter(objects.into_iter().map(Ok)).boxed())
        .try_collect::<Vec<_>>()
        .await
        .with_context(|| format!("DELETE under s3://{}/{prefix}", env.fixture_bucket))?;
    let remaining = env.objects_under(&store, prefix).await?;
    if !remaining.is_empty() {
        bail!("{} object(s) remain after the delete", remaining.len());
    }
    Ok(())
}

const PARQUET_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
const PARQUET_OUTPUT_FORMAT: &str =
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
const PARQUET_SERDE: &str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
pub const ORC_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
const ORC_OUTPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
const ORC_SERDE: &str = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
const EXTERNAL_TABLE: &str = "EXTERNAL_TABLE";
const VIRTUAL_VIEW: &str = "VIRTUAL_VIEW";

pub const ICEBERG_ORDERS: &str = "iceberg_orders";
pub const ALL_TYPES: &str = "all_types";
pub const BINARY_VALUES: &str = "binary_values";
pub const PARTITIONED: &str = "partitioned";
const PROJECTED: &str = "projected";
const A_VIEW: &str = "a_view";
const ORC_TABLE: &str = "orc_table";
const DELTA_TABLE: &str = "delta_table";

/// The tables the listing admits, in name order.
pub const ROUTED_TABLES: [&str; 4] = [ALL_TYPES, BINARY_VALUES, ICEBERG_ORDERS, PARTITIONED];

/// The metadata-only registrations the listing skips, each with a fragment of its reason.
pub const SKIPPED_TABLES: [(&str, &str); 4] = [
    (A_VIEW, "TableType=VIRTUAL_VIEW"),
    (DELTA_TABLE, "table_type=DELTA"),
    (
        ORC_TABLE,
        "InputFormat=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    ),
    (PROJECTED, "projection.enabled=true"),
];

/// Glue's own copy of the Iceberg columns: the listing must ignore it for `metadata.json`.
pub const STALE_GLUE_COLUMN: &str = "stale_glue_column";

pub struct Order {
    pub order_id: i64,
    pub customer: Option<&'static str>,
    pub amount_hundredths: i64,
    pub order_date: &'static str,
}

impl Order {
    pub fn amount(&self) -> String {
        format!(
            "{}.{:02}",
            self.amount_hundredths / 100,
            self.amount_hundredths % 100
        )
    }
}

const fn order(
    order_id: i64,
    customer: Option<&'static str>,
    amount_hundredths: i64,
    order_date: &'static str,
) -> Order {
    Order {
        order_id,
        customer,
        amount_hundredths,
        order_date,
    }
}

pub const ORDERS: [Order; 5] = [
    order(1, Some("ada"), 1225, "2024-01-01"),
    order(2, Some("bob"), 2575, "2024-01-02"),
    order(3, Some("cyd"), 9999, "2024-01-03"),
    order(4, Some("dee"), 1995, "2024-01-04"),
    order(5, None, 4215, "2024-01-05"),
];

/// One `all_types` column per Hive type of `glue/glue-hive-type-mapping`; `data` is
/// `None` for a column the data file leaves out, because the reader refuses it at plan time.
struct HiveTypeColumn {
    column: &'static str,
    hive_type: &'static str,
    data: Option<HiveColumnData>,
}

type HiveColumnData = fn() -> ArrayRef;

const fn hive(
    column: &'static str,
    hive_type: &'static str,
    data: Option<HiveColumnData>,
) -> HiveTypeColumn {
    HiveTypeColumn {
        column,
        hive_type,
        data,
    }
}

const HIVE_TYPE_COLUMNS: &[HiveTypeColumn] = &[
    hive("h_tinyint", "tinyint", Some(int8_values)),
    hive("h_smallint", "smallint", Some(int16_values)),
    hive("h_int", "int", Some(int32_values)),
    hive(
        "h_integer",
        "integer",
        Some(|| Arc::new(Int32Array::from(vec![Some(7), Some(-7), None]))),
    ),
    hive(
        "h_bigint",
        "bigint",
        Some(|| Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN), None]))),
    ),
    hive("h_float", "float", Some(float32_values)),
    hive(
        "h_double",
        "double",
        Some(|| Arc::new(Float64Array::from(vec![Some(2.5), Some(-0.125), None]))),
    ),
    hive("h_boolean", "boolean", Some(boolean_values)),
    hive("h_string", "string", Some(text_values)),
    // A legacy writer's string: a `BYTE_ARRAY` without the string annotation.
    hive(
        "h_string_over_binary",
        "string",
        Some(|| {
            Arc::new(BinaryArray::from_opt_vec(vec![
                Some(b"legacy-a".as_slice()),
                Some(b"legacy-b".as_slice()),
                None,
            ]))
        }),
    ),
    hive(
        "h_varchar",
        "varchar(10)",
        Some(|| Arc::new(StringArray::from(vec![Some("short"), Some("text"), None]))),
    ),
    hive(
        "h_char",
        "char(5)",
        Some(|| Arc::new(StringArray::from(vec![Some("abcde"), Some("fghij"), None]))),
    ),
    hive("h_decimal_10_2", "decimal(10,2)", Some(decimal_10_2_values)),
    hive(
        "h_decimal",
        "decimal",
        Some(|| {
            Arc::new(
                Decimal128Array::from(vec![Some(42), Some(-42), None])
                    .with_precision_and_scale(10, 0)
                    .expect("Decimal128(10,0)"),
            )
        }),
    ),
    hive(
        "h_decimal_spaced",
        "DECIMAL( 38 , 10 )",
        Some(decimal_38_10_values),
    ),
    hive("h_date", "date", Some(date_values)),
    hive("h_timestamp", "timestamp", Some(|| timestamp_values(None))),
    hive(
        "h_binary",
        "binary",
        Some(|| binary_values(&DataType::Binary)),
    ),
    hive("h_array_int", "array<int>", Some(int_list_values)),
    hive(
        "h_array_struct",
        "array<struct<a:decimal(5,2)>>",
        Some(h_array_struct_data),
    ),
    hive(
        "h_map_string",
        "map<string,int>",
        Some(|| string_int_map_values(vec!["k1", "k2"], vec![1, 2], [2, 0, 0])),
    ),
    hive(
        "h_map_varchar",
        "map<varchar(1),int>",
        Some(|| string_int_map_values(vec!["a"], vec![1], [1, 0, 0])),
    ),
    hive(
        "h_struct_xy",
        "struct<x:int,y:string>",
        Some(|| int_string_struct_values("x", "y", "p")),
    ),
    hive("h_struct_binary", "struct<b:binary>", None),
    hive("h_uniontype", "uniontype<int,string>", None),
    hive("h_interval", "interval_day_time", None),
    hive("h_malformed_map", "map<int>", None),
    hive("h_empty_type", "", None),
];

pub struct PartitionedRow {
    pub id: i64,
    pub v: &'static str,
}

/// Where a partition's registered location points. `OutsideTable` is relative to the run
/// prefix; `UnderTable` and `S3a`, an `s3a://` address of the same store, to the table location.
#[derive(Clone, Copy)]
pub enum PartitionPlace {
    UnderTable(&'static str),
    OutsideTable(&'static str),
    S3a(&'static str),
}

/// One data file: an extensionless or `.parquet` name and the rows it holds.
pub struct PartitionFile {
    pub name: &'static str,
    pub rows: &'static [PartitionedRow],
}

pub struct FixturePartition {
    pub values: [&'static str; 3],
    pub place: PartitionPlace,
    pub input_format: &'static str,
    pub files: &'static [PartitionFile],
}

/// The `p_int=9` ORC partition holds no file; every other partition has `p_int < 9`, so a
/// query carrying `P_INT < 9` prunes it.
const fn row(id: i64, v: &'static str) -> PartitionedRow {
    PartitionedRow { id, v }
}

const fn file(name: &'static str, rows: &'static [PartitionedRow]) -> PartitionFile {
    PartitionFile { name, rows }
}

const fn parquet_partition(
    values: [&'static str; 3],
    place: PartitionPlace,
    files: &'static [PartitionFile],
) -> FixturePartition {
    FixturePartition {
        values,
        place,
        input_format: PARQUET_INPUT_FORMAT,
        files,
    }
}

pub static PARTITIONS: [FixturePartition; 6] = [
    parquet_partition(
        ["1", "2024-01-01", "alpha"],
        PartitionPlace::UnderTable("p_int=1/p_date=2024-01-01/p_str=alpha/"),
        &[
            file("20240101_000000_00001_p1", &[row(1, "a"), row(2, "b")]),
            file("part-00000-p1.snappy.parquet", &[row(3, "c")]),
        ],
    ),
    parquet_partition(
        ["1", "2024-01-02", HIVE_DEFAULT_PARTITION],
        PartitionPlace::UnderTable("p_int=1/p_date=2024-01-02/p_str=__HIVE_DEFAULT_PARTITION__/"),
        &[file("20240102_000000_00001_p2", &[row(4, "d")])],
    ),
    parquet_partition(
        ["2", "2024-01-01", "a b/c"],
        PartitionPlace::UnderTable("p_int=2/p_date=2024-01-01/p_str=a b%2Fc/"),
        &[file("20240101_000000_00001_p3", &[row(5, "e")])],
    ),
    parquet_partition(
        ["3", "2024-02-01", "outside"],
        PartitionPlace::OutsideTable("outside_root/partition_4/"),
        &[file("20240201_000000_00001_p4", &[row(6, "f")])],
    ),
    parquet_partition(
        ["4", "2024-02-02", "s3a"],
        PartitionPlace::S3a("p_int=4/p_date=2024-02-02/p_str=s3a/"),
        &[file("20240202_000000_00001_p5", &[row(7, "g")])],
    ),
    FixturePartition {
        input_format: ORC_INPUT_FORMAT,
        ..parquet_partition(
            ["9", "2024-03-01", "orc"],
            PartitionPlace::UnderTable("p_int=9/p_date=2024-03-01/p_str=orc/"),
            &[],
        )
    },
];

const PARTITION_KEYS: [(&str, &str); 3] =
    [("p_int", "int"), ("p_date", "date"), ("p_str", "string")];

/// A Hadoop marker in the first partition's location, which `FilePattern::AnyDirectChild` must
/// skip.
pub const SUCCESS_MARKER: &str = "_SUCCESS";

impl FixturePartition {
    pub fn p_int(&self) -> i64 {
        self.values[0]
            .parse()
            .expect("a fixture p_int is an integer")
    }

    pub fn p_date(&self) -> &'static str {
        self.values[1]
    }

    pub fn p_str(&self) -> Option<&'static str> {
        Some(self.values[2]).filter(|value| *value != HIVE_DEFAULT_PARTITION)
    }
}

/// Every data file's rows in id order, each with the partition whose Glue values it reads.
pub fn partitioned_rows() -> Vec<(&'static FixturePartition, &'static PartitionedRow)> {
    let mut rows: Vec<_> = PARTITIONS
        .iter()
        .flat_map(|partition| {
            partition
                .files
                .iter()
                .flat_map(move |file| file.rows.iter().map(move |row| (partition, row)))
        })
        .collect();
    rows.sort_by_key(|(_, row)| row.id);
    rows
}

impl PartitionPlace {
    /// The object key relative to the run prefix.
    pub fn relative_key(self) -> String {
        match self {
            Self::UnderTable(relative) | Self::S3a(relative) => {
                format!("{PARTITIONED}/{relative}")
            }
            Self::OutsideTable(relative) => relative.to_string(),
        }
    }

    pub fn location(self, run: &GlueRun) -> String {
        let location = run.uri(&self.relative_key());
        match self {
            Self::S3a(_) => location.replacen("s3://", "s3a://", 1),
            Self::UnderTable(_) | Self::OutsideTable(_) => location,
        }
    }
}

/// The relative key of each metadata-only table and of the ORC partition, none of which may
/// hold a data file.
pub fn metadata_only_keys() -> Vec<String> {
    let mut keys: Vec<String> = SKIPPED_TABLES
        .iter()
        .map(|(table, _)| format!("{table}/"))
        .collect();
    keys.extend(
        PARTITIONS
            .iter()
            .filter(|partition| partition.files.is_empty())
            .map(|partition| partition.place.relative_key()),
    );
    keys
}

pub fn glue_connection_password(env: &GlueEnv) -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: String::new(),
        endpoint: String::new(),
        region: env.region.clone(),
        access_key: env.access_key_id.clone(),
        secret_key: env.secret_access_key.clone(),
        path_style: false,
        use_sigv4: true,
        use_vended_credentials: false,
        ..Default::default()
    }
}

/// The base identity names the role, so the adapter reaches Glue and S3 only as the role.
pub fn glue_assume_role_connection_password(env: &GlueEnv) -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        aws_assume_role_arn: Some(env.assume_role_arn.clone()),
        aws_external_id: Some(env.assume_role_external_id.clone()),
        ..glue_base_identity_connection_password(env)
    }
}

/// The base identity without its role, which Glue denies.
pub fn glue_base_identity_connection_password(env: &GlueEnv) -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        access_key: env.assume_role_base_access_key_id.clone(),
        secret_key: env.assume_role_base_secret_access_key.clone(),
        ..glue_connection_password(env)
    }
}

/// One Glue client and S3 store for a whole registration, which runs on a single runtime.
struct FixtureWriter<'a> {
    run: &'a GlueRun,
    glue: aws_sdk_glue::Client,
    store: AmazonS3,
}

impl<'a> FixtureWriter<'a> {
    fn new(run: &'a GlueRun) -> Result<Self> {
        Ok(Self {
            run,
            glue: run.env.glue_client(),
            store: run.env.fixture_store()?,
        })
    }

    async fn put_object(&self, relative_key: &str, bytes: bytes::Bytes) -> Result<()> {
        let key = format!("{}{relative_key}", self.run.object_prefix());
        put_object(&self.store, &key, bytes).await
    }

    async fn create_table(&self, input: TableInput) -> Result<()> {
        let name = input.name().to_string();
        let database = self.run.database();
        self.glue
            .create_table()
            .database_name(&database)
            .table_input(input)
            .send()
            .await
            .with_context(|| format!("Glue CreateTable {database}.{name}"))?;
        Ok(())
    }

    /// `BatchCreatePartition` reports a rejected partition in its response, not as an error,
    /// so a partial registration fails here instead of silently shrinking the fixture.
    async fn create_partitions(&self, table: &str, inputs: Vec<PartitionInput>) -> Result<()> {
        let database = self.run.database();
        let output = self
            .glue
            .batch_create_partition()
            .database_name(&database)
            .table_name(table)
            .set_partition_input_list(Some(inputs))
            .send()
            .await
            .with_context(|| format!("Glue BatchCreatePartition {database}.{table}"))?;
        if !output.errors().is_empty() {
            bail!(
                "BatchCreatePartition {database}.{table} rejected {} partition(s): {:?}",
                output.errors().len(),
                output.errors()
            );
        }
        Ok(())
    }
}

/// Registers the Glue E2E suite's whole fixture set in the run's database.
pub async fn register_fixture_set(run: &GlueRun) -> Result<()> {
    let writer = FixtureWriter::new(run)?;
    try_join!(
        register_iceberg_orders(&writer),
        register_all_types(&writer),
        register_binary_values(&writer),
        register_partitioned(&writer),
        register_metadata_only_tables(&writer),
    )?;
    Ok(())
}

/// One object and one Parquet table: enough for a teardown to have something of each kind.
pub async fn register_probe_table(run: &GlueRun) -> Result<()> {
    let writer = FixtureWriter::new(run)?;
    try_join!(
        writer.put_object(
            "probe/20240101_000000_00001_probe",
            bytes::Bytes::from_static(b"probe"),
        ),
        writer.create_table(parquet_table(
            "probe",
            &run.table_location("probe"),
            vec![hive_column("order_id", "bigint")?],
            Vec::new(),
        )?),
    )?;
    Ok(())
}

fn hive_column(name: &str, hive_type: &str) -> Result<Column> {
    Column::builder()
        .name(name)
        .r#type(hive_type)
        .build()
        .with_context(|| format!("describe Glue column {name}"))
}

fn storage_descriptor(
    location: &str,
    columns: Vec<Column>,
    (input_format, output_format, serde): (&str, &str, &str),
) -> StorageDescriptor {
    StorageDescriptor::builder()
        .set_columns(Some(columns))
        .location(location)
        .input_format(input_format)
        .output_format(output_format)
        .serde_info(SerDeInfo::builder().serialization_library(serde).build())
        .build()
}

fn parquet_formats() -> (&'static str, &'static str, &'static str) {
    (PARQUET_INPUT_FORMAT, PARQUET_OUTPUT_FORMAT, PARQUET_SERDE)
}

fn formats_of(input_format: &str) -> (&'static str, &'static str, &'static str) {
    if input_format == ORC_INPUT_FORMAT {
        (ORC_INPUT_FORMAT, ORC_OUTPUT_FORMAT, ORC_SERDE)
    } else {
        parquet_formats()
    }
}

fn external_table(
    name: &str,
    location: &str,
    columns: Vec<Column>,
    formats: (&str, &str, &str),
) -> TableInputBuilder {
    TableInput::builder()
        .name(name)
        .table_type(EXTERNAL_TABLE)
        .storage_descriptor(storage_descriptor(location, columns, formats))
}

fn parquet_table(
    name: &str,
    location: &str,
    columns: Vec<Column>,
    partition_keys: Vec<Column>,
) -> Result<TableInput> {
    external_table(name, location, columns, parquet_formats())
        .parameters("EXTERNAL", "TRUE")
        .parameters("classification", "parquet")
        .set_partition_keys(Some(partition_keys))
        .build()
        .with_context(|| format!("describe Glue table {name}"))
}

fn orders_schema() -> Result<IcebergSchema> {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "order_id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "customer", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(
                3,
                "amount",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
            )
            .into(),
            NestedField::optional(4, "order_date", Type::Primitive(PrimitiveType::Date)).into(),
        ])
        .build()
        .context("build the iceberg_orders schema")
}

fn orders_columns() -> Result<Vec<ArrayRef>> {
    Ok(vec![
        Arc::new(Int64Array::from_iter_values(
            ORDERS.iter().map(|o| o.order_id),
        )),
        Arc::new(StringArray::from_iter(ORDERS.iter().map(|o| o.customer))),
        Arc::new(
            Decimal128Array::from_iter_values(
                ORDERS.iter().map(|o| i128::from(o.amount_hundredths)),
            )
            .with_precision_and_scale(10, 2)
            .context("type the amount column")?,
        ),
        cast(
            &StringArray::from_iter_values(ORDERS.iter().map(|o| o.order_date)),
            &DataType::Date32,
        )
        .context("type the order_date column")?,
    ])
}

/// `iceberg-rust` writes the table through its memory catalog on S3 `FileIO`; Glue then holds
/// only the pointer to the resulting `metadata.json`, as an Athena registration does.
async fn register_iceberg_orders(writer: &FixtureWriter<'_>) -> Result<()> {
    let run = writer.run;
    let env = run.env();
    let props = HashMap::from([
        (MEMORY_CATALOG_WAREHOUSE.to_string(), run.uri("iceberg")),
        (S3_REGION.to_string(), env.region.clone()),
        (S3_ACCESS_KEY_ID.to_string(), env.access_key_id.clone()),
        (
            S3_SECRET_ACCESS_KEY.to_string(),
            env.secret_access_key.clone(),
        ),
        (S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string()),
        (S3_DISABLE_EC2_METADATA.to_string(), "true".to_string()),
    ]);
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("glue-e2e-iceberg", props)
        .await
        .context("open the memory catalog")?;
    let namespace = NamespaceIdent::new(run.database());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .context("create the memory-catalog namespace")?;
    let location = run.uri(ICEBERG_ORDERS);
    let created = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(ICEBERG_ORDERS.to_string())
                .location(location.clone())
                .schema(orders_schema()?)
                .properties(HashMap::new())
                .build(),
        )
        .await
        .with_context(|| format!("create {ICEBERG_ORDERS}"))?;
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(created.metadata().current_schema())
            .context("derive the iceberg_orders Arrow schema")?,
    );
    let batch = RecordBatch::try_new(arrow_schema, orders_columns()?)
        .context("build the iceberg_orders batch")?;
    write_one_file_append(&catalog, &created, ICEBERG_ORDERS, [batch])
        .await
        .with_context(|| format!("append to {ICEBERG_ORDERS}"))?;
    let committed = catalog
        .load_table(&TableIdent::new(namespace, ICEBERG_ORDERS.to_string()))
        .await
        .context("reload iceberg_orders after its append")?;
    let metadata_location = committed
        .metadata_location()
        .context("the committed iceberg_orders table has no metadata location")?
        .to_string();

    let input = TableInput::builder()
        .name(ICEBERG_ORDERS)
        .table_type(EXTERNAL_TABLE)
        .parameters("table_type", "ICEBERG")
        .parameters("metadata_location", metadata_location)
        .storage_descriptor(
            StorageDescriptor::builder()
                .location(location)
                .columns(hive_column(STALE_GLUE_COLUMN, "string")?)
                .build(),
        )
        .build()
        .context("describe the iceberg_orders registration")?;
    writer.create_table(input).await
}

fn h_array_struct_data() -> ArrayRef {
    let member_fields = Fields::from(vec![Field::new("a", DataType::Decimal128(5, 2), true)]);
    let decimals = Decimal128Array::from(vec![125])
        .with_precision_and_scale(5, 2)
        .expect("Decimal128(5,2)");
    let members = StructArray::try_new(member_fields.clone(), vec![Arc::new(decimals)], None)
        .expect("struct<a:decimal(5,2)>");
    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("element", DataType::Struct(member_fields), true)),
            OffsetBuffer::from_lengths([1, 0, 0]),
            Arc::new(members),
            all_types_validity(),
        )
        .expect("array<struct<a:decimal(5,2)>>"),
    )
}

/// `all_types` declares every `HIVE_TYPE_COLUMNS` type; its one extensionless data file holds
/// a value for each column a reader can bind.
async fn register_all_types(writer: &FixtureWriter<'_>) -> Result<()> {
    let run = writer.run;
    let mut columns = vec![hive_column("id", "bigint")?];
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut arrays = vec![all_types_ids()];
    for hive_type_column in HIVE_TYPE_COLUMNS {
        let column = hive_type_column.column;
        columns.push(hive_column(column, hive_type_column.hive_type)?);
        if let Some(data) = hive_type_column.data {
            let values = data();
            fields.push(Field::new(column, values.data_type().clone(), true));
            arrays.push(values);
        }
    }
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays)
        .context("build the Glue all_types batch")?;

    let data_key = format!("{ALL_TYPES}/20240115_000000_00001_{ALL_TYPES}");
    let marker_key = format!("{ALL_TYPES}/{SUCCESS_MARKER}");
    try_join!(
        writer.put_object(&data_key, encode_parquet(&batch)),
        writer.put_object(&marker_key, bytes::Bytes::new()),
        writer.create_table(parquet_table(
            ALL_TYPES,
            &run.table_location(ALL_TYPES),
            columns,
            Vec::new(),
        )?),
    )?;
    Ok(())
}

/// `binary_values` declares `c_bytes string` over a `BYTE_ARRAY` with no annotation and no
/// embedded Arrow schema whose bytes are not valid UTF-8.
async fn register_binary_values(writer: &FixtureWriter<'_>) -> Result<()> {
    let run = writer.run;
    let bytes = non_utf8_parquet(
        "message binary_values {
            REQUIRED INT64 id;
            OPTIONAL BYTE_ARRAY c_bytes;
        }",
    );
    let data_key = format!("{BINARY_VALUES}/20240115_000000_00001_{BINARY_VALUES}");
    try_join!(
        writer.put_object(&data_key, bytes),
        writer.create_table(parquet_table(
            BINARY_VALUES,
            &run.table_location(BINARY_VALUES),
            vec![
                hive_column("id", "bigint")?,
                hive_column("c_bytes", "string")?,
            ],
            Vec::new(),
        )?),
    )?;
    Ok(())
}

fn partition_file_bytes(rows: &[PartitionedRow]) -> Result<bytes::Bytes> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.id))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.v))),
        ],
    )
    .context("build a partitioned data file batch")?;
    Ok(encode_parquet(&batch))
}

async fn register_partitioned(writer: &FixtureWriter<'_>) -> Result<()> {
    let run = writer.run;
    let data_files = try_join_all(PARTITIONS.iter().flat_map(|partition| {
        let key = partition.place.relative_key();
        partition.files.iter().map(move |file| {
            let key = format!("{key}{}", file.name);
            async move {
                writer
                    .put_object(&key, partition_file_bytes(file.rows)?)
                    .await
            }
        })
    }));
    let marker_key = format!("{}{SUCCESS_MARKER}", PARTITIONS[0].place.relative_key());
    let partition_keys = PARTITION_KEYS
        .iter()
        .map(|(name, hive_type)| hive_column(name, hive_type))
        .collect::<Result<Vec<_>>>()?;
    try_join!(
        data_files,
        writer.put_object(&marker_key, bytes::Bytes::new()),
        writer.create_table(parquet_table(
            PARTITIONED,
            &run.table_location(PARTITIONED),
            vec![hive_column("id", "bigint")?, hive_column("v", "string")?],
            partition_keys,
        )?),
    )?;

    let inputs = PARTITIONS
        .iter()
        .map(|partition| {
            PartitionInput::builder()
                .set_values(Some(
                    partition
                        .values
                        .iter()
                        .map(|value| value.to_string())
                        .collect(),
                ))
                .storage_descriptor(storage_descriptor(
                    &partition.place.location(run),
                    Vec::new(),
                    formats_of(partition.input_format),
                ))
                .build()
        })
        .collect();
    writer.create_partitions(PARTITIONED, inputs).await
}

/// None of these gets a data file: the listing must skip each on its metadata alone.
async fn register_metadata_only_tables(writer: &FixtureWriter<'_>) -> Result<()> {
    let run = writer.run;
    let id_column = || hive_column("id", "bigint");

    let id_table = |name: &str, formats| -> Result<TableInputBuilder> {
        Ok(external_table(
            name,
            &run.table_location(name),
            vec![id_column()?],
            formats,
        ))
    };
    let projected = id_table(PROJECTED, parquet_formats())?
        .parameters("projection.enabled", "true")
        .parameters("projection.p.type", "integer")
        .parameters("projection.p.range", "1,3")
        .partition_keys(hive_column("p", "int")?);
    let view = TableInput::builder()
        .name(A_VIEW)
        .table_type(VIRTUAL_VIEW)
        .parameters("presto_view", "true")
        .view_original_text("/* Presto View */")
        .view_expanded_text("/* Presto View */")
        .storage_descriptor(StorageDescriptor::builder().columns(id_column()?).build());
    let orc = id_table(ORC_TABLE, formats_of(ORC_INPUT_FORMAT))?;
    // A Parquet storage descriptor, so only the table_type check can skip it.
    let delta = id_table(DELTA_TABLE, parquet_formats())?.parameters("table_type", "DELTA");

    let inputs = [projected, view, orc, delta].map(|builder| {
        builder
            .build()
            .context("describe a metadata-only registration")
    });
    try_join_all(
        inputs
            .into_iter()
            .map(|input| async { writer.create_table(input?).await }),
    )
    .await?;
    Ok(())
}

mod glue_naming_and_variable_tests {
    use super::super::cloud_fixture::panic_message;
    use super::{
        ACCESS_KEY_ID_VAR, ASSUME_ROLE_ARN_VAR, ASSUME_ROLE_BASE_ACCESS_KEY_ID_VAR,
        ASSUME_ROLE_BASE_SECRET_ACCESS_KEY_VAR, ASSUME_ROLE_EXTERNAL_ID_VAR, DATABASE_PREFIX,
        FIXTURE_BUCKET_VAR, GlueEnv, MAX_DATABASE_NAME_LEN, REGION_VAR, SECRET_ACCESS_KEY_VAR,
        database_name, derive_run_id,
    };

    const FIXED_MILLIS: u128 = 1_762_000_000_000;

    const VALUES: [(&str, &str); 8] = [
        (ACCESS_KEY_ID_VAR, "AKIAACCESSKEYSENTINEL"),
        (SECRET_ACCESS_KEY_VAR, "secret/access+key/sentinel"),
        (REGION_VAR, "eu-sentinel-1"),
        (FIXTURE_BUCKET_VAR, "bucket-sentinel"),
        (ASSUME_ROLE_BASE_ACCESS_KEY_ID_VAR, "AKIABASEKEYSENTINEL"),
        (
            ASSUME_ROLE_BASE_SECRET_ACCESS_KEY_VAR,
            "base/secret+key/sentinel",
        ),
        (
            ASSUME_ROLE_ARN_VAR,
            "arn:aws:iam::000000000000:role/sentinel",
        ),
        (ASSUME_ROLE_EXTERNAL_ID_VAR, "external-id-sentinel"),
    ];

    fn value_of(name: &str) -> Option<String> {
        VALUES
            .iter()
            .find(|(var, _)| *var == name)
            .map(|(_, value)| value.to_string())
    }

    /// specs/testing.md § Per-run cloud resources
    #[test]
    fn run_id_is_a_legal_glue_database_name() {
        let three_hundred_chars = "A".repeat(300);
        for user in ["", "ÜBER-user", three_hundred_chars.as_str()] {
            let name = database_name(&derive_run_id(user, FIXED_MILLIS));
            assert!(
                name.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "user {user:?}: name {name:?} must contain only lowercase letters, digits, and _"
            );
            assert!(
                name.starts_with(DATABASE_PREFIX) && !name.contains("__"),
                "user {user:?}: name {name:?} must keep the {DATABASE_PREFIX} prefix the sweep \
                 matches, joined by a single underscore"
            );
        }
        assert_eq!(
            database_name(&derive_run_id(&three_hundred_chars, FIXED_MILLIS)).len(),
            MAX_DATABASE_NAME_LEN,
            "an over-long user is truncated to exactly Glue's limit"
        );
    }

    /// specs/testing.md § Failure contract
    #[test]
    fn glue_env_reads_all_eight_variables_up_front() {
        let env = GlueEnv::from_lookup(value_of);
        assert_eq!(
            [
                env.access_key_id.as_str(),
                env.secret_access_key(),
                env.region.as_str(),
                env.fixture_bucket.as_str(),
                env.assume_role_base_access_key_id.as_str(),
                env.assume_role_base_secret_access_key.as_str(),
                env.assume_role_arn.as_str(),
                env.assume_role_external_id.as_str(),
            ],
            VALUES.map(|(_, value)| value)
        );

        for (missing, _) in VALUES {
            let message = panic_message(|| {
                GlueEnv::from_lookup(|name| (name != missing).then(|| value_of(name)).flatten());
            });
            assert!(
                message.contains(missing),
                "the panic for a missing {missing} must name the variable, got: {message}"
            );
            for (_, value) in VALUES {
                assert!(
                    !message.contains(value),
                    "the panic for a missing {missing} must echo no variable value, got: {message}"
                );
            }
        }
    }

    /// specs/testing.md § Credentials in tests
    #[test]
    fn glue_env_redacts_both_secret_keys_and_the_external_id() {
        let env = GlueEnv::from_lookup(value_of);
        let redacted = env.redact(&VALUES.map(|(_, value)| value).join(" "));
        for secret in [
            SECRET_ACCESS_KEY_VAR,
            ASSUME_ROLE_BASE_SECRET_ACCESS_KEY_VAR,
            ASSUME_ROLE_EXTERNAL_ID_VAR,
        ]
        .map(|name| value_of(name).unwrap())
        {
            assert!(!redacted.contains(&secret), "{secret} leaked: {redacted}");
        }
    }
}
