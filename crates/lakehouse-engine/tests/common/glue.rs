//! AWS Glue E2E harness: every `GlueRun` owns one Glue database and one S3 prefix and deletes
//! both when it drops, so two concurrent runs never share a fixture (`glue-e2e/glue-e2e-harness`).

use super::cloud_fixture::{
    derive_run_segment, per_run_segment, require_var, run_teardown_off_runtime,
};
use super::raw_parquet::encode_parquet;
use super::seed::{
    all_types_ids, all_types_validity, binary_values, boolean_values, date_values,
    decimal_10_2_values, decimal_38_10_values, float32_values, int_list_values, int8_values,
    int16_values, int32_values, non_utf8_parquet, string_int_map_values, text_values,
    timestamp_values, write_one_file_append,
};
use super::stack::CatalogConnectionPassword;

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{
    ArrayRef, BinaryArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    ListArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use aws_sdk_glue::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_glue::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_glue::types::{
    Column, DatabaseInput, PartitionInput, SerDeInfo, StorageDescriptor, TableInput,
};
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_REGION,
    S3_SECRET_ACCESS_KEY,
};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_storage_opendal::OpenDalStorageFactory;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use std::collections::HashMap;
use std::sync::Arc;

pub const ACCESS_KEY_ID_VAR: &str = "GLUE_ACCESS_KEY_ID";
pub const SECRET_ACCESS_KEY_VAR: &str = "GLUE_SECRET_ACCESS_KEY";
pub const REGION_VAR: &str = "GLUE_REGION";
pub const FIXTURE_BUCKET_VAR: &str = "GLUE_FIXTURE_BUCKET";

/// Glue's database-name length limit.
const MAX_DATABASE_NAME_LEN: usize = 255;

/// Also the pattern `glue-orphan-sweep.yml` deletes after 24 hours.
const DATABASE_PREFIX: &str = "lh_e2e_";

/// Also the prefix `glue-orphan-sweep.yml` deletes after 24 hours.
const OBJECT_PREFIX_ROOT: &str = "lh_e2e";

const CREDENTIALS_PROVIDER: &str = "glue-e2e-harness";
const ALREADY_EXISTS_CODE: &str = "AlreadyExistsException";
const NOT_FOUND_CODE: &str = "EntityNotFoundException";
const REDACTED: &str = "<redacted>";

/// Plain owned data so a clone can cross into the teardown thread. It derives no `Debug`, so
/// the secret access key can never be formatted into test output.
#[derive(Clone)]
pub struct GlueEnv {
    pub access_key_id: String,
    secret_access_key: String,
    pub region: String,
    pub fixture_bucket: String,
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
        text.replace(&self.secret_access_key, REDACTED)
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
            .map_err(|error| {
                anyhow!(self.redact(&format!(
                    "build the S3 store for bucket {}: {error}",
                    self.fixture_bucket
                )))
            })
    }

    pub async fn database_exists(&self, database: &str) -> Result<bool> {
        match self
            .glue_client()
            .get_database()
            .name(database)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.code() == Some(NOT_FOUND_CODE) => Ok(false),
            Err(error) => bail!(self.glue_failure(&format!("GetDatabase {database}"), &error)),
        }
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
            .map_err(|error| {
                anyhow!(self.redact(&format!(
                    "list s3://{}/{prefix}: {error}",
                    self.fixture_bucket
                )))
            })?;
        Ok(objects.into_iter().map(|object| object.location).collect())
    }

    pub fn glue_failure(&self, operation: &str, error: &impl std::error::Error) -> String {
        self.redact(&format!("Glue {operation}: {}", DisplayErrorContext(error)))
    }
}

/// Leaves room for `DATABASE_PREFIX`, which already ends in `_`.
const RUN_ID_LEN: usize = MAX_DATABASE_NAME_LEN - DATABASE_PREFIX.len();

/// Athena and Hive accept only lowercase letters, digits, and `_` in a database name, so the
/// user segment is folded into that alphabet within Glue's length limit.
pub fn per_run_id() -> String {
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
            Err(error) => bail!(env.glue_failure(&format!("CreateDatabase {database}"), &error)),
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
    for table in database_table_names(env, &client, database).await? {
        match client
            .delete_table()
            .database_name(database)
            .name(&table)
            .send()
            .await
        {
            Ok(_) => {}
            Err(error) if error.code() == Some(NOT_FOUND_CODE) => {}
            Err(error) => {
                bail!(env.glue_failure(&format!("DeleteTable {database}.{table}"), &error))
            }
        }
    }
    match client.delete_database().name(database).send().await {
        Ok(_) => Ok(()),
        Err(error) if error.code() == Some(NOT_FOUND_CODE) => Ok(()),
        Err(error) => bail!(env.glue_failure(&format!("DeleteDatabase {database}"), &error)),
    }
}

/// A database already absent has no table left to delete.
async fn database_table_names(
    env: &GlueEnv,
    client: &aws_sdk_glue::Client,
    database: &str,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut token = None;
    loop {
        let page = match client
            .get_tables()
            .database_name(database)
            .set_next_token(token.take())
            .send()
            .await
        {
            Ok(page) => page,
            Err(error) if error.code() == Some(NOT_FOUND_CODE) => return Ok(Vec::new()),
            Err(error) => bail!(env.glue_failure(&format!("GetTables {database}"), &error)),
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
        .map_err(|error| {
            anyhow!(env.redact(&format!(
                "DELETE under s3://{}/{prefix}: {error}",
                env.fixture_bucket
            )))
        })?;
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
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";
const EXTERNAL_TABLE: &str = "EXTERNAL_TABLE";
const VIRTUAL_VIEW: &str = "VIRTUAL_VIEW";

pub const ICEBERG_ORDERS: &str = "iceberg_orders";
pub const ALL_TYPES: &str = "all_types";
pub const BINARY_VALUES: &str = "binary_values";
pub const PARTITIONED: &str = "partitioned";
pub const PROJECTED: &str = "projected";
pub const A_VIEW: &str = "a_view";
pub const ORC_TABLE: &str = "orc_table";
pub const DELTA_TABLE: &str = "delta_table";

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

pub const ORDERS: [Order; 5] = [
    Order {
        order_id: 1,
        customer: Some("ada"),
        amount_hundredths: 1225,
        order_date: "2024-01-01",
    },
    Order {
        order_id: 2,
        customer: Some("bob"),
        amount_hundredths: 2575,
        order_date: "2024-01-02",
    },
    Order {
        order_id: 3,
        customer: Some("cyd"),
        amount_hundredths: 9999,
        order_date: "2024-01-03",
    },
    Order {
        order_id: 4,
        customer: Some("dee"),
        amount_hundredths: 1995,
        order_date: "2024-01-04",
    },
    Order {
        order_id: 5,
        customer: None,
        amount_hundredths: 4215,
        order_date: "2024-01-05",
    },
];

/// One `all_types` column per Hive type of `vs-adapter/glue-hive-type-mapping`; `data` is
/// `None` for a column the data file leaves out, because the reader refuses it at plan time.
pub struct HiveTypeColumn {
    pub column: &'static str,
    pub hive_type: &'static str,
    pub data: Option<HiveColumnData>,
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

pub const HIVE_TYPE_COLUMNS: &[HiveTypeColumn] = &[
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
        Some(h_struct_xy_data),
    ),
    hive("h_struct_binary", "struct<b:binary>", None),
    hive("h_uniontype", "uniontype<int,string>", None),
    hive("h_interval", "interval_day_time", None),
    hive("h_malformed_map", "map<int>", None),
    hive("h_empty_type", "", None),
];

/// A row's partition values are those of the `PARTITIONS` entry whose file holds its id.
pub struct PartitionedRow {
    pub id: i64,
    pub v: &'static str,
}

impl PartitionedRow {
    fn partition(&self) -> &'static FixturePartition {
        let partitions: &'static [FixturePartition] = &PARTITIONS;
        partitions
            .iter()
            .find(|partition| {
                partition
                    .files
                    .iter()
                    .any(|file| file.ids.contains(&self.id))
            })
            .unwrap_or_else(|| panic!("row {} is in no PARTITIONS file", self.id))
    }

    pub fn p_int(&self) -> i64 {
        self.partition().values[0]
            .parse()
            .expect("a fixture p_int is an integer")
    }

    pub fn p_date(&self) -> &'static str {
        self.partition().values[1]
    }

    pub fn p_str(&self) -> Option<&'static str> {
        Some(self.partition().values[2]).filter(|value| *value != HIVE_DEFAULT_PARTITION)
    }
}

pub const PARTITIONED_ROWS: [PartitionedRow; 7] = [
    PartitionedRow { id: 1, v: "a" },
    PartitionedRow { id: 2, v: "b" },
    PartitionedRow { id: 3, v: "c" },
    PartitionedRow { id: 4, v: "d" },
    PartitionedRow { id: 5, v: "e" },
    PartitionedRow { id: 6, v: "f" },
    PartitionedRow { id: 7, v: "g" },
];

/// Where a partition's registered location points. `OutsideTable` is relative to the run
/// prefix; `UnderTable` and `S3a`, an `s3a://` address of the same store, to the table location.
#[derive(Clone, Copy)]
pub enum PartitionPlace {
    UnderTable(&'static str),
    OutsideTable(&'static str),
    S3a(&'static str),
}

/// One data file: an extensionless or `.parquet` name and the `PARTITIONED_ROWS` ids it holds.
pub struct PartitionFile {
    pub name: &'static str,
    pub ids: &'static [i64],
}

pub struct FixturePartition {
    pub values: [&'static str; 3],
    pub place: PartitionPlace,
    pub input_format: &'static str,
    pub files: &'static [PartitionFile],
}

/// The `p_int=9` ORC partition holds no file; every other partition has `p_int < 9`, so a
/// query carrying `P_INT < 9` prunes it.
pub const PARTITIONS: [FixturePartition; 6] = [
    FixturePartition {
        values: ["1", "2024-01-01", "alpha"],
        place: PartitionPlace::UnderTable("p_int=1/p_date=2024-01-01/p_str=alpha/"),
        input_format: PARQUET_INPUT_FORMAT,
        files: &[
            PartitionFile {
                name: "20240101_000000_00001_p1",
                ids: &[1, 2],
            },
            PartitionFile {
                name: "part-00000-p1.snappy.parquet",
                ids: &[3],
            },
        ],
    },
    FixturePartition {
        values: ["1", "2024-01-02", HIVE_DEFAULT_PARTITION],
        place: PartitionPlace::UnderTable(
            "p_int=1/p_date=2024-01-02/p_str=__HIVE_DEFAULT_PARTITION__/",
        ),
        input_format: PARQUET_INPUT_FORMAT,
        files: &[PartitionFile {
            name: "20240102_000000_00001_p2",
            ids: &[4],
        }],
    },
    FixturePartition {
        values: ["2", "2024-01-01", "a b/c"],
        place: PartitionPlace::UnderTable("p_int=2/p_date=2024-01-01/p_str=a b%2Fc/"),
        input_format: PARQUET_INPUT_FORMAT,
        files: &[PartitionFile {
            name: "20240101_000000_00001_p3",
            ids: &[5],
        }],
    },
    FixturePartition {
        values: ["3", "2024-02-01", "outside"],
        place: PartitionPlace::OutsideTable("outside_root/partition_4/"),
        input_format: PARQUET_INPUT_FORMAT,
        files: &[PartitionFile {
            name: "20240201_000000_00001_p4",
            ids: &[6],
        }],
    },
    FixturePartition {
        values: ["4", "2024-02-02", "s3a"],
        place: PartitionPlace::S3a("p_int=4/p_date=2024-02-02/p_str=s3a/"),
        input_format: PARQUET_INPUT_FORMAT,
        files: &[PartitionFile {
            name: "20240202_000000_00001_p5",
            ids: &[7],
        }],
    },
    FixturePartition {
        values: ["9", "2024-03-01", "orc"],
        place: PartitionPlace::UnderTable("p_int=9/p_date=2024-03-01/p_str=orc/"),
        input_format: ORC_INPUT_FORMAT,
        files: &[],
    },
];

pub const PARTITION_KEYS: [(&str, &str); 3] =
    [("p_int", "int"), ("p_date", "date"), ("p_str", "string")];

/// A Hadoop marker in the first partition's location, which the `*` file pattern must skip.
pub const SUCCESS_MARKER: &str = "_SUCCESS";

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
    let mut keys: Vec<String> = [PROJECTED, A_VIEW, ORC_TABLE, DELTA_TABLE]
        .iter()
        .map(|table| format!("{table}/"))
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
        let path = ObjectStorePath::parse(&key)
            .with_context(|| format!("fixture key {key} is not a valid object path"))?;
        self.store
            .put(&path, PutPayload::from(bytes))
            .await
            .map_err(|error| {
                anyhow!(
                    self.run
                        .env
                        .redact(&format!("PUT {}: {error}", self.run.uri(relative_key)))
                )
            })?;
        Ok(())
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
            .map_err(|error| {
                anyhow!(
                    self.run
                        .env
                        .glue_failure(&format!("CreateTable {database}.{name}"), &error)
                )
            })?;
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
            .map_err(|error| {
                anyhow!(
                    self.run
                        .env
                        .glue_failure(&format!("BatchCreatePartition {database}.{table}"), &error)
                )
            })?;
        if !output.errors().is_empty() {
            bail!(self.run.env.redact(&format!(
                "BatchCreatePartition {database}.{table} rejected {} partition(s): {:?}",
                output.errors().len(),
                output.errors()
            )));
        }
        Ok(())
    }
}

/// Registers the whole fixture set of `glue-e2e/glue-e2e-harness` in the run's database.
pub async fn register_fixture_set(run: &GlueRun) -> Result<()> {
    let writer = FixtureWriter::new(run)?;
    register_iceberg_orders(&writer).await?;
    register_all_types(&writer).await?;
    register_binary_values(&writer).await?;
    register_partitioned(&writer).await?;
    register_metadata_only_tables(&writer).await
}

/// One object and one Parquet table: enough for a teardown to have something of each kind.
pub async fn register_probe_table(run: &GlueRun) -> Result<()> {
    let writer = FixtureWriter::new(run)?;
    writer
        .put_object("probe/20240101_000000_00001_probe", orders_file_bytes()?)
        .await?;
    writer
        .create_table(parquet_table(
            "probe",
            &run.uri("probe/"),
            vec![hive_column("order_id", "bigint")?],
            Vec::new(),
        )?)
        .await
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

fn parquet_table(
    name: &str,
    location: &str,
    columns: Vec<Column>,
    partition_keys: Vec<Column>,
) -> Result<TableInput> {
    TableInput::builder()
        .name(name)
        .table_type(EXTERNAL_TABLE)
        .parameters("EXTERNAL", "TRUE")
        .parameters("classification", "parquet")
        .storage_descriptor(storage_descriptor(location, columns, parquet_formats()))
        .set_partition_keys(Some(partition_keys))
        .build()
        .with_context(|| format!("describe Glue table {name}"))
}

fn epoch_days(date: &str) -> i32 {
    let day = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .unwrap_or_else(|error| panic!("fixture date {date} is not ISO-8601: {error}"));
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the UNIX epoch is a date");
    i32::try_from((day - epoch).num_days()).expect("a fixture date fits Date32")
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
        Arc::new(Date32Array::from_iter_values(
            ORDERS.iter().map(|o| epoch_days(o.order_date)),
        )),
    ])
}

fn orders_file_bytes() -> Result<bytes::Bytes> {
    let schema = Arc::new(
        schema_to_arrow_schema(&orders_schema()?).context("derive the orders Arrow schema")?,
    );
    let batch =
        RecordBatch::try_new(schema, orders_columns()?).context("build the orders batch")?;
    Ok(encode_parquet(&batch))
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
        .map_err(|error| anyhow!(env.redact(&format!("open the memory catalog: {error}"))))?;
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
        .map_err(|error| anyhow!(env.redact(&format!("create {ICEBERG_ORDERS}: {error}"))))?;
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(created.metadata().current_schema())
            .context("derive the iceberg_orders Arrow schema")?,
    );
    let batch = RecordBatch::try_new(arrow_schema, orders_columns()?)
        .context("build the iceberg_orders batch")?;
    write_one_file_append(&catalog, &created, ICEBERG_ORDERS, [batch])
        .await
        .map_err(|error| anyhow!(env.redact(&format!("append to {ICEBERG_ORDERS}: {error:#}"))))?;
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

fn h_struct_xy_data() -> ArrayRef {
    let fields = Fields::from(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Utf8, true),
    ]);
    Arc::new(
        StructArray::try_new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("p"), None, None])),
            ],
            all_types_validity(),
        )
        .expect("struct<x:int,y:string>"),
    )
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

    writer
        .put_object(
            &format!("{ALL_TYPES}/20240115_000000_00001_{ALL_TYPES}"),
            encode_parquet(&batch),
        )
        .await?;
    writer
        .put_object(
            &format!("{ALL_TYPES}/{SUCCESS_MARKER}"),
            bytes::Bytes::new(),
        )
        .await?;
    writer
        .create_table(parquet_table(
            ALL_TYPES,
            &run.uri(&format!("{ALL_TYPES}/")),
            columns,
            Vec::new(),
        )?)
        .await
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
    writer
        .put_object(
            &format!("{BINARY_VALUES}/20240115_000000_00001_{BINARY_VALUES}"),
            bytes,
        )
        .await?;
    writer
        .create_table(parquet_table(
            BINARY_VALUES,
            &run.uri(&format!("{BINARY_VALUES}/")),
            vec![
                hive_column("id", "bigint")?,
                hive_column("c_bytes", "string")?,
            ],
            Vec::new(),
        )?)
        .await
}

fn partition_file_bytes(ids: &[i64]) -> Result<bytes::Bytes> {
    let rows: Vec<&PartitionedRow> = PARTITIONED_ROWS
        .iter()
        .filter(|row| ids.contains(&row.id))
        .collect();
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
    let mut inputs = Vec::with_capacity(PARTITIONS.len());
    for partition in &PARTITIONS {
        let key = partition.place.relative_key();
        for file in partition.files {
            writer
                .put_object(
                    &format!("{key}{}", file.name),
                    partition_file_bytes(file.ids)?,
                )
                .await?;
        }
        inputs.push(
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
                .build(),
        );
    }
    let first_key = PARTITIONS[0].place.relative_key();
    writer
        .put_object(&format!("{first_key}{SUCCESS_MARKER}"), bytes::Bytes::new())
        .await?;

    let partition_keys = PARTITION_KEYS
        .iter()
        .map(|(name, hive_type)| hive_column(name, hive_type))
        .collect::<Result<Vec<_>>>()?;
    writer
        .create_table(parquet_table(
            PARTITIONED,
            &run.uri(&format!("{PARTITIONED}/")),
            vec![hive_column("id", "bigint")?, hive_column("v", "string")?],
            partition_keys,
        )?)
        .await?;
    writer.create_partitions(PARTITIONED, inputs).await
}

/// None of these gets a data file: the listing must skip each on its metadata alone.
async fn register_metadata_only_tables(writer: &FixtureWriter<'_>) -> Result<()> {
    let run = writer.run;
    let id_column = || hive_column("id", "bigint");

    let projected = TableInput::builder()
        .name(PROJECTED)
        .table_type(EXTERNAL_TABLE)
        .parameters("projection.enabled", "true")
        .parameters("projection.p.type", "integer")
        .parameters("projection.p.range", "1,3")
        .storage_descriptor(storage_descriptor(
            &run.uri(&format!("{PROJECTED}/")),
            vec![id_column()?],
            parquet_formats(),
        ))
        .partition_keys(hive_column("p", "int")?)
        .build()
        .context("describe the projected registration")?;

    let view = TableInput::builder()
        .name(A_VIEW)
        .table_type(VIRTUAL_VIEW)
        .parameters("presto_view", "true")
        .view_original_text("/* Presto View */")
        .view_expanded_text("/* Presto View */")
        .storage_descriptor(StorageDescriptor::builder().columns(id_column()?).build())
        .build()
        .context("describe the a_view registration")?;

    let orc = TableInput::builder()
        .name(ORC_TABLE)
        .table_type(EXTERNAL_TABLE)
        .storage_descriptor(storage_descriptor(
            &run.uri(&format!("{ORC_TABLE}/")),
            vec![id_column()?],
            formats_of(ORC_INPUT_FORMAT),
        ))
        .build()
        .context("describe the orc_table registration")?;

    // A Parquet storage descriptor, so only the table_type check can skip it.
    let delta = TableInput::builder()
        .name(DELTA_TABLE)
        .table_type(EXTERNAL_TABLE)
        .parameters("table_type", "DELTA")
        .storage_descriptor(storage_descriptor(
            &run.uri(&format!("{DELTA_TABLE}/")),
            vec![id_column()?],
            parquet_formats(),
        ))
        .build()
        .context("describe the delta_table registration")?;

    for input in [projected, view, orc, delta] {
        writer.create_table(input).await?;
    }
    Ok(())
}

mod glue_naming_and_variable_tests {
    use super::super::cloud_fixture::panic_message;
    use super::{
        ACCESS_KEY_ID_VAR, DATABASE_PREFIX, FIXTURE_BUCKET_VAR, GlueEnv, MAX_DATABASE_NAME_LEN,
        REGION_VAR, SECRET_ACCESS_KEY_VAR, database_name, derive_run_id,
    };

    const FIXED_MILLIS: u128 = 1_762_000_000_000;

    const VALUES: [(&str, &str); 4] = [
        (ACCESS_KEY_ID_VAR, "AKIAACCESSKEYSENTINEL"),
        (SECRET_ACCESS_KEY_VAR, "secret/access+key/sentinel"),
        (REGION_VAR, "eu-sentinel-1"),
        (FIXTURE_BUCKET_VAR, "bucket-sentinel"),
    ];

    fn assert_legal_database_name(name: &str, user: &str) {
        assert!(
            name.len() <= MAX_DATABASE_NAME_LEN,
            "user {user:?}: name {name:?} exceeds Glue's {MAX_DATABASE_NAME_LEN}-character limit"
        );
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "user {user:?}: name {name:?} must contain only lowercase letters, digits, and _"
        );
        assert!(
            name.starts_with(DATABASE_PREFIX),
            "user {user:?}: name {name:?} must keep the {DATABASE_PREFIX} prefix the sweep matches"
        );
        assert!(
            name.ends_with(&format!("_{FIXED_MILLIS}")),
            "user {user:?}: name {name:?} must end with the millisecond suffix"
        );
        assert!(
            !name.contains("__"),
            "user {user:?}: name {name:?} must not contain consecutive underscores"
        );
    }

    /// Scenario: Each run provisions its own Glue database and S3 prefix and removes both, including on panic
    #[test]
    fn run_id_is_a_legal_glue_database_name() {
        let three_hundred_chars = "A".repeat(300);
        let truncated_at_a_separator = format!("{}.tail", "a".repeat(233));
        for user in [
            "",
            "-",
            "___",
            "Antoni.Reus",
            "a..b",
            "ÜBER-user",
            "9",
            three_hundred_chars.as_str(),
            truncated_at_a_separator.as_str(),
        ] {
            assert_legal_database_name(&database_name(&derive_run_id(user, FIXED_MILLIS)), user);
        }

        assert_eq!(
            derive_run_id("", FIXED_MILLIS),
            FIXED_MILLIS.to_string(),
            "an empty user leaves no segment rather than a leading underscore"
        );
        assert_eq!(
            derive_run_id("___", FIXED_MILLIS),
            FIXED_MILLIS.to_string(),
            "a user of only separators leaves no segment"
        );
        assert_eq!(
            derive_run_id("Antoni.Reus", FIXED_MILLIS),
            format!("antoni_reus_{FIXED_MILLIS}")
        );
        assert_eq!(
            derive_run_id("a..b", FIXED_MILLIS),
            format!("a_b_{FIXED_MILLIS}"),
            "consecutive illegal characters collapse to one underscore"
        );
        assert_eq!(
            derive_run_id("ÜBER-user", FIXED_MILLIS),
            format!("ber_user_{FIXED_MILLIS}"),
            "a multi-byte character maps to one underscore, trimmed at the segment start"
        );
        assert_eq!(
            database_name(&derive_run_id(&three_hundred_chars, FIXED_MILLIS)).len(),
            MAX_DATABASE_NAME_LEN,
            "an over-long user is truncated to exactly the remaining budget"
        );
        assert_eq!(
            derive_run_id(&truncated_at_a_separator, FIXED_MILLIS),
            format!("{}_{FIXED_MILLIS}", "a".repeat(233)),
            "truncation on a separator drops it instead of leaving a double underscore"
        );
    }

    /// Scenario: The suite fails, never skips, when a variable or the stack is missing
    #[test]
    fn missing_glue_variable_fails_loud() {
        for (missing, _) in VALUES {
            for absent in [None, Some(""), Some("   ")] {
                let message = panic_message(|| {
                    GlueEnv::from_lookup(|name| {
                        if name == missing {
                            absent.map(str::to_string)
                        } else {
                            VALUES
                                .iter()
                                .find(|(var, _)| *var == name)
                                .map(|(_, value)| value.to_string())
                        }
                    });
                });
                assert!(
                    message.contains(missing),
                    "the panic for {missing} = {absent:?} must name the variable, got: {message}"
                );
                for (_, value) in VALUES {
                    assert!(
                        !message.contains(value),
                        "the panic for {missing} = {absent:?} must echo no variable value, got: \
                         {message}"
                    );
                }
            }
        }
    }

    #[test]
    fn present_glue_variables_are_read_without_surrounding_whitespace() {
        let env = GlueEnv::from_lookup(|name| {
            VALUES
                .iter()
                .find(|(var, _)| *var == name)
                .map(|(_, value)| format!(" {value}\n"))
        });
        assert_eq!(env.access_key_id, "AKIAACCESSKEYSENTINEL");
        assert_eq!(env.secret_access_key, "secret/access+key/sentinel");
        assert_eq!(env.region, "eu-sentinel-1");
        assert_eq!(env.fixture_bucket, "bucket-sentinel");
    }
}
