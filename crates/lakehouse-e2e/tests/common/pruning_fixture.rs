//! One row set for the pruning case table. The Iceberg seed, the Hive and Delta copies, and the
//! native oracle all read it, so every format answers the same predicate over the same rows.

use super::raw_parquet::{delete_fixture_prefix, encode_parquet_with, put_fixture_object};

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use std::collections::BTreeSet;
use std::sync::Arc;

pub const PRUNING_NAMESPACE: &str = "e2e_pruning";
pub const PRUNING_TABLE: &str = "pruning_cases";
pub const PRUNING_PARTITION_COLUMN: &str = "p";
pub const PRUNING_TYPED_PARTITION_COLUMN: &str = "q";
pub const PRUNING_HIVE_BASE: &str = "s3://warehouse/direct_pruning/";
const PRUNING_ORACLE_SCHEMA: &str = "PRUNING_ORACLE";
pub const PRUNING_ORACLE_TABLE: &str = "PRUNING_ORACLE.CASES";

const ROWS_PER_ROW_GROUP: usize = 4;
const ROWS_PER_PAGE: usize = 2;

/// 2024-01-01T00:00:00 in microseconds since the epoch.
const BASE_TS_MICROS: i64 = 1_704_067_200_000_000;
/// 2024-01-01 in days since the epoch.
const BASE_DAYS: i32 = 19_723;
/// The Hive directory value of a NULL partition.
const HIVE_NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

pub struct PruningRow {
    id: i64,
    k: Option<i64>,
    s: Option<&'static str>,
    x: Option<f64>,
    ts_millis: Option<i64>,
}

const fn row(
    id: i64,
    k: Option<i64>,
    s: Option<&'static str>,
    x: Option<f64>,
    ts_millis: Option<i64>,
) -> PruningRow {
    PruningRow {
        id,
        k,
        s,
        x,
        ts_millis,
    }
}

pub struct PruningFile {
    pub label: &'static str,
    pub partition: Option<&'static str>,
    /// The `decimal(4,1)` partition value, with one fraction digit.
    pub typed_partition: Option<&'static str>,
    rows: &'static [PruningRow],
}

impl PruningFile {
    /// Row by row, so a caller derives per-file statistics from the rows, not from a footer.
    pub fn k_values(&self) -> Vec<Option<i64>> {
        self.rows.iter().map(|row| row.k).collect()
    }

    pub fn s_values(&self) -> Vec<Option<&'static str>> {
        self.rows.iter().map(|row| row.s).collect()
    }

    pub fn x_values(&self) -> Vec<Option<f64>> {
        self.rows.iter().map(|row| row.x).collect()
    }
}

/// The `k` boundary of the `NOT (k < 30 AND u)` cases.
pub const K_SPLIT: i64 = 30;
/// Files whose row groups a wrong `k >= 30` would skip in part.
pub const ROW_GROUP_SPLIT_FILES: [&str; 3] = ["a2", "b1", "n1"];
/// Files whose first row group spans `K_SPLIT` while its first page stays below it.
pub const PAGE_SPLIT_FILES: [&str; 2] = ["a2", "n1"];

/// The files in `ROW_GROUP_SPLIT_FILES` and `PAGE_SPLIT_FILES` hold a row group or page with
/// only `k` below `K_SPLIT` and a row that `NOT (k < 30 AND u)` matches, so a wrong `k >= 30`
/// at that level drops a matching row.
pub const PRUNING_FILES: [PruningFile; 6] = [
    PruningFile {
        label: "a1",
        partition: Some("a"),
        typed_partition: Some("2.5"),
        rows: &[
            row(1, Some(10), Some("xa"), Some(0.5), Some(5_250)),
            row(2, Some(12), Some("ya"), Some(0.05), Some(500)),
            row(3, Some(18), Some("xb"), Some(-0.5), Some(250)),
            row(4, Some(20), Some("yb"), None, Some(7_000)),
        ],
    },
    PruningFile {
        label: "a2",
        partition: Some("a"),
        typed_partition: Some("10.0"),
        rows: &[
            row(5, Some(15), Some("ya"), Some(0.05), Some(500)),
            row(6, Some(18), Some("xa"), Some(0.5), Some(5_250)),
            row(7, Some(35), Some("xc"), Some(0.5), Some(5_250)),
            row(8, Some(40), None, Some(-0.05), None),
            row(9, None, Some("xd"), Some(0.5), Some(3_000)),
            row(10, Some(22), Some("yc"), Some(0.75), Some(750)),
        ],
    },
    PruningFile {
        label: "b1",
        partition: Some("b"),
        typed_partition: Some("2.5"),
        rows: &[
            row(11, Some(5), Some("xa"), Some(0.5), Some(5_250)),
            row(12, Some(8), Some("yd"), Some(0.05), Some(500)),
            row(13, Some(12), Some("xe"), Some(0.5), Some(2_000)),
            row(14, Some(14), Some("xf"), Some(0.5), Some(6_000)),
            row(15, Some(30), Some("ye"), Some(-0.05), Some(125)),
            row(16, Some(35), Some("xg"), Some(0.5), Some(9_000)),
            row(17, Some(48), Some("yf"), None, Some(900)),
            row(18, Some(50), Some("xh"), Some(0.5), Some(4_000)),
        ],
    },
    PruningFile {
        label: "b2",
        partition: Some("b"),
        typed_partition: None,
        rows: &[
            row(19, None, Some("xi"), Some(0.5), Some(5_250)),
            row(20, None, Some("yg"), Some(0.05), Some(500)),
            row(21, None, None, None, None),
        ],
    },
    PruningFile {
        label: "n1",
        partition: None,
        typed_partition: Some("-1.5"),
        rows: &[
            row(22, Some(5), Some("yh"), Some(0.05), Some(500)),
            row(23, Some(7), Some("xj"), Some(0.5), Some(5_250)),
            row(24, Some(33), Some("xk"), Some(0.5), Some(8_000)),
            row(25, Some(35), Some("yi"), Some(-0.5), Some(1_500)),
            row(26, Some(24), Some("xl"), Some(0.05), Some(750)),
            row(27, Some(25), None, Some(0.5), Some(5_250)),
        ],
    },
    PruningFile {
        label: "n2",
        partition: None,
        typed_partition: Some("10.0"),
        rows: &[
            row(28, Some(45), Some("xm"), Some(0.5), Some(5_250)),
            row(29, Some(60), Some("yj"), Some(0.05), Some(500)),
        ],
    },
];

/// Every copy uses these, so the Iceberg, Hive, and Delta files share one row-group and page
/// layout, with a column index and an offset index.
pub fn pruning_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP))
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        // The page row limit is checked only between write batches.
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build()
}

pub fn pruning_batch(file: &PruningFile) -> RecordBatch {
    let rows = file.rows;
    let utc = Some(Arc::from("+00:00"));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(PRUNING_PARTITION_COLUMN, DataType::Utf8, true),
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("x", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new(
            PRUNING_TYPED_PARTITION_COLUMN,
            DataType::Decimal128(4, 1),
            true,
        ),
        Field::new("d", DataType::Decimal128(9, 2), true),
        Field::new("f", DataType::Float32, true),
        Field::new("dt", DataType::Date32, true),
        Field::new(
            "tz",
            DataType::Timestamp(TimeUnit::Microsecond, utc.clone()),
            true,
        ),
    ]));
    let typed_partition = file.typed_partition.map(decimal_unscaled);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.id))),
        Arc::new(StringArray::from(vec![file.partition; rows.len()])),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.k))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.s))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.x))),
        Arc::new(TimestampMicrosecondArray::from_iter(
            rows.iter().map(|r| r.ts_millis.map(ts_micros)),
        )),
        Arc::new(
            Decimal128Array::from(vec![typed_partition; rows.len()])
                .with_precision_and_scale(4, 1)
                .expect("decimal(4,1) partition"),
        ),
        Arc::new(
            Decimal128Array::from_iter(derived(rows, d_unscaled))
                .with_precision_and_scale(9, 2)
                .expect("decimal(9,2) column"),
        ),
        Arc::new(Float32Array::from_iter(derived(rows, f_value))),
        Arc::new(Date32Array::from_iter(derived(rows, dt_days))),
        Arc::new(
            TimestampMicrosecondArray::from_iter(derived(rows, tz_micros)).with_timezone_opt(utc),
        ),
    ];
    RecordBatch::try_new(schema, columns).expect("pruning fixture batch construction")
}

/// `d`, `f`, `dt`, and `tz` rise with `k`, so their per-file bounds are those of `k`.
fn derived<T: 'static>(
    rows: &[PruningRow],
    derive: fn(i64) -> T,
) -> impl Iterator<Item = Option<T>> + '_ {
    rows.iter().map(move |r| r.k.map(derive))
}

pub fn d_unscaled(k: i64) -> i128 {
    i128::from(k) * 100 + 25
}

pub fn f_value(k: i64) -> f32 {
    f32::from(i16::try_from(k).expect("fixture k fits i16")) / 100.0
}

pub fn dt_days(k: i64) -> i32 {
    BASE_DAYS + i32::try_from(k).expect("fixture k fits i32")
}

pub fn tz_micros(k: i64) -> i64 {
    BASE_TS_MICROS + k * 60_000_000
}

/// A scale-2 decimal as SQL and the Delta statistics write it.
pub fn d_text(unscaled: i128) -> String {
    let sign = if unscaled < 0 { "-" } else { "" };
    let magnitude = unscaled.unsigned_abs();
    format!("{sign}{}.{:02}", magnitude / 100, magnitude % 100)
}

pub fn day(days: i32) -> chrono::NaiveDate {
    chrono::DateTime::UNIX_EPOCH
        .date_naive()
        .checked_add_signed(chrono::TimeDelta::days(days.into()))
        .expect("fixture date is a valid day")
}

pub fn instant(micros: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_micros(micros).expect("fixture timestamp is a valid instant")
}

pub fn decimal_unscaled(text: &str) -> i128 {
    text.replace('.', "")
        .parse()
        .expect("fixture decimal text parses")
}

fn ts_micros(millis: i64) -> i64 {
    BASE_TS_MICROS + millis * 1_000
}

/// The Iceberg data file name starts with this prefix, followed by `-`.
pub fn iceberg_file_prefix(file: &PruningFile) -> String {
    format!("pc_{}", file.label)
}

/// The Iceberg file name carries `<prefix>-`, and the Hive and Delta keys `/<label>.parquet`.
pub fn labels_named(text: &str) -> BTreeSet<String> {
    PRUNING_FILES
        .iter()
        .filter(|file| {
            text.contains(&format!("{}-", iceberg_file_prefix(file)))
                || text.contains(&format!("/{}.parquet", file.label))
        })
        .map(|file| file.label.to_string())
        .collect()
}

/// The Hive-style path below the table root, which the Hive and Delta copies share.
pub fn pruning_relative_path(file: &PruningFile) -> String {
    let segment = |value: Option<&str>| value.unwrap_or(HIVE_NULL_PARTITION).to_string();
    format!(
        "{PRUNING_PARTITION_COLUMN}={}/{PRUNING_TYPED_PARTITION_COLUMN}={}/{}.parquet",
        segment(file.partition),
        segment(file.typed_partition),
        file.label
    )
}

pub fn hive_object_uri(base: &str, file: &PruningFile) -> String {
    format!("{base}{PRUNING_TABLE}/{}", pruning_relative_path(file))
}

/// Omits the partition columns, whose values the Hive path and the Delta log carry, so every
/// copy writes these same bytes.
pub fn pruning_data_file(file: &PruningFile) -> Bytes {
    let mut batch = pruning_batch(file);
    for column in [PRUNING_PARTITION_COLUMN, PRUNING_TYPED_PARTITION_COLUMN] {
        let index = batch
            .schema()
            .index_of(column)
            .expect("the pruning batch holds the partition column");
        batch.remove_column(index);
    }
    encode_parquet_with(&batch, pruning_writer_properties())
}

/// Drops the table root first, so no file of an earlier layout stays listed.
pub fn write_pruning_hive_copy(base: &str) {
    delete_fixture_prefix(&format!("{base}{PRUNING_TABLE}"));
    for file in &PRUNING_FILES {
        put_fixture_object(&hive_object_uri(base, file), pruning_data_file(file));
    }
}

/// Rebuilds the oracle on every run, with `FILE_LABEL` naming the data file of each row.
pub fn oracle_statements() -> [String; 3] {
    let values: Vec<String> = PRUNING_FILES
        .iter()
        .flat_map(|file| file.rows.iter().map(move |r| oracle_values(file, r)))
        .collect();
    [
        format!("CREATE SCHEMA IF NOT EXISTS {PRUNING_ORACLE_SCHEMA}"),
        format!(
            "CREATE OR REPLACE TABLE {PRUNING_ORACLE_TABLE} (ID DECIMAL(20,0), P VARCHAR(2000000), \
             K DECIMAL(20,0), S VARCHAR(2000000), X DOUBLE, TS TIMESTAMP, Q DECIMAL(4,1), \
             D DECIMAL(9,2), F DOUBLE, DT DATE, TZ TIMESTAMP, FILE_LABEL VARCHAR(10))"
        ),
        format!(
            "INSERT INTO {PRUNING_ORACLE_TABLE} (ID, P, K, S, X, TS, Q, D, F, DT, TZ, FILE_LABEL) \
             VALUES {}",
            values.join(", ")
        ),
    ]
}

fn oracle_values(file: &PruningFile, r: &PruningRow) -> String {
    let timestamp = |micros: i64| {
        instant(micros)
            .format("TIMESTAMP '%Y-%m-%d %H:%M:%S%.3f'")
            .to_string()
    };
    let values = [
        Some(r.id.to_string()),
        file.partition.map(sql_string),
        r.k.map(|k| k.to_string()),
        r.s.map(sql_string),
        r.x.map(|x| format!("{x:?}")),
        r.ts_millis.map(|millis| timestamp(ts_micros(millis))),
        file.typed_partition.map(str::to_string),
        r.k.map(|k| d_text(d_unscaled(k))),
        r.k.map(|k| format!("{:?}", f64::from(f_value(k)))),
        r.k.map(|k| day(dt_days(k)).format("DATE '%Y-%m-%d'").to_string()),
        r.k.map(|k| timestamp(tz_micros(k))),
        Some(sql_string(file.label)),
    ]
    .map(|value| value.unwrap_or_else(|| "NULL".to_string()));
    format!("({})", values.join(", "))
}

fn sql_string(value: &str) -> String {
    format!("'{value}'")
}
