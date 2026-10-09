//! One row set for the pruning case table. The Iceberg seed, the Hive raw-Parquet copy, and
//! the native oracle all read it, so every format answers the same predicate over the same rows.

use super::raw_parquet::{encode_parquet_with, put_fixture_object};

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use std::collections::BTreeSet;
use std::sync::Arc;

pub const PRUNING_NAMESPACE: &str = "e2e_pruning";
pub const PRUNING_TABLE: &str = "pruning_cases";
pub const PRUNING_PARTITION_COLUMN: &str = "p";
pub const PRUNING_HIVE_BASE: &str = "s3://warehouse/direct_pruning/";
const PRUNING_ORACLE_SCHEMA: &str = "PRUNING_ORACLE";
pub const PRUNING_ORACLE_TABLE: &str = "PRUNING_ORACLE.CASES";

const ROWS_PER_ROW_GROUP: usize = 4;
const ROWS_PER_PAGE: usize = 2;

/// 2024-01-01T00:00:00 in microseconds since the epoch.
const BASE_TS_MICROS: i64 = 1_704_067_200_000_000;
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
    rows: &'static [PruningRow],
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
        rows: &[
            row(19, None, Some("xi"), Some(0.5), Some(5_250)),
            row(20, None, Some("yg"), Some(0.05), Some(500)),
            row(21, None, None, None, None),
        ],
    },
    PruningFile {
        label: "n1",
        partition: None,
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
        rows: &[
            row(28, Some(45), Some("xm"), Some(0.5), Some(5_250)),
            row(29, Some(60), Some("yj"), Some(0.05), Some(500)),
        ],
    },
];

/// Both writers use these, so the Iceberg files and the Hive copy share one row-group and page
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

/// Holds the partition column `p`, as an Iceberg data file does.
pub fn pruning_batch(file: &PruningFile) -> RecordBatch {
    let rows = file.rows;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(PRUNING_PARTITION_COLUMN, DataType::Utf8, true),
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("x", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.id))),
        Arc::new(StringArray::from(vec![file.partition; rows.len()])),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.k))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.s))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.x))),
        Arc::new(TimestampMicrosecondArray::from_iter(
            rows.iter().map(|r| r.ts_millis.map(ts_micros)),
        )),
    ];
    RecordBatch::try_new(schema, columns).expect("pruning fixture batch construction")
}

fn ts_micros(millis: i64) -> i64 {
    BASE_TS_MICROS + millis * 1_000
}

/// The Iceberg data file name starts with this prefix, followed by `-`.
pub fn iceberg_file_prefix(file: &PruningFile) -> String {
    format!("pc_{}", file.label)
}

/// The Iceberg file name carries `<prefix>-` and the Hive object key `/<label>.parquet`.
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

pub fn hive_object_uri(file: &PruningFile) -> String {
    let segment = file.partition.unwrap_or(HIVE_NULL_PARTITION);
    format!(
        "{PRUNING_HIVE_BASE}{PRUNING_TABLE}/{PRUNING_PARTITION_COLUMN}={segment}/{}.parquet",
        file.label
    )
}

/// The Hive copy carries the partition value in its path, so its files omit `p`.
pub fn write_pruning_hive_copy() {
    for file in &PRUNING_FILES {
        let mut batch = pruning_batch(file);
        let partition = batch
            .schema()
            .index_of(PRUNING_PARTITION_COLUMN)
            .expect("the pruning batch holds the partition column");
        batch.remove_column(partition);
        put_fixture_object(
            &hive_object_uri(file),
            encode_parquet_with(&batch, pruning_writer_properties()),
        );
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
            "CREATE OR REPLACE TABLE {PRUNING_ORACLE_TABLE} (ID DECIMAL(19,0), P VARCHAR(10), \
             K DECIMAL(19,0), S VARCHAR(10), X DOUBLE, TS TIMESTAMP, FILE_LABEL VARCHAR(10))"
        ),
        format!(
            "INSERT INTO {PRUNING_ORACLE_TABLE} (ID, P, K, S, X, TS, FILE_LABEL) VALUES {}",
            values.join(", ")
        ),
    ]
}

fn oracle_values(file: &PruningFile, r: &PruningRow) -> String {
    let ts = r.ts_millis.map(|millis| {
        chrono::DateTime::from_timestamp_micros(ts_micros(millis))
            .expect("fixture timestamp is a valid instant")
            .format("TIMESTAMP '%Y-%m-%d %H:%M:%S%.3f'")
            .to_string()
    });
    let values = [
        Some(r.id.to_string()),
        file.partition.map(sql_string),
        r.k.map(|k| k.to_string()),
        r.s.map(sql_string),
        r.x.map(|x| format!("{x:?}")),
        ts,
        Some(sql_string(file.label)),
    ]
    .map(|value| value.unwrap_or_else(|| "NULL".to_string()));
    format!("({})", values.join(", "))
}

fn sql_string(value: &str) -> String {
    format!("'{value}'")
}
