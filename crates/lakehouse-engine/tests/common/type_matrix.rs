//! Type-mapping live matrix: one data table (`MATRIX`), the fixtures that write a column for
//! every row each source can hold, and the check of a virtual schema against it
//! (`datafusion-scan/type-mapping-live-matrix`).
#![cfg(any(feature = "exasol-e2e", feature = "glue-e2e"))]

use super::e2e_harness::value_to_string;
use super::exasol_ws::ExaConn;
use super::raw_parquet::{write_parquet_bytes, write_parquet_fixture};
use super::seed::{build_seed_catalog, write_one_file_append};

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    DurationMicrosecondArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, IntervalDayTimeArray, LargeBinaryArray, LargeStringArray,
    ListArray, MapArray, RecordBatch, StringArray, StructArray, Time32MillisecondArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{
    DataType, Field, Fields, IntervalDayTime, IntervalUnit, Schema as ArrowSchema, TimeUnit, i256,
};
use bytes::Bytes;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ListType, MapType, NestedField,
    NestedFieldRef, PrimitiveType, Schema as IcebergSchema, StructType, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType as ParquetDataType, FixedLenByteArray,
    FixedLenByteArrayType, Int64Type, Int96, Int96Type,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::parser::parse_message_type;
use serde_json::Value;

use std::collections::HashMap;
use std::sync::Arc;

pub const ROWS: usize = 3;
pub const IDS: [i64; ROWS] = [1, 2, 3];

pub const DIRECT_BASE: &str = "s3://warehouse/type_matrix/";
pub const ICEBERG_NAMESPACE: &str = "e2e_type_matrix";

pub const VARCHAR: &str = "VARCHAR(2000000)";

const ICEBERG_NO_NARROW_INT: &str = "Iceberg has no 8-bit, 16-bit, or unsigned integer type";
const ICEBERG_NO_LARGE_UTF8: &str = "Iceberg has one string type, which reads back as Utf8";
const ICEBERG_NO_DECIMAL256: &str = "Iceberg decimal stops at precision 38";
const ICEBERG_NO_TIME32: &str = "Iceberg has one time type, which iceberg-rust writes as Time64";
const ICEBERG_NO_DURATION: &str = "Iceberg has no duration type";
const ICEBERG_NO_INTERVAL: &str = "Iceberg has no interval type";
const ICEBERG_NO_LARGE_BINARY: &str =
    "Iceberg has one binary type, covered by c_binary, which iceberg-rust writes as LargeBinary";
const ICEBERG_NO_ENUM: &str = "Iceberg has no ENUM type";
const ICEBERG_NO_INT96: &str = "iceberg-rust writes no INT96";
const ICEBERG_NO_BARE_BYTE_ARRAY: &str = "Iceberg declares every byte array as binary or string";

const GLUE_NO_UNSIGNED: &str = "Hive has no type string for an unsigned integer";
const GLUE_NO_LARGE_UTF8: &str = "Hive has one string type, which arrow-rs writes as Utf8";
const GLUE_NO_DECIMAL256: &str = "Hive decimal stops at precision 38";
const GLUE_NO_TIME: &str = "Hive has no time type";
const GLUE_NO_DURATION: &str = "Hive has no duration type";
const GLUE_NO_INTERVAL: &str = "Hive has no interval column type";
const GLUE_NO_LARGE_BINARY: &str = "Hive has one binary type, covered by c_binary";
const GLUE_NO_FIXED: &str = "Hive has no fixed-length binary type";
const GLUE_NO_ENUM: &str = "Hive has no ENUM type";
const GLUE_NO_UUID: &str = "Hive has no UUID type";
const GLUE_NO_INT96: &str = "arrow-rs writes no INT96";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    DirectStorage,
    Iceberg,
    Glue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatrixTable {
    AllTypes,
    Annotated,
    BinaryValues,
}

impl MatrixTable {
    pub const fn name(self) -> &'static str {
        match self {
            MatrixTable::AllTypes => "all_types",
            MatrixTable::Annotated => "annotated",
            MatrixTable::BinaryValues => "binary_values",
        }
    }
}

/// `Refused` lists fragments that must all appear in the error; `Fails` names one fragment of
/// the error a query fails with when no refusal applies.
#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    Values([Option<&'static str>; ROWS]),
    Refused(&'static [&'static str]),
    Fails(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub enum Cell {
    Omitted(&'static str),
    Written {
        table: MatrixTable,
        source_type: &'static str,
        exasol_type: &'static str,
        outcome: Outcome,
    },
}

/// A Glue cell's `source_type` is the Hive type string its table declares for the column.
#[derive(Clone, Copy, Debug)]
pub struct MatrixRow {
    pub column: &'static str,
    pub direct: Cell,
    pub iceberg: Cell,
    pub glue: Cell,
}

impl MatrixRow {
    pub fn cell(&self, source: Source) -> &Cell {
        match source {
            Source::DirectStorage => &self.direct,
            Source::Iceberg => &self.iceberg,
            Source::Glue => &self.glue,
        }
    }
}

const fn written(source_type: &'static str, exasol_type: &'static str, outcome: Outcome) -> Cell {
    Cell::Written {
        table: AllTypes,
        source_type,
        exasol_type,
        outcome,
    }
}

const fn omitted(reason: &'static str) -> Cell {
    Cell::Omitted(reason)
}

const fn values(v: [Option<&'static str>; ROWS]) -> Outcome {
    Outcome::Values(v)
}

const fn row(column: &'static str, direct: Cell, iceberg: Cell, glue: Cell) -> MatrixRow {
    MatrixRow {
        column,
        direct,
        iceberg,
        glue,
    }
}

use MatrixTable::{AllTypes, Annotated, BinaryValues};

pub const MATRIX: &[MatrixRow] = &[
    row(
        "c_int8",
        written(
            "Int8",
            "DECIMAL(3,0)",
            values([Some("127"), Some("-128"), None]),
        ),
        omitted(ICEBERG_NO_NARROW_INT),
        written(
            "tinyint",
            "DECIMAL(3,0)",
            values([Some("127"), Some("-128"), None]),
        ),
    ),
    row(
        "c_int16",
        written(
            "Int16",
            "DECIMAL(5,0)",
            values([Some("32767"), Some("-32768"), None]),
        ),
        omitted(ICEBERG_NO_NARROW_INT),
        written(
            "smallint",
            "DECIMAL(5,0)",
            values([Some("32767"), Some("-32768"), None]),
        ),
    ),
    row(
        "c_int32",
        written(
            "Int32",
            "DECIMAL(10,0)",
            values([Some("2147483647"), Some("-2147483648"), None]),
        ),
        written(
            "int",
            "DECIMAL(10,0)",
            values([Some("2147483647"), Some("-2147483648"), None]),
        ),
        written(
            "int",
            "DECIMAL(10,0)",
            values([Some("2147483647"), Some("-2147483648"), None]),
        ),
    ),
    row(
        "c_int64",
        written(
            "Int64",
            "DECIMAL(20,0)",
            values([
                Some("9223372036854775807"),
                Some("-9223372036854775808"),
                None,
            ]),
        ),
        written(
            "long",
            "DECIMAL(20,0)",
            values([
                Some("9223372036854775807"),
                Some("-9223372036854775808"),
                None,
            ]),
        ),
        written(
            "bigint",
            "DECIMAL(20,0)",
            values([
                Some("9223372036854775807"),
                Some("-9223372036854775808"),
                None,
            ]),
        ),
    ),
    row(
        "c_uint8",
        written(
            "UInt8",
            "DECIMAL(3,0)",
            values([Some("255"), Some("0"), None]),
        ),
        omitted(ICEBERG_NO_NARROW_INT),
        omitted(GLUE_NO_UNSIGNED),
    ),
    row(
        "c_uint16",
        written(
            "UInt16",
            "DECIMAL(5,0)",
            values([Some("65535"), Some("0"), None]),
        ),
        omitted(ICEBERG_NO_NARROW_INT),
        omitted(GLUE_NO_UNSIGNED),
    ),
    row(
        "c_uint32",
        written(
            "UInt32",
            "DECIMAL(20,0)",
            values([Some("4294967295"), Some("0"), None]),
        ),
        omitted(ICEBERG_NO_NARROW_INT),
        omitted(GLUE_NO_UNSIGNED),
    ),
    row(
        "c_uint64",
        written(
            "UInt64",
            "DECIMAL(20,0)",
            values([Some("18446744073709551615"), Some("0"), None]),
        ),
        omitted(ICEBERG_NO_NARROW_INT),
        omitted(GLUE_NO_UNSIGNED),
    ),
    row(
        "c_float32",
        written(
            "Float32",
            "DOUBLE",
            values([Some("1.5"), Some("-0.25"), None]),
        ),
        written(
            "float",
            "DOUBLE",
            values([Some("1.5"), Some("-0.25"), None]),
        ),
        written(
            "float",
            "DOUBLE",
            values([Some("1.5"), Some("-0.25"), None]),
        ),
    ),
    row(
        "c_float64",
        written(
            "Float64",
            "DOUBLE",
            values([Some("2.5"), Some("-0.125"), None]),
        ),
        written(
            "double",
            "DOUBLE",
            values([Some("2.5"), Some("-0.125"), None]),
        ),
        written(
            "double",
            "DOUBLE",
            values([Some("2.5"), Some("-0.125"), None]),
        ),
    ),
    row(
        "c_boolean",
        written(
            "Boolean",
            "BOOLEAN",
            values([Some("true"), Some("false"), None]),
        ),
        written(
            "boolean",
            "BOOLEAN",
            values([Some("true"), Some("false"), None]),
        ),
        written(
            "boolean",
            "BOOLEAN",
            values([Some("true"), Some("false"), None]),
        ),
    ),
    row(
        "c_utf8",
        written(
            "Utf8",
            VARCHAR,
            values([Some("h\u{e9}llo"), Some("w\u{f6}rld"), None]),
        ),
        written(
            "string",
            VARCHAR,
            values([Some("h\u{e9}llo"), Some("w\u{f6}rld"), None]),
        ),
        written(
            "string",
            VARCHAR,
            values([Some("h\u{e9}llo"), Some("w\u{f6}rld"), None]),
        ),
    ),
    row(
        "c_largeutf8",
        written(
            "LargeUtf8",
            VARCHAR,
            values([Some("h\u{e9}llo"), Some("w\u{f6}rld"), None]),
        ),
        omitted(ICEBERG_NO_LARGE_UTF8),
        omitted(GLUE_NO_LARGE_UTF8),
    ),
    row(
        "c_decimal128_10_2",
        written(
            "Decimal128(10,2)",
            "DECIMAL(10,2)",
            values([Some("12.34"), Some("-0.05"), None]),
        ),
        written(
            "decimal(10,2)",
            "DECIMAL(10,2)",
            values([Some("12.34"), Some("-0.05"), None]),
        ),
        written(
            "decimal(10,2)",
            "DECIMAL(10,2)",
            values([Some("12.34"), Some("-0.05"), None]),
        ),
    ),
    row(
        "c_decimal128_38_10",
        written(
            "Decimal128(38,10)",
            VARCHAR,
            values([
                Some("1234567890123456789012345678.9012345678"),
                Some("-0.0000000005"),
                None,
            ]),
        ),
        written(
            "decimal(38,10)",
            VARCHAR,
            values([
                Some("1234567890123456789012345678.9012345678"),
                Some("-0.0000000005"),
                None,
            ]),
        ),
        written(
            "decimal(38,10)",
            VARCHAR,
            values([
                Some("1234567890123456789012345678.9012345678"),
                Some("-0.0000000005"),
                None,
            ]),
        ),
    ),
    row(
        "c_decimal256_50_2",
        written(
            "Decimal256(50,2)",
            VARCHAR,
            values([Some("12.34"), Some("-0.05"), None]),
        ),
        omitted(ICEBERG_NO_DECIMAL256),
        omitted(GLUE_NO_DECIMAL256),
    ),
    row(
        "c_date32",
        written(
            "Date32",
            "DATE",
            values([Some("2024-01-15"), Some("1970-01-01"), None]),
        ),
        written(
            "date",
            "DATE",
            values([Some("2024-01-15"), Some("1970-01-01"), None]),
        ),
        written(
            "date",
            "DATE",
            values([Some("2024-01-15"), Some("1970-01-01"), None]),
        ),
    ),
    row(
        "c_timestamp",
        written(
            "Timestamp(us)",
            "TIMESTAMP(3)",
            values([
                Some("2024-01-15 10:30:45.123000"),
                Some("1970-01-01 00:00:00.000000"),
                None,
            ]),
        ),
        written(
            "timestamp",
            "TIMESTAMP(6)",
            values([
                Some("2024-01-15 10:30:45.123456"),
                Some("1970-01-01 00:00:00.000000"),
                None,
            ]),
        ),
        written(
            "timestamp",
            "TIMESTAMP(6)",
            values([
                Some("2024-01-15 10:30:45.123456"),
                Some("1970-01-01 00:00:00.000000"),
                None,
            ]),
        ),
    ),
    row(
        "c_int96",
        Cell::Written {
            table: Annotated,
            source_type: "INT96",
            exasol_type: "TIMESTAMP(3)",
            outcome: values([
                Some("2024-01-15 10:30:45.000000"),
                Some("1970-01-01 00:00:00.000000"),
                None,
            ]),
        },
        omitted(ICEBERG_NO_INT96),
        omitted(GLUE_NO_INT96),
    ),
    row(
        "c_time32",
        written(
            "Time32(ms)",
            VARCHAR,
            values([Some("12:34:56"), Some("00:00:00"), None]),
        ),
        omitted(ICEBERG_NO_TIME32),
        omitted(GLUE_NO_TIME),
    ),
    row(
        "c_time64",
        written(
            "Time64(us)",
            VARCHAR,
            values([Some("12:34:56.123456"), Some("00:00:00"), None]),
        ),
        written(
            "time",
            VARCHAR,
            values([Some("12:34:56.123456"), Some("00:00:00"), None]),
        ),
        omitted(GLUE_NO_TIME),
    ),
    row(
        "c_duration",
        written(
            "Duration(us)",
            VARCHAR,
            values([
                Some("0 days 0 hours 0 mins 1.500000 secs"),
                Some("0 days 0 hours 0 mins 0.000000 secs"),
                None,
            ]),
        ),
        omitted(ICEBERG_NO_DURATION),
        omitted(GLUE_NO_DURATION),
    ),
    row(
        "c_interval",
        written(
            "Interval(DayTime)",
            VARCHAR,
            values([Some("1 days 2.000 secs"), Some("0 secs"), None]),
        ),
        omitted(ICEBERG_NO_INTERVAL),
        omitted(GLUE_NO_INTERVAL),
    ),
    row(
        "c_binary",
        written(
            "Binary",
            VARCHAR,
            Outcome::Refused(&["type 'binary'", "#351"]),
        ),
        written(
            "binary",
            VARCHAR,
            Outcome::Refused(&["type 'binary'", "#351"]),
        ),
        written(
            "binary",
            VARCHAR,
            Outcome::Refused(&["type 'binary'", "#351"]),
        ),
    ),
    row(
        "c_largebinary",
        written(
            "LargeBinary",
            VARCHAR,
            Outcome::Refused(&["type 'binary'", "#351"]),
        ),
        omitted(ICEBERG_NO_LARGE_BINARY),
        omitted(GLUE_NO_LARGE_BINARY),
    ),
    row(
        "c_fixedsizebinary",
        written(
            "FixedSizeBinary(16)",
            VARCHAR,
            Outcome::Refused(&["type 'fixed(16)'", "#351"]),
        ),
        written(
            "fixed(16)",
            VARCHAR,
            Outcome::Refused(&["type 'fixed(16)'", "#351"]),
        ),
        omitted(GLUE_NO_FIXED),
    ),
    row(
        "c_enum",
        Cell::Written {
            table: Annotated,
            source_type: "BYTE_ARRAY (ENUM)",
            exasol_type: VARCHAR,
            outcome: values([Some("red"), Some("green"), None]),
        },
        omitted(ICEBERG_NO_ENUM),
        omitted(GLUE_NO_ENUM),
    ),
    row(
        "c_uuid",
        Cell::Written {
            table: Annotated,
            source_type: "FIXED_LEN_BYTE_ARRAY(16) (UUID)",
            exasol_type: VARCHAR,
            outcome: Outcome::Refused(&["type 'uuid'", "#351"]),
        },
        written("uuid", VARCHAR, Outcome::Refused(&["type 'uuid'", "#351"])),
        omitted(GLUE_NO_UUID),
    ),
    row(
        "c_byte_array",
        Cell::Written {
            table: Annotated,
            source_type: "BYTE_ARRAY, valid UTF-8, no annotation, no Arrow schema",
            exasol_type: VARCHAR,
            outcome: Outcome::Refused(&["type 'binary'", "#351"]),
        },
        omitted(ICEBERG_NO_BARE_BYTE_ARRAY),
        written(
            "string",
            VARCHAR,
            values([Some("legacy-a"), Some("legacy-b"), None]),
        ),
    ),
    row(
        "c_list",
        written(
            "List<Int32>",
            VARCHAR,
            values([Some("[1,2]"), Some("[]"), None]),
        ),
        written(
            "list<int>",
            VARCHAR,
            values([Some("[1,2]"), Some("[]"), None]),
        ),
        written(
            "array<int>",
            VARCHAR,
            values([Some("[1,2]"), Some("[]"), None]),
        ),
    ),
    row(
        "c_struct",
        written(
            "Struct<a: Int32, b: Utf8>",
            VARCHAR,
            values([
                Some("{\"a\":1,\"b\":\"x\"}"),
                Some("{\"a\":2,\"b\":null}"),
                None,
            ]),
        ),
        written(
            "struct<a: int, b: string>",
            VARCHAR,
            values([
                Some("{\"a\":1,\"b\":\"x\"}"),
                Some("{\"a\":2,\"b\":null}"),
                None,
            ]),
        ),
        written(
            "struct<a:int,b:string>",
            VARCHAR,
            values([
                Some("{\"a\":1,\"b\":\"x\"}"),
                Some("{\"a\":2,\"b\":null}"),
                None,
            ]),
        ),
    ),
    row(
        "c_map",
        written(
            "Map<Utf8, Int32>",
            VARCHAR,
            values([Some("{\"k1\":1,\"k2\":2}"), Some("{}"), None]),
        ),
        written(
            "map<string, int>",
            VARCHAR,
            values([Some("{\"k1\":1,\"k2\":2}"), Some("{}"), None]),
        ),
        written(
            "map<string,int>",
            VARCHAR,
            values([Some("{\"k1\":1,\"k2\":2}"), Some("{}"), None]),
        ),
    ),
    row(
        "c_struct_binary",
        written(
            "Struct<x: Binary>",
            VARCHAR,
            Outcome::Refused(&["member 'c_struct_binary.x'", "type 'binary'", "#351"]),
        ),
        written(
            "struct<x: binary>",
            VARCHAR,
            Outcome::Refused(&["member 'c_struct_binary.x'", "type 'binary'", "#351"]),
        ),
        written(
            "struct<x:binary>",
            VARCHAR,
            Outcome::Refused(&["member 'c_struct_binary.x'", "type 'binary'", "#351"]),
        ),
    ),
    row(
        "c_struct_enum",
        Cell::Written {
            table: Annotated,
            source_type: "Struct<k: BYTE_ARRAY (ENUM)>",
            exasol_type: VARCHAR,
            outcome: Outcome::Refused(&["member 'c_struct_enum.k'", "type 'enum'", "#351"]),
        },
        omitted(ICEBERG_NO_ENUM),
        omitted(GLUE_NO_ENUM),
    ),
    row(
        "c_bytes",
        Cell::Written {
            table: BinaryValues,
            source_type: "BYTE_ARRAY, not valid UTF-8, no annotation, no Arrow schema",
            exasol_type: VARCHAR,
            outcome: Outcome::Refused(&["type 'binary'", "#351"]),
        },
        Cell::Written {
            table: BinaryValues,
            source_type: "string over bytes that are not valid UTF-8",
            exasol_type: VARCHAR,
            outcome: Outcome::Fails("non UTF-8 data"),
        },
        Cell::Written {
            table: BinaryValues,
            source_type: "string",
            exasol_type: VARCHAR,
            outcome: Outcome::Fails("Invalid UTF8 sequence"),
        },
    ),
];

pub fn rows_of(source: Source, table: MatrixTable) -> Vec<&'static MatrixRow> {
    MATRIX
        .iter()
        .filter(|r| matches!(r.cell(source), Cell::Written { table: t, .. } if *t == table))
        .collect()
}

/// One column a virtual schema must declare and return as its matrix row, or another fixture
/// table of Hive types, states.
#[derive(Clone, Copy, Debug)]
pub struct ExpectedColumn {
    pub table: &'static str,
    pub column: &'static str,
    pub exasol_type: &'static str,
    pub outcome: Outcome,
}

pub fn expected_columns(source: Source, table: MatrixTable) -> Vec<ExpectedColumn> {
    rows_of(source, table)
        .into_iter()
        .filter_map(|matrix_row| match *matrix_row.cell(source) {
            Cell::Written {
                exasol_type,
                outcome,
                ..
            } => Some(ExpectedColumn {
                table: table.name(),
                column: matrix_row.column,
                exasol_type,
                outcome,
            }),
            Cell::Omitted(_) => None,
        })
        .collect()
}

/// Declared types keyed by `(TABLE, COLUMN)`, whitespace and the character-set suffix removed.
pub fn declared_types(conn: &mut ExaConn, vs_name: &str) -> HashMap<(String, String), String> {
    let columns = conn.query_columns(&format!(
        "SELECT COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS \
         WHERE COLUMN_SCHEMA = '{vs_name}'"
    ));
    (0..columns[0].len())
        .map(|i| {
            let declared: String = value_to_string(&columns[2][i])
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            let declared = declared
                .strip_suffix("UTF8")
                .or_else(|| declared.strip_suffix("ASCII"))
                .unwrap_or(&declared)
                .to_string();
            (
                (
                    value_to_string(&columns[0][i]),
                    value_to_string(&columns[1][i]),
                ),
                declared,
            )
        })
        .collect()
}

enum QueryResult {
    Columns(Vec<Vec<Value>>),
    Error(String),
}

fn run_select(conn: &mut ExaConn, sql: &str) -> QueryResult {
    let response = conn.try_execute(sql);
    if response["status"].as_str() != Some("ok") {
        return QueryResult::Error(
            response["exception"]["text"]
                .as_str()
                .unwrap_or("")
                .to_string(),
        );
    }
    let result_set = &response["responseData"]["results"][0]["resultSet"];
    QueryResult::Columns(conn.fetch_result_columns(result_set))
}

fn cell_text(cell: &Value) -> Option<String> {
    (!cell.is_null()).then(|| value_to_string(cell))
}

fn parse_ids(column: &[Value]) -> Vec<i64> {
    column
        .iter()
        .filter_map(|v| value_to_string(v).parse().ok())
        .collect()
}

/// Collects every mismatch of one virtual schema against its expected columns, so a run reports
/// all of them in one failure.
pub struct MatrixCheck<'a> {
    conn: &'a mut ExaConn,
    vs_name: &'a str,
    label: String,
    declared: HashMap<(String, String), String>,
    mismatches: Vec<String>,
}

impl<'a> MatrixCheck<'a> {
    pub fn new(conn: &'a mut ExaConn, vs_name: &'a str, label: &str) -> Self {
        let declared = declared_types(conn, vs_name);
        Self {
            conn,
            vs_name,
            label: label.to_string(),
            declared,
            mismatches: Vec::new(),
        }
    }

    pub fn check_ids(&mut self, table: &str) {
        let served = table.to_uppercase();
        let label = format!("[{}] {served}.ID", self.label);
        let sql = format!("SELECT ID FROM {}.{served} ORDER BY ID", self.vs_name);
        match run_select(self.conn, &sql) {
            QueryResult::Columns(result) => {
                let ids = parse_ids(&result[0]);
                if ids != IDS {
                    self.mismatches
                        .push(format!("{label}: ids {ids:?}, expected {IDS:?}"));
                }
            }
            QueryResult::Error(error) => {
                self.mismatches
                    .push(format!("{label}: SELECT ID failed: {error}"));
            }
        }
    }

    pub fn check_column(&mut self, expected: &ExpectedColumn) {
        let served = expected.table.to_uppercase();
        let upper = expected.column.to_uppercase();
        let label = format!("[{}] {served}.{upper}", self.label);
        match self.declared.get(&(served.clone(), upper.clone())) {
            Some(observed) if observed == expected.exasol_type => {}
            Some(observed) => self.mismatches.push(format!(
                "{label}: declared {observed}, expected {}",
                expected.exasol_type
            )),
            None => self
                .mismatches
                .push(format!("{label}: column is absent from the virtual schema")),
        }
        let result = run_select(
            self.conn,
            &format!(
                "SELECT ID, {upper} FROM {}.{served} ORDER BY ID",
                self.vs_name
            ),
        );
        self.check_outcome(&label, result, expected.outcome);
    }

    pub fn into_mismatches(self) -> Vec<String> {
        self.mismatches
    }

    fn check_outcome(&mut self, label: &str, result: QueryResult, outcome: Outcome) {
        match (outcome, result) {
            (Outcome::Values(expected), QueryResult::Columns(columns)) => {
                let ids = parse_ids(&columns[0]);
                if ids != IDS {
                    self.mismatches
                        .push(format!("{label}: ids {ids:?}, expected {IDS:?}"));
                    return;
                }
                let observed: Vec<Option<String>> = columns[1].iter().map(cell_text).collect();
                let expected: Vec<Option<String>> =
                    expected.iter().map(|v| v.map(str::to_string)).collect();
                if observed != expected {
                    self.mismatches.push(format!(
                        "{label}: values {observed:?}, expected {expected:?}"
                    ));
                }
            }
            (Outcome::Values(_), QueryResult::Error(error)) => {
                self.mismatches
                    .push(format!("{label}: expected values, query failed: {error}"));
            }
            (Outcome::Refused(fragments), QueryResult::Error(error)) => {
                if let Some(missing) = fragments.iter().find(|f| !error.contains(**f)) {
                    self.mismatches
                        .push(format!("{label}: error lacks {missing:?}: {error}"));
                }
            }
            (Outcome::Fails(fragment), QueryResult::Error(error)) => {
                if !error.contains(fragment) {
                    self.mismatches
                        .push(format!("{label}: error lacks {fragment:?}: {error}"));
                }
            }
            (Outcome::Refused(fragments), QueryResult::Columns(columns)) => {
                self.mismatches.push(format!(
                    "{label}: expected refusal naming {fragments:?}, query returned {:?}",
                    columns.get(1)
                ));
            }
            (Outcome::Fails(_), QueryResult::Columns(columns)) => {
                self.mismatches.push(format!(
                    "{label}: expected the query to fail, it returned {:?}",
                    columns.get(1)
                ));
            }
        }
    }
}

/// Checks every table of `source`: `SELECT ID` and each written column's declaration and outcome.
pub fn matrix_mismatches(conn: &mut ExaConn, source: Source, vs_name: &str) -> Vec<String> {
    let mut check = MatrixCheck::new(conn, vs_name, &format!("{source:?}"));
    for table in [
        MatrixTable::AllTypes,
        MatrixTable::Annotated,
        MatrixTable::BinaryValues,
    ] {
        let columns = expected_columns(source, table);
        if columns.is_empty() {
            continue;
        }
        check.check_ids(table.name());
        for column in &columns {
            check.check_column(column);
        }
    }
    check.into_mismatches()
}

pub fn assert_no_mismatches(mismatches: Vec<String>) {
    assert!(
        mismatches.is_empty(),
        "{} matrix mismatch(es):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

const TIMESTAMP_MICROS: i64 = 1_705_314_645_123_456;
const DATE_DAYS: i32 = 19_737;
const TIME_OF_DAY_SECONDS: i64 = 12 * 3600 + 34 * 60 + 56;
const UNIX_EPOCH_JULIAN_DAY: u32 = 2_440_588;
const INT96_NANOS_OF_DAY: u64 = 37_845 * 1_000_000_000;
const BYTES_ROW_1: &[u8] = &[0xFF, 0xFE, 0x00, 0x80];
const BYTES_ROW_2: &[u8] = &[0xC3, 0x28];
const UUID_ROW_1: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
];
const UUID_ROW_2: [u8; 16] = [0x11; 16];

fn row_validity() -> NullBuffer {
    NullBuffer::from(vec![true, true, false])
}

fn binary_values(data_type: &DataType) -> ArrayRef {
    let values = vec![Some(BYTES_ROW_1), Some(BYTES_ROW_2), None];
    match data_type {
        DataType::Binary => Arc::new(BinaryArray::from_opt_vec(values)),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from_opt_vec(values)),
        other => panic!("binary member has type {other}"),
    }
}

fn fixed_16_values() -> ArrayRef {
    Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(UUID_ROW_1), Some(UUID_ROW_2), None].into_iter(),
            16,
        )
        .expect("build FixedSizeBinary(16) array"),
    )
}

fn struct_fields(data_type: &DataType, column: &str) -> Fields {
    match data_type {
        DataType::Struct(fields) => fields.clone(),
        other => panic!("{column} must be a struct, got {other}"),
    }
}

/// Builds the three values of `column` (two populated rows, one null) for the given
/// target type, so nested fields carry whatever field ids the target type holds.
fn column_values(column: &str, data_type: &DataType) -> ArrayRef {
    match column {
        "c_int8" => Arc::new(Int8Array::from(vec![Some(127), Some(-128), None])),
        "c_int16" => Arc::new(Int16Array::from(vec![Some(32_767), Some(-32_768), None])),
        "c_int32" => Arc::new(Int32Array::from(vec![Some(i32::MAX), Some(i32::MIN), None])),
        "c_int64" => Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN), None])),
        "c_uint8" => Arc::new(UInt8Array::from(vec![Some(255), Some(0), None])),
        "c_uint16" => Arc::new(UInt16Array::from(vec![Some(65_535), Some(0), None])),
        "c_uint32" => Arc::new(UInt32Array::from(vec![Some(u32::MAX), Some(0), None])),
        "c_uint64" => Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0), None])),
        "c_float32" => Arc::new(Float32Array::from(vec![Some(1.5), Some(-0.25), None])),
        "c_float64" => Arc::new(Float64Array::from(vec![Some(2.5), Some(-0.125), None])),
        "c_boolean" => Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        "c_utf8" => Arc::new(StringArray::from(vec![
            Some("h\u{e9}llo"),
            Some("w\u{f6}rld"),
            None,
        ])),
        "c_largeutf8" => Arc::new(LargeStringArray::from(vec![
            Some("h\u{e9}llo"),
            Some("w\u{f6}rld"),
            None,
        ])),
        "c_decimal128_10_2" => Arc::new(
            Decimal128Array::from(vec![Some(1234), Some(-5), None])
                .with_data_type(data_type.clone()),
        ),
        "c_decimal128_38_10" => Arc::new(
            Decimal128Array::from(vec![
                Some(12_345_678_901_234_567_890_123_456_789_012_345_678_i128),
                Some(-5),
                None,
            ])
            .with_data_type(data_type.clone()),
        ),
        "c_decimal256_50_2" => Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(1234)),
                Some(i256::from_i128(-5)),
                None,
            ])
            .with_data_type(data_type.clone()),
        ),
        "c_date32" => Arc::new(Date32Array::from(vec![Some(DATE_DAYS), Some(0), None])),
        "c_timestamp" => Arc::new(
            TimestampMicrosecondArray::from(vec![Some(TIMESTAMP_MICROS), Some(0), None])
                .with_data_type(data_type.clone()),
        ),
        "c_time32" => Arc::new(Time32MillisecondArray::from(vec![
            Some((TIME_OF_DAY_SECONDS * 1000) as i32),
            Some(0),
            None,
        ])),
        "c_time64" => Arc::new(Time64MicrosecondArray::from(vec![
            Some(TIME_OF_DAY_SECONDS * 1_000_000 + 123_456),
            Some(0),
            None,
        ])),
        "c_duration" => Arc::new(DurationMicrosecondArray::from(vec![
            Some(1_500_000),
            Some(0),
            None,
        ])),
        "c_interval" => Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(1, 2000)),
            Some(IntervalDayTime::new(0, 0)),
            None,
        ])),
        "c_binary" | "c_largebinary" => binary_values(data_type),
        "c_byte_array" => Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"legacy-a".as_slice()),
            Some(b"legacy-b".as_slice()),
            None,
        ])),
        "c_fixedsizebinary" | "c_uuid" => fixed_16_values(),
        "c_list" => {
            let DataType::List(element) = data_type else {
                panic!("c_list must be a list, got {data_type}");
            };
            Arc::new(
                ListArray::try_new(
                    element.clone(),
                    OffsetBuffer::from_lengths([2, 0, 0]),
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Some(row_validity()),
                )
                .expect("build c_list"),
            )
        }
        "c_struct" => {
            let fields = struct_fields(data_type, column);
            Arc::new(
                StructArray::try_new(
                    fields,
                    vec![
                        Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
                        Arc::new(StringArray::from(vec![Some("x"), None, None])),
                    ],
                    Some(row_validity()),
                )
                .expect("build c_struct"),
            )
        }
        "c_struct_binary" => {
            let fields = struct_fields(data_type, column);
            let member = binary_values(fields[0].data_type());
            Arc::new(
                StructArray::try_new(fields, vec![member], Some(row_validity()))
                    .expect("build c_struct_binary"),
            )
        }
        "c_map" => {
            let DataType::Map(entries_field, sorted) = data_type else {
                panic!("c_map must be a map, got {data_type}");
            };
            let entries = StructArray::try_new(
                struct_fields(entries_field.data_type(), column),
                vec![
                    Arc::new(StringArray::from(vec!["k1", "k2"])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![1, 2])),
                ],
                None,
            )
            .expect("build c_map entries");
            Arc::new(
                MapArray::try_new(
                    entries_field.clone(),
                    OffsetBuffer::from_lengths([2, 0, 0]),
                    entries,
                    Some(row_validity()),
                    *sorted,
                )
                .expect("build c_map"),
            )
        }
        other => panic!("type matrix has no values for column {other}"),
    }
}

fn direct_data_type(column: &str) -> DataType {
    match column {
        "c_int8" => DataType::Int8,
        "c_int16" => DataType::Int16,
        "c_int32" => DataType::Int32,
        "c_int64" => DataType::Int64,
        "c_uint8" => DataType::UInt8,
        "c_uint16" => DataType::UInt16,
        "c_uint32" => DataType::UInt32,
        "c_uint64" => DataType::UInt64,
        "c_float32" => DataType::Float32,
        "c_float64" => DataType::Float64,
        "c_boolean" => DataType::Boolean,
        "c_utf8" => DataType::Utf8,
        "c_largeutf8" => DataType::LargeUtf8,
        "c_decimal128_10_2" => DataType::Decimal128(10, 2),
        "c_decimal128_38_10" => DataType::Decimal128(38, 10),
        "c_decimal256_50_2" => DataType::Decimal256(50, 2),
        "c_date32" => DataType::Date32,
        "c_timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "c_time32" => DataType::Time32(TimeUnit::Millisecond),
        "c_time64" => DataType::Time64(TimeUnit::Microsecond),
        "c_duration" => DataType::Duration(TimeUnit::Microsecond),
        "c_interval" => DataType::Interval(IntervalUnit::DayTime),
        "c_binary" => DataType::Binary,
        "c_largebinary" => DataType::LargeBinary,
        "c_fixedsizebinary" => DataType::FixedSizeBinary(16),
        "c_list" => DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        "c_struct" => DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ])),
        "c_map" => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Int32, true),
                ])),
                false,
            )),
            false,
        ),
        "c_struct_binary" => {
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Binary, true)]))
        }
        other => panic!("type matrix has no direct-storage Arrow type for column {other}"),
    }
}

fn direct_all_types_batch() -> RecordBatch {
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(IDS.to_vec()))];
    for matrix_row in rows_of(Source::DirectStorage, AllTypes) {
        let data_type = direct_data_type(matrix_row.column);
        columns.push(column_values(matrix_row.column, &data_type));
        fields.push(Field::new(matrix_row.column, data_type, true));
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
        .expect("build direct-storage all_types batch")
}

fn write_optional_column<W: std::io::Write + Send, T: ParquetDataType>(
    row_group: &mut SerializedRowGroupWriter<'_, W>,
    values: &[T::T],
    definition_levels: &[i16],
) {
    let mut column = row_group
        .next_column()
        .expect("open column writer")
        .expect("schema has another column");
    column
        .typed::<T>()
        .write_batch(values, Some(definition_levels), None)
        .expect("write column batch");
    column.close().expect("close column writer");
}

fn parquet_bytes(
    message: &str,
    write: impl FnOnce(&mut SerializedRowGroupWriter<'_, &mut Vec<u8>>),
) -> Bytes {
    let schema = Arc::new(parse_message_type(message).expect("parse Parquet message type"));
    let mut buffer = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut buffer,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("open Parquet file writer");
    let mut row_group = writer.next_row_group().expect("open row group");
    write(&mut row_group);
    row_group.close().expect("close row group");
    writer.close().expect("close Parquet file");
    Bytes::from(buffer)
}

fn annotated_bytes() -> Bytes {
    parquet_bytes(
        "message annotated {
            REQUIRED INT64 id;
            OPTIONAL BYTE_ARRAY c_enum (ENUM);
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) c_uuid (UUID);
            OPTIONAL INT96 c_int96;
            OPTIONAL BYTE_ARRAY c_byte_array;
            OPTIONAL GROUP c_struct_enum {
                OPTIONAL BYTE_ARRAY k (ENUM);
            }
        }",
        |row_group| {
            let mut id = row_group.next_column().unwrap().unwrap();
            id.typed::<Int64Type>()
                .write_batch(&IDS, None, None)
                .expect("write id");
            id.close().expect("close id");

            let enum_values = [ByteArray::from("red"), ByteArray::from("green")];
            write_optional_column::<_, ByteArrayType>(row_group, &enum_values, &[1, 1, 0]);

            let uuids = [
                FixedLenByteArray::from(UUID_ROW_1.to_vec()),
                FixedLenByteArray::from(UUID_ROW_2.to_vec()),
            ];
            write_optional_column::<_, FixedLenByteArrayType>(row_group, &uuids, &[1, 1, 0]);

            let mut epoch_day = Int96::new();
            epoch_day.set_data(0, 0, UNIX_EPOCH_JULIAN_DAY);
            let mut ts = Int96::new();
            ts.set_data(
                INT96_NANOS_OF_DAY as u32,
                (INT96_NANOS_OF_DAY >> 32) as u32,
                UNIX_EPOCH_JULIAN_DAY + DATE_DAYS as u32,
            );
            write_optional_column::<_, Int96Type>(row_group, &[ts, epoch_day], &[1, 1, 0]);

            let legacy = [ByteArray::from("legacy-a"), ByteArray::from("legacy-b")];
            write_optional_column::<_, ByteArrayType>(row_group, &legacy, &[1, 1, 0]);

            let nested_enum = [ByteArray::from("blue"), ByteArray::from("amber")];
            write_optional_column::<_, ByteArrayType>(row_group, &nested_enum, &[2, 2, 0]);
        },
    )
}

fn binary_values_file(message: &str) -> Bytes {
    parquet_bytes(message, |row_group| {
        let mut id = row_group.next_column().unwrap().unwrap();
        id.typed::<Int64Type>()
            .write_batch(&IDS, None, None)
            .expect("write id");
        id.close().expect("close id");

        let values = [
            ByteArray::from(BYTES_ROW_1.to_vec()),
            ByteArray::from(BYTES_ROW_2.to_vec()),
        ];
        write_optional_column::<_, ByteArrayType>(row_group, &values, &[1, 1, 0]);
    })
}

pub fn write_direct_storage_fixtures() {
    write_parquet_fixture(
        &format!("{DIRECT_BASE}{}/file1.parquet", AllTypes.name()),
        direct_all_types_batch(),
    );
    write_parquet_bytes(
        &format!("{DIRECT_BASE}{}/file1.parquet", Annotated.name()),
        annotated_bytes(),
    );
    write_parquet_bytes(
        &format!("{DIRECT_BASE}{}/file1.parquet", BinaryValues.name()),
        unannotated_binary_values_bytes(),
    );
}

/// The `binary_values` file of the sources that declare no field ids: `c_bytes` is a
/// `BYTE_ARRAY` with no annotation and no embedded Arrow schema.
pub fn unannotated_binary_values_bytes() -> Bytes {
    binary_values_file(
        "message binary_values {
            REQUIRED INT64 id;
            OPTIONAL BYTE_ARRAY c_bytes;
        }",
    )
}

/// Hive and Spark name a list member `element` and a map's entries `key_value`, `key`, and
/// `value`, so the Glue files carry the member names the Hive writers do.
fn glue_data_type(column: &str) -> DataType {
    match column {
        "c_byte_array" => DataType::Binary,
        "c_list" => DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
        "c_map" => DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ])),
                false,
            )),
            false,
        ),
        other => direct_data_type(other),
    }
}

/// One Glue `all_types` column per matrix row the Glue source writes there, with the Hive type
/// its table declares.
pub fn glue_all_types_columns() -> Vec<(&'static str, &'static str, Field, ArrayRef)> {
    rows_of(Source::Glue, AllTypes)
        .into_iter()
        .map(|matrix_row| {
            let Cell::Written { source_type, .. } = matrix_row.glue else {
                unreachable!("rows_of returns written cells only")
            };
            let data_type = glue_data_type(matrix_row.column);
            let values = column_values(matrix_row.column, &data_type);
            (
                matrix_row.column,
                source_type,
                Field::new(matrix_row.column, data_type, true),
                values,
            )
        })
        .collect()
}

fn iceberg_type(column: &str, first_nested_id: i32) -> Type {
    let optional_field = |id: i32, name: &str, ty: PrimitiveType| -> NestedFieldRef {
        NestedField::optional(id, name, Type::Primitive(ty)).into()
    };
    match column {
        "c_int32" => Type::Primitive(PrimitiveType::Int),
        "c_int64" => Type::Primitive(PrimitiveType::Long),
        "c_float32" => Type::Primitive(PrimitiveType::Float),
        "c_float64" => Type::Primitive(PrimitiveType::Double),
        "c_boolean" => Type::Primitive(PrimitiveType::Boolean),
        "c_utf8" | "c_bytes" => Type::Primitive(PrimitiveType::String),
        "c_decimal128_10_2" => Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        }),
        "c_decimal128_38_10" => Type::Primitive(PrimitiveType::Decimal {
            precision: 38,
            scale: 10,
        }),
        "c_date32" => Type::Primitive(PrimitiveType::Date),
        "c_timestamp" => Type::Primitive(PrimitiveType::Timestamp),
        "c_time64" => Type::Primitive(PrimitiveType::Time),
        "c_binary" => Type::Primitive(PrimitiveType::Binary),
        "c_fixedsizebinary" => Type::Primitive(PrimitiveType::Fixed(16)),
        "c_uuid" => Type::Primitive(PrimitiveType::Uuid),
        "c_list" => Type::List(ListType::new(
            NestedField::list_element(first_nested_id, Type::Primitive(PrimitiveType::Int), false)
                .into(),
        )),
        "c_struct" => Type::Struct(StructType::new(vec![
            optional_field(first_nested_id, "a", PrimitiveType::Int),
            optional_field(first_nested_id + 1, "b", PrimitiveType::String),
        ])),
        "c_map" => Type::Map(MapType::optional(
            first_nested_id,
            Type::Primitive(PrimitiveType::String),
            first_nested_id + 1,
            Type::Primitive(PrimitiveType::Int),
        )),
        "c_struct_binary" => Type::Struct(StructType::new(vec![optional_field(
            first_nested_id,
            "x",
            PrimitiveType::Binary,
        )])),
        other => panic!("type matrix has no Iceberg type for column {other}"),
    }
}

const FIRST_COLUMN_FIELD_ID: i32 = 2;
const FIRST_NESTED_FIELD_ID: i32 = 100;
const NESTED_FIELD_IDS_PER_COLUMN: i32 = 10;

fn iceberg_schema(table: MatrixTable) -> Result<IcebergSchema> {
    let mut fields =
        vec![NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into()];
    for (index, matrix_row) in rows_of(Source::Iceberg, table).into_iter().enumerate() {
        let index = index as i32;
        fields.push(
            NestedField::optional(
                FIRST_COLUMN_FIELD_ID + index,
                matrix_row.column,
                iceberg_type(
                    matrix_row.column,
                    FIRST_NESTED_FIELD_ID + NESTED_FIELD_IDS_PER_COLUMN * index,
                ),
            )
            .into(),
        );
    }
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .context("build type-matrix Iceberg schema")
}

async fn create_fresh_iceberg_table(catalog: &impl Catalog, table: MatrixTable) -> Result<Table> {
    let namespace = NamespaceIdent::new(ICEBERG_NAMESPACE.to_string());
    let ident = TableIdent::new(namespace.clone(), table.name().to_string());
    if catalog
        .table_exists(&ident)
        .await
        .context("check type-matrix table exists")?
    {
        catalog
            .drop_table(&ident)
            .await
            .context("drop stale type-matrix table")?;
    }
    if !catalog
        .namespace_exists(&namespace)
        .await
        .context("check type-matrix namespace")?
    {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .context("create the type-matrix namespace")?;
    }
    let creation = TableCreation::builder()
        .name(table.name().to_string())
        .schema(iceberg_schema(table)?)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .context("create type-matrix table")
}

/// `iceberg-rest-fixture` assigns fresh nested field ids on `create_table`, so the batch is
/// built against the created table's own Arrow schema, and the table is recreated on every
/// run so a stale one never pins old columns.
async fn seed_all_types(catalog: &impl Catalog) -> Result<()> {
    let created = create_fresh_iceberg_table(catalog, AllTypes).await?;
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(created.metadata().current_schema())
            .context("derive type-matrix Arrow schema")?,
    );
    let columns: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "id" => Arc::new(Int64Array::from(IDS.to_vec())) as ArrayRef,
            column => column_values(column, field.data_type()),
        })
        .collect();
    let batch = RecordBatch::try_new(arrow_schema, columns).context("build type-matrix batch")?;
    write_one_file_append(catalog, &created, AllTypes.name(), [batch])
        .await
        .context("append type-matrix batch")
}

/// `iceberg-rust` refuses to write a string column whose bytes are not valid UTF-8, so the
/// data file is written with `parquet` directly and committed as an existing data file.
async fn seed_binary_values(catalog: &impl Catalog) -> Result<()> {
    let created = create_fresh_iceberg_table(catalog, BinaryValues).await?;
    let schema = created.metadata().current_schema();
    let field_id = |name: &str| {
        schema
            .field_id_by_name(name)
            .with_context(|| format!("created table has no field {name}"))
    };
    let bytes = binary_values_file(&format!(
        "message binary_values {{
            REQUIRED INT64 id = {};
            OPTIONAL BYTE_ARRAY c_bytes (STRING) = {};
        }}",
        field_id("id")?,
        field_id("c_bytes")?
    ));

    let path = format!(
        "{}/data/{}.parquet",
        created.metadata().location(),
        BinaryValues.name()
    );
    let file_size = bytes.len() as u64;
    created
        .file_io()
        .new_output(&path)
        .context("open type-matrix data file")?
        .write(bytes)
        .await
        .context("write type-matrix data file")?;
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path)
        .file_format(DataFileFormat::Parquet)
        .record_count(ROWS as u64)
        .file_size_in_bytes(file_size)
        .partition_spec_id(created.metadata().default_partition_spec_id())
        .build()
        .context("describe type-matrix data file")?;

    let transaction = Transaction::new(&created);
    let transaction = transaction
        .fast_append()
        .add_data_files(vec![data_file])
        .apply(transaction)
        .context("apply type-matrix fast-append")?;
    transaction
        .commit(catalog)
        .await
        .context("commit type-matrix data file")?;
    Ok(())
}

pub async fn seed_iceberg_tables(catalog_url: &str, warehouse: &str) -> Result<()> {
    let catalog =
        build_seed_catalog(catalog_url, warehouse, "lakehouse-e2e-seed-type-matrix").await?;
    seed_all_types(&catalog).await?;
    seed_binary_values(&catalog).await
}
