use super::*;
use crate::export::iceberg_sink::table_columns;
use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::{Decimal, ExaType, Value};
use iceberg::TableIdent;
use iceberg::spec::{NestedField, Schema as IcebergSchema, Type};
use vs_expression::udf_column_test_support::measured_column;

const SPEC: &str = "the writer spec";

/// The columns of table `ns.t` with `columns`' names and Iceberg types, each written as in table
/// metadata JSON, such as `"long"` or `{"type": "list", ...}`, and required when `required`.
fn iceberg_table(columns: &[(&str, serde_json::Value)], required: bool) -> TableColumns {
    let fields = columns.iter().enumerate().map(|(index, (name, iceberg))| {
        let field_type: Type = serde_json::from_value(iceberg.clone()).expect("an Iceberg type");
        let id = i32::try_from(index).unwrap() + 1;
        NestedField::new(id, *name, field_type, required).into()
    });
    let schema = IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .expect("an Iceberg schema");
    table_columns(&TableIdent::from_strs(["ns", "t"]).unwrap(), &schema).expect("table columns")
}

/// Optional table columns of primitive Iceberg types, such as `("ID", "long")`.
fn table(columns: &[(&str, &str)]) -> TableColumns {
    let columns: Vec<_> = columns
        .iter()
        .map(|(name, iceberg)| (*name, serde_json::Value::from(*iceberg)))
        .collect();
    iceberg_table(&columns, false)
}

/// Source columns with Exasol types, such as `("ID", "DECIMAL(18,0)")`.
fn source(columns: &[(&str, &str)]) -> SourceColumns {
    SourceColumns {
        names: columns.iter().map(|(name, _)| name.to_string()).collect(),
        columns: columns
            .iter()
            .map(|(_, exasol)| measured_column(exasol))
            .collect(),
    }
}

/// A table and a source with the same column names, from `(name, Exasol type, Iceberg type)`.
fn same_names(columns: &[(&str, &str, &str)]) -> (TableColumns, SourceColumns) {
    let table_columns: Vec<_> = columns.iter().map(|(n, _, i)| (*n, *i)).collect();
    let source_columns: Vec<_> = columns.iter().map(|(n, e, _)| (*n, *e)).collect();
    (table(&table_columns), source(&source_columns))
}

/// The batch of `rows`, each given in source column order without the writer spec of input
/// column 0.
fn batch_of(
    table: &TableColumns,
    source: &SourceColumns,
    rows: Vec<Vec<Value>>,
) -> Result<RecordBatch, UdfError> {
    let mut builder = BatchBuilder::bind(table, source)?;
    let rows = rows
        .into_iter()
        .map(|row| {
            std::iter::once(Value::String(SPEC.into()))
                .chain(row)
                .collect()
        })
        .collect();
    let mut ctx = TestContext::set(rows);
    while ctx.next()? {
        builder.append_row(&ctx)?;
    }
    builder.take()
}

fn column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .expect("the column has the schema's type")
}

fn timestamp(text: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").expect("a timestamp")
}

fn numeric(unscaled: i128, scale: u8) -> Value {
    Value::Numeric(Decimal { unscaled, scale })
}

fn error_text<T>(result: Result<T, UdfError>) -> String {
    match result {
        Err(error) => error.to_string(),
        Ok(_) => panic!("expected a refusal"),
    }
}

const MISMATCH: &str =
    "LAKEHOUSE_EXPORT: the export columns do not match the columns of table ns.t: ";

#[test]
fn each_type_and_null_is_written_with_its_table_type() {
    let (table, source) = same_names(&[
        ("D", "DECIMAL(18,2)", "decimal(18, 2)"),
        ("F", "DOUBLE", "double"),
        ("B", "BOOLEAN", "boolean"),
        ("S", "VARCHAR(10) UTF8", "string"),
        ("DT", "DATE", "date"),
        ("TS", "TIMESTAMP(6)", "timestamp"),
        ("I", "DECIMAL(9,0)", "int"),
        ("L", "DECIMAL(18,0)", "long"),
    ]);
    let values = vec![
        numeric(15, 1),
        Value::Double(-1.5),
        Value::Bool(true),
        Value::String("ab   ".into()),
        Value::Date(NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()),
        Value::Timestamp(timestamp("1970-01-01 00:00:01.000002")),
        Value::Int32(-4),
        Value::Int64(5),
    ];
    let nulls = vec![Value::Null; values.len()];

    let batch = batch_of(&table, &source, vec![values, nulls]).expect("a batch");

    assert_eq!(batch.schema(), table.schema);
    assert_eq!(column::<Decimal128Array>(&batch, 0).value(0), 150);
    assert_eq!(column::<Float64Array>(&batch, 1).value(0), -1.5);
    assert!(column::<BooleanArray>(&batch, 2).value(0));
    assert_eq!(column::<StringArray>(&batch, 3).value(0), "ab   ");
    assert_eq!(column::<Date32Array>(&batch, 4).value(0), 1);
    assert_eq!(
        column::<TimestampMicrosecondArray>(&batch, 5).value(0),
        1_000_002
    );
    assert_eq!(column::<Int32Array>(&batch, 6).value(0), -4);
    assert_eq!(column::<Int64Array>(&batch, 7).value(0), 5);
    for index in 0..batch.num_columns() {
        assert!(batch.column(index).is_null(1), "column {index}");
    }
}

#[test]
fn every_decimal_variant_is_rescaled_to_the_table_scale() {
    let (table, source) = same_names(&[
        ("I32", "DECIMAL(9,0)", "decimal(9, 0)"),
        ("I64", "DECIMAL(19,0)", "decimal(19, 0)"),
        ("TRIMMED", "DECIMAL(18,2)", "decimal(18, 2)"),
        ("WHOLE", "DECIMAL(18,2)", "decimal(18, 2)"),
        ("INT", "DECIMAL(12,2)", "decimal(12, 2)"),
    ]);
    let row = vec![
        Value::Int32(-7),
        Value::Int64(9_000_000_000_000_000_000),
        numeric(15, 1),
        numeric(100, 0),
        Value::Int64(3),
    ];

    let batch = batch_of(&table, &source, vec![row]).expect("a batch");

    let unscaled: Vec<i128> = (0..batch.num_columns())
        .map(|index| column::<Decimal128Array>(&batch, index).value(0))
        .collect();
    assert_eq!(unscaled, [-7, 9_000_000_000_000_000_000, 150, 10_000, 300]);
}

#[test]
fn a_decimal_finer_than_the_table_scale_is_refused_naming_the_column() {
    let (table, source) = same_names(&[("PRICE", "DECIMAL(18,2)", "decimal(18, 2)")]);

    let message = error_text(batch_of(&table, &source, vec![vec![numeric(1_005, 3)]]));

    assert!(message.contains("column PRICE"), "{message}");
    assert!(message.contains("scale 3"), "{message}");
}

#[test]
fn the_edge_values_are_written_unchanged() {
    let (table, source) = same_names(&[
        ("DT", "DATE", "date"),
        ("TS", "TIMESTAMP(6)", "timestamp"),
        ("D", "DECIMAL(36,6)", "decimal(36, 6)"),
        ("F", "DOUBLE", "double"),
        ("S", "VARCHAR(20) UTF8", "string"),
    ]);
    let largest_decimal = 10_i128.pow(36) - 1;
    let low = vec![
        Value::Date(NaiveDate::from_ymd_opt(1, 1, 1).unwrap()),
        Value::Timestamp(timestamp("0001-01-01 00:00:00.000001")),
        numeric(-largest_decimal, 6),
        Value::Double(f64::MIN),
        Value::String("Grüße, 世界".into()),
    ];
    let high = vec![
        Value::Date(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()),
        Value::Timestamp(timestamp("9999-12-31 23:59:59.999999")),
        numeric(largest_decimal, 6),
        Value::Double(f64::MAX),
        Value::String(String::new()),
    ];

    let batch = batch_of(&table, &source, vec![low, high]).expect("a batch");

    let dates = column::<Date32Array>(&batch, 0);
    assert_eq!(dates.value_as_date(0), NaiveDate::from_ymd_opt(1, 1, 1));
    assert_eq!(
        dates.value_as_date(1),
        NaiveDate::from_ymd_opt(9999, 12, 31)
    );
    let timestamps = column::<TimestampMicrosecondArray>(&batch, 1);
    assert_eq!(
        timestamps.value_as_datetime(0),
        Some(timestamp("0001-01-01 00:00:00.000001"))
    );
    assert_eq!(
        timestamps.value_as_datetime(1),
        Some(timestamp("9999-12-31 23:59:59.999999"))
    );
    let decimals = column::<Decimal128Array>(&batch, 2);
    assert_eq!(
        (decimals.value(0), decimals.value(1)),
        (-largest_decimal, largest_decimal)
    );
    let doubles = column::<Float64Array>(&batch, 3);
    assert_eq!((doubles.value(0), doubles.value(1)), (f64::MIN, f64::MAX));
    assert_eq!(column::<StringArray>(&batch, 4).value(0), "Grüße, 世界");
}

#[test]
fn a_value_of_another_type_is_refused_naming_the_column_and_the_variant() {
    let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let text = ("VARCHAR(10) UTF8", "string");
    let cases = [
        (
            ("BOOLEAN", "boolean"),
            Value::String("yes".into()),
            "String",
        ),
        (
            ("DECIMAL(18,2)", "decimal(18, 2)"),
            Value::Double(1.5),
            "Double",
        ),
        (text, Value::Int32(1), "Int32"),
        (text, Value::Int64(1), "Int64"),
        (text, numeric(1, 0), "Numeric"),
        (text, Value::Bool(true), "Bool"),
        (text, Value::Date(date), "Date"),
        (text, Value::Timestamp(date.into()), "Timestamp"),
    ];

    for ((exasol, iceberg), value, variant) in cases {
        let (table, source) = same_names(&[("C", exasol, iceberg)]);

        let message = error_text(batch_of(&table, &source, vec![vec![value]]));

        assert!(
            message.contains(&format!("column C received a {variant} value")),
            "{message}"
        );
        assert!(!message.contains("yes"), "{message}");
    }
}

#[test]
fn a_decimal_too_large_for_the_table_column_is_refused_naming_the_column() {
    let (table, source) = same_names(&[("BIG", "DECIMAL(36,36)", "decimal(38, 36)")]);

    let message = error_text(batch_of(
        &table,
        &source,
        vec![vec![numeric(10_i128.pow(36), 0)]],
    ));

    assert!(
        message.contains("column BIG holds a value too large for the table column"),
        "{message}"
    );
}

#[test]
fn a_batch_fills_at_its_row_limit_and_starts_empty_after_it_is_taken() {
    let (table, source) = same_names(&[("ID", "DECIMAL(9,0)", "decimal(9, 0)")]);
    let mut builder = BatchBuilder::bind(&table, &source).unwrap();
    let rows = (0..BATCH_ROWS as i32)
        .map(|id| vec![Value::String(SPEC.into()), Value::Int32(id)])
        .collect();
    let mut ctx = TestContext::set(rows);

    while ctx.next().unwrap() {
        assert!(!builder.is_full());
        builder.append_row(&ctx).unwrap();
    }

    assert!(builder.is_full());
    assert_eq!(builder.take().unwrap().num_rows(), BATCH_ROWS);
    assert!(builder.is_empty());
}

/// The source column each table column binds to: one row whose values are the source names lands
/// each name in the table column it fills.
fn bound_names(table_names: &[&str], source_names: &[&str]) -> Result<Vec<String>, String> {
    let table = table(
        &table_names
            .iter()
            .map(|n| (*n, "string"))
            .collect::<Vec<_>>(),
    );
    let source = source(
        &source_names
            .iter()
            .map(|n| (*n, "VARCHAR(10) UTF8"))
            .collect::<Vec<_>>(),
    );
    let row = source_names
        .iter()
        .map(|name| Value::String(name.to_string()))
        .collect();
    let batch = batch_of(&table, &source, vec![row]).map_err(|error| error.to_string())?;
    Ok((0..batch.num_columns())
        .map(|index| column::<StringArray>(&batch, index).value(0).to_string())
        .collect())
}

/// A case name, the table and source column names, and each table column's bound source name.
type BindingCase = (
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
    Result<&'static [&'static str], String>,
);

/// Scenario: Unquoted Exasol names append to a table that another engine created
#[test]
fn each_table_column_binds_the_export_column_of_its_name_exactly_then_by_letter_case() {
    let text = "(VARCHAR(10) UTF8)";
    let cases: [BindingCase; 8] = [
        (
            "exact",
            &["id", "name"],
            &["name", "id"],
            Ok(&["id", "name"]),
        ),
        (
            "letter case",
            &["id", "name"],
            &["NAME", "ID"],
            Ok(&["ID", "NAME"]),
        ),
        (
            "an exact match leaves one column for the other name",
            &["code", "Code"],
            &["CODE", "Code"],
            Ok(&["CODE", "Code"]),
        ),
        (
            "an export name that matches two table columns",
            &["code", "Code"],
            &["CODE"],
            Err(format!(
                "export column CODE {text} matches table columns code (string) and Code (string) \
                 only by letter case"
            )),
        ),
        (
            "an export name that matches three table columns",
            &["code", "Code", "cODE"],
            &["CODE"],
            Err(format!(
                "export column CODE {text} matches table columns code (string), Code (string), \
                 and cODE (string) only by letter case"
            )),
        ),
        (
            "a table name that matches two export columns",
            &["code"],
            &["CODE", "Code"],
            Err(format!(
                "table column code (string) matches export columns CODE {text} and Code {text} \
                 only by letter case"
            )),
        ),
        (
            "missing",
            &["id", "name"],
            &["ID"],
            Err("table column name (string) has no export column".to_string()),
        ),
        (
            "extra",
            &["id"],
            &["ID", "EXTRA"],
            Err(format!("export column EXTRA {text} has no table column")),
        ),
    ];

    for (case, table_names, source_names, expected) in cases {
        let expected = expected
            .map(|names| names.iter().map(ToString::to_string).collect())
            .map_err(|mismatch| format!("{MISMATCH}{mismatch}"));

        assert_eq!(bound_names(table_names, source_names), expected, "{case}");
    }
}

/// Scenario: Every column mismatch fails the export in one error before any data is written
#[test]
fn one_error_lists_every_mismatch_with_both_types() {
    let table = table(&[
        ("ID", "long"),
        ("Code", "string"),
        ("code", "string"),
        ("AMOUNT", "decimal(10, 2)"),
    ]);
    let source = source(&[
        ("CODE", "VARCHAR(10) UTF8"),
        ("AMOUNT", "DOUBLE"),
        ("EXTRA", "VARCHAR(10) UTF8"),
    ]);

    let message = error_text(BatchBuilder::bind(&table, &source));

    assert_eq!(
        message,
        format!(
            "{MISMATCH}table column ID (long) has no export column; export column EXTRA \
             (VARCHAR(10) UTF8) has no table column; export column AMOUNT (DOUBLE) cannot fill \
             table column AMOUNT (decimal(10, 2)); export column CODE (VARCHAR(10) UTF8) matches \
             table columns Code (string) and code (string) only by letter case"
        )
    );
}

/// Scenario: A narrower export type widens into the table column
#[test]
fn each_accepted_type_pair_binds() {
    let cases = [
        ("DECIMAL(9,2)", "decimal(9, 2)"),
        ("DECIMAL(9,2)", "decimal(20, 2)"),
        ("DECIMAL(36,0)", "decimal(38, 0)"),
        ("DECIMAL(9,0)", "int"),
        ("DECIMAL(10,0)", "int"),
        ("DECIMAL(36,0)", "int"),
        ("DECIMAL(18,0)", "long"),
        ("DECIMAL(20,0)", "long"),
        ("DECIMAL(36,0)", "long"),
        ("DOUBLE", "double"),
        ("BOOLEAN", "boolean"),
        ("VARCHAR(10) UTF8", "string"),
        ("CHAR(5) UTF8", "string"),
        ("DATE", "date"),
        ("TIMESTAMP(0)", "timestamp"),
        ("TIMESTAMP(6)", "timestamp"),
        ("TIMESTAMP(3)", "timestamptz"),
        ("TIMESTAMP(6)", "timestamptz"),
    ];

    for (exasol, iceberg) in cases {
        let (table, source) = same_names(&[("C", exasol, iceberg)]);

        let bound = BatchBuilder::bind(&table, &source);

        assert!(bound.is_ok(), "{exasol} into {iceberg}");
    }
}

#[test]
fn each_refused_type_pair_is_a_mismatch_naming_both_types() {
    let list = serde_json::json!({
        "type": "list", "element-id": 10, "element": "string", "element-required": false
    });
    let map = serde_json::json!({
        "type": "map", "key-id": 10, "key": "string", "value-id": 11, "value": "string",
        "value-required": false
    });
    let structure = serde_json::json!({
        "type": "struct",
        "fields": [{"id": 10, "name": "a", "required": false, "type": "string"}]
    });
    let cases = [
        ("DECIMAL(10,2)", serde_json::json!("int")),
        ("DECIMAL(10,2)", serde_json::json!("long")),
        ("DECIMAL(10,2)", serde_json::json!("decimal(12, 3)")),
        ("DECIMAL(12,2)", serde_json::json!("decimal(10, 2)")),
        ("DECIMAL(9,0)", serde_json::json!("double")),
        ("DOUBLE", serde_json::json!("float")),
        ("DOUBLE", serde_json::json!("decimal(10, 2)")),
        ("TIMESTAMP(9)", serde_json::json!("timestamptz")),
        ("TIMESTAMP(6)", serde_json::json!("timestamp_ns")),
        ("TIMESTAMP(6)", serde_json::json!("timestamptz_ns")),
        ("TIMESTAMP(6)", serde_json::json!("time")),
        ("TIMESTAMP(6)", serde_json::json!("date")),
        ("DATE", serde_json::json!("timestamp")),
        ("VARCHAR(36) UTF8", serde_json::json!("uuid")),
        ("VARCHAR(16) UTF8", serde_json::json!("fixed[16]")),
        ("VARCHAR(16) UTF8", serde_json::json!("binary")),
        ("VARCHAR(16) UTF8", list),
        ("VARCHAR(16) UTF8", map),
        ("VARCHAR(16) UTF8", structure),
    ];

    for (exasol, iceberg) in cases {
        let table = iceberg_table(&[("c", iceberg.clone())], false);
        let source = source(&[("C", exasol)]);

        let message = error_text(BatchBuilder::bind(&table, &source));

        assert_eq!(
            message,
            format!(
                "{MISMATCH}export column C ({exasol}) cannot fill table column c ({})",
                table.type_names[0]
            ),
            "{exasol} into {iceberg}"
        );
    }
}

/// Scenario: Rows read through a virtual schema append back into their table
#[test]
fn an_int_or_long_column_takes_every_value_in_its_range_from_each_integer_variant() {
    let out_of_range = |iceberg: &str| {
        format!(
            "LAKEHOUSE_EXPORT: an export row holds a value outside the range of column C \
             ({iceberg}) of table ns.t"
        )
    };
    let cases = [
        ("int", Value::Int32(i32::MAX), Ok(i64::from(i32::MAX))),
        (
            "int",
            Value::Int64(i64::from(i32::MIN)),
            Ok(i64::from(i32::MIN)),
        ),
        (
            "int",
            numeric(i128::from(i32::MAX), 0),
            Ok(i64::from(i32::MAX)),
        ),
        (
            "int",
            Value::Int64(i64::from(i32::MAX) + 1),
            Err(out_of_range("int")),
        ),
        (
            "int",
            numeric(i128::from(i32::MIN) - 1, 0),
            Err(out_of_range("int")),
        ),
        ("long", Value::Int32(i32::MIN), Ok(i64::from(i32::MIN))),
        ("long", Value::Int64(i64::MAX), Ok(i64::MAX)),
        ("long", numeric(i128::from(i64::MIN), 0), Ok(i64::MIN)),
        (
            "long",
            numeric(i128::from(i64::MAX) + 1, 0),
            Err(out_of_range("long")),
        ),
        (
            "long",
            numeric(i128::from(i64::MIN) - 1, 0),
            Err(out_of_range("long")),
        ),
    ];

    for (iceberg, value, expected) in cases {
        let (table, source) = same_names(&[("C", "DECIMAL(36,0)", iceberg)]);
        let case = format!("{value:?} into {iceberg}");

        let written = batch_of(&table, &source, vec![vec![value]]).map(|batch| {
            match batch.column(0).data_type() {
                DataType::Int32 => i64::from(column::<Int32Array>(&batch, 0).value(0)),
                _ => column::<Int64Array>(&batch, 0).value(0),
            }
        });

        assert_eq!(
            written.map_err(|error| error.to_string()),
            expected,
            "{case}"
        );
    }
}

#[test]
fn a_timestamptz_column_takes_the_date_and_time_as_utc() {
    let (table, source) = same_names(&[("C", "TIMESTAMP(3)", "timestamptz")]);
    let written = timestamp("2024-01-15 10:30:00.123");

    let batch = batch_of(&table, &source, vec![vec![Value::Timestamp(written)]]).unwrap();

    assert_eq!(
        batch.column(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
    );
    assert_eq!(
        column::<TimestampMicrosecondArray>(&batch, 0).value(0),
        written.and_utc().timestamp_micros()
    );
}

#[test]
fn a_null_for_a_required_column_is_refused_naming_the_table_and_the_column() {
    let table = iceberg_table(&[("id", serde_json::json!("long"))], true);
    let source = source(&[("ID", "DECIMAL(18,0)")]);

    let message = error_text(batch_of(&table, &source, vec![vec![Value::Null]]));

    assert_eq!(
        message,
        "LAKEHOUSE_EXPORT: an export row holds NULL for the required column id (long) of table ns.t"
    );
}

fn schema_of(type_names: &[&str]) -> Result<Schema, UdfError> {
    let names: Vec<String> = (0..type_names.len()).map(|i| format!("C{i}")).collect();
    let columns: Vec<ColumnInfo> = type_names.iter().map(|t| measured_column(t)).collect();
    arrow_schema(&names, &columns)
}

fn refusal_of(type_names: &[&str]) -> String {
    match schema_of(type_names) {
        Err(error) => error.to_string(),
        Ok(schema) => panic!("{type_names:?} must be refused, got {schema:?}"),
    }
}

#[test]
fn each_writable_type_maps_to_its_arrow_type() {
    let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
    let cases = [
        ("DECIMAL(9,0)", DataType::Decimal128(9, 0)),
        ("DECIMAL(18,0)", DataType::Decimal128(18, 0)),
        ("DECIMAL(36,6)", DataType::Decimal128(36, 6)),
        ("DOUBLE", DataType::Float64),
        ("BOOLEAN", DataType::Boolean),
        ("VARCHAR(20) UTF8", DataType::Utf8),
        ("VARCHAR(10) ASCII", DataType::Utf8),
        ("CHAR(5) UTF8", DataType::Utf8),
        ("DATE", DataType::Date32),
        ("TIMESTAMP(0)", micros.clone()),
        ("TIMESTAMP(3)", micros.clone()),
        ("TIMESTAMP(6)", micros),
    ];
    for (type_name, expected) in cases {
        let schema = schema_of(&[type_name]).unwrap_or_else(|e| panic!("{type_name}: {e}"));
        assert_eq!(schema.field(0).data_type(), &expected, "{type_name}");
    }
}

#[test]
fn fields_keep_the_writer_spec_names_in_input_order_and_are_nullable() {
    let schema = schema_of(&["DATE", "DOUBLE", "BOOLEAN"]).expect("writable types");

    let fields: Vec<(&str, bool)> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.is_nullable()))
        .collect();
    assert_eq!(fields, [("C0", true), ("C1", true), ("C2", true)]);
}

#[test]
fn a_timestamp_finer_than_microseconds_is_refused_with_a_cast_hint() {
    for type_name in ["TIMESTAMP(7)", "TIMESTAMP(9)"] {
        let message = refusal_of(&["DATE", type_name]);
        for fragment in [
            &format!("column C1 has type {type_name}, which the export cannot write"),
            "TIMESTAMP(0) to TIMESTAMP(6)",
            "cast a TIMESTAMP(7) to TIMESTAMP(9) column to TIMESTAMP(6)",
        ] {
            assert!(message.contains(fragment), "{message:?} lacks {fragment:?}");
        }
    }
}

#[test]
fn a_column_without_a_measured_spelling_is_refused_naming_it() {
    let cases = [
        ColumnInfo {
            typ: ExaType::Unsupported,
            type_name: "GEOMETRY".to_string(),
            ..measured_column("DOUBLE")
        },
        ColumnInfo {
            type_name: "TIMESTAMP(3) WITH LOCAL TIME ZONE".to_string(),
            ..measured_column("TIMESTAMP(3)")
        },
    ];
    for column in cases {
        let message = arrow_schema(&["G".to_string()], std::slice::from_ref(&column))
            .expect_err("an unmeasured spelling is refused")
            .to_string();
        assert!(
            message.contains(&format!("column G has type {}", column.type_name)),
            "{message}"
        );
    }
}
