//! The export's batches of the sink's table schema: source schemas, name binding, and value
//! conversion. The type rules live in `types::export_mapping`.

use super::refusal;
use crate::types::exasol_type::{UdfColumnType, exasol_type_from_column_info};
use crate::types::export_mapping::{exasol_to_arrow, writable};
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{ColumnInfo, Value};
use std::sync::Arc;

pub(super) const BATCH_ROWS: usize = 8_192;

/// The columns of the table a writer fills, as its sink reads them from the table.
pub(super) struct TableColumns {
    /// The table's name, for errors.
    pub(super) table: String,
    /// The Arrow schema every batch must have.
    pub(super) schema: SchemaRef,
    /// The type of each field of `schema` as the table format names it, for errors.
    pub(super) type_names: Vec<String>,
}

/// The export's source columns in input order: each one's name and its Exasol metadata.
pub(super) struct SourceColumns {
    pub(super) names: Vec<String>,
    pub(super) columns: Vec<ColumnInfo>,
}

/// One Arrow batch in the making.
pub(super) struct BatchBuilder {
    schema: SchemaRef,
    columns: Vec<ColumnBuilder>,
    rows: usize,
}

struct ColumnBuilder {
    /// The table column's name.
    name: String,
    /// The table column, its type, and its table, for a value error.
    described: String,
    required: bool,
    /// The input column, after the writer spec in column 0.
    input: usize,
    values: Values,
}

enum Values {
    Decimal {
        builder: Decimal128Builder,
        scale: i8,
    },
    Int(Int32Builder),
    Long(Int64Builder),
    Double(Float64Builder),
    Boolean(BooleanBuilder),
    Text(StringBuilder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
}

impl BatchBuilder {
    /// Binds each table column to its source column by name and checks each type pair. Every
    /// mismatch fails in one error, before any row is read.
    pub(super) fn bind(table: &TableColumns, source: &SourceColumns) -> Result<Self, UdfError> {
        let table_names: Vec<&str> = table
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let source_names: Vec<&str> = source.names.iter().map(String::as_str).collect();
        let names = NameMatch::of(&table_names, &source_names);
        let found = mismatches(table, source, &names);
        if !found.is_empty() {
            return Err(mismatch_error(table, &found));
        }
        let columns = bound_pairs(&names)
            .map(|(field, column)| {
                let values = Values::filling(table.schema.field(field), &source.columns[column])
                    .ok_or_else(|| {
                        mismatch_error(table, &[type_mismatch(table, source, field, column)])
                    })?;
                Ok(ColumnBuilder {
                    name: table_names[field].to_string(),
                    described: format!(
                        "column {} of table {}",
                        table.described(field),
                        table.table
                    ),
                    required: !table.schema.field(field).is_nullable(),
                    input: column + 1,
                    values,
                })
            })
            .collect::<Result<_, UdfError>>()?;
        Ok(Self {
            schema: table.schema.clone(),
            columns,
            rows: 0,
        })
    }

    /// Appends the context's current row.
    pub(super) fn append_row(&mut self, ctx: &dyn UdfContext) -> Result<(), UdfError> {
        for column in &mut self.columns {
            let value = ctx.get_value(column.input)?;
            column.append(value)?;
        }
        self.rows += 1;
        Ok(())
    }

    pub(super) fn is_full(&self) -> bool {
        self.rows >= BATCH_ROWS
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// The rows appended since the last batch was taken.
    pub(super) fn take(&mut self) -> Result<RecordBatch, UdfError> {
        let arrays = self
            .columns
            .iter_mut()
            .map(|column| column.values.finish())
            .collect();
        self.rows = 0;
        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|error| refusal(format_args!("could not build an Arrow batch: {error}")))
    }
}

impl TableColumns {
    fn described(&self, field: usize) -> String {
        format!(
            "{} ({})",
            self.schema.field(field).name(),
            self.type_names[field]
        )
    }
}

impl SourceColumns {
    fn described(&self, column: usize) -> String {
        format!(
            "{} ({})",
            self.names[column], self.columns[column].type_name
        )
    }
}

/// How the table's column names match the source's: each table column's source column, and the
/// columns that match none or match ambiguously. Each list holds column indexes.
#[derive(Debug, Default, PartialEq)]
struct NameMatch {
    bound: Vec<Option<usize>>,
    missing: Vec<usize>,
    extra: Vec<usize>,
    /// A source column and the unmatched table columns its name matches only by letter case.
    ambiguous_sources: Vec<(usize, Vec<usize>)>,
    /// A table column and the unmatched source columns its name matches only by letter case.
    ambiguous_fields: Vec<(usize, Vec<usize>)>,
}

impl NameMatch {
    /// Exact names bind first; a name without one binds the one leftover name equal but for case.
    /// A name that matches several is ambiguous, and the names it matches are listed under it only.
    fn of(table: &[&str], source: &[&str]) -> Self {
        let exact: Vec<Option<usize>> = table
            .iter()
            .map(|name| source.iter().position(|column| column == name))
            .collect();
        let open_fields: Vec<usize> = (0..table.len()).filter(|&f| exact[f].is_none()).collect();
        let open_sources: Vec<usize> = (0..source.len())
            .filter(|&c| !exact.contains(&Some(c)))
            .collect();
        let folded = |a: &str, b: &str| a.to_lowercase() == b.to_lowercase();
        let field_matches = |f: usize| -> Vec<usize> {
            let matching = |&&c: &&usize| folded(table[f], source[c]);
            open_sources.iter().filter(matching).copied().collect()
        };
        let source_matches = |c: usize| -> Vec<usize> {
            let matching = |&&f: &&usize| folded(table[f], source[c]);
            open_fields.iter().filter(matching).copied().collect()
        };
        let mut found = Self {
            bound: exact,
            ..Self::default()
        };
        for &field in &open_fields {
            match field_matches(field).as_slice() {
                [] => found.missing.push(field),
                &[column] if source_matches(column) == [field] => {
                    found.bound[field] = Some(column);
                }
                [_] => {}
                several => found.ambiguous_fields.push((field, several.to_vec())),
            }
        }
        for &column in &open_sources {
            match source_matches(column).as_slice() {
                [] => found.extra.push(column),
                [_] => {}
                several => found.ambiguous_sources.push((column, several.to_vec())),
            }
        }
        found
    }
}

/// Each table column and the source column bound to it.
fn bound_pairs(names: &NameMatch) -> impl Iterator<Item = (usize, usize)> + '_ {
    let bound = names.bound.iter().enumerate();
    bound.filter_map(|(field, column)| Some((field, (*column)?)))
}

/// Every mismatch of the source with the table: missing and extra columns, bound pairs of types
/// that do not fill, and names that match only ambiguously, in that order.
fn mismatches(table: &TableColumns, source: &SourceColumns, names: &NameMatch) -> Vec<String> {
    let mut found = Vec::new();
    for &field in &names.missing {
        found.push(format!(
            "table column {} has no export column",
            table.described(field)
        ));
    }
    for &column in &names.extra {
        found.push(format!(
            "export column {} has no table column",
            source.described(column)
        ));
    }
    for (field, column) in bound_pairs(names) {
        if Values::filling(table.schema.field(field), &source.columns[column]).is_none() {
            found.push(type_mismatch(table, source, field, column));
        }
    }
    for (column, fields) in &names.ambiguous_sources {
        let fields: Vec<String> = fields.iter().map(|&f| table.described(f)).collect();
        found.push(format!(
            "export column {} matches table columns {} only by letter case",
            source.described(*column),
            listed(&fields)
        ));
    }
    for (field, sources) in &names.ambiguous_fields {
        let sources: Vec<String> = sources.iter().map(|&c| source.described(c)).collect();
        found.push(format!(
            "table column {} matches export columns {} only by letter case",
            table.described(*field),
            listed(&sources)
        ));
    }
    found
}

fn type_mismatch(
    table: &TableColumns,
    source: &SourceColumns,
    field: usize,
    column: usize,
) -> String {
    format!(
        "export column {} cannot fill table column {}",
        source.described(column),
        table.described(field)
    )
}

fn mismatch_error(table: &TableColumns, mismatches: &[String]) -> UdfError {
    refusal(format_args!(
        "the export columns do not match the columns of table {}: {}",
        table.table,
        mismatches.join("; ")
    ))
}

/// `items` joined as an English list: `a`, `a and b`, `a, b, and c`.
fn listed(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [only] => only.clone(),
        [first, second] => format!("{first} and {second}"),
        [rest @ .., last] => format!("{}, and {last}", rest.join(", ")),
    }
}

impl ColumnBuilder {
    fn append(&mut self, value: Option<Value>) -> Result<(), UdfError> {
        match value {
            None if self.required => Err(refusal(format_args!(
                "an export row holds NULL for the required {}",
                self.described
            ))),
            None => {
                self.values.append_null();
                Ok(())
            }
            Some(value) => self.values.append(&self.name, &self.described, value),
        }
    }
}

impl Values {
    /// The builder of `field` for `source` values, `None` for a refused pair.
    fn filling(field: &Field, source: &ColumnInfo) -> Option<Self> {
        let UdfColumnType::Known(source) = exasol_type_from_column_info(source) else {
            return None;
        };
        if !writable(field.data_type(), &source) {
            return None;
        }
        Some(match field.data_type() {
            DataType::Decimal128(precision, scale) => Self::Decimal {
                builder: Decimal128Builder::with_capacity(BATCH_ROWS)
                    .with_precision_and_scale(*precision, *scale)
                    .ok()?,
                scale: *scale,
            },
            DataType::Int32 => Self::Int(Int32Builder::with_capacity(BATCH_ROWS)),
            DataType::Int64 => Self::Long(Int64Builder::with_capacity(BATCH_ROWS)),
            DataType::Float64 => Self::Double(Float64Builder::with_capacity(BATCH_ROWS)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(BATCH_ROWS)),
            DataType::Utf8 => Self::Text(StringBuilder::new()),
            DataType::Date32 => Self::Date(Date32Builder::with_capacity(BATCH_ROWS)),
            DataType::Timestamp(_, zone) => Self::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(BATCH_ROWS)
                    .with_timezone_opt(zone.clone()),
            ),
            _ => return None,
        })
    }

    /// Appends `value` to the column `name`; `described` names its type and table for a value
    /// outside its range.
    fn append(&mut self, name: &str, described: &str, value: Value) -> Result<(), UdfError> {
        let out_of_range = || {
            refusal(format_args!(
                "an export row holds a value outside the range of {described}"
            ))
        };
        match (self, value) {
            (Self::Decimal { builder, scale }, value) => {
                builder.append_value(rescaled(name, value, *scale)?)
            }
            (Self::Int(builder), value) => builder.append_value(
                i32::try_from(rescaled(name, value, 0)?).map_err(|_| out_of_range())?,
            ),
            (Self::Long(builder), value) => builder.append_value(
                i64::try_from(rescaled(name, value, 0)?).map_err(|_| out_of_range())?,
            ),
            (Self::Double(builder), Value::Double(double)) => builder.append_value(double),
            (Self::Boolean(builder), Value::Bool(flag)) => builder.append_value(flag),
            (Self::Text(builder), Value::String(text)) => builder.append_value(text),
            (Self::Date(builder), Value::Date(date)) => {
                builder.append_value(days_since_epoch(date))
            }
            (Self::Timestamp(builder), Value::Timestamp(timestamp)) => {
                builder.append_value(timestamp.and_utc().timestamp_micros())
            }
            (_, value) => return Err(unexpected(name, &value)),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            Self::Decimal { builder, .. } => builder.append_null(),
            Self::Int(builder) => builder.append_null(),
            Self::Long(builder) => builder.append_null(),
            Self::Double(builder) => builder.append_null(),
            Self::Boolean(builder) => builder.append_null(),
            Self::Text(builder) => builder.append_null(),
            Self::Date(builder) => builder.append_null(),
            Self::Timestamp(builder) => builder.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Decimal { builder, .. } => Arc::new(builder.finish()),
            Self::Int(builder) => Arc::new(builder.finish()),
            Self::Long(builder) => Arc::new(builder.finish()),
            Self::Double(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::Text(builder) => Arc::new(builder.finish()),
            Self::Date(builder) => Arc::new(builder.finish()),
            Self::Timestamp(builder) => Arc::new(builder.finish()),
        }
    }
}

/// The unscaled value of `value` at `scale`. Exasol sends a `DECIMAL` value with the scale of
/// its trimmed digits, so `1.50` arrives as 15 at scale 1, and an integer one as `Int32` or
/// `Int64`.
fn rescaled(name: &str, value: Value, scale: i8) -> Result<i128, UdfError> {
    let (unscaled, value_scale) = match value {
        Value::Int32(int) => (i128::from(int), 0),
        Value::Int64(int) => (i128::from(int), 0),
        Value::Numeric(decimal) => (decimal.unscaled, i32::from(decimal.scale)),
        other => return Err(unexpected(name, &other)),
    };
    let shift = u32::try_from(i32::from(scale) - value_scale).map_err(|_| {
        refusal(format_args!(
            "column {name} holds a value of scale {value_scale}, finer than the table column's \
             scale {scale}"
        ))
    })?;
    10_i128
        .checked_pow(shift)
        .and_then(|factor| unscaled.checked_mul(factor))
        .ok_or_else(|| {
            refusal(format_args!(
                "column {name} holds a value too large for the table column"
            ))
        })
}

fn days_since_epoch(date: NaiveDate) -> i32 {
    // Exasol's DATE spans years 1 to 9999, far inside i32 days.
    (date - DateTime::UNIX_EPOCH.date_naive()).num_days() as i32
}

fn unexpected(name: &str, value: &Value) -> UdfError {
    let variant = match value {
        Value::Null => "Null",
        Value::Double(_) => "Double",
        Value::Int32(_) => "Int32",
        Value::Int64(_) => "Int64",
        Value::Numeric(_) => "Numeric",
        Value::Bool(_) => "Bool",
        Value::String(_) => "String",
        Value::Date(_) => "Date",
        Value::Timestamp(_) => "Timestamp",
    };
    refusal(format_args!(
        "column {name} received a {variant} value, which does not match its table type"
    ))
}

/// The Arrow schema of the source columns, named by `names`, in input order and every field
/// nullable, so every writer of one export derives the same schema.
pub(super) fn arrow_schema(names: &[String], columns: &[ColumnInfo]) -> Result<Schema, UdfError> {
    names
        .iter()
        .zip(columns)
        .map(|(name, column)| Ok(Field::new(name, arrow_type(name, column)?, true)))
        .collect::<Result<Vec<_>, UdfError>>()
        .map(Schema::new)
}

fn arrow_type(name: &str, column: &ColumnInfo) -> Result<DataType, UdfError> {
    match exasol_type_from_column_info(column) {
        UdfColumnType::Known(exasol_type) => exasol_to_arrow(&exasol_type),
        UdfColumnType::Unrecognized { .. } => None,
    }
    .ok_or_else(|| unwritable(name, &column.type_name))
}

fn unwritable(name: &str, type_name: &str) -> UdfError {
    refusal(format_args!(
        "column {name} has type {type_name}, which the export cannot write; it writes DECIMAL, \
         DOUBLE, BOOLEAN, VARCHAR, CHAR, DATE, and TIMESTAMP(0) to TIMESTAMP(6) columns; cast a \
         TIMESTAMP(7) to TIMESTAMP(9) column to TIMESTAMP(6)"
    ))
}

#[cfg(test)]
#[path = "batch_tests.rs"]
mod tests;
