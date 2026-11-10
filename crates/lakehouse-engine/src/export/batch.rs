//! Maps the export's source columns to Arrow, the type model every target format derives its own
//! schema from, and turns the writer's input rows into Arrow batches of the sink's table schema,
//! binding each source column to its table field by name and taking decimal scales from the table.
//! The type rules and refusals are the export's (decisions [3] and [8]).

use super::refusal;
use crate::types::exasol_type::{
    ExasolType, UdfColumnType, exasol_type_from_column_info, is_reported_spelling,
};
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{ColumnInfo, Value};
use std::sync::Arc;

pub(super) const BATCH_ROWS: usize = 8_192;

/// One Arrow batch in the making.
pub(super) struct BatchBuilder {
    schema: SchemaRef,
    columns: Vec<ColumnBuilder>,
    rows: usize,
}

struct ColumnBuilder {
    name: String,
    /// The input column, after the writer spec in column 0.
    input: usize,
    values: Values,
}

enum Values {
    Decimal {
        builder: Decimal128Builder,
        scale: i8,
    },
    Double(Float64Builder),
    Boolean(BooleanBuilder),
    Text(StringBuilder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
}

impl BatchBuilder {
    /// Binds each field of `schema` to the source column of its name in `source_columns`.
    pub(super) fn bind(schema: SchemaRef, source_columns: &[String]) -> Result<Self, UdfError> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let name = field.name();
                let position = source_columns
                    .iter()
                    .position(|source| source == name)
                    .ok_or_else(|| {
                        refusal(format_args!(
                            "table column {name} has no source column of that name"
                        ))
                    })?;
                Ok(ColumnBuilder {
                    name: name.clone(),
                    input: position + 1,
                    values: Values::of(name, field.data_type())?,
                })
            })
            .collect::<Result<_, UdfError>>()?;
        Ok(Self {
            schema,
            columns,
            rows: 0,
        })
    }

    /// Appends the context's current row.
    pub(super) fn append_row(&mut self, ctx: &dyn UdfContext) -> Result<(), UdfError> {
        for column in &mut self.columns {
            let value = ctx.get_value(column.input)?;
            column.values.append(&column.name, value)?;
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

impl Values {
    fn of(name: &str, data_type: &DataType) -> Result<Self, UdfError> {
        Ok(match data_type {
            DataType::Decimal128(precision, scale) => Self::Decimal {
                builder: Decimal128Builder::with_capacity(BATCH_ROWS)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| refusal(format_args!("column {name}: {error}")))?,
                scale: *scale,
            },
            DataType::Float64 => Self::Double(Float64Builder::with_capacity(BATCH_ROWS)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(BATCH_ROWS)),
            DataType::Utf8 => Self::Text(StringBuilder::new()),
            DataType::Date32 => Self::Date(Date32Builder::with_capacity(BATCH_ROWS)),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Self::Timestamp(TimestampMicrosecondBuilder::with_capacity(BATCH_ROWS))
            }
            other => {
                return Err(refusal(format_args!(
                    "table column {name} has the Arrow type {other}, which the export cannot write"
                )));
            }
        })
    }

    fn append(&mut self, name: &str, value: Option<Value>) -> Result<(), UdfError> {
        let Some(value) = value else {
            self.append_null();
            return Ok(());
        };
        match (self, value) {
            (Self::Decimal { builder, scale }, value) => {
                builder.append_value(rescaled(name, value, *scale)?)
            }
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

/// Exasol truncates finer timestamps to microseconds before the writer reads them.
const MAX_TIMESTAMP_PRECISION: u8 = 6;

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
    let exasol_type = match exasol_type_from_column_info(column) {
        UdfColumnType::Known(exasol_type)
            if is_reported_spelling(exasol_type, &column.type_name) =>
        {
            exasol_type
        }
        _ => return Err(unwritable(name, &column.type_name)),
    };
    Ok(match exasol_type {
        ExasolType::Decimal(decimal) => {
            DataType::Decimal128(decimal.precision(), decimal.scale() as i8)
        }
        ExasolType::DoublePrecision => DataType::Float64,
        ExasolType::Boolean => DataType::Boolean,
        ExasolType::Varchar { .. } | ExasolType::Char { .. } => DataType::Utf8,
        ExasolType::Date => DataType::Date32,
        ExasolType::Timestamp { precision } if precision <= MAX_TIMESTAMP_PRECISION => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        _ => return Err(unwritable(name, &column.type_name)),
    })
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
