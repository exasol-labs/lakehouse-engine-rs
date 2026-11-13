//! Pruning-only: every emitted predicate is implied by the user predicate. An untranslatable node
//! is dropped, widening the file set, and `NOT` negates only an exactly translated child.

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Schema};
use serde_json::Value as Json;

use crate::scan::spec::LogicalField;

use super::filter_json::Comparison;
use super::literal::{UTC, exact_literal};
use super::pruning_translation::{PruningTarget, translate};

/// Resolves a filter column through its declared field id to the field the scanned schema carries
/// under that id: bounds are stored by field id, so a rename must not prune by another's bounds.
struct IcebergTarget<'a> {
    declared: &'a [LogicalField],
    scanned: &'a Schema,
}

impl<'a> PruningTarget for IcebergTarget<'a> {
    type Predicate = Predicate;
    type Column = (&'a str, &'a PrimitiveType);
    type Literal = Datum;

    fn resolve(&self, column: &str) -> Option<Self::Column> {
        let column = column.to_uppercase();
        let is_column = |field: &&LogicalField| field.name.to_uppercase() == column;
        let field_id = self.declared.iter().find(is_column)?.field_id?;
        let name = self.scanned.name_by_field_id(field_id)?;
        let field = self.scanned.field_by_id(field_id)?;
        Some((name, field.field_type.as_primitive_type()?))
    }

    fn literal(&self, (_, primitive): Self::Column, literal: &Json) -> Option<Datum> {
        literal_to_datum(literal, primitive)
    }

    fn is_string(&self, (_, primitive): Self::Column) -> bool {
        *primitive == PrimitiveType::String
    }

    fn string(&self, value: String) -> Datum {
        Datum::string(value)
    }

    fn compare(&self, (name, _): Self::Column, comparison: Comparison, datum: Datum) -> Predicate {
        let reference = Reference::new(name);
        match comparison {
            Comparison::Equal => reference.equal_to(datum),
            Comparison::NotEqual => reference.not_equal_to(datum),
            Comparison::Less => reference.less_than(datum),
            Comparison::LessEqual => reference.less_than_or_equal_to(datum),
            Comparison::Greater => reference.greater_than(datum),
            Comparison::GreaterEqual => reference.greater_than_or_equal_to(datum),
        }
    }

    fn null_test(&self, (name, _): Self::Column, is_null: bool) -> Predicate {
        let reference = Reference::new(name);
        if is_null {
            reference.is_null()
        } else {
            reference.is_not_null()
        }
    }

    fn in_list(&self, (name, _): Self::Column, datums: Vec<Datum>) -> Predicate {
        Reference::new(name).is_in(datums)
    }

    fn starts_with(&self, (name, _): Self::Column, prefix: String) -> Predicate {
        Reference::new(name).starts_with(Datum::string(prefix))
    }

    fn and(predicates: Vec<Predicate>) -> Predicate {
        predicates
            .into_iter()
            .reduce(Predicate::and)
            .unwrap_or(Predicate::AlwaysTrue)
    }

    fn or(predicates: Vec<Predicate>) -> Predicate {
        predicates
            .into_iter()
            .reduce(Predicate::or)
            .unwrap_or(Predicate::AlwaysFalse)
    }

    fn not(predicate: Predicate) -> Predicate {
        predicate.negate()
    }
}

fn arrow_type(primitive: &PrimitiveType) -> Option<DataType> {
    let utc = || Some(UTC.into());
    Some(match primitive {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => {
            DataType::Decimal128(u8::try_from(*precision).ok()?, i8::try_from(*scale).ok()?)
        }
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, utc()),
        PrimitiveType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        PrimitiveType::TimestamptzNs => DataType::Timestamp(TimeUnit::Nanosecond, utc()),
        PrimitiveType::String => DataType::Utf8,
        PrimitiveType::Binary => DataType::Binary,
        PrimitiveType::Fixed(width) => DataType::FixedSizeBinary(i32::try_from(*width).ok()?),
        // No `uuid` arm: writers store `uuid` bounds in signed or unsigned order and iceberg-rust
        // reads them unsigned, so pruning could drop matching files (apache/iceberg#14216).
        _ => return None,
    })
}

fn literal_to_datum(literal: &Json, primitive: &PrimitiveType) -> Option<Datum> {
    Some(match exact_literal(literal, &arrow_type(primitive)?)? {
        ScalarValue::Boolean(Some(value)) => Datum::bool(value),
        ScalarValue::Int32(Some(value)) => Datum::int(value),
        ScalarValue::Int64(Some(value)) => Datum::long(value),
        ScalarValue::Float32(Some(value)) => Datum::float(value),
        ScalarValue::Float64(Some(value)) => Datum::double(value),
        // Built at the column's own precision and scale: binding refuses another scale.
        ScalarValue::Decimal128(Some(unscaled), ..) => {
            Datum::try_from_bytes(&unscaled.to_be_bytes(), primitive.clone()).ok()?
        }
        ScalarValue::Utf8(Some(value)) => Datum::string(value),
        ScalarValue::Date32(Some(days)) => Datum::date(days),
        ScalarValue::TimestampMicrosecond(Some(micros), None) => Datum::timestamp_micros(micros),
        ScalarValue::TimestampMicrosecond(Some(micros), Some(_)) => {
            Datum::timestamptz_micros(micros)
        }
        ScalarValue::TimestampNanosecond(Some(nanos), None) => Datum::timestamp_nanos(nanos),
        ScalarValue::TimestampNanosecond(Some(nanos), Some(_)) => Datum::timestamptz_nanos(nanos),
        ScalarValue::Binary(Some(bytes)) => Datum::binary(bytes),
        ScalarValue::FixedSizeBinary(_, Some(bytes)) => Datum::fixed(bytes),
        _ => return None,
    })
}

/// `None` means no constraint: the caller must pass all files, never none. `scanned` is the schema
/// the scan binds the predicate against; a column whose `declared` field id it lacks imposes none.
pub(super) fn to_iceberg_predicate(
    filter_json: &Json,
    declared: &[LogicalField],
    scanned: &Schema,
) -> Option<Predicate> {
    let target = IcebergTarget { declared, scanned };
    translate(filter_json, &target).map(|translated| translated.predicate)
}

#[cfg(test)]
#[path = "iceberg_predicate_tests.rs"]
mod tests;
