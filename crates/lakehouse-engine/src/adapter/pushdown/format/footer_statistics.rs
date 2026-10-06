//! What one row group's Parquet footer proves about a column, for plan-time file pruning.

use std::collections::BTreeMap;
use std::iter;

use arrow::array::{Array, ArrayRef};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};
use datafusion::scalar::ScalarValue;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::basic::{ColumnOrder, SortOrder, Type as PhysicalType};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use serde_json::Value as Json;

use super::partition_predicate::{
    ColumnFacts, Nulls, Orderings, PartitionPredicate, folds_to, is_partition_key,
};
use crate::adapter::parquet_directory::ParquetFile;
use crate::scan::spec::LogicalField;
use crate::types::mapping::arrow_type_from_tag;
use crate::types::widening::widen;

/// The footer view of the part of a request filter the scan evaluates. Its predicate types the
/// literals and its row-group view types the bounds from one column set, so a literal is only
/// ever compared against a bound of the type the scan compares that column in.
pub(super) struct FooterStatisticsFilter {
    predicate: PartitionPredicate,
    columns: Vec<(String, DataType)>,
}

impl FooterStatisticsFilter {
    /// `schema` is the folded schema and `logical` the fields planned from it.
    pub(super) fn new(statistics_filter: &Json, schema: &Schema, logical: &[LogicalField]) -> Self {
        let columns = comparable_columns(schema, logical);
        Self {
            predicate: PartitionPredicate::for_footer_statistics(Some(statistics_filter), &columns),
            columns,
        }
    }

    /// Keeps `file` unless its footer proves that no row group can make the filter TRUE. A file
    /// without a read footer is kept, and a statistic this view cannot trust counts as unknown,
    /// so a statistics failure keeps the file and never fails the query.
    pub(super) fn keeps(&self, file: &ParquetFile) -> bool {
        let Some(footer) = file.footer.as_deref() else {
            return true;
        };
        let file_metadata = footer.file_metadata();
        let Ok(schema) = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        ) else {
            return true;
        };
        let view = FileView {
            footer,
            schema: &schema,
            partition_values: &file.partition_values,
            columns: &self.columns,
        };
        footer
            .row_groups()
            .iter()
            .filter(|row_group| row_group.num_rows() > 0)
            .any(|row_group| {
                self.predicate.keeps(&RowGroupFacts {
                    file: &view,
                    row_group,
                })
            })
    }
}

/// The folded columns the scan compares in their folded type. A DATE64, DECIMAL256, FLOAT16, or
/// nested column is planned as a string, so its bounds would compare in a type the scan does not.
fn comparable_columns(schema: &Schema, logical: &[LogicalField]) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .filter(|field| {
            logical.iter().any(|planned| {
                planned.name == *field.name()
                    && arrow_type_from_tag(&planned.arrow_type) == *field.data_type()
            })
        })
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect()
}

/// One read file, with each column typed as the fold read it.
struct FileView<'a> {
    footer: &'a ParquetMetaData,
    schema: &'a Schema,
    partition_values: &'a BTreeMap<String, Option<String>>,
    columns: &'a [(String, DataType)],
}

struct RowGroupFacts<'a> {
    file: &'a FileView<'a>,
    row_group: &'a RowGroupMetaData,
}

/// One column chunk's statistics once every gate passed, its bounds still in the file's type.
struct ChunkStatistics {
    folded: DataType,
    min: ArrayRef,
    max: ArrayRef,
    null_count: Option<u64>,
    rows: u64,
    nan_possible: bool,
}

impl ColumnFacts for RowGroupFacts<'_> {
    fn orderings(&self, column: &str, literal: &ScalarValue) -> Option<Orderings> {
        if is_partition_key(self.file.partition_values, column) {
            return self.file.partition_values.orderings(column, literal);
        }
        let statistics = self.chunk_statistics(column)?;
        if statistics.null_count == Some(statistics.rows) {
            return Some(Orderings::NONE);
        }
        let (min, max) = statistics.bounds(literal)?;
        Orderings::within(&min, &max, literal).map(|orderings| {
            if statistics.nan_possible {
                orderings.with_nan_row()
            } else {
                orderings
            }
        })
    }

    fn nulls(&self, column: &str) -> Option<Nulls> {
        if is_partition_key(self.file.partition_values, column) {
            return self.file.partition_values.nulls(column);
        }
        let statistics = self.chunk_statistics(column)?;
        Some(Nulls::counted(statistics.null_count?, statistics.rows))
    }
}

impl RowGroupFacts<'_> {
    /// `None` at the first gate that fails. The converter reads a missing null count as zero
    /// unless told otherwise, which Parquet forbids.
    fn chunk_statistics(&self, column: &str) -> Option<ChunkStatistics> {
        let (name, folded) = self
            .file
            .columns
            .iter()
            .find(|(name, _)| folds_to(name, column))?;
        let file_type = self.file.schema.field_with_name(name).ok()?.data_type();
        if !compares_as_folded(file_type, folded) {
            return None;
        }
        let file_metadata = self.file.footer.file_metadata();
        let parquet_schema = file_metadata.schema_descr();
        let converter = StatisticsConverter::try_new(name, self.file.schema, parquet_schema)
            .ok()?
            .with_missing_null_counts_as_zero(false);
        let leaf = converter.parquet_column_index()?;
        let descriptor = parquet_schema.column(leaf);
        if descriptor.max_rep_level() > 0 || !has_defined_order(file_metadata.column_order(leaf)) {
            return None;
        }
        let chunk = self.row_group.column(leaf).statistics()?;
        // The crate also reads a chunk without bounds, such as an all-NULL one, as deprecated.
        let bounded = chunk.min_bytes_opt().is_some() || chunk.max_bytes_opt().is_some();
        if chunk.is_min_max_deprecated() && bounded {
            return None;
        }
        let row_group = iter::once(self.row_group);
        let null_counts = converter.row_group_null_counts(row_group.clone()).ok()?;
        Some(ChunkStatistics {
            folded: folded.clone(),
            min: converter.row_group_mins(row_group.clone()).ok()?,
            max: converter.row_group_maxes(row_group).ok()?,
            null_count: null_counts.is_valid(0).then(|| null_counts.value(0)),
            rows: u64::try_from(self.row_group.num_rows()).ok()?,
            nan_possible: matches!(
                descriptor.physical_type(),
                PhysicalType::FLOAT | PhysicalType::DOUBLE
            ),
        })
    }
}

impl ChunkStatistics {
    /// The bounds in the literal's type: the folded type, or DOUBLE for a FLOAT column, whose
    /// footer-scope literal is a DOUBLE and whose widening is exact.
    fn bounds(&self, literal: &ScalarValue) -> Option<(ScalarValue, ScalarValue)> {
        let target = literal.data_type();
        let float_widened = self.folded == DataType::Float32 && target == DataType::Float64;
        if target != self.folded && !float_widened {
            return None;
        }
        let min = typed_bound(&self.min, &target)?;
        let max = typed_bound(&self.max, &target)?;
        Some((widened_zero(min, -0.0), widened_zero(max, 0.0)))
    }
}

/// `None` for an absent, undecodable, or NaN bound.
fn typed_bound(bound: &ArrayRef, target: &DataType) -> Option<ScalarValue> {
    let bound = cast(bound, target).ok()?;
    let value = ScalarValue::try_from_array(&bound, 0).ok()?;
    let is_nan = matches!(value, ScalarValue::Float64(Some(value)) if value.is_nan());
    (!value.is_null() && !is_nan).then_some(value)
}

/// Parquet's `TYPE_ORDER` read rules: a `+0` minimum may hide a `-0` and a `-0` maximum a `+0`,
/// which the scan's total order separates.
fn widened_zero(bound: ScalarValue, zero: f64) -> ScalarValue {
    match bound {
        ScalarValue::Float64(Some(0.0)) => ScalarValue::Float64(Some(zero)),
        other => other,
    }
}

/// The scan casts a widenable file type to the folded type. Nothing verifies how it compares a
/// time-zoned timestamp or a FLOAT16 against a pushed literal.
fn compares_as_folded(file_type: &DataType, folded: &DataType) -> bool {
    widen(file_type, folded).as_ref() == Some(folded)
        && !matches!(folded, DataType::Float16 | DataType::Timestamp(_, Some(_)))
}

/// `parquet` 58.3.0 reads a footer without `column_orders` as `UNDEFINED`, maps INT96 and
/// INTERVAL to `SortOrder::UNDEFINED`, and reads an unknown union member as `UNKNOWN`.
fn has_defined_order(order: ColumnOrder) -> bool {
    matches!(
        order,
        ColumnOrder::TYPE_DEFINED_ORDER(sort_order) if sort_order != SortOrder::UNDEFINED
    )
}

#[cfg(test)]
#[path = "footer_statistics_tests.rs"]
mod tests;
