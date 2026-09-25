//! Partition columns are recorded once per file in the table's log, not in the data file, so a
//! Parquet-only read returns NULL for them. They become DataFusion `table_partition_cols`
//! substituted as scan-time literals, so projection, filters, aggregation, and pruning all see
//! the real value with no post-scan rewrite.

use crate::scan::spec::FileEntry;
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::scalar::ScalarValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Declared order (what the query and [`crate::scan::positional_deletes::PositionalDeleteScanTable::schema`]
/// expose) differs from DataFusion's `file_schema ++ table_partition_cols` order whenever a
/// partition column is not last; [`Self::remap_projection`] alone reconciles them.
#[derive(Debug, Clone)]
pub(crate) struct PartitionedScanSchema {
    declared: SchemaRef,
    file_source: TableSchema,
    /// Where each declared column sits in `file_schema ++ table_partition_cols`.
    scan_index_by_declared: Vec<usize>,
}

impl PartitionedScanSchema {
    /// `partition_columns` is taken in PARTITION order, the order DataFusion's Parquet opener
    /// zips `table_partition_cols` against a file's `partition_values`. Fields are moved, never
    /// rebuilt, so name, type, nullability, and metadata survive.
    ///
    /// Errors when a partition column is undeclared, named twice, or matches more than one
    /// declared field: each breaks the one-to-one column mapping.
    pub(crate) fn split(declared: SchemaRef, partition_columns: &[String]) -> Result<Self, String> {
        if partition_columns.is_empty() {
            return Ok(Self {
                scan_index_by_declared: (0..declared.fields().len()).collect(),
                file_source: TableSchema::from_file_schema(Arc::clone(&declared)),
                declared,
            });
        }

        let mut partition_position: HashMap<&str, usize> = HashMap::new();
        for (position, name) in partition_columns.iter().enumerate() {
            if declared.index_of(name).is_err() {
                return Err(format!(
                    "partition column '{name}' is not declared in the table's logical schema"
                ));
            }
            if partition_position.insert(name.as_str(), position).is_some() {
                return Err(format!(
                    "partition column '{name}' is named twice in the table's partition columns"
                ));
            }
        }

        let mut file_fields: Vec<FieldRef> = Vec::with_capacity(declared.fields().len());
        let mut partition_slots: Vec<(usize, FieldRef)> =
            Vec::with_capacity(partition_columns.len());
        let mut partition_declared: Vec<(usize, usize)> =
            Vec::with_capacity(partition_columns.len());
        let mut scan_index_by_declared = vec![0; declared.fields().len()];
        for (declared_index, field) in declared.fields().iter().enumerate() {
            match partition_position.get(field.name().as_str()) {
                Some(&position) => {
                    if partition_slots.iter().any(|(taken, _)| *taken == position) {
                        return Err(format!(
                            "partition column '{}' matches more than one field in the table's \
                             logical schema",
                            field.name()
                        ));
                    }
                    partition_slots.push((position, Arc::clone(field)));
                    partition_declared.push((declared_index, position));
                }
                None => {
                    scan_index_by_declared[declared_index] = file_fields.len();
                    file_fields.push(Arc::clone(field));
                }
            }
        }

        let file_count = file_fields.len();
        for (declared_index, position) in partition_declared {
            scan_index_by_declared[declared_index] = file_count + position;
        }
        debug_assert_eq!(
            scan_index_by_declared.iter().collect::<HashSet<_>>().len(),
            scan_index_by_declared.len(),
            "every declared column must map to its own scan index"
        );

        partition_slots.sort_by_key(|(position, _)| *position);
        Ok(Self {
            file_source: TableSchema::new(
                Arc::new(Schema::new(file_fields)),
                partition_slots
                    .into_iter()
                    .map(|(_, field)| field)
                    .collect(),
            ),
            scan_index_by_declared,
            declared,
        })
    }

    pub(crate) fn declared_schema(&self) -> &SchemaRef {
        &self.declared
    }

    pub(crate) fn file_source_schema(&self) -> &TableSchema {
        &self.file_source
    }

    /// `FileScanConfig` applies indices in the given order, so the remap alone restores declared
    /// order. An absent projection is made explicit on a partitioned table, where the orders differ.
    pub(crate) fn remap_projection(&self, projection: Option<&Vec<usize>>) -> Option<Vec<usize>> {
        if self.file_source.table_partition_cols().is_empty() {
            return projection.cloned();
        }
        let declared_indices = match projection {
            Some(indices) => indices.clone(),
            None => (0..self.declared.fields().len()).collect(),
        };
        Some(
            declared_indices
                .into_iter()
                .map(|index| self.scan_index_by_declared[index])
                .collect(),
        )
    }

    /// Values come back in partition order, typed as each column's DECLARED Arrow type.
    /// Delta serializes values as strings with empty string meaning null; both that and an
    /// absent value become a typed NULL. Errors when a value the type cannot represent would be
    /// coerced, or when an entry logs no value for a partition column (a planning defect).
    pub(crate) fn partition_values_for(
        &self,
        entry: &FileEntry,
    ) -> Result<Vec<ScalarValue>, String> {
        self.file_source
            .table_partition_cols()
            .iter()
            .map(|field| {
                let logged = entry.partition_values.get(field.name()).ok_or_else(|| {
                    format!(
                        "data file '{}' logs no partition value for partition column '{}'",
                        entry.path,
                        field.name()
                    )
                })?;
                match logged.as_deref().filter(|value| !value.is_empty()) {
                    Some(value) => ScalarValue::try_from_string(
                        value.to_string(),
                        field.data_type(),
                    )
                    .map_err(|e| {
                        format!(
                            "partition value '{value}' for column '{}' is not a valid {} ({e})",
                            field.name(),
                            field.data_type()
                        )
                    }),
                    None => ScalarValue::try_new_null(field.data_type()).map_err(|e| {
                        format!(
                            "partition column '{}' cannot hold a null value of its declared type {} ({e})",
                            field.name(),
                            field.data_type()
                        )
                    }),
                }
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "partition_values_tests.rs"]
mod tests;
