//! Scan-side materialization of **partition columns** — the logical columns a
//! partitioned writer records once per file in the table's log instead of once per
//! row in the data file, so a Parquet-only read returns NULL for every one of them.
//!
//! The concept is format-neutral: the shard-invariant
//! [`CommonScanSpec::partition_columns`] names them in partition order and each
//! [`FileEntry::partition_values`] carries that file's own values, whichever table
//! format planned the scan.
//!
//! [`PartitionedScanSchema`] turns those two neutral fields into DataFusion's own
//! partitioning mechanism: the declared logical schema is split into the fields a
//! data file carries and the fields supplied per file, the latter becoming
//! `table_partition_cols` on the [`TableSchema`] and one
//! [`ScalarValue`] per `PartitionedFile::partition_values`. DataFusion then
//! substitutes each value as a scan-time literal, so projection, filters,
//! aggregation, and file pruning all observe the real value with no post-scan batch
//! rewrite and no extra plan node.
//!
//! [`CommonScanSpec::partition_columns`]: crate::scan::spec::CommonScanSpec::partition_columns
//! [`PartitionedFile::partition_values`]: datafusion::datasource::listing::PartitionedFile::partition_values

use crate::scan::spec::FileEntry;
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::scalar::ScalarValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The table's declared logical schema paired with the split DataFusion scans it
/// through: `file_schema ++ table_partition_cols`.
///
/// The two orders differ whenever a partition column is not declared last, which is
/// why this type owns both the split and the index remap between them. The declared
/// order is what the query and the Exasol-facing column list expect and is what
/// [`crate::scan::positional_deletes::PositionalDeleteScanTable::schema`] reports;
/// the split order is an internal detail of the `FileScanConfig`, reconciled by
/// [`Self::remap_projection`] alone rather than by a reordering plan node.
///
/// An unpartitioned table splits into itself: the file schema IS the declared
/// schema, there are no partition columns, and every projection passes through
/// untouched — so an Iceberg scan's registered schema and plan shape are what they
/// were before this type existed.
#[derive(Debug, Clone)]
pub(crate) struct PartitionedScanSchema {
    declared: SchemaRef,
    file_source: TableSchema,
    /// Where each declared column sits in `file_schema ++ table_partition_cols`.
    scan_index_by_declared: Vec<usize>,
}

impl PartitionedScanSchema {
    /// Split `declared` into the fields a data file carries and the fields named by
    /// `partition_columns`, which are taken in PARTITION order — the order
    /// DataFusion's Parquet opener zips `table_partition_cols` against a file's
    /// `partition_values`.
    ///
    /// Each declared [`Field`](arrow::datatypes::Field) is moved, never rebuilt, so
    /// a projection of the split schema yields the same fields — name, type,
    /// nullability, and metadata — as the same projection of the declared schema.
    ///
    /// Returns an error naming the offending column when `partition_columns` names a
    /// column the schema does not declare, names one twice, or names one that matches
    /// MORE than one declared field: each would leave the declared schema and the scan
    /// schema without a one-to-one column mapping, and the last would additionally
    /// leave a remapped projection index past the end of the scan schema.
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

        // The file half's width is known only once the pass has seen every field.
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

    /// The table's schema in DECLARED column order — what the query, and the
    /// Exasol-facing column list behind it, expect.
    pub(crate) fn declared_schema(&self) -> &SchemaRef {
        &self.declared
    }

    /// The file schema and partition columns as DataFusion pairs them, for the
    /// `FileSource` the scan's `FileScanConfig` is built on.
    pub(crate) fn file_source_schema(&self) -> &TableSchema {
        &self.file_source
    }

    /// Translate a projection over the DECLARED schema into one over
    /// `file_schema ++ table_partition_cols`.
    ///
    /// `FileScanConfig` applies projection indices in the order given, so the
    /// remapped list alone restores declared order in the scan's output. An absent
    /// projection means "every column in declared order", which the split no longer
    /// expresses implicitly, so it is made explicit — except on an unpartitioned
    /// table, where both orders coincide and the projection is passed through
    /// exactly as it arrived.
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

    /// This file's partition values as scan-time constants, one per partition column
    /// in partition order, each already of its column's DECLARED Arrow type.
    ///
    /// The Delta protocol serializes every partition value as a string and an empty
    /// string as null; the scan spec carries a null as an explicit absent value. Both
    /// become a typed NULL here — never the partition-directory text a Hive-style
    /// default partition is named after.
    ///
    /// Returns an error naming the column, its declared type, and the rejected value
    /// when a logged value the declared type cannot represent would otherwise be
    /// silently coerced, truncated, or nulled; and one naming the file when an entry
    /// logs no value at all for a declared partition column, which is a planning
    /// defect rather than a null.
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
