//! Partition columns are recorded once per file in the table's log, not in the data file, so a
//! Parquet-only read returns NULL for them. They become DataFusion `table_partition_cols`
//! substituted as scan-time literals, so projection, filters, aggregation, and pruning all see
//! the real value with no post-scan rewrite. A per-file value for a column the data file may hold
//! rides the same mechanism in a hidden partition column read by [`IdentityFallbackExpr`].

use crate::scan::spec::FileEntry;
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion::scalar::ScalarValue;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Declared order (what the query and [`crate::scan::scan_table::ParquetScanTable::schema`]
/// expose) differs from DataFusion's `file_schema ++ table_partition_cols` order whenever a
/// partition column is not last; [`Self::projection_exprs`] alone reconciles them.
#[derive(Debug, Clone)]
pub(crate) struct PartitionedScanSchema {
    declared: SchemaRef,
    file_source: TableSchema,
    /// Where each declared column sits in `file_schema ++ table_partition_cols`.
    scan_index_by_declared: Vec<usize>,
    /// The declared field behind each hidden identity column, which follow the listed
    /// partition columns in `table_partition_cols`.
    identity_fields: Vec<FieldRef>,
    /// Where each fallback column's hidden identity column sits, keyed by declared index.
    identity_index_by_declared: HashMap<usize, usize>,
}

impl PartitionedScanSchema {
    /// `partition_columns` is taken in PARTITION order, the order DataFusion's Parquet opener
    /// zips `table_partition_cols` against a file's `partition_values`. Fields are moved, never
    /// rebuilt, so name, type, nullability, and metadata survive. A `partition_values` key of
    /// `files` that `partition_columns` does not list stays a file column and gains a hidden
    /// identity column, because the opener substitutes partition literals by name before the
    /// file adapter can tell whether a file holds the column.
    ///
    /// Errors when a partition column is undeclared, named twice, or matches more than one
    /// declared field, or when an unlisted key matches more than one: each breaks the one-to-one
    /// column mapping. An unlisted key naming no declared field has no column to fill.
    pub(crate) fn split(
        declared: SchemaRef,
        partition_columns: &[String],
        files: &[FileEntry],
    ) -> Result<Self, String> {
        let partition_position = listed_partition_positions(&declared, partition_columns)?;
        let identity_declared = identity_fallback_indices(&declared, partition_columns, files)?;
        if partition_columns.is_empty() && identity_declared.is_empty() {
            return Ok(Self {
                scan_index_by_declared: (0..declared.fields().len()).collect(),
                file_source: TableSchema::from(Arc::clone(&declared)),
                declared,
                identity_fields: Vec::new(),
                identity_index_by_declared: HashMap::new(),
            });
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
        let first_identity_index = file_count + partition_columns.len();
        let identity_index_by_declared: HashMap<usize, usize> = identity_declared
            .iter()
            .enumerate()
            .map(|(position, declared_index)| (*declared_index, first_identity_index + position))
            .collect();
        debug_assert_eq!(
            scan_index_by_declared
                .iter()
                .chain(identity_index_by_declared.values())
                .collect::<HashSet<_>>()
                .len(),
            scan_index_by_declared.len() + identity_index_by_declared.len(),
            "every declared column and hidden identity column must map to its own scan index"
        );

        partition_slots.sort_by_key(|(position, _)| *position);
        let identity_fields: Vec<FieldRef> = identity_declared
            .iter()
            .map(|index| Arc::clone(&declared.fields()[*index]))
            .collect();
        let hidden_fields = identity_declared
            .iter()
            .zip(&identity_fields)
            .map(|(index, field)| {
                Arc::new(IdentityFallbackExpr::hidden_field(
                    hidden_identity_name(&declared, *index),
                    field,
                ))
            });
        Ok(Self {
            file_source: TableSchema::builder(Arc::new(Schema::new(file_fields)))
                .with_table_partition_cols(
                    partition_slots
                        .into_iter()
                        .map(|(_, field)| field)
                        .chain(hidden_fields)
                        .collect::<Vec<_>>(),
                )
                .build(),
            scan_index_by_declared,
            identity_fields,
            identity_index_by_declared,
            declared,
        })
    }

    pub(crate) fn declared_schema(&self) -> &SchemaRef {
        &self.declared
    }

    pub(crate) fn file_source_schema(&self) -> &TableSchema {
        &self.file_source
    }

    /// `FileScanConfig` evaluates the expressions in the given order, so they alone restore
    /// declared order, and a fallback column reads through its [`IdentityFallbackExpr`]. An
    /// absent projection stays absent only when the split moved nothing, so an unsplit scan plans
    /// exactly as `with_projection_indices` would.
    pub(crate) fn projection_exprs(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Option<ProjectionExprs> {
        let declared_indices = match projection {
            Some(indices) => indices.clone(),
            None if self.file_source.table_partition_cols().is_empty() => return None,
            None => (0..self.declared.fields().len()).collect(),
        };
        Some(
            declared_indices
                .into_iter()
                .map(|index| self.projection_expr(index))
                .collect(),
        )
    }

    fn projection_expr(&self, declared_index: usize) -> ProjectionExpr {
        let file_column = self.scan_column(self.scan_index_by_declared[declared_index]);
        let expr = match self.identity_index_by_declared.get(&declared_index) {
            Some(&identity_index) => Arc::new(IdentityFallbackExpr::new(
                file_column,
                self.scan_column(identity_index),
            )),
            None => file_column,
        };
        ProjectionExpr::new(expr, self.declared.field(declared_index).name())
    }

    fn scan_column(&self, scan_index: usize) -> Arc<dyn PhysicalExpr> {
        let field = self.file_source.table_schema().field(scan_index);
        Arc::new(Column::new(field.name(), scan_index))
    }

    /// Values come back in `table_partition_cols` order: each listed partition column typed as
    /// its DECLARED Arrow type, then one [`IdentityFallbackExpr`] struct per hidden column.
    /// Delta serializes values as strings with empty string meaning null; both that and an
    /// absent value become a typed NULL. Errors when a value the type cannot represent would be
    /// coerced, or when an entry logs no value for a partition column (a planning defect).
    pub(crate) fn partition_values_for(
        &self,
        entry: &FileEntry,
    ) -> Result<Vec<ScalarValue>, String> {
        let partition_fields = self.file_source.table_partition_cols();
        let listed = partition_fields[..partition_fields.len() - self.identity_fields.len()]
            .iter()
            .map(|field| {
                let logged = entry.partition_values.get(field.name()).ok_or_else(|| {
                    format!(
                        "data file '{}' logs no partition value for partition column '{}'",
                        entry.path,
                        field.name()
                    )
                })?;
                declared_partition_value(logged.as_deref(), field)
            });
        let identity = self
            .identity_fields
            .iter()
            .map(|field| IdentityFallbackExpr::file_value(entry, field));
        listed.chain(identity).collect()
    }
}

fn listed_partition_positions<'a>(
    declared: &Schema,
    partition_columns: &'a [String],
) -> Result<HashMap<&'a str, usize>, String> {
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
    Ok(partition_position)
}

/// In declared order, so the hidden columns of one spec are laid out the same in every shard.
fn identity_fallback_indices(
    declared: &Schema,
    partition_columns: &[String],
    files: &[FileEntry],
) -> Result<Vec<usize>, String> {
    let unlisted: BTreeSet<&str> = files
        .iter()
        .flat_map(|entry| entry.partition_values.keys())
        .map(String::as_str)
        .filter(|key| !partition_columns.iter().any(|listed| listed == key))
        .collect();
    let mut indices = Vec::with_capacity(unlisted.len());
    for key in unlisted {
        let mut matching = declared
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == key)
            .map(|(index, _)| index);
        match (matching.next(), matching.next()) {
            (Some(index), None) => indices.push(index),
            (Some(_), Some(_)) => {
                return Err(format!(
                    "the identity partition value of column '{key}' matches more than one field \
                     in the table's logical schema"
                ));
            }
            (None, _) => {}
        }
    }
    indices.sort_unstable();
    Ok(indices)
}

/// DataFusion substitutes partition literals BY NAME (`replace_columns_with_literals`), so the
/// hidden name must differ from every declared name a query or filter can reference.
fn hidden_identity_name(declared: &Schema, declared_index: usize) -> String {
    let mut name = format!("__identity_{declared_index}");
    while declared.fields().iter().any(|field| field.name() == &name) {
        name.push('_');
    }
    name
}

fn declared_partition_value(logged: Option<&str>, field: &Field) -> Result<ScalarValue, String> {
    match non_null_partition_value(logged) {
        Some(value) => partition_scalar(value, field.data_type()).map_err(|e| {
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
}

/// Absent and empty both read as NULL: Delta logs a NULL partition value as an empty string.
pub(crate) fn non_null_partition_value(logged: Option<&str>) -> Option<&str> {
    logged.filter(|value| !value.is_empty())
}

/// The planning-time partition predicate converts with this too, so it and the scan agree on
/// every value (`file-planning/partition-predicate-declared-types`).
pub(crate) fn partition_scalar(
    value: &str,
    data_type: &DataType,
) -> Result<ScalarValue, DataFusionError> {
    ScalarValue::try_from_string(value.to_string(), data_type)
}

/// The struct child that carries a file's identity value at the column's declared type.
const IDENTITY_VALUE_FIELD: &str = "value";

/// Reads a column from the data file, or from the file's identity partition value where the file
/// lacks the column (Iceberg Column Projection rule 1). Its hidden partition column carries one
/// struct per file: a null struct is no value, a null child a null value, and a non-null child
/// the value. DataFusion's opener substitutes that struct as a literal before the file adapter
/// runs, so the adapter, which alone knows whether a file binds the column, reduces this
/// expression through [`Self::reduce_for_file`]. Both inputs are children, so the substitution
/// reaches the hidden column inside a pushed-down predicate too.
#[derive(Debug, Eq)]
pub(crate) struct IdentityFallbackExpr {
    file_column: Arc<dyn PhysicalExpr>,
    identity: Arc<dyn PhysicalExpr>,
}

// Not derived: rust-lang/rust#78808 blocks deriving for a struct holding `Arc<dyn Trait>`.
impl PartialEq for IdentityFallbackExpr {
    fn eq(&self, other: &Self) -> bool {
        self.file_column.eq(&other.file_column) && self.identity.eq(&other.identity)
    }
}

impl std::hash::Hash for IdentityFallbackExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.file_column.hash(state);
        self.identity.hash(state);
    }
}

impl IdentityFallbackExpr {
    fn new(file_column: Arc<dyn PhysicalExpr>, identity: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            file_column,
            identity,
        }
    }

    fn hidden_field(name: String, declared: &Field) -> Field {
        Field::new(
            name,
            DataType::Struct(identity_value_fields(declared)),
            true,
        )
    }

    fn file_value(entry: &FileEntry, declared: &Field) -> Result<ScalarValue, String> {
        let fields = identity_value_fields(declared);
        let Some(logged) = entry.partition_values.get(declared.name()) else {
            return Ok(ScalarStructBuilder::new_null(fields));
        };
        let value = declared_partition_value(logged.as_deref(), declared)?;
        ScalarStructBuilder::new()
            .with_scalar(Arc::clone(&fields[0]), value)
            .build()
            .map_err(|e| {
                format!(
                    "the identity partition value of column '{}' cannot be carried at its \
                     declared type {} ({e})",
                    declared.name(),
                    declared.data_type()
                )
            })
    }

    /// The file column where `binds_field` says this file holds it, so a stored value always
    /// wins; otherwise the per-file value as a literal of the declared type, or the file column
    /// again for a file with no value, which leaves the field to the absent-field chain.
    pub(crate) fn reduce_for_file(
        &self,
        binds_field: impl Fn(&Column) -> bool,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        let Some(column) = self.file_column.downcast_ref::<Column>() else {
            return Err(DataFusionError::Internal(format!(
                "the identity fallback {self} reached the file adapter with a file side that is \
                 not a column"
            )));
        };
        if binds_field(column) {
            return Ok(Arc::clone(&self.file_column));
        }
        let Some(ScalarValue::Struct(identity)) =
            self.identity.downcast_ref::<Literal>().map(Literal::value)
        else {
            return Err(DataFusionError::Internal(format!(
                "the identity partition value of column '{}' reached the file adapter without \
                 its per-file literal",
                column.name()
            )));
        };
        if identity.is_null(0) {
            return Ok(Arc::clone(&self.file_column));
        }
        let value = ScalarValue::try_from_array(identity.column(0), 0)?;
        Ok(Arc::new(Literal::new(value)))
    }
}

fn identity_value_fields(declared: &Field) -> arrow::datatypes::Fields {
    vec![Field::new(
        IDENTITY_VALUE_FIELD,
        declared.data_type().clone(),
        true,
    )]
    .into()
}

impl std::fmt::Display for IdentityFallbackExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "identity_fallback({}, {})",
            self.file_column, self.identity
        )
    }
}

impl PhysicalExpr for IdentityFallbackExpr {
    fn return_field(&self, input_schema: &Schema) -> datafusion::error::Result<FieldRef> {
        self.file_column.return_field(input_schema)
    }

    /// DataFusion skips the file adapter only for a file whose schema equals the logical file
    /// schema (`opener/mod.rs:1015-1016`), and such a file binds the column.
    fn evaluate(&self, batch: &RecordBatch) -> datafusion::error::Result<ColumnarValue> {
        self.file_column.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.file_column, &self.identity]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        match <[Arc<dyn PhysicalExpr>; 2]>::try_from(children) {
            Ok([file_column, identity]) => Ok(Arc::new(Self::new(file_column, identity))),
            Err(children) => Err(DataFusionError::Internal(format!(
                "identity_fallback reads a file column and its identity value, so it takes \
                 exactly two child expressions, but {} were given",
                children.len()
            ))),
        }
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "identity_fallback(")?;
        self.file_column.fmt_sql(f)?;
        write!(f, ", ")?;
        self.identity.fmt_sql(f)?;
        write!(f, ")")
    }
}

#[cfg(test)]
#[path = "partition_values_tests.rs"]
mod tests;
