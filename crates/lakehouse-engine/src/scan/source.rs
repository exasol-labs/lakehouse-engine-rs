use arrow::datatypes::DataType;
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::error::UdfError;

use crate::scan::spec::{ColumnRef, ScanSpec};
use crate::scan::storage_ref::ResolvedScanStorage;

use super::join_scan::{join_source, register_join_tables};
use super::raw_scan::{SCAN_TARGET, register_files};
use super::sql_support::aliased_select;

/// The relation a scan shape (rows, top-N, partial aggregate) reads: one table, or the broadcast
/// join of two. A shape splices [`Self::relation`] and types its columns through
/// [`Self::column_type`], so it never asks which of the two it reads.
pub(super) struct ScanSource {
    from: String,
    columns: Vec<(ColumnRef, DataType)>,
}

impl ScanSource {
    pub(super) fn new(from: String, columns: Vec<(ColumnRef, DataType)>) -> Self {
        Self { from, columns }
    }

    /// Registers the spec's tables, then resolves the relation over them.
    pub(super) async fn register(
        ctx: &SessionContext,
        spec: &ScanSpec,
        storage: &ResolvedScanStorage,
    ) -> Result<Self, UdfError> {
        if spec.common.join.is_some() {
            register_join_tables(ctx, spec, storage).await?;
            join_source(ctx, spec).await
        } else {
            register_files(ctx, SCAN_TARGET, spec, storage).await?;
            Self::table(ctx, SCAN_TARGET, spec.common.binary_as_string).await
        }
    }

    /// Over a table already registered as `table_name`.
    pub(super) async fn table(
        ctx: &SessionContext,
        table_name: &str,
        binary_as_string: bool,
    ) -> Result<Self, UdfError> {
        let table = ctx
            .table(table_name)
            .await
            .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
        let schema = table.schema();
        // DataFusion matches quoted identifiers case-sensitively, while the adapter speaks Exasol's
        // uppercase and Parquet keeps Iceberg casing.
        let aliased = aliased_select(table_name, schema, binary_as_string);
        Ok(Self::new(
            format!("({aliased})"),
            relation_columns(None, schema),
        ))
    }

    /// Spliced verbatim after `FROM`.
    pub(super) fn relation(&self) -> &str {
        &self.from
    }

    pub(super) fn columns(&self) -> impl Iterator<Item = &ColumnRef> {
        self.columns.iter().map(|(column, _)| column)
    }

    /// A single table's columns carry no relation and match by name alone. A join's match their
    /// relation too, so a column name both sides carry stays two columns.
    pub(super) fn column_type(&self, column: &ColumnRef) -> Option<&DataType> {
        let name = column.name.to_uppercase();
        self.columns
            .iter()
            .find(|(key, _)| {
                key.name == name && (key.relation.is_none() || key.relation == column.relation)
            })
            .map(|(_, data_type)| data_type)
    }
}

/// Each column keyed by its uppercased name, under `alias` when the relation has one.
pub(super) fn relation_columns(
    alias: Option<&str>,
    schema: &DFSchema,
) -> Vec<(ColumnRef, DataType)> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let column = ColumnRef {
                relation: alias.map(str::to_string),
                name: field.name().to_uppercase(),
            };
            (column, field.data_type().clone())
        })
        .collect()
}
