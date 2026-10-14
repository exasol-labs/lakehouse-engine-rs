use arrow::datatypes::DataType as ArrowType;
use delta_kernel::schema::{StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;
use lakehouse_catalog::{CatalogColumn, CatalogTable, ColumnSourceType, TableFormat};

use super::RefusedColumn;
use super::delta_schema::classify_spark_schema;
use crate::adapter::column_notes::ColumnDeclaration;
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::LogicalField;
use crate::types::hive_type::parse_hive_type;
use crate::types::mapping::{arrow_type_to_tag, unity_spark_field};

/// The table property carrying a Delta table's column-mapping mode (PROTOCOL.md § Column Mapping).
const COLUMN_MAPPING_MODE_PROPERTY: &str = "delta.columnMapping.mode";

/// The Unity Delta, Unity Parquet, and Glue Parquet arm of [`super::declare_columns`], one entry
/// per column in catalog order. Unusable metadata refuses columns, never the statement.
pub(super) fn declare(table: &CatalogTable) -> Vec<ColumnDeclaration> {
    let (label, mode) = match (&table.format, table.columns.first().map(|c| &c.source_type)) {
        (TableFormat::Parquet, Some(ColumnSourceType::Unity { .. })) => ("Unity Parquet", None),
        (TableFormat::Parquet, Some(ColumnSourceType::Glue { .. })) => ("Glue", None),
        (TableFormat::Delta, Some(ColumnSourceType::Unity { .. })) => {
            ("Delta", table.properties.get(COLUMN_MAPPING_MODE_PROPERTY))
        }
        _ => return Vec::new(),
    };
    let (logical_schema, refused_columns) = classify_columns(table, label, mode);
    table
        .columns
        .iter()
        .map(|column| ColumnDeclaration {
            source_type: logical_schema
                .iter()
                .find(|field| field.name == column.name)
                .cloned()
                .unwrap_or_else(|| refused_placeholder(&column.name)),
            partition: table
                .partition_columns
                .iter()
                .position(|name| *name == column.name),
            refused: refused_columns
                .iter()
                .find(|refused| refused.column_name == column.name)
                .map(|refused| refused.reason.clone()),
        })
        .collect()
}

/// A refused column maps to no type, so its field only names it.
fn refused_placeholder(name: &str) -> LogicalField {
    LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type_to_tag(&ArrowType::Utf8),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    }
}

/// `mode` is the catalog's copy of the column-mapping property, absent for a Parquet table.
fn classify_columns(
    table: &CatalogTable,
    label: &str,
    mode: Option<&String>,
) -> (Vec<LogicalField>, Vec<RefusedColumn>) {
    let every_column_refused = |problem: String| {
        let refused = table.columns.iter().map(|column| RefusedColumn {
            column_name: column.name.clone(),
            reason: format!(
                "{label} column '{}' cannot be declared: {problem}",
                column.name
            ),
        });
        (Vec::new(), refused.collect())
    };
    let table_name = catalog_identifier_string(&table.ident);
    let mode = match mode.map(|raw| (raw, raw.parse::<ColumnMappingMode>())) {
        None => ColumnMappingMode::None,
        Some((_, Ok(mode))) => mode,
        Some((raw, Err(_))) => {
            return every_column_refused(format!(
                "table {table_name} sets {COLUMN_MAPPING_MODE_PROPERTY} to '{raw}', which is none \
                 of 'none', 'id', and 'name'"
            ));
        }
    };
    let mut fields = Vec::with_capacity(table.columns.len());
    let mut refused_columns = Vec::new();
    for column in &table.columns {
        match spark_field(column, &table.format) {
            Ok(field) => fields.push(field),
            Err(reason) => refused_columns.push(RefusedColumn {
                column_name: column.name.clone(),
                reason: format!("{label} column '{}': {reason}", column.name),
            }),
        }
    }
    let schema = match StructType::try_new(fields) {
        Ok(schema) => schema,
        Err(error) => {
            return every_column_refused(format!(
                "the columns of table {table_name} do not form one Spark schema: {error}"
            ));
        }
    };
    let (logical_schema, classified_refusals) = classify_spark_schema(&schema, mode, label);
    refused_columns.extend(classified_refusals);
    (logical_schema, refused_columns)
}

/// A Delta column's nullability is its `type_json`'s; a Parquet column is forced nullable, because
/// a file missing the column reads NULL, which a required declaration would fail.
fn spark_field(column: &CatalogColumn, format: &TableFormat) -> Result<StructField, String> {
    match &column.source_type {
        ColumnSourceType::Unity { type_json } => {
            unity_spark_field(type_json.as_deref()).map(|field| StructField {
                name: column.name.clone(),
                nullable: field.nullable || *format == TableFormat::Parquet,
                ..field
            })
        }
        ColumnSourceType::Glue { hive_type } => parse_hive_type(hive_type)
            .map(|data_type| StructField::nullable(column.name.clone(), data_type)),
        ColumnSourceType::Iceberg(_) | ColumnSourceType::Parquet(_) => {
            Err("its catalog declares no Spark or Hive type for it".to_string())
        }
    }
}
