use arrow::datatypes::DataType;
use datafusion::common::DFSchema;

use crate::scan::spec::ProjectionItem;
use crate::types::mapping::{is_binary, needs_json_fallback, needs_nested_json_rendering};

use super::binary_hex::{BINARY_HEX_UDF_NAME, raw_bytes_alias};
use super::raw_scan::NESTED_JSON_RENDER_UDF_NAME;

/// Uppercase aliases let identifiers resolved in uppercase match any Parquet field casing.
/// A binary column is aliased to its hex text, so every expression over it sees what Exasol
/// sees, and its raw bytes get a second alias for the comparisons the adapter translates.
/// Public so `tests/it/scan_agg_projection_pruning.rs` builds the same inner SELECT as production.
pub fn build_alias_items(schema: &DFSchema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let column = quote_ident(f.name());
            let alias = f.name().to_uppercase();
            if is_binary(f.data_type()) {
                format!(
                    "{BINARY_HEX_UDF_NAME}({column}, '{}') AS {}, {column} AS {}",
                    alias.replace('\'', "''"),
                    quote_ident(&alias),
                    quote_ident(&raw_bytes_alias(&alias))
                )
            } else {
                format!("{column} AS {}", quote_ident(&alias))
            }
        })
        .collect()
}

/// All columns are aliased, since the filter may reference an unprojected one.
pub(super) fn aliased_select(table_name: &str, schema: &DFSchema) -> String {
    format!(
        "SELECT {} FROM {table_name}",
        build_alias_items(schema).join(", ")
    )
}

pub(super) fn append_where(sql: &mut String, filter: Option<&str>) {
    if let Some(filter) = filter
        && !filter.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }
}

/// A rendered expression is spliced verbatim and gets a unique positional alias: a bare column
/// and a CAST of it derive equal names, which DataFusion rejects as duplicates (#136, #178).
/// Exasol already de-duplicates bare column references. Emission is positional.
pub(super) fn render_projection<'a>(
    items: &[ProjectionItem],
    column_type: impl Fn(&str) -> Option<&'a DataType>,
) -> String {
    items
        .iter()
        .enumerate()
        .map(|(i, item)| match item {
            ProjectionItem::Expr { expr } => {
                format!("{expr} AS {}", quote_ident(&format!("_LH_PROJ_{i}")))
            }
            ProjectionItem::Column(name) => render_column(name, column_type(name)),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Nested columns call [`NESTED_JSON_RENDER_UDF_NAME`]; other incompatible columns get
/// `CAST(col AS VARCHAR)` so they arrive as Utf8. A binary alias already holds its hex text.
pub(super) fn render_column(name: &str, data_type: Option<&DataType>) -> String {
    let ident = quote_ident(&name.to_uppercase());
    match data_type {
        Some(dt) if is_binary(dt) => ident,
        Some(dt) if needs_nested_json_rendering(dt) => {
            format!("{NESTED_JSON_RENDER_UDF_NAME}({ident})")
        }
        Some(dt) if needs_json_fallback(dt) => format!("CAST({ident} AS VARCHAR)"),
        _ => ident,
    }
}

pub(super) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
#[path = "sql_support_tests.rs"]
mod tests;
