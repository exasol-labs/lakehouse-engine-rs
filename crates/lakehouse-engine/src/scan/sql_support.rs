/// Uppercase aliases let identifiers resolved in uppercase match any Parquet field casing.
/// Public so `tests/scan_agg_projection_pruning.rs` builds the same inner SELECT as production.
pub fn build_alias_items(schema: &datafusion::common::DFSchema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let arrow_name = f.name();
            format!(
                "{} AS {}",
                quote_ident(arrow_name),
                quote_ident(&arrow_name.to_uppercase())
            )
        })
        .collect()
}

pub(super) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
