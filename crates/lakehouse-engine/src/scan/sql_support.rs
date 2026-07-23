//! Generic SQL-identifier helpers shared across the raw, join, and
//! partial-aggregate scan paths.

/// Build `"col" AS "COL"` alias items for all fields in `schema`.
///
/// Used to wrap a listing table in an inner SELECT that exposes uppercase column
/// names, so projection/filter expressions resolved against uppercase identifiers
/// find a match regardless of the Parquet field casing.
///
/// Public so the aggregate physical-projection-pruning integration test
/// (`tests/scan_agg_projection_pruning.rs`) builds the SAME aliased inner SELECT
/// the production aggregate path uses, rather than a hand-simplified stand-in —
/// alongside the [`build_partial_agg_sql_filtered`]/[`build_grouped_partial_agg_sql`]
/// builders it already exposes.
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

/// Double-quote an identifier (SQL-safe column name).
pub(super) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
