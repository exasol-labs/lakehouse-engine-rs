//! Adapter fixtures shared across the adapter's own test files: virtual schema property maps and
//! skipped-table entries.

use lakehouse_catalog::{CatalogTableIdent, SkipReason, SkippedTable};
use serde_json::{Value as Json, json};

pub(crate) fn props(pairs: &[(&str, &str)]) -> Json {
    Json::Object(
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), json!(value)))
            .collect(),
    )
}

pub(crate) fn skipped_entry(table: &str, reason: SkipReason) -> SkippedTable {
    SkippedTable {
        ident: CatalogTableIdent {
            namespace: vec!["sales".to_string()],
            name: table.to_string(),
        },
        reason,
    }
}
