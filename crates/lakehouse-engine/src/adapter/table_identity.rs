//! A virtual table's identifier note: the table-level `adapterNotes` that records the table's
//! original-cased catalog identifier under `name`. The listing writes it and both pushdown paths
//! read it, so the key and the failure text live here and nowhere else.

use crate::adapter::pushdown::AdapterRequest;
use crate::adapter::tables::catalog_identifier_string;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::CatalogTableIdent;
use serde_json::{Value as Json, json};

const NOTE_NAME: &str = "name";

/// The note the listing attaches to the virtual table of `ident`.
pub fn encode_identifier_note(ident: &CatalogTableIdent) -> String {
    json!({ NOTE_NAME: catalog_identifier_string(ident) }).to_string()
}

/// The catalog identifier in the note of the involved table named `table_name`. Any note member
/// other than `name` is ignored. A table without a readable note fails rather than scan a
/// different or stale table, and the error names the refresh that writes the note.
pub(crate) fn read_identifier(
    request: &AdapterRequest,
    table_name: &str,
) -> Result<String, UdfError> {
    request
        .involved_table(table_name)
        .and_then(|table| table.adapter_notes.as_deref())
        .and_then(|note| serde_json::from_str::<Json>(note).ok())
        .and_then(|note| note.get(NOTE_NAME)?.as_str().map(str::to_string))
        .ok_or_else(|| {
            UdfError::User(format!(
                "pushdown: virtual table '{table_name}' has no readable identifier note; \
                 run ALTER VIRTUAL SCHEMA ... REFRESH to write it"
            ))
        })
}

#[cfg(test)]
#[path = "table_identity_tests.rs"]
mod tests;
