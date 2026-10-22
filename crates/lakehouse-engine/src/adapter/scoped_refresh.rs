//! The partial refresh that `ALTER VIRTUAL SCHEMA ... REFRESH TABLES` sends: which Exasol names it
//! requests, which listed identifiers it loads, and how it merges the skipped-table list. Exasol
//! keeps every unrequested virtual table, so the response carries only the requested tables.

use crate::adapter::skipped_tables::{SkipEntry, SkippedTables};
use crate::adapter::tables::flatten_table_name;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{CatalogTableIdent, SkippedTable};
use serde_json::Value as Json;
use std::collections::HashSet;

/// The Exasol table names a partial refresh requests, as Exasol sends them: an unquoted name
/// arrives uppercased, so a name matches only the exact flattened Exasol name.
#[derive(Debug)]
pub struct RequestedTables {
    names: HashSet<String>,
}

impl RequestedTables {
    /// `None` when the request refreshes everything: any request type but `refresh`, and a
    /// `refresh` that carries no `requestedTables`.
    pub fn from_request(request: &Json) -> Result<Option<Self>, UdfError> {
        if request.get("type").and_then(Json::as_str) != Some("refresh") {
            return Ok(None);
        }
        let Some(value) = request.get("requestedTables") else {
            return Ok(None);
        };
        let names = value
            .as_array()
            .and_then(|names| {
                names
                    .iter()
                    .map(|name| name.as_str().map(str::to_string))
                    .collect::<Option<HashSet<String>>>()
            })
            .ok_or_else(|| {
                UdfError::User(format!(
                    "refresh: requestedTables must be an array of table names, but is {value}"
                ))
            })?;
        Ok(Some(Self { names }))
    }

    /// The only test of whether a virtual table is requested.
    pub fn contains(&self, exasol_name: &str) -> bool {
        self.names.contains(exasol_name)
    }
}

/// A partial refresh over one identifier listing of the source. A listed identifier is requested
/// when its flattened Exasol name is, so a requested name resolves exactly as a full refresh
/// resolves it, including to several identifiers that differ only in case or `__` placement.
pub struct ScopedRefresh<'a> {
    requested: &'a RequestedTables,
    configured_ns: &'a [String],
    listed: &'a [CatalogTableIdent],
}

impl<'a> ScopedRefresh<'a> {
    pub fn new(
        requested: &'a RequestedTables,
        configured_ns: &'a [String],
        listed: &'a [CatalogTableIdent],
    ) -> Self {
        Self {
            requested,
            configured_ns,
            listed,
        }
    }

    /// Every listed identifier the refresh loads, in listing order. No other table is loaded, so
    /// an unrequested table that fails to load cannot fail the refresh.
    pub fn selection(&self) -> Vec<CatalogTableIdent> {
        self.listed
            .iter()
            .filter(|ident| self.is_requested(ident))
            .cloned()
            .collect()
    }

    /// The skipped-table list in listing order: a requested identifier contributes the entry its
    /// load gave it, and an unrequested one its persisted entry. A persisted entry whose table the
    /// listing no longer returns is dropped, and the persisted omitted count is carried forward.
    pub fn merge_skipped_tables(
        &self,
        loaded: &[SkippedTable],
        persisted: &SkippedTables,
    ) -> SkippedTables {
        let persisted_entry = persisted.entry_lookup();
        let entries = self
            .listed
            .iter()
            .filter_map(|ident| {
                if self.is_requested(ident) {
                    loaded
                        .iter()
                        .find(|skipped| skipped.ident == *ident)
                        .map(SkipEntry::from)
                } else {
                    persisted_entry(ident).cloned()
                }
            })
            .collect();
        SkippedTables {
            entries,
            omitted: persisted.omitted,
        }
    }

    fn is_requested(&self, ident: &CatalogTableIdent) -> bool {
        self.requested
            .contains(&flatten_table_name(self.configured_ns, ident))
    }
}

#[cfg(test)]
#[path = "scoped_refresh_tests.rs"]
mod tests;
