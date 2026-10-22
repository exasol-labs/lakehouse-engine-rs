//! The schema-level `SKIPPED_TABLES` and `SKIPPED_TABLES_OMITTED` adapterNotes entries: their
//! encoding, their cap to Exasol's adapterNotes limit, and the read-back of a persisted list. No
//! other module names their keys, so the writer and the reader cannot drift apart.

use crate::adapter::tables::catalog_identifier_string;
use lakehouse_catalog::{CatalogTableIdent, SkipReason, SkippedTable};
use serde_json::{Map, Value as Json, json};
use std::collections::HashMap;

const NOTE_SKIPPED_TABLES: &str = "SKIPPED_TABLES";
const NOTE_SKIPPED_TABLES_OMITTED: &str = "SKIPPED_TABLES_OMITTED";
/// Exasol rejects a longer adapterNotes with sqlCode 04000 on CREATE and REFRESH (measured live).
const ADAPTER_NOTES_MAX_BYTES: usize = 2_000_000;

/// One recorded skip: the skipped table's catalog identifier and the reason a user reads.
#[derive(Debug, Clone, PartialEq)]
pub struct SkipEntry {
    pub table: String,
    pub reason: String,
}

impl SkipEntry {
    fn to_json(&self) -> Json {
        json!({ "table": self.table, "reason": self.reason })
    }

    fn from_json(entry: &Json) -> Option<Self> {
        Some(Self {
            table: entry.get("table")?.as_str()?.to_string(),
            reason: entry.get("reason")?.as_str()?.to_string(),
        })
    }
}

impl From<&SkippedTable> for SkipEntry {
    fn from(skipped: &SkippedTable) -> Self {
        Self {
            table: catalog_identifier_string(&skipped.ident),
            reason: skip_kind_and_reason(skipped).1,
        }
    }
}

/// The skipped-table list a response records, before the cap: its entries in listing order, and
/// the count of entries an earlier response already dropped, which the cap adds its own drops to.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SkippedTables {
    pub entries: Vec<SkipEntry>,
    pub omitted: usize,
}

impl SkippedTables {
    /// Looks up the entry recorded for a catalog identifier, so callers need not know how an
    /// entry keys its table.
    pub fn entry_lookup<'a>(&'a self) -> impl Fn(&CatalogTableIdent) -> Option<&'a SkipEntry> + 'a {
        let by_table: HashMap<&str, &SkipEntry> = self
            .entries
            .iter()
            .map(|entry| (entry.table.as_str(), entry))
            .collect();
        move |ident| {
            by_table
                .get(catalog_identifier_string(ident).as_str())
                .copied()
        }
    }

    /// A full listing's skips. It carries no earlier omission, so it replaces a stale count.
    pub fn listed(skipped: &[SkippedTable]) -> Self {
        Self {
            entries: skipped.iter().map(SkipEntry::from).collect(),
            omitted: 0,
        }
    }

    /// The list and count a previous response recorded in `notes`. The list is diagnostic only,
    /// so a malformed entry or count reads as absent instead of failing the request.
    pub fn persisted(notes: &Map<String, Json>) -> Self {
        let entries = notes
            .get(NOTE_SKIPPED_TABLES)
            .and_then(Json::as_array)
            .map(|entries| entries.iter().filter_map(SkipEntry::from_json).collect())
            .unwrap_or_default();
        let omitted = notes
            .get(NOTE_SKIPPED_TABLES_OMITTED)
            .and_then(Json::as_str)
            .and_then(|count| count.parse().ok())
            .unwrap_or(0);
        Self { entries, omitted }
    }
}

/// The warning's noun for the skipped entry, and the reason both the warning and the notes record.
pub fn skip_kind_and_reason(entry: &SkippedTable) -> (&'static str, String) {
    match &entry.reason {
        SkipReason::NotLoadableIcebergTable => (
            "non-Iceberg table",
            "catalog reported it is not a loadable Iceberg table".to_string(),
        ),
        SkipReason::NotDeltaBaseTable { detail } => ("non-Delta-base entry", detail.clone()),
        SkipReason::NoDataFile => ("directory", "holds no data file".to_string()),
        SkipReason::NotPlannableGlueTable { detail } => ("Glue table", detail.clone()),
    }
}

/// Records `skipped` in `notes`, capped to the longest listing-order prefix that keeps the whole
/// notes within Exasol's limit. `SKIPPED_TABLES_OMITTED` holds the carried count plus the entries
/// the cap drops, and is removed when that sum is 0.
pub fn insert_skipped_tables(notes: &mut Map<String, Json>, skipped: &SkippedTables) {
    let entries: Vec<Json> = skipped.entries.iter().map(SkipEntry::to_json).collect();
    notes.insert(NOTE_SKIPPED_TABLES.to_string(), Json::Array(Vec::new()));
    notes.remove(NOTE_SKIPPED_TABLES_OMITTED);
    let base_len = serde_json::to_string(&*notes).map_or(0, |text| text.len());
    let (kept, omitted) = fit_skipped_tables(entries, base_len, skipped.omitted);
    notes.insert(NOTE_SKIPPED_TABLES.to_string(), Json::Array(kept));
    if omitted > 0 {
        notes.insert(
            NOTE_SKIPPED_TABLES_OMITTED.to_string(),
            Json::String(omitted.to_string()),
        );
    }
}

/// The longest listing-order prefix of `entries` within [`ADAPTER_NOTES_MAX_BYTES`], and the
/// omitted count it records, `carried` included; `base_len` is the notes length with an empty list.
fn fit_skipped_tables(
    mut entries: Vec<Json>,
    base_len: usize,
    carried: usize,
) -> (Vec<Json>, usize) {
    let total = entries.len();
    let omitted_after = |kept: usize| carried.saturating_add(total - kept);
    let notes_len =
        |kept: usize, kept_len: usize| base_len + kept_len + omitted_entry_len(omitted_after(kept));
    let (mut kept, mut kept_len) = (0, 0);
    for entry in &entries {
        let next_len = kept_len + usize::from(kept > 0) + entry.to_string().len();
        if notes_len(kept + 1, next_len) > ADAPTER_NOTES_MAX_BYTES {
            break;
        }
        (kept, kept_len) = (kept + 1, next_len);
    }
    entries.truncate(kept);
    (entries, omitted_after(kept))
}

/// Serialized length of the `SKIPPED_TABLES_OMITTED` entry, leading comma included.
fn omitted_entry_len(omitted: usize) -> usize {
    if omitted == 0 {
        return 0;
    }
    let entry = json!({ NOTE_SKIPPED_TABLES_OMITTED: omitted.to_string() }).to_string();
    entry.len() - "{}".len() + ",".len()
}

#[cfg(test)]
#[path = "skipped_tables_tests.rs"]
mod tests;
