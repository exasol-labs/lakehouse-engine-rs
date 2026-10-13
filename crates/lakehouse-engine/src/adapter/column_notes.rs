//! The one owner of a virtual column's `adapterNotes`: the note's encoding, its size limit, and
//! the parse of a table's notes into the declared schema a pushdown plans from. CREATE, REFRESH,
//! and SET write notes and every pushdown reads them, so no other module knows the format.

use exasol_udf_sdk::error::UdfError;
use serde::{Deserialize, Serialize};

use crate::scan::spec::LogicalField;

/// Exasol persists at most this many characters of one column's `adapterNotes` and rejects the
/// statement above it (sqlCode 04000). The limit counts characters, not bytes.
pub const MAX_COLUMN_NOTE_CHARS: usize = 2_000_000;

/// Never a `ScanSpec` field: only the pre-scan pushdown-resolution gate reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedColumn {
    pub column_name: String,
    pub reason: String,
}

/// One column's declared source shape, exactly as it is persisted in the column's note.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnDeclaration {
    pub source_type: LogicalField,
    /// 0-based position among the table's ordered partition columns; absent on any other column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<usize>,
    /// The reason a pushdown refuses the column; the column stays declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
}

impl ColumnDeclaration {
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("a column declaration always serializes")
    }
}

/// Passes `note` through unchanged when Exasol will persist it, so a pushdown never plans from a
/// truncated declaration.
pub fn checked_note(table: &str, column: &str, note: String) -> Result<String, UdfError> {
    let length = note.chars().count();
    if length > MAX_COLUMN_NOTE_CHARS {
        return Err(UdfError::User(format!(
            "the declaration note of column '{column}' in table '{table}' is {length} characters \
             long, over the {MAX_COLUMN_NOTE_CHARS} characters Exasol persists for one column"
        )));
    }
    Ok(note)
}

/// What a pushdown plans from, in the column order of the involved table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredSchema {
    /// The virtual table the notes belong to; every check of the declaration names it.
    pub table: String,
    pub logical_schema: Vec<LogicalField>,
    /// Ordered by each column's recorded position.
    pub partition_columns: Vec<String>,
    pub refused_columns: Vec<RefusedColumn>,
}

/// One involved table's columns and each raw note, `None` where Exasol sent none. Borrowed from
/// the request, so a note of up to [`MAX_COLUMN_NOTE_CHARS`] characters is never copied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableNotes<'r> {
    pub table: &'r str,
    pub columns: Vec<(&'r str, Option<&'r str>)>,
}

impl TableNotes<'_> {
    /// The one parse every format plans from. A column without a readable note fails the call: no
    /// fallback derives its shape from the source.
    pub fn declared_schema(&self) -> Result<DeclaredSchema, UdfError> {
        let mut logical_schema = Vec::with_capacity(self.columns.len());
        let mut refused_columns = Vec::new();
        let mut partitions: Vec<(usize, String)> = Vec::new();
        for &(column, note) in &self.columns {
            let declaration: ColumnDeclaration = match note {
                None => return Err(self.unreadable(column, "the column carries no note")),
                Some(note) => serde_json::from_str(note)
                    .map_err(|error| self.unreadable(column, &error.to_string()))?,
            };
            let field = declaration.source_type;
            if let Some(position) = declaration.partition {
                partitions.push((position, field.name.clone()));
            }
            match declaration.refused {
                Some(reason) => refused_columns.push(RefusedColumn {
                    column_name: field.name,
                    reason,
                }),
                None => logical_schema.push(field),
            }
        }
        partitions.sort_by_key(|(position, _)| *position);
        let declared = DeclaredSchema {
            table: self.table.to_string(),
            logical_schema,
            partition_columns: partitions.into_iter().map(|(_, name)| name).collect(),
            refused_columns,
        };
        declared.ensure_scannable()?;
        Ok(declared)
    }

    fn unreadable(&self, column: &str, cause: &str) -> UdfError {
        UdfError::User(format!(
            "the declaration note of column '{column}' in virtual table '{}' is missing or \
             unreadable; ALTER VIRTUAL SCHEMA ... REFRESH rebuilds it ({cause})",
            self.table
        ))
    }
}

impl DeclaredSchema {
    /// `raw_scan` infers a schema from the first data file when given none, and materializes a
    /// partition column only from the logical schema.
    fn ensure_scannable(&self) -> Result<(), UdfError> {
        let table = &self.table;
        if self.logical_schema.is_empty() && !self.refused_columns.is_empty() {
            let reasons = self
                .refused_columns
                .iter()
                .map(|column| format!("'{}': {}", column.column_name, column.reason))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(UdfError::User(format!(
                "virtual table '{table}' has no mappable column; every column is refused: \
                 {reasons}"
            )));
        }
        let partition =
            |column: &&RefusedColumn| self.partition_columns.contains(&column.column_name);
        match self.refused_columns.iter().find(partition) {
            None => Ok(()),
            Some(refused) => Err(UdfError::User(format!(
                "virtual table '{table}' declares '{}' as a partition column, but that column is \
                 refused ({}); the scan cannot materialize a partition column absent from the \
                 logical schema",
                refused.column_name, refused.reason
            ))),
        }
    }
}

/// Shared by every source, so each binary refusal reads alike
/// (`vs-adapter/binary-column-refusal`).
pub fn binary_cause(declared: &str) -> String {
    format!(
        "has type '{declared}', which this engine refuses: rendering binary data is tracked as \
         issue #351"
    )
}

/// `member_path` is `None` when the column's own type is the binary one.
pub fn binary_refusal(
    label: &str,
    column: &str,
    member_path: Option<&str>,
    declared: &str,
) -> String {
    let subject = match member_path {
        Some(member_path) => format!("{label} column '{column}', whose member '{member_path}'"),
        None => format!("{label} column '{column}'"),
    };
    format!("{subject} {}", binary_cause(declared))
}

#[cfg(test)]
#[path = "column_notes_tests.rs"]
mod tests;
