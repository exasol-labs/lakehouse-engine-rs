//! The rules of an existing table the export appends to, checked when a writer opens the table
//! and again on every reload before a commit.

use iceberg::spec::{FormatVersion, PartitionSpec, Schema, TableMetadata, Transform};

/// Why the export cannot append to a table.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// Format version 3 or later, whose row lineage and encryption keys the export does not
    /// maintain.
    FormatVersion(u8),
    /// The partition fields named like a column without being its identity, each as
    /// `name (transform)`, which a commit cannot land on.
    FieldsNamedLikeColumns(Vec<String>),
}

/// Refuses a table the export cannot append to.
pub fn refuse_unappendable(metadata: &TableMetadata) -> Result<(), Refusal> {
    refusal_of(
        metadata.format_version(),
        metadata.default_partition_spec(),
        metadata.current_schema(),
    )
}

fn refusal_of(
    version: FormatVersion,
    spec: &PartitionSpec,
    schema: &Schema,
) -> Result<(), Refusal> {
    if version > FormatVersion::V2 {
        return Err(Refusal::FormatVersion(version as u8));
    }
    // iceberg-rust refuses such a field unless it is identity or void, and Lakekeeper refuses the
    // void one Spark leaves in version 1 with a 409 that the commit loop retries for minutes.
    let named_like_columns: Vec<String> = spec
        .fields()
        .iter()
        .filter(|field| {
            schema.field_by_name(&field.name).is_some_and(|column| {
                field.transform != Transform::Identity || column.id != field.source_id
            })
        })
        .map(|field| format!("{} ({})", field.name, field.transform))
        .collect();
    if !named_like_columns.is_empty() {
        return Err(Refusal::FieldsNamedLikeColumns(named_like_columns));
    }
    Ok(())
}

#[cfg(test)]
#[path = "unappendable_tests.rs"]
mod tests;
