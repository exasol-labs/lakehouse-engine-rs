//! A writer's snapshot as iceberg-rust builds it, captured instead of committed. iceberg-rust
//! writes the manifests through the table's FileIO and hands the commit's requirements and
//! updates to a private `Catalog` that only records them, so the commit loop sends them itself:
//! iceberg-rust's `Transaction::commit` would re-apply blindly after HTTP 409, and its
//! `TableCommit` builder is crate-private.

use async_trait::async_trait;
use iceberg::spec::{DataFile, TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    TableRequirement, TableUpdate,
};
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

/// The requirements and updates of one commit, as iceberg-rust built them.
#[derive(Debug)]
pub(crate) struct CapturedCommit {
    pub(crate) requirements: Vec<TableRequirement>,
    pub(crate) updates: Vec<TableUpdate>,
}

/// The commit of one fast append of `files` onto `table`, whose snapshot summary carries
/// `summary`. The manifests are written through `table`'s FileIO; nothing reaches a catalog.
pub(crate) async fn capture_fast_append(
    table: &Table,
    files: Vec<DataFile>,
    summary: HashMap<String, String>,
) -> iceberg::Result<CapturedCommit> {
    let capture = CommitCapture {
        table: table.clone(),
        recorded: Mutex::new(None),
    };
    let transaction = Transaction::new(table);
    // Each writer's files carry its own tag, and the check re-reads every manifest per attempt:
    // 300 writers on the Docker Exasol took 12 to 18 s without it and 35 to 60 s with it.
    let append = transaction
        .fast_append()
        .with_check_duplicate(false)
        .set_snapshot_properties(summary)
        .add_data_files(files);
    append.apply(transaction)?.commit(&capture).await?;
    capture
        .recorded
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner)
        .ok_or_else(|| Error::new(ErrorKind::Unexpected, "the fast append built no commit"))
}

/// The updates that create `staged`'s metadata from nothing, as `TableMetadataBuilder` derives
/// them from a table creation, preceded by the staged table's UUID and format version (the order
/// Iceberg Java's create transaction sends). A commit of these with `assert-create` creates the
/// staged table.
pub(crate) fn creation_updates(staged: &TableMetadata) -> iceberg::Result<Vec<TableUpdate>> {
    let creation = TableCreation::builder()
        .name(String::new())
        .location(staged.location().to_string())
        .schema(staged.current_schema().as_ref().clone())
        .partition_spec(
            staged
                .default_partition_spec()
                .as_ref()
                .clone()
                .into_unbound(),
        )
        .sort_order(staged.default_sort_order().as_ref().clone())
        .properties(staged.properties().clone())
        .format_version(staged.format_version())
        .build();
    let created = TableMetadataBuilder::from_table_creation(creation)?.build()?;
    let mut updates = vec![
        TableUpdate::AssignUuid {
            uuid: staged.uuid(),
        },
        TableUpdate::UpgradeFormatVersion {
            format_version: staged.format_version(),
        },
    ];
    updates.extend(created.changes);
    Ok(updates)
}

/// Serves one table to a transaction and records the commit it builds instead of sending it.
#[derive(Debug)]
struct CommitCapture {
    table: Table,
    recorded: Mutex<Option<CapturedCommit>>,
}

fn unsupported() -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        "the commit capture only serves its table and records its commit",
    )
}

#[async_trait]
impl Catalog for CommitCapture {
    async fn load_table(&self, _: &TableIdent) -> iceberg::Result<Table> {
        Ok(self.table.clone())
    }

    async fn update_table(&self, mut commit: TableCommit) -> iceberg::Result<Table> {
        let captured = CapturedCommit {
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        };
        *self.recorded.lock().unwrap_or_else(PoisonError::into_inner) = Some(captured);
        Ok(self.table.clone())
    }

    async fn list_namespaces(
        &self,
        _: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        Err(unsupported())
    }

    async fn create_namespace(
        &self,
        _: &NamespaceIdent,
        _: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        Err(unsupported())
    }

    async fn get_namespace(&self, _: &NamespaceIdent) -> iceberg::Result<Namespace> {
        Err(unsupported())
    }

    async fn namespace_exists(&self, _: &NamespaceIdent) -> iceberg::Result<bool> {
        Err(unsupported())
    }

    async fn update_namespace(
        &self,
        _: &NamespaceIdent,
        _: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        Err(unsupported())
    }

    async fn drop_namespace(&self, _: &NamespaceIdent) -> iceberg::Result<()> {
        Err(unsupported())
    }

    async fn list_tables(&self, _: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        Err(unsupported())
    }

    async fn create_table(&self, _: &NamespaceIdent, _: TableCreation) -> iceberg::Result<Table> {
        Err(unsupported())
    }

    async fn drop_table(&self, _: &TableIdent) -> iceberg::Result<()> {
        Err(unsupported())
    }

    async fn purge_table(&self, _: &TableIdent) -> iceberg::Result<()> {
        Err(unsupported())
    }

    async fn table_exists(&self, _: &TableIdent) -> iceberg::Result<bool> {
        Err(unsupported())
    }

    async fn rename_table(&self, _: &TableIdent, _: &TableIdent) -> iceberg::Result<()> {
        Err(unsupported())
    }

    async fn register_table(&self, _: &TableIdent, _: String) -> iceberg::Result<Table> {
        Err(unsupported())
    }
}

#[cfg(test)]
#[path = "snapshot_capture_tests.rs"]
mod tests;
