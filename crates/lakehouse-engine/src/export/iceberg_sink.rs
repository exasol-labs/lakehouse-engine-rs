//! The Iceberg sink: Arrow batches become Parquet data files under the table location, and one
//! commit tagged with the writer adds them to the table.

use super::target::ExportSink;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use exasol_udf_sdk::error::UdfError;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFile, DataFileFormat, PartitionKey};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use lakehouse_catalog::{error_chain, redact_error_text};
use lakehouse_catalog_write::{CommitTag, StagedTable, TableWriteSession, WriteTable};
use parquet::file::properties::WriterProperties;
use std::sync::{Arc, Mutex, PoisonError};

const TARGET_FILE_SIZE: usize = 128 * 1024 * 1024;

/// The table a writer's files go into: one the catalog holds, or one staged for `SINGLE_COMMIT`.
pub(super) enum OpenedTable {
    Created(WriteTable),
    Staged(StagedTable),
}

impl OpenedTable {
    fn table(&self) -> &Table {
        match self {
            Self::Created(created) => created.table(),
            Self::Staged(staged) => staged.table(),
        }
    }

    fn secret_values(&self) -> Vec<String> {
        match self {
            Self::Created(created) => created.secret_values(),
            Self::Staged(staged) => staged.secret_values(),
        }
    }
}

type DataWriter = DataFileWriter<ParquetWriterBuilder, RecordedLocations, DefaultFileNameGenerator>;

pub(super) struct IcebergSink {
    session: TableWriteSession,
    opened: OpenedTable,
    tag: CommitTag,
    schema: SchemaRef,
    writer: DataWriter,
    locations: RecordedLocations,
    /// Set after a commit whose outcome is unknown, when the table may hold the files.
    files_may_be_committed: bool,
}

impl IcebergSink {
    /// The table's schema carries its field IDs, which the Parquet writer matches columns by.
    pub(super) async fn open(
        session: TableWriteSession,
        opened: OpenedTable,
        tag: CommitTag,
    ) -> Result<Self, UdfError> {
        let table = opened.table();
        let unwritable = |error: iceberg::Error| {
            UdfError::User(format!(
                "could not prepare the data files of table {}: {}",
                table.identifier(),
                error_chain(&error)
            ))
        };
        let iceberg_schema = table.metadata().current_schema().clone();
        let schema = Arc::new(schema_to_arrow_schema(&iceberg_schema).map_err(unwritable)?);
        let locations = RecordedLocations {
            inner: DefaultLocationGenerator::new(table.metadata()).map_err(unwritable)?,
            recorded: Arc::default(),
        };
        let names = DefaultFileNameGenerator::new(
            format!("{}-{}", tag.export_id, tag.writer_id),
            None,
            DataFileFormat::Parquet,
        );
        let files = RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema),
            TARGET_FILE_SIZE,
            table.file_io().clone(),
            locations.clone(),
            names,
        );
        let writer = DataFileWriterBuilder::new(files)
            .build(None)
            .await
            .map_err(unwritable)?;
        Ok(Self {
            session,
            opened,
            tag,
            schema,
            writer,
            locations,
            files_may_be_committed: false,
        })
    }

    fn storage_error(&self, error: iceberg::Error) -> UdfError {
        self.redacted(
            format!(
                "writing the data files of table {} to storage failed: {}",
                self.opened.table().identifier(),
                error_chain(&error)
            ),
            Vec::new(),
        )
    }

    /// `message` without any storage credential the sink's tables have signed with.
    fn redacted(&self, message: String, mut secrets: Vec<String>) -> UdfError {
        secrets.extend(self.opened.secret_values());
        let secrets: Vec<&str> = secrets.iter().map(String::as_str).collect();
        UdfError::User(redact_error_text(&message, &secrets))
    }

    /// Appends to the table reloaded with every writer's commits so far, or creates the staged
    /// table with the files.
    async fn commit(&mut self, files: Vec<DataFile>) -> Result<(), UdfError> {
        let (committed, reloaded_secrets) = match &self.opened {
            OpenedTable::Created(created) => {
                let table = self
                    .session
                    .load_table(created.table().identifier())
                    .await?;
                let committed = self.session.append_files(&table, files, &self.tag).await;
                (committed, table.secret_values())
            }
            OpenedTable::Staged(staged) => {
                let committed = self.session.commit_staged(staged, files, &self.tag).await;
                (committed, Vec::new())
            }
        };
        match committed {
            Ok(()) => Ok(()),
            Err(error) => {
                self.files_may_be_committed = error.outcome_unknown();
                Err(self.redacted(error.to_string(), reloaded_secrets))
            }
        }
    }
}

#[async_trait(?Send)]
impl ExportSink for IcebergSink {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn write(&mut self, batch: &RecordBatch) -> Result<(), UdfError> {
        let written = self.writer.write(batch.clone()).await;
        written.map_err(|error| self.storage_error(error))
    }

    async fn finish(&mut self) -> Result<usize, UdfError> {
        let closed = self.writer.close().await;
        let files = closed.map_err(|error| self.storage_error(error))?;
        let count = files.len();
        self.commit(files).await?;
        Ok(count)
    }

    async fn abort(&mut self) {
        if self.files_may_be_committed {
            return;
        }
        let file_io = self.opened.table().file_io();
        for location in self.locations.recorded() {
            // Best effort: a file left behind lies under the table location, or under the
            // location of a staged table that never appeared.
            let _ = file_io.delete(&location).await;
        }
    }
}

/// The table's default data file locations, each recorded so that `abort` can delete it.
#[derive(Clone)]
struct RecordedLocations {
    inner: DefaultLocationGenerator,
    recorded: Arc<Mutex<Vec<String>>>,
}

impl RecordedLocations {
    fn recorded(&self) -> Vec<String> {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl LocationGenerator for RecordedLocations {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        let location = self.inner.generate_location(partition_key, file_name);
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(location.clone());
        location
    }
}

#[cfg(test)]
#[path = "iceberg_sink_tests.rs"]
mod tests;
