//! The Iceberg sink: Arrow batches become Parquet data files under their partition's path, and one
//! commit tagged with the writer adds them to the table.

use super::batch::TableColumns;
use super::fanout::{Partition, PartitionBuffer};
use super::target::ExportSink;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFile, DataFileFormat, PartitionKey, Schema};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
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

/// The table a writer's files go into: one the catalog lists, created or loaded, or one staged for
/// `SINGLE_COMMIT`.
pub(super) enum OpenedTable {
    Listed(WriteTable),
    Staged(StagedTable),
}

impl OpenedTable {
    fn table(&self) -> &Table {
        match self {
            Self::Listed(listed) => listed.table(),
            Self::Staged(staged) => staged.table(),
        }
    }

    fn secret_values(&self) -> Vec<String> {
        match self {
            Self::Listed(listed) => listed.secret_values(),
            Self::Staged(staged) => staged.secret_values(),
        }
    }
}

type DataWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, RecordedLocations, DefaultFileNameGenerator>;

pub(super) struct IcebergSink {
    session: TableWriteSession,
    opened: OpenedTable,
    tag: CommitTag,
    columns: TableColumns,
    buffer: PartitionBuffer,
    writers: DataWriterBuilder,
    /// The data files written so far, which the commit adds.
    files: Vec<DataFile>,
    locations: RecordedLocations,
    /// Set after a commit whose outcome is unknown, when the table may hold the files.
    files_may_be_committed: bool,
}

impl IcebergSink {
    /// The table's current schema carries its field IDs, which the Parquet writer matches columns
    /// by, and every data file carries the table's default partition spec. The rows buffered per
    /// partition stay within `budget` bytes of Arrow memory.
    pub(super) async fn open(
        session: TableWriteSession,
        opened: OpenedTable,
        tag: CommitTag,
        budget: usize,
    ) -> Result<Self, UdfError> {
        let table = opened.table();
        let unwritable = |error: iceberg::Error| {
            UdfError::User(format!(
                "could not prepare the data files of table {}: {}",
                table.identifier(),
                error_chain(&error)
            ))
        };
        let metadata = table.metadata();
        let iceberg_schema = metadata.current_schema().clone();
        let columns = table_columns(table.identifier(), &iceberg_schema).map_err(unwritable)?;
        let buffer = PartitionBuffer::new(
            iceberg_schema.clone(),
            metadata.default_partition_spec(),
            budget,
        )
        .map_err(unwritable)?;
        let locations = RecordedLocations {
            inner: DefaultLocationGenerator::new(table.metadata()).map_err(unwritable)?,
            recorded: Arc::default(),
        };
        let names = DefaultFileNameGenerator::new(
            format!("{}-{}", tag.export_id, tag.writer_id),
            None,
            DataFileFormat::Parquet,
        );
        // A write-out is at most the budget, so it rolls over to a second file only when Parquet
        // encodes the rows larger than Arrow holds them.
        let files = RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema.clone()),
            budget,
            table.file_io().clone(),
            locations.clone(),
            names,
        );
        Ok(Self {
            session,
            opened,
            tag,
            columns,
            buffer,
            writers: DataFileWriterBuilder::new(files),
            files: Vec::new(),
            locations,
            files_may_be_committed: false,
        })
    }

    fn storage_error(&self, error: iceberg::Error) -> UdfError {
        self.redacted(format!(
            "writing the data files of table {} to storage failed: {}",
            self.opened.table().identifier(),
            error_chain(&error)
        ))
    }

    /// Writes each of `partitions` into its own data file, one file open at a time.
    async fn write_out(&mut self, partitions: Vec<Partition>) -> Result<(), UdfError> {
        for (key, batches) in partitions {
            let written = write_partition(&self.writers, key, batches).await;
            self.files
                .extend(written.map_err(|error| self.storage_error(error))?);
        }
        Ok(())
    }

    /// `message` without any storage credential the sink's table signs with.
    fn redacted(&self, message: String) -> UdfError {
        let secrets = self.opened.secret_values();
        let secrets: Vec<&str> = secrets.iter().map(String::as_str).collect();
        UdfError::User(redact_error_text(&message, &secrets))
    }

    /// Appends to the table reloaded with every writer's commits so far, under the schema and
    /// partition spec the files were written with, or creates the staged table with the files.
    async fn commit(&mut self, files: Vec<DataFile>) -> Result<(), UdfError> {
        let committed = match &self.opened {
            OpenedTable::Listed(listed) => {
                self.session.append_files(listed, files, &self.tag).await
            }
            OpenedTable::Staged(staged) => {
                self.session.commit_staged(staged, files, &self.tag).await
            }
        };
        let Err(error) = committed else {
            return Ok(());
        };
        self.files_may_be_committed = error.outcome_unknown();
        Err(self.redacted(error.to_string()))
    }
}

#[async_trait(?Send)]
impl ExportSink for IcebergSink {
    fn columns(&self) -> &TableColumns {
        &self.columns
    }

    async fn write(&mut self, batch: &RecordBatch) -> Result<(), UdfError> {
        let full = self.buffer.push(batch).map_err(|error| {
            UdfError::User(format!(
                "could not compute the partition values of table {}: {}",
                self.opened.table().identifier(),
                error_chain(&error)
            ))
        })?;
        self.write_out(full).await
    }

    async fn finish(&mut self) -> Result<usize, UdfError> {
        let rest = self.buffer.drain();
        self.write_out(rest).await?;
        let files = std::mem::take(&mut self.files);
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

/// The columns of `schema` as the batch builder binds the source to them, with each column's
/// Iceberg type name, such as `long` or `decimal(20, 2)`, for a mismatch error.
pub(super) fn table_columns(table: &TableIdent, schema: &Schema) -> iceberg::Result<TableColumns> {
    Ok(TableColumns {
        table: table.to_string(),
        schema: Arc::new(schema_to_arrow_schema(schema)?),
        type_names: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.field_type.to_string())
            .collect(),
    })
}

/// One data file of `key`'s rows, or several when Parquet encodes them past the rolling size.
async fn write_partition(
    writers: &DataWriterBuilder,
    key: PartitionKey,
    batches: Vec<RecordBatch>,
) -> iceberg::Result<Vec<DataFile>> {
    let mut writer = writers.build(Some(key)).await?;
    for batch in batches {
        writer.write(batch).await?;
    }
    writer.close().await
}

/// The table's default data file locations under each file's partition path, each recorded so
/// that `abort` can delete it.
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
    fn generate_location(&self, partition: Option<&PartitionKey>, file_name: &str) -> String {
        let location = self.inner.generate_location(partition, file_name);
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
