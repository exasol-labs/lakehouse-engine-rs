//! The Iceberg sink: Arrow batches become Parquet data files under their partition's path, and one
//! commit tagged with the writer adds them to the table.

use super::batch::TableColumns;
use super::fanout::{Partition, PartitionBuffer, null_key};
use super::target::ExportSink;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::compression::CompressionCodec;
use iceberg::spec::{DataFile, DataFileFormat, PartitionKey, Schema, SchemaRef, TableProperties};
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
use std::collections::HashMap;
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
type DataWriter = <DataWriterBuilder as IcebergWriterBuilder>::R;

/// Where a writer's rows wait before they reach a data file.
enum Rows {
    /// Per partition, within the budget.
    Buffered(Box<PartitionBuffer>),
    /// An unpartitioned table has one partition, so its rows go straight into one rolling file.
    Streamed {
        key: PartitionKey,
        writer: Option<Box<DataWriter>>,
    },
}

pub(super) struct IcebergSink {
    session: TableWriteSession,
    opened: OpenedTable,
    tag: CommitTag,
    columns: TableColumns,
    rows: Rows,
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
    /// partition of a partitioned table stay within `budget` bytes of Arrow memory.
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
        let spec = metadata.default_partition_spec();
        let rows = if spec.is_unpartitioned() {
            Rows::Streamed {
                key: null_key(spec, iceberg_schema.clone()),
                writer: None,
            }
        } else {
            Rows::Buffered(Box::new(
                PartitionBuffer::new(iceberg_schema.clone(), spec, budget).map_err(unwritable)?,
            ))
        };
        let (parquet, target_file_size) =
            data_file_settings(metadata.properties(), iceberg_schema.clone()).map_err(|error| {
                UdfError::User(format!(
                    "could not prepare the data files of table {}: {error}",
                    table.identifier()
                ))
            })?;
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
            parquet,
            target_file_size,
            table.file_io().clone(),
            locations.clone(),
            names,
        );
        Ok(Self {
            session,
            opened,
            tag,
            columns,
            rows,
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
        let buffer = match &mut self.rows {
            Rows::Buffered(buffer) => buffer,
            Rows::Streamed { key, writer } => {
                let written = match writer {
                    Some(open) => open.write(batch.clone()).await,
                    None => match self.writers.build(Some(key.clone())).await {
                        Ok(open) => writer.insert(Box::new(open)).write(batch.clone()).await,
                        Err(error) => Err(error),
                    },
                };
                return written.map_err(|error| self.storage_error(error));
            }
        };
        let full = buffer.push(batch).map_err(|error| {
            UdfError::User(format!(
                "could not compute the partition values of table {}: {}",
                self.opened.table().identifier(),
                error_chain(&error)
            ))
        })?;
        self.write_out(full).await
    }

    async fn finish(&mut self) -> Result<usize, UdfError> {
        match &mut self.rows {
            Rows::Buffered(buffer) => {
                let rest = buffer.drain();
                self.write_out(rest).await?;
            }
            Rows::Streamed { writer, .. } => {
                if let Some(mut open) = writer.take() {
                    let written = open.close().await;
                    self.files
                        .extend(written.map_err(|error| self.storage_error(error))?);
                }
            }
        }
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

/// A key of the table's write properties, and the reading of it that fails on an unusable value.
type WriteProperty = (
    &'static str,
    fn(&TableProperties<'_>) -> iceberg::Result<()>,
);

/// Every property that `ParquetWriterBuilder::from_table_properties` reads, other than the
/// compression level, which the codec's reading parses too, and the target file size.
const WRITE_PROPERTIES: [WriteProperty; 6] = [
    (TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC, |t| {
        t.parquet_compression_codec().map(drop)
    }),
    (
        TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES,
        |t| t.parquet_row_group_size_bytes().map(drop),
    ),
    (TableProperties::PROPERTY_PARQUET_PAGE_SIZE_BYTES, |t| {
        t.parquet_page_size_bytes().map(drop)
    }),
    (TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT, |t| {
        t.parquet_page_row_limit().map(drop)
    }),
    (TableProperties::PROPERTY_PARQUET_DICT_SIZE_BYTES, |t| {
        t.parquet_dict_size_bytes().map(drop)
    }),
    (
        TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES,
        |t| t.write_target_file_size_bytes().map(drop),
    ),
];

/// The content-defined chunking properties, read only when chunking is enabled.
const CHUNKING_PROPERTIES: [WriteProperty; 3] = [
    (TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE, |t| {
        t.cdc_min_chunk_size().map(drop)
    }),
    (TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE, |t| {
        t.cdc_max_chunk_size().map(drop)
    }),
    (TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL, |t| {
        t.cdc_norm_level().map(drop)
    }),
];

/// The Parquet writer and the data file size that a table's write properties set, or an error
/// that names the first unusable property and its value.
pub(super) fn data_file_settings(
    properties: &HashMap<String, String>,
    schema: SchemaRef,
) -> Result<(ParquetWriterBuilder, usize), String> {
    let table = TableProperties::new(properties);
    let named = |key: &str, cause: &dyn std::fmt::Display| {
        let value = properties.get(key).map_or("", String::as_str);
        format!("invalid write property {key} = '{value}': {cause}")
    };
    if let Some((key, cause)) = unreadable(&table).or_else(|| refused(&table)) {
        return Err(named(key, &cause));
    }
    let settings = ParquetWriterBuilder::from_table_properties(&table, schema)
        .and_then(|writer| Ok((writer, table.write_target_file_size_bytes()?)));
    // Every property reads, so only the codec's refusal of its compression level remains.
    settings.map_err(|error| {
        named(
            TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL,
            &error_chain(&error),
        )
    })
}

/// The first property whose value does not read, with iceberg-rust's error.
fn unreadable(table: &TableProperties<'_>) -> Option<(&'static str, String)> {
    let chunking = match table.cdc_enabled() {
        Ok(enabled) => enabled,
        Err(error) => {
            return Some((
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED,
                error_chain(&error),
            ));
        }
    };
    let chunked = if chunking {
        &CHUNKING_PROPERTIES[..]
    } else {
        &[]
    };
    WRITE_PROPERTIES
        .iter()
        .chain(chunked)
        .find_map(|(key, read)| {
            let error = read(table).err()?;
            let cause = error_chain(&error);
            // The codec's reading also parses the compression level.
            let key = if *key == TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC
                && cause.contains(TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL)
            {
                TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL
            } else {
                key
            };
            Some((key, cause))
        })
}

/// A value that iceberg-rust reads but parquet-rs panics or hangs on, or refuses only at the first
/// write.
fn refused(table: &TableProperties<'_>) -> Option<(&'static str, String)> {
    let refusal = |key, cause: &str| Some((key, cause.to_string()));
    if table.parquet_compression_codec().ok() == Some(CompressionCodec::Lzo) {
        return refusal(
            TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC,
            "the export cannot write lzo",
        );
    }
    if table.parquet_row_group_size_bytes().ok() == Some(0) {
        return refusal(
            TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES,
            "a row group needs at least 1 byte",
        );
    }
    // parquet-rs writes a column in steps of this many rows, so 0 never advances.
    if table.parquet_page_row_limit().ok() == Some(0) {
        return refusal(
            TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT,
            "a page needs at least 1 row",
        );
    }
    if table.cdc_enabled().ok() != Some(true) {
        return None;
    }
    let min = table.cdc_min_chunk_size().ok()?;
    let max = table.cdc_max_chunk_size().ok()?;
    let norm = table.cdc_norm_level().ok()?;
    if min == 0 {
        return refusal(
            TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE,
            "the minimum chunk size must be at least 1",
        );
    }
    if max <= min {
        return refusal(
            TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE,
            "the maximum chunk size must exceed the minimum",
        );
    }
    // parquet-rs's chunking mask: floor(log2(((min + max) / 2 - min) / 8)) - norm, within 1..=63.
    // `(max - min) / 2` equals `(min + max) / 2 - min` and cannot overflow.
    let target = (max - min) / 2 / 8;
    let bits = target.checked_ilog2().map_or(0, |bits| bits as i64) - i64::from(norm);
    if !(1..=63).contains(&bits) {
        return refusal(
            TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL,
            "the chunk sizes and the normalization level leave no valid chunking mask",
        );
    }
    None
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
