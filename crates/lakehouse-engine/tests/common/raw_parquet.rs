//! Raw-Parquet fixture writer: PUTs Parquet bytes at a chosen object key, with no
//! catalog table, snapshot, or Iceberg field-id metadata.
#![cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]

use super::e2e_harness::{local_stack_s3_store, split_s3_bucket_and_key};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::data_type::DataType as ParquetDataType;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::parser::parse_message_type;

use std::sync::Arc;

pub fn encode_parquet(batch: &RecordBatch) -> Bytes {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)
        .unwrap_or_else(|e| panic!("open Arrow Parquet writer for a raw fixture: {e}"));
    writer
        .write(batch)
        .unwrap_or_else(|e| panic!("write raw-Parquet fixture batch: {e}"));
    writer
        .close()
        .unwrap_or_else(|e| panic!("close raw-Parquet fixture writer: {e}"));
    Bytes::from(buf)
}

/// Writes one row group of a Parquet `message` schema with no embedded Arrow schema, so a
/// reader sees only the Parquet physical and logical types; `write` fills every column in order.
pub fn encode_parquet_message(
    message: &str,
    write: impl FnOnce(&mut SerializedRowGroupWriter<'_, &mut Vec<u8>>),
) -> Bytes {
    let schema = Arc::new(parse_message_type(message).expect("parse Parquet message type"));
    let mut buffer = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut buffer,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("open Parquet file writer");
    let mut row_group = writer.next_row_group().expect("open row group");
    write(&mut row_group);
    row_group.close().expect("close row group");
    writer.close().expect("close Parquet file");
    Bytes::from(buffer)
}

/// `definition_levels` is `None` for a `REQUIRED` column.
pub fn write_parquet_column<W: std::io::Write + Send, T: ParquetDataType>(
    row_group: &mut SerializedRowGroupWriter<'_, W>,
    values: &[T::T],
    definition_levels: Option<&[i16]>,
) {
    let mut column = row_group
        .next_column()
        .expect("open column writer")
        .expect("schema has another column");
    column
        .typed::<T>()
        .write_batch(values, definition_levels, None)
        .expect("write column batch");
    column.close().expect("close column writer");
}

pub fn write_parquet_fixture(uri: &str, batch: RecordBatch) {
    put_fixture_object(uri, encode_parquet(&batch));
}

/// Keys the object verbatim so `region=a%2Fb` stays literal, as Spark writes it.
pub fn put_fixture_object(uri: &str, bytes: Bytes) {
    let (bucket, key) = split_s3_bucket_and_key(uri);
    let store = local_stack_s3_store(bucket);
    let key = ObjectStorePath::parse(key)
        .unwrap_or_else(|e| panic!("fixture key {key} is not a valid store path: {e}"));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for fixture write");
    rt.block_on(async {
        store
            .put(&key, PutPayload::from(bytes))
            .await
            .unwrap_or_else(|e| panic!("PUT fixture object at {uri}: {e}"));
    });
}
