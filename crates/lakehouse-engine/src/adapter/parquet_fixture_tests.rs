use crate::adapter::parquet_directory::{DirectoryOptions, MergeMode};
use arrow::array::{ArrayRef, new_empty_array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type as SchemaType;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) fn nullable(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

/// A Parquet file declaring `fields` and holding `rows` rows of nulls (use 0 for a non-nullable
/// field, since a null value would fail the batch's own validation).
pub(crate) fn parquet_bytes(fields: Vec<Field>, rows: usize) -> Vec<u8> {
    let schema = Arc::new(Schema::new(fields));
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match rows {
            0 => new_empty_array(field.data_type()),
            _ => new_null_array(field.data_type(), rows),
        })
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .expect("the fixture batch matches its own schema");

    let mut bytes = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut bytes, schema, None).expect("the fixture schema is writable");
    writer.write(&batch).expect("the fixture batch is writable");
    writer.close().expect("the fixture file closes");
    bytes
}

/// A row-less Parquet file whose footer declares the `message` type, so a test sets each leaf's
/// Parquet annotation itself instead of taking the Arrow writer's.
pub(crate) fn parquet_footer_bytes(message: &str) -> Vec<u8> {
    parquet_schema_footer_bytes(
        parse_message_type(message).expect("the fixture message type parses"),
    )
}

/// As [`parquet_footer_bytes`], for a schema the message syntax cannot state, such as an `ENUM`
/// converted type without its logical type.
pub(crate) fn parquet_schema_footer_bytes(schema: SchemaType) -> Vec<u8> {
    let mut bytes = Vec::new();
    SerializedFileWriter::new(
        &mut bytes,
        Arc::new(schema),
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("the fixture schema is writable")
    .close()
    .expect("the fixture file closes");
    bytes
}

pub(crate) async fn in_memory_store(objects: &[(&str, &[u8])]) -> Arc<InMemory> {
    let store = InMemory::new();
    for (key, bytes) in objects {
        store
            .put(
                &StorePath::parse(key).expect("the fixture key is a valid store path"),
                PutPayload::from(bytes.to_vec()),
            )
            .await
            .expect("the in-memory store accepts the fixture object");
    }
    Arc::new(store)
}

pub(crate) fn directory_options(
    merge_mode: MergeMode,
    hive_partitioning: bool,
) -> DirectoryOptions {
    DirectoryOptions {
        merge_mode,
        hive_partitioning,
    }
}

pub(crate) fn values(pairs: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
        .collect()
}
