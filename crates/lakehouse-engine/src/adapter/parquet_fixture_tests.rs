//! Parquet-directory fixtures shared by the direct-storage test modules.

use crate::adapter::parquet_directory::{DirectoryOptions, MergeMode};
use arrow::array::{ArrayRef, new_empty_array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
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
