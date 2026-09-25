//! Raw-Parquet fixture writer: PUTs Parquet bytes at a chosen object key, with no
//! catalog table, snapshot, or Iceberg field-id metadata.
#![cfg(any(feature = "exasol-e2e", feature = "azure-e2e"))]

use super::e2e_harness::{local_stack_s3_store, split_s3_bucket_and_key};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

fn encode_parquet(batch: &RecordBatch) -> Bytes {
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

/// Keys the object verbatim so `region=a%2Fb` stays literal, as Spark writes it.
pub fn write_parquet_fixture(uri: &str, batch: RecordBatch) {
    let (bucket, key) = split_s3_bucket_and_key(uri);
    let store = local_stack_s3_store(bucket);
    let bytes = encode_parquet(&batch);
    let key = ObjectStorePath::parse(key)
        .unwrap_or_else(|e| panic!("raw-Parquet fixture key {key} is not a valid store path: {e}"));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for raw-Parquet fixture write");
    rt.block_on(async {
        store
            .put(&key, PutPayload::from(bytes))
            .await
            .unwrap_or_else(|e| panic!("PUT raw-Parquet fixture at {uri}: {e}"));
    });
}
