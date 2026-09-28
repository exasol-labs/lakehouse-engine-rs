//! Raw-Parquet fixture writer for the direct-storage E2E suites: PUTs Parquet bytes
//! directly at a chosen object key (no catalog table, snapshot, or Iceberg field-id
//! metadata), unlike every other fixture, which goes through `seed.rs`'s iceberg-rust
//! writer stack.
#![cfg(any(feature = "exasol-e2e", feature = "azure-e2e", feature = "unity-e2e"))]

use super::e2e_harness::{local_stack_s3_store, split_s3_bucket_and_key};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

/// Encodes `batch` as Parquet with no Iceberg field-id metadata — just `batch`'s own Arrow schema.
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

/// Writes `batch` as one Parquet object at `uri` (e.g.
/// `"s3://warehouse/direct/events/file1.parquet"`) via the shared local-stack S3
/// backend, keying it verbatim so `region=a%2Fb` stays literal as Spark writes it.
/// Idempotent: a repeated call at the same `uri` overwrites the object.
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
