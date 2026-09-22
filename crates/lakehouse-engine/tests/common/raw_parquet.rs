//! Raw-Parquet fixture writer for the direct-storage E2E suites.
//!
//! Every other fixture in this workspace is authored by `seed.rs` through the
//! iceberg-rust writer stack, which creates a catalog table and commits a
//! snapshot. The direct-storage catalog kind reaches no catalog, so its
//! fixtures need the opposite: Parquet bytes PUT at a chosen object key, with
//! no catalog table, no snapshot, and no Iceberg field-id metadata on the
//! written Arrow schema.
#![cfg(any(feature = "exasol-e2e", feature = "azure-e2e"))]

use super::e2e_harness::local_stack_storage;

use lakehouse_engine::scan::spec::StorageBackend;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

/// Encode `batch` as a standalone Parquet file, with no writer-level schema
/// decoration (no Iceberg field-id metadata) beyond `batch`'s own Arrow schema.
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

/// Configure the S3 object store for `bucket` through the shared
/// `local_stack_storage()` backend — the same MinIO endpoint, key, and secret
/// every other E2E helper reads through.
fn local_stack_s3_store(bucket: &str) -> Box<dyn ObjectStore> {
    let StorageBackend::S3(storage) = local_stack_storage() else {
        panic!("local_stack_storage() must be S3 for the raw-Parquet fixture writer")
    };
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_endpoint(&storage.endpoint)
        .with_allow_http(storage.allow_http)
        .with_virtual_hosted_style_request(!storage.path_style)
        .build()
        .unwrap_or_else(|e| panic!("configure MinIO object store for raw-Parquet fixtures: {e}"));
    Box::new(store)
}

/// Split an `s3://<bucket>/<key>` URI into its bucket and key.
fn split_bucket_and_key(uri: &str) -> (&str, &str) {
    let without_scheme = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .unwrap_or_else(|| panic!("raw-Parquet fixture URI must be an s3/s3a URI, got: {uri}"));
    without_scheme.split_once('/').unwrap_or_else(|| {
        panic!("raw-Parquet fixture URI must have a <bucket>/<key> form, got: {uri}")
    })
}

/// Write `batch` as one Parquet object at `uri` (e.g.
/// `"s3://warehouse/direct/events/file1.parquet"`) through the shared
/// local-stack S3 backend.
///
/// Creates no catalog table, commits no snapshot, and attaches no Iceberg
/// field-id metadata — `batch`'s own Arrow schema is written as-is. Idempotent:
/// a repeated call at the same `uri` overwrites the object, so fixture
/// authoring is safe to re-run against a MinIO volume that outlives one test
/// run.
pub fn write_parquet_fixture(uri: &str, batch: RecordBatch) {
    let (bucket, key) = split_bucket_and_key(uri);
    let store = local_stack_s3_store(bucket);
    let bytes = encode_parquet(&batch);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for raw-Parquet fixture write");
    rt.block_on(async {
        store
            .put(&ObjectStorePath::from(key), PutPayload::from(bytes))
            .await
            .unwrap_or_else(|e| panic!("PUT raw-Parquet fixture at {uri}: {e}"));
    });
}
