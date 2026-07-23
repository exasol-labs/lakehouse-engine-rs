//! Test-only fixtures shared across the `scan` submodule test modules.
//!
//! Extracted verbatim from the former flat `mod tests` helpers block. Each
//! functional submodule's `#[cfg(test)] mod tests` reaches these through
//! `super::test_support` (or `crate::scan::test_support` from a nested module).

use crate::scan::spec::{FileEntry, ScanSpec, StorageProps};

/// The byte size of the local file behind a `file://` URL.
///
/// The custom `ParquetSource`-backed provider builds each file's `ObjectMeta`
/// from the spec-supplied size (the no-HEAD design), so tests that register a
/// local Parquet file must supply its real size instead of a `0` placeholder.
pub(super) fn local_file_size(file_url: &str) -> u64 {
    let path = url::Url::parse(file_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    std::fs::metadata(path).expect("stat local parquet").len()
}

/// Minimal ScanSpec with a valid-looking S3 URI for build_session_context tests.
pub(super) fn minimal_spec() -> ScanSpec {
    ScanSpec {
        table_root: String::new(),
        files: vec![FileEntry::new("s3://test-bucket/data/part-0.parquet", 1024)],
        projection: vec![],
        filter: None,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        distinct: false,
        emit_exa_types: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join: None,
        storage: StorageProps {
            endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            access_key: "testkey".into(),
            secret_key: "testsecret".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        },
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}
