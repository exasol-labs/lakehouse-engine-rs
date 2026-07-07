//! Scan-side Puffin open + deletion-vector blob fetch plumbing.
//!
//! Opens the Puffin container that holds a v3 `deletion-vector-v1` blob through
//! iceberg-rust's [`PuffinReader`] (footer parse + blob decompression), selects
//! the blob addressed by the reference's `offset`/`length`, cross-checks the
//! blob's `referenced-data-file` against the data file being read, decodes the
//! blob into a [`RoaringTreemap`] via
//! [`crate::scan::deletion_vectors::decode_deletion_vector_v1`], and unions its
//! positions into the caller's per-data-file delete set.
//!
//! iceberg-rust reads only the Puffin container; the `deletion-vector-v1` payload
//! is decoded by [`crate::scan::deletion_vectors`]. All open/read failures surface
//! credential-redacted errors.
//!
//! A single Puffin container is commonly shared by many data files (decision [8]
//! interns it once per shard). [`PuffinReaders`] opens each distinct container
//! ONCE per shard and reuses its [`PuffinReader`] — whose `FileMetadata` (footer)
//! is parsed once via an internal `OnceCell` — across every data file that
//! references it, rather than re-opening and re-reading the footer per data file.

use crate::scan::deletion_vectors::decode_deletion_vector_v1;
use crate::scan::emit::redact;
use crate::scan::spec::StorageProps;
use exasol_udf_sdk::error::UdfError;
use iceberg::io::{
    FileIOBuilder, LocalFsStorageFactory, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS,
    S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg::puffin::PuffinReader;
use iceberg_storage_opendal::OpenDalStorageFactory;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;

/// Puffin `BlobMetadata` property carrying the number of deleted positions.
const CARDINALITY_PROPERTY: &str = "cardinality";
/// Puffin `BlobMetadata` property carrying the path of the data file the
/// deletion vector applies to.
const REFERENCED_DATA_FILE_PROPERTY: &str = "referenced-data-file";

/// Build a `FileIO` able to open `puffin_abs`, mirroring the adapter's
/// manifest-read `FileIO`. A `file://` path uses iceberg's built-in local
/// filesystem storage (host-runnable tests); every other scheme uses the S3
/// (MinIO) storage factory. Credentials live only in this builder and never
/// appear in errors.
fn build_file_io(storage: &StorageProps, puffin_abs: &str) -> iceberg::io::FileIO {
    if puffin_abs.starts_with("file:/") {
        return FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
    }
    let mut builder = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }));
    if !storage.endpoint.is_empty() {
        builder = builder.with_prop(S3_ENDPOINT, &storage.endpoint);
    }
    if !storage.region.is_empty() {
        builder = builder.with_prop(S3_REGION, &storage.region);
    }
    if !storage.access_key.is_empty() {
        builder = builder.with_prop(S3_ACCESS_KEY_ID, &storage.access_key);
    }
    if !storage.secret_key.is_empty() {
        builder = builder.with_prop(S3_SECRET_ACCESS_KEY, &storage.secret_key);
    }
    if let Some(token) = &storage.session_token {
        builder = builder.with_prop(S3_SESSION_TOKEN, token);
    }
    builder = builder.with_prop(S3_PATH_STYLE_ACCESS, storage.path_style.to_string());
    builder.build()
}

/// Per-shard cache of open Puffin containers, keyed by absolute path.
///
/// A shard's data files commonly share one Puffin container (decision [8] interns
/// it once). Opening the container and parsing its footer ONCE and reusing the
/// [`PuffinReader`] across every referencing data file avoids N re-opens /
/// footer reads per shard.
pub(crate) struct PuffinReaders {
    storage: StorageProps,
    readers: HashMap<String, Arc<PuffinReader>>,
}

impl PuffinReaders {
    pub(crate) fn new(storage: StorageProps) -> Self {
        Self {
            storage,
            readers: HashMap::new(),
        }
    }

    /// Return the [`PuffinReader`] for `puffin_abs`, opening it on first use.
    ///
    /// The `PuffinReader` reads the container footer lazily (and once, via an
    /// internal `OnceCell`) on the first `file_metadata()` call. iceberg-rust's
    /// `InputFile`/`FileIO` API exposes no way to supply the container's known
    /// byte size, so the footer read issues one object-store HEAD (`InputFile::
    /// metadata()`) — but caching the reader here means that HEAD happens ONCE
    /// per container per shard, not once per referencing data file.
    async fn reader(
        &mut self,
        puffin_abs: &str,
        secrets: &[String],
    ) -> Result<Arc<PuffinReader>, UdfError> {
        if let Some(reader) = self.readers.get(puffin_abs) {
            return Ok(Arc::clone(reader));
        }
        let file_io = build_file_io(&self.storage, puffin_abs);
        let input = file_io.new_input(puffin_abs).map_err(|e| {
            UdfError::User(redact(
                format!("failed to open deletion-vector Puffin file: {e}"),
                secrets,
            ))
        })?;
        let reader = Arc::new(PuffinReader::new(input));
        self.readers
            .insert(puffin_abs.to_string(), Arc::clone(&reader));
        Ok(reader)
    }

    /// Fetch the `deletion-vector-v1` blob at (`offset`, `length`) from the
    /// (cached) Puffin container at `puffin_abs`, validate it (magic, CRC,
    /// cardinality, and the blob's `referenced-data-file` against
    /// `data_file_abs`), and union its decoded positions into `out`.
    ///
    /// Fails loud (credential-redacted) on any open/read error, a missing blob at
    /// the coordinates, a missing `cardinality`/`referenced-data-file` property,
    /// or a referenced-data-file mismatch — never silently misapplying a delete
    /// set.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn union_deletion_vector_positions(
        &mut self,
        puffin_abs: &str,
        offset: u64,
        length: u64,
        data_file_abs: &str,
        out: &mut RoaringTreemap,
        secrets: &[String],
    ) -> Result<(), UdfError> {
        let reader = self.reader(puffin_abs, secrets).await?;
        let file_metadata = reader.file_metadata().await.map_err(|e| {
            UdfError::User(redact(
                format!("failed to read deletion-vector Puffin metadata: {e}"),
                secrets,
            ))
        })?;

        // Select the blob addressed by the reference's coordinates. The planning
        // layer resolved these once from the manifest; the scan MUST NOT re-derive
        // them.
        let blob_metadata = file_metadata
            .blobs()
            .iter()
            .find(|b| b.offset() == offset && b.length() == length)
            .ok_or_else(|| {
                UdfError::User(format!(
                    "deletion-vector Puffin container has no blob at offset {offset} \
                     length {length}"
                ))
            })?;

        let blob = reader.blob(blob_metadata).await.map_err(|e| {
            UdfError::User(redact(
                format!("failed to read deletion-vector blob: {e}"),
                secrets,
            ))
        })?;

        // Cross-check the blob's referenced-data-file against the data file the DV
        // is being applied to. The wire no longer carries this field, so this
        // restores the correctness the dropped field would otherwise have
        // guaranteed.
        let referenced = blob
            .properties()
            .get(REFERENCED_DATA_FILE_PROPERTY)
            .ok_or_else(|| {
                UdfError::User(
                    "deletion-vector blob metadata has no referenced-data-file property".into(),
                )
            })?;
        if referenced != data_file_abs {
            return Err(UdfError::User(format!(
                "deletion-vector referenced-data-file mismatch: blob references '{}' but is being \
                 applied to '{}'; refusing to apply a mismatched delete set",
                redact(referenced.clone(), secrets),
                redact(data_file_abs.to_string(), secrets),
            )));
        }

        let cardinality = blob
            .properties()
            .get(CARDINALITY_PROPERTY)
            .ok_or_else(|| {
                UdfError::User("deletion-vector blob metadata has no cardinality property".into())
            })?
            .parse::<u64>()
            .map_err(|e| {
                UdfError::User(format!(
                    "deletion-vector blob has a non-numeric cardinality property: {e}"
                ))
            })?;

        let positions = decode_deletion_vector_v1(blob.data(), cardinality)?;
        *out |= positions;
        Ok(())
    }
}
