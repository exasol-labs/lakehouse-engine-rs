//! Decodes Delta and Iceberg deletion vectors into the [`RoaringTreemap`] the positional-delete
//! pipeline consumes. Both decoders are used only as pure bytes-to-bitmap functions: the scan
//! fetches the bytes itself, and [`DeletionVector::decode`] hands them to `delta_kernel` through
//! an in-memory [`StorageHandler`] that performs no I/O.

use crate::scan::emit::{redact_credentials, redact_secret_values};
use crate::scan::spec::DeltaDeletionVectorStorage;
use bytes::Bytes;
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::{
    FileMeta, FileSlice, KernelError, KernelResult, KernelResultIteratorStatic, StorageHandler,
};
use exasol_udf_sdk::error::UdfError;
use iceberg::delete_vector::DeleteVector;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use url::Url;

/// A persisted vector's declared size covers this magic, and the decoder derives its bitmap
/// bounds from that size without re-checking it.
const PORTABLE_MAGIC_BYTES: i32 = 4;

/// Z85 packs four bytes into five characters; two chunks are the fewest yielding the magic under
/// every accepted encoding, keeping the decoder's magic read inside the decoded payload.
const MIN_INLINE_PAYLOAD_CHARS: usize = 10;

/// Never read: persisted paths are absolute by decode time and inline bytes are the descriptor.
const UNUSED_DECODE_PARENT: &str = "memory:///";

#[derive(Debug, Clone, Copy)]
pub(crate) struct LoggedDeletionVector<'a> {
    pub(crate) storage: DeltaDeletionVectorStorage,
    pub(crate) path_or_inline_dv: &'a str,
    pub(crate) offset: Option<i32>,
    pub(crate) size_in_bytes: i32,
    pub(crate) cardinality: i64,
}

/// Resolution and decoding are separate so the scan can dedup and fetch sidecars on its own
/// bounded path; decoding takes the bytes as an argument so this type cannot perform I/O.
#[derive(Debug, Clone)]
pub(crate) struct DeletionVector {
    descriptor: DeletionVectorDescriptor,
    sidecar: Option<Url>,
    cardinality: u64,
}

impl DeletionVector {
    /// `data_file_path` is what a refusal names: a deletion vector has no identity of its own.
    pub(crate) fn resolve(
        logged: LoggedDeletionVector<'_>,
        table_root: &str,
        data_file_path: &str,
        secrets: &[String],
    ) -> Result<Self, UdfError> {
        resolve_descriptor(logged, table_root)
            .map_err(|reason| refusal("Delta", data_file_path, &reason, secrets))
    }

    /// `None` for an inline vector.
    pub(crate) fn sidecar_url(&self) -> Option<&Url> {
        self.sidecar.as_ref()
    }

    /// `sidecar_bytes` must be the WHOLE sidecar body: the version byte sits at position 0.
    /// A decoded set disagreeing with the declared cardinality is refused, never emitted.
    pub(crate) fn decode(
        &self,
        sidecar_bytes: Option<Bytes>,
        data_file_path: &str,
        secrets: &[String],
    ) -> Result<RoaringTreemap, UdfError> {
        self.decode_positions(sidecar_bytes)
            .map_err(|reason| refusal("Delta", data_file_path, &reason, secrets))
    }

    fn decode_positions(&self, sidecar_bytes: Option<Bytes>) -> Result<RoaringTreemap, String> {
        let storage: Arc<dyn StorageHandler> = match (self.sidecar.as_ref(), sidecar_bytes) {
            (Some(location), Some(body)) => Arc::new(PrefetchedDeletionVectorBytes::holding(
                location.clone(),
                body,
            )),
            _ => Arc::new(PrefetchedDeletionVectorBytes::empty()),
        };
        let parent = Url::parse(UNUSED_DECODE_PARENT)
            .map_err(|e| format!("the decoder's placeholder parent is not a URL: {e}"))?;

        let positions = self
            .descriptor
            .read(storage, &parent)
            .map_err(|e| format!("it could not be decoded: {e}"))?;
        if positions.len() != self.cardinality {
            return Err(format!(
                "it decodes to {} positions but the log declares a cardinality of {}",
                positions.len(),
                self.cardinality
            ));
        }
        Ok(positions)
    }
}

/// Free of data-file identity and redaction so [`refusal`] is the one place adding them.
fn resolve_descriptor(
    logged: LoggedDeletionVector<'_>,
    table_root: &str,
) -> Result<DeletionVector, String> {
    let cardinality = u64::try_from(logged.cardinality).map_err(|_| {
        format!(
            "the log declares a negative cardinality of {}",
            logged.cardinality
        )
    })?;
    let inline = logged.storage == DeltaDeletionVectorStorage::Inline;
    if inline && logged.path_or_inline_dv.len() < MIN_INLINE_PAYLOAD_CHARS {
        return Err("its inline payload is too short to carry a bitmap".to_string());
    }
    if !inline && logged.size_in_bytes < PORTABLE_MAGIC_BYTES {
        return Err(format!(
            "the log declares a size of {} bytes, smaller than the {PORTABLE_MAGIC_BYTES}-byte \
             magic every bitmap starts with",
            logged.size_in_bytes
        ));
    }

    let sidecar = match logged.storage {
        DeltaDeletionVectorStorage::Inline => None,
        DeltaDeletionVectorStorage::UuidRelative => Some(
            kernel_descriptor(
                DeletionVectorStorageType::PersistedRelative,
                logged.path_or_inline_dv,
                &logged,
            )?
            .absolute_path(&table_root_url(table_root)?)
            .map_err(|e| format!("its sidecar path could not be reconstructed: {e}"))?
            .ok_or_else(|| "its sidecar path could not be reconstructed".to_string())?,
        ),
        DeltaDeletionVectorStorage::AbsolutePath => Some(
            Url::parse(logged.path_or_inline_dv)
                .map_err(|e| format!("its absolute sidecar path is not a URL: {e}"))?,
        ),
    };

    // Both persisted kinds are absolute from here on, so the decoder needs no parent.
    let (storage_type, path) = match &sidecar {
        Some(location) => (
            DeletionVectorStorageType::PersistedAbsolute,
            location.as_str(),
        ),
        None => (DeletionVectorStorageType::Inline, logged.path_or_inline_dv),
    };
    Ok(DeletionVector {
        descriptor: kernel_descriptor(storage_type, path, &logged)?,
        sidecar,
        cardinality,
    })
}

fn kernel_descriptor(
    storage_type: DeletionVectorStorageType,
    path_or_inline_dv: &str,
    logged: &LoggedDeletionVector<'_>,
) -> Result<DeletionVectorDescriptor, String> {
    DeletionVectorDescriptor::try_new(
        storage_type,
        path_or_inline_dv,
        logged.offset,
        logged.size_in_bytes,
        logged.cardinality,
    )
    .map_err(|e| format!("the log describes it invalidly: {e}"))
}

fn table_root_url(table_root: &str) -> Result<Url, String> {
    let mut base = table_root.to_string();
    if !base.ends_with('/') {
        base.push('/');
    }
    Url::parse(&base).map_err(|e| format!("the scan's table root is not a URL: {e}"))
}

/// A Puffin deletion vector's manifest coordinates, as the scan spec carries them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ManifestDeletionVector<'a> {
    pub(crate) path: &'a str,
    /// The Puffin file's size; a blob range reaching past it is refused before any fetch.
    pub(crate) size: u64,
    pub(crate) content_offset: u64,
    pub(crate) content_size_in_bytes: u64,
    pub(crate) record_count: u64,
}

/// One `deletion-vector-v1` blob inside a Puffin file that may hold many data files' vectors.
#[derive(Debug, Clone)]
pub(crate) struct PuffinDeletionVector<'a> {
    pub(crate) path: &'a str,
    pub(crate) range: Range<u64>,
    cardinality: u64,
}

impl<'a> PuffinDeletionVector<'a> {
    pub(crate) fn resolve(
        manifest: ManifestDeletionVector<'a>,
        data_file_path: &str,
        secrets: &[String],
    ) -> Result<Self, UdfError> {
        let ManifestDeletionVector {
            path,
            size,
            content_offset,
            content_size_in_bytes,
            record_count,
        } = manifest;
        let end = content_offset
            .checked_add(content_size_in_bytes)
            .filter(|end| *end <= size)
            .ok_or_else(|| {
                let reason = format!(
                    "its blob at offset {content_offset} with {content_size_in_bytes} bytes lies \
                     outside the {size}-byte Puffin file"
                );
                refusal("Iceberg", data_file_path, &reason, secrets)
            })?;
        Ok(Self {
            path,
            range: content_offset..end,
            cardinality: record_count,
        })
    }

    /// `blob` must be exactly [`Self::range`] of the Puffin file.
    pub(crate) fn decode(
        &self,
        blob: &[u8],
        data_file_path: &str,
        secrets: &[String],
    ) -> Result<RoaringTreemap, UdfError> {
        self.decode_positions(blob)
            .map_err(|reason| refusal("Iceberg", data_file_path, &reason, secrets))
    }

    fn decode_positions(&self, blob: &[u8]) -> Result<RoaringTreemap, String> {
        let vector =
            DeleteVector::deserialize(blob).map_err(|e| format!("it could not be decoded: {e}"))?;
        if vector.len() != self.cardinality {
            return Err(format!(
                "it decodes to {} positions but the manifest declares a cardinality of {}",
                vector.len(),
                self.cardinality
            ));
        }
        RoaringTreemap::from_sorted_iter(vector.iter())
            .map_err(|e| format!("its positions are not ascending: {e}"))
    }
}

/// Names the data file but never echoes an opaque inline payload or credential.
fn refusal(format: &str, data_file_path: &str, reason: &str, secrets: &[String]) -> UdfError {
    let message = format!(
        "data file '{data_file_path}' carries a {format} deletion vector this scan cannot \
         apply: {reason}; refusing to emit rows for the affected data file"
    );
    let borrowed: Vec<&str> = secrets.iter().map(String::as_str).collect();
    UdfError::User(redact_credentials(&redact_secret_values(
        &message, &borrowed,
    )))
}

/// Every operation other than reading an already-fetched body returns an error rather than
/// panicking: a UDF panic is an abnormal VM exit that SIGKILLs every sibling VM.
#[derive(Debug)]
struct PrefetchedDeletionVectorBytes {
    bodies: HashMap<Url, Bytes>,
}

impl PrefetchedDeletionVectorBytes {
    fn holding(location: Url, body: Bytes) -> Self {
        Self {
            bodies: HashMap::from([(location, body)]),
        }
    }

    fn empty() -> Self {
        Self {
            bodies: HashMap::new(),
        }
    }

    fn body(&self, location: &Url, range: Option<Range<u64>>) -> KernelResult<Bytes> {
        let body = self.bodies.get(location).ok_or_else(|| {
            KernelError::missing_data(format!(
                "no pre-fetched deletion-vector body for {location}"
            ))
        })?;
        let Some(range) = range else {
            return Ok(body.clone());
        };
        let start = usize::try_from(range.start).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end).unwrap_or(usize::MAX);
        if start > end || end > body.len() {
            return Err(KernelError::generic(format!(
                "bytes {start}..{end} lie outside the {}-byte pre-fetched deletion-vector body \
                 for {location}",
                body.len()
            )));
        }
        Ok(body.slice(start..end))
    }
}

fn unsupported(operation: &str) -> KernelError {
    KernelError::unsupported(format!(
        "{operation} is unavailable while decoding a deletion vector: the scan serves \
         already-fetched bytes and performs no storage access of its own"
    ))
}

impl StorageHandler for PrefetchedDeletionVectorBytes {
    fn list_from(&self, _path: &Url) -> KernelResult<KernelResultIteratorStatic<FileMeta>> {
        Err(unsupported("list_from"))
    }

    fn read_files(&self, files: Vec<FileSlice>) -> KernelResult<KernelResultIteratorStatic<Bytes>> {
        let bodies: Vec<KernelResult<Bytes>> = files
            .into_iter()
            .map(|(location, range)| self.body(&location, range))
            .collect();
        Ok(Box::new(bodies.into_iter()))
    }

    fn copy_atomic(&self, _src: &Url, _dest: &Url) -> KernelResult<()> {
        Err(unsupported("copy_atomic"))
    }

    fn put(&self, _path: &Url, _data: Bytes, _overwrite: bool) -> KernelResult<()> {
        Err(unsupported("put"))
    }

    fn head(&self, _path: &Url) -> KernelResult<FileMeta> {
        Err(unsupported("head"))
    }

    fn delete(&self, _path: &Url) -> KernelResult<()> {
        Err(unsupported("delete"))
    }
}

#[cfg(test)]
#[path = "deletion_vectors_tests.rs"]
mod tests;
