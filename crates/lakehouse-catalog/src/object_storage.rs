//! The iceberg storage of every FileIO the engine opens, over object_store clients (#701). It
//! builds one client per bucket or container a path names, through the closure its FileIO was
//! opened with, which decides the credentials and which roots it addresses at all. The E2E harness
//! compiles this file into its own crate, so it names no other module of this crate.

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::io::AsyncWriteExt;

/// A file up to one part is one PUT; a larger one is a multipart upload of parts this size, at most
/// `PARTS_IN_FLIGHT` of them buffered at once.
const PART_SIZE: usize = 5 * 1024 * 1024;
const PARTS_IN_FLIGHT: usize = 8;

/// Builds the client of one root, `<scheme>://<authority>/` with a lowercase scheme, or refuses it.
pub type StoreFor = dyn Fn(&str) -> object_store::Result<Arc<dyn ObjectStore>> + Send + Sync;

/// The FileIO whose clients `store_for` builds.
pub fn object_file_io(store_for: Arc<StoreFor>) -> FileIO {
    FileIOBuilder::new(object_storage_factory(store_for)).build()
}

/// The storage factory whose clients `store_for` builds, for a catalog that opens its own FileIOs.
pub fn object_storage_factory(store_for: Arc<StoreFor>) -> Arc<dyn StorageFactory> {
    Arc::new(ObjectStorageFactory {
        storage: ObjectStorage::new(store_for),
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ObjectStorageFactory {
    storage: ObjectStorage,
}

#[typetag::serde]
impl StorageFactory for ObjectStorageFactory {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.storage.clone()))
    }
}

/// Every field is skipped on serialization, so a restored storage refuses every request and no
/// credential the closure captured is written out.
#[derive(Clone, Default, Serialize, Deserialize)]
struct ObjectStorage {
    #[serde(skip)]
    store_for: Option<Arc<StoreFor>>,
    #[serde(skip)]
    stores: Arc<Mutex<HashMap<String, Arc<dyn ObjectStore>>>>,
}

impl fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stores = self.stores.lock().unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("ObjectStorage")
            .field("roots", &stores.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl ObjectStorage {
    fn new(store_for: Arc<StoreFor>) -> Self {
        Self {
            store_for: Some(store_for),
            stores: Arc::default(),
        }
    }

    /// The client of `path`'s root and the object key of `path` in it.
    fn locate(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
        let (root, key) = split_path(path)?;
        let store_for = self.store_for.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "a storage restored from its serialized form has no credentials and sends no \
                 request",
            )
        })?;
        let mut stores = self.stores.lock().unwrap_or_else(PoisonError::into_inner);
        let store = match stores.get(&root) {
            Some(store) => store.clone(),
            None => {
                let store = store_for(&root).map_err(|error| {
                    Error::new(ErrorKind::DataInvalid, format!("could not address {path}"))
                        .with_source(error)
                })?;
                stores.insert(root, store.clone());
                store
            }
        };
        Ok((store, key))
    }
}

/// The root `<scheme>://<authority>/` of `path` that a [`StoreFor`] closure receives.
pub fn root_of(path: &str) -> Result<String> {
    Ok(split_path(path)?.0)
}

/// The root `<scheme>://<authority>/` of `path`, its scheme lowercased (RFC 3986 §3.1), and the
/// object key under it.
fn split_path(path: &str) -> Result<(String, Path)> {
    let (scheme, rest) = path.split_once("://").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("the path {path} has no scheme"),
        )
    })?;
    let (authority, key) = rest.split_once('/').unwrap_or((rest, ""));
    let key = Path::parse(key.trim_start_matches('/')).map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("the path {path} is no object key"),
        )
        .with_source(error)
    })?;
    Ok((
        format!("{}://{authority}/", scheme.to_ascii_lowercase()),
        key,
    ))
}

fn request_error(
    action: &str,
    path: &str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::new(ErrorKind::Unexpected, format!("could not {action} {path}")).with_source(error)
}

#[async_trait]
#[typetag::serde]
impl Storage for ObjectStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (store, key) = self.locate(path)?;
        match store.head(&key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(request_error("look up", path, error)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (store, key) = self.locate(path)?;
        let meta = store
            .head(&key)
            .await
            .map_err(|error| request_error("look up", path, error))?;
        Ok(FileMetadata { size: meta.size })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (store, key) = self.locate(path)?;
        let read = store
            .get(&key)
            .await
            .map_err(|error| request_error("read", path, error))?;
        read.bytes()
            .await
            .map_err(|error| request_error("read", path, error))
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (store, key) = self.locate(path)?;
        Ok(Box::new(ObjectFileRead {
            store,
            key,
            path: path.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (store, key) = self.locate(path)?;
        store
            .put(&key, PutPayload::from(bs))
            .await
            .map(|_| ())
            .map_err(|error| request_error("write", path, error))
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (store, key) = self.locate(path)?;
        let writer =
            BufWriter::with_capacity(store, key, PART_SIZE).with_max_concurrency(PARTS_IN_FLIGHT);
        Ok(Box::new(ObjectFileWrite {
            writer: Some(writer),
            written: 0,
            path: path.to_string(),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (store, key) = self.locate(path)?;
        match store.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(request_error("delete", path, error)),
        }
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (store, key) = self.locate(path)?;
        let mut listed = store.list(Some(&key));
        while let Some(meta) = listed.next().await {
            let meta = meta.map_err(|error| request_error("list", path, error))?;
            match store.delete(&meta.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(request_error("delete", meta.location.as_ref(), error)),
            }
        }
        Ok(())
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

struct ObjectFileRead {
    store: Arc<dyn ObjectStore>,
    key: Path,
    path: String,
}

#[async_trait]
impl FileRead for ObjectFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store
            .get_range(&self.key, range)
            .await
            .map_err(|error| request_error("read", &self.path, error))
    }
}

/// Each request of the upload is signed with the credential current when it is sent.
struct ObjectFileWrite {
    /// `None` once closed.
    writer: Option<BufWriter>,
    written: u64,
    path: String,
}

fn closed(path: &str) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!("the file {path} is already closed"),
    )
}

#[async_trait]
impl FileWrite for ObjectFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let writer = self.writer.as_mut().ok_or_else(|| closed(&self.path))?;
        let len = bs.len() as u64;
        writer
            .put(bs)
            .await
            .map_err(|error| request_error("write", &self.path, error))?;
        self.written += len;
        Ok(())
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        let mut writer = self.writer.take().ok_or_else(|| closed(&self.path))?;
        writer
            .shutdown()
            .await
            .map_err(|error| request_error("write", &self.path, error))?;
        Ok(FileMetadata { size: self.written })
    }
}

#[cfg(test)]
#[path = "object_storage_tests.rs"]
mod tests;
