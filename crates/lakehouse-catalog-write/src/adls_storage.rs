//! The ADLS Gen2 storage of an export table's FileIO, over object_store's Azure client. Every
//! request asks the table's ADLS credential source for the credential it signs with, so a SAS
//! renewed while a writer runs signs every later request, each part of a multipart upload too.

use crate::object_store_dns::caching_client_options;
use crate::vended_refresh::AdlsCredentials;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

/// Bounds a writer's buffered upload parts, each of `WriteMultipart`'s 5 MiB chunk size.
const PARTS_IN_FLIGHT: usize = 8;

/// The FileIO of the table at `location`, whose requests sign with `credentials`.
pub(crate) fn adls_file_io(location: &str, credentials: AdlsCredentials) -> FileIO {
    FileIOBuilder::new(Arc::new(AdlsStorageFactory {
        location: location.to_string(),
        credentials: Some(credentials),
    }))
    .build()
}

/// The credential source is skipped on serialization, as iceberg-storage-opendal skips its
/// custom loader, so a deserialized factory builds a storage that refuses every request.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct AdlsStorageFactory {
    location: String,
    #[serde(skip)]
    credentials: Option<AdlsCredentials>,
}

impl fmt::Debug for AdlsStorageFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdlsStorageFactory")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

#[typetag::serde]
impl StorageFactory for AdlsStorageFactory {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(AdlsStorage::new(
            &self.location,
            self.credentials.clone(),
        )?))
    }
}

/// The container of one table location. It addresses only paths under that container.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct AdlsStorage {
    /// The location's scheme, as written.
    scheme: String,
    /// `<container>@<host>`.
    authority: String,
    /// object_store's Azure client in production.
    #[serde(skip)]
    store: Option<Arc<dyn ObjectStore>>,
}

impl fmt::Debug for AdlsStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdlsStorage")
            .field(
                "container",
                &format!("{}://{}", self.scheme, self.authority),
            )
            .finish_non_exhaustive()
    }
}

impl AdlsStorage {
    /// The storage of the container `location` names. The account is the first label of the
    /// location's host, as on the read path.
    #[expect(
        clippy::disallowed_methods,
        reason = "the export's ADLS store, built with `caching_client_options`"
    )]
    pub(crate) fn new(location: &str, credentials: Option<AdlsCredentials>) -> Result<Self> {
        let (scheme, rest) = location.split_once("://").ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("the ADLS location {location} has no scheme"),
            )
        })?;
        let authority = rest
            .split_once('/')
            .map_or(rest, |(authority, _)| authority);
        let store = credentials
            .map(|credentials| {
                MicrosoftAzureBuilder::new()
                    .with_url(format!("{scheme}://{authority}/"))
                    .with_credentials(Arc::new(credentials))
                    .with_client_options(caching_client_options())
                    .build()
                    .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                    .map_err(|error| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("could not address the ADLS location {location}"),
                        )
                        .with_source(error)
                    })
            })
            .transpose()?;
        Ok(Self {
            scheme: scheme.to_string(),
            authority: authority.to_string(),
            store,
        })
    }

    /// The object key of `path`, which must lie in this storage's container.
    pub(crate) fn key(&self, path: &str) -> Result<Path> {
        let outside = || {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "the path {path} lies outside the ADLS container {}://{} of the table",
                    self.scheme, self.authority
                ),
            )
        };
        let (scheme, rest) = path.split_once("://").ok_or_else(outside)?;
        let key = rest
            .strip_prefix(self.authority.as_str())
            .filter(|key| {
                scheme.eq_ignore_ascii_case(&self.scheme)
                    && (key.is_empty() || key.starts_with('/'))
            })
            .ok_or_else(outside)?;
        Path::parse(key.trim_start_matches('/')).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("the ADLS path {path} is no object key"),
            )
            .with_source(error)
        })
    }

    fn store(&self) -> Result<&Arc<dyn ObjectStore>> {
        self.store.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "an ADLS storage restored from its serialized form has no credentials and sends \
                 no request",
            )
        })
    }
}

fn request_error(action: &str, path: &str, error: object_store::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!("could not {action} {path} on ADLS"),
    )
    .with_source(error)
}

#[async_trait]
#[typetag::serde]
impl Storage for AdlsStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        match self.store()?.head(&self.key(path)?).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(request_error("look up", path, error)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let meta = self
            .store()?
            .head(&self.key(path)?)
            .await
            .map_err(|error| request_error("look up", path, error))?;
        Ok(FileMetadata { size: meta.size })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let read = self
            .store()?
            .get(&self.key(path)?)
            .await
            .map_err(|error| request_error("read", path, error))?;
        read.bytes()
            .await
            .map_err(|error| request_error("read", path, error))
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(AdlsFileRead {
            store: self.store()?.clone(),
            key: self.key(path)?,
            path: path.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.store()?
            .put(&self.key(path)?, PutPayload::from(bs))
            .await
            .map(|_| ())
            .map_err(|error| request_error("write", path, error))
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let upload = self
            .store()?
            .put_multipart(&self.key(path)?)
            .await
            .map_err(|error| request_error("start writing", path, error))?;
        Ok(Box::new(AdlsFileWrite {
            upload: Some(WriteMultipart::new(upload)),
            written: 0,
            path: path.to_string(),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        match self.store()?.delete(&self.key(path)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(request_error("delete", path, error)),
        }
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let store = self.store()?;
        let mut listed = store.list(Some(&self.key(path)?));
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

struct AdlsFileRead {
    store: Arc<dyn ObjectStore>,
    key: Path,
    path: String,
}

#[async_trait]
impl FileRead for AdlsFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store
            .get_range(&self.key, range)
            .await
            .map_err(|error| request_error("read", &self.path, error))
    }
}

/// A multipart upload: each part is its own request, signed with the credential current then.
struct AdlsFileWrite {
    /// `None` once closed.
    upload: Option<WriteMultipart>,
    written: u64,
    path: String,
}

fn closed(path: &str) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!("the ADLS file {path} is already closed"),
    )
}

#[async_trait]
impl FileWrite for AdlsFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let upload = self.upload.as_mut().ok_or_else(|| closed(&self.path))?;
        upload
            .wait_for_capacity(PARTS_IN_FLIGHT)
            .await
            .map_err(|error| request_error("write", &self.path, error))?;
        let len = bs.len() as u64;
        upload.put(bs);
        self.written += len;
        Ok(())
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        let upload = self.upload.take().ok_or_else(|| closed(&self.path))?;
        upload
            .finish()
            .await
            .map_err(|error| request_error("write", &self.path, error))?;
        Ok(FileMetadata { size: self.written })
    }
}

#[cfg(test)]
#[path = "adls_storage_tests.rs"]
mod tests;
