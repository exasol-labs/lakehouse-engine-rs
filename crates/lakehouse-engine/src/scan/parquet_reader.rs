use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use chrono::TimeZone;
use datafusion::datasource::listing::ListingTableUrl;
use exasol_udf_sdk::error::UdfError;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

/// Built without a HEAD; the caller supplies the size.
pub(crate) fn object_meta_for(
    abs_uri: &str,
    size: u64,
) -> std::result::Result<ObjectMeta, UdfError> {
    let url = ListingTableUrl::parse(abs_uri)
        .map_err(|e| UdfError::User(format!("invalid file URL '{abs_uri}': {e}")))?;
    Ok(ObjectMeta {
        location: url.prefix().clone(),
        last_modified: chrono::Utc.timestamp_nanos(0),
        size,
        e_tag: None,
        version: None,
    })
}

/// Reads one Parquet object whose size the planner already knows, so the footer fetch issues
/// no HEAD.
#[derive(Clone)]
pub(crate) struct SizedParquetReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
    metadata_size_hint: Option<usize>,
}

impl SizedParquetReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path, size: u64) -> Self {
        Self {
            store,
            path,
            size,
            metadata_size_hint: None,
        }
    }

    /// Without a hint, the footer read is two GETs: the 8-byte footer, then the metadata.
    pub(crate) fn with_metadata_size_hint(mut self, hint: Option<usize>) -> Self {
        self.metadata_size_hint = hint;
        self
    }
}

impl AsyncFileReader for SizedParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map(|r| r.map_err(|e| ParquetError::External(Box::new(e))))
            .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            let mut reader = ParquetMetaDataReader::new()
                .with_prefetch_hint(self.metadata_size_hint)
                .with_metadata_options(options.map(|o| o.metadata_options().clone()));
            if let Some(options) = options {
                reader = reader
                    .with_column_index_policy(options.column_index_policy())
                    .with_offset_index_policy(options.offset_index_policy());
            }
            let size = self.size;
            Ok(Arc::new(reader.load_and_finish(self, size).await?))
        }
        .boxed()
    }
}
