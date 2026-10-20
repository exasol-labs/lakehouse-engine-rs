use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

/// Reads one Parquet object whose size the planner already knows, so the footer fetch issues
/// no HEAD.
pub(crate) struct SizedParquetReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
}

impl SizedParquetReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path, size: u64) -> Self {
        Self { store, path, size }
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
