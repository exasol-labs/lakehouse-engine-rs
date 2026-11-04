//! Scan-side builders and runners: in-memory Parquet objects, logical fields, minimal scan and
//! join specs, inline and refusing storage backends, and a host runner that scans into a sink.

use crate::scan::field_id_projection::build_logical_arrow_schema;
use crate::scan::raw_scan::{build_scan_sql, register_files};
use crate::scan::session_config_for_spec;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, LogicalField, ScanSpec, ScanStorage,
    StorageBackend, StorageProps,
};
use crate::scan::{
    FieldIdExprAdapterFactory, FieldIdResolution, ResolvedScanStorage, resolve_scan_storage,
};
use ::object_store::ObjectStoreExt;
use ::object_store::memory::InMemory;
use ::object_store::path::Path as ObjectPath;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

/// `batch` as Parquet bytes, cut into row groups of `rows_per_row_group` rows when given.
pub(super) fn parquet_bytes(batch: RecordBatch, rows_per_row_group: Option<usize>) -> Vec<u8> {
    let props = rows_per_row_group.map(|rows| {
        parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows))
            .build()
    });
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), props).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

pub(super) fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// The bucket [`minimal_spec`]'s storage names, served from [`MemoryParquet`].
const MEMORY_BUCKET: &str = "s3://test-bucket";

/// Parquet objects held in memory under [`minimal_spec`]'s bucket, so a scan decodes real
/// Parquet without writing a file.
#[derive(Default)]
pub(super) struct MemoryParquet {
    store: Arc<InMemory>,
}

impl MemoryParquet {
    /// The directory URL the objects sit under, usable as a spec's table root.
    pub(super) fn root(&self) -> String {
        format!("{MEMORY_BUCKET}/")
    }

    /// Stores `batch` as the Parquet object `name` and returns its file entry.
    pub(super) fn put(&self, name: &str, batch: RecordBatch) -> FileEntry {
        let bytes = parquet_bytes(batch, None);
        let size = bytes.len() as u64;
        futures::executor::block_on(self.store.put(&ObjectPath::from(name), bytes.into()))
            .expect("an in-memory put succeeds");
        FileEntry::new(format!("{MEMORY_BUCKET}/{name}"), size)
    }

    /// [`Self::put`] of one batch whose `columns` are all non-nullable.
    pub(super) fn put_columns(&self, name: &str, columns: Vec<(&str, ArrayRef)>) -> FileEntry {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
                .collect::<Vec<_>>(),
        ));
        let arrays = columns.into_iter().map(|(_, array)| array).collect();
        self.put(
            name,
            RecordBatch::try_new(schema, arrays).expect("record batch"),
        )
    }

    /// The Arrow types the footer of the object behind `entry` declares.
    pub(super) fn column_types(&self, entry: &FileEntry) -> Vec<DataType> {
        let name = entry
            .path
            .strip_prefix(&self.root())
            .expect("the entry names an object of this store");
        let bytes = futures::executor::block_on(async {
            self.store.get(&ObjectPath::from(name)).await?.bytes().await
        })
        .expect("the stored object reads back");
        ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("parquet footer")
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    }

    /// Serves the objects to `ctx` under the bucket URL.
    pub(super) fn register(&self, ctx: &SessionContext) {
        ctx.runtime_env().register_object_store(
            &url::Url::parse(MEMORY_BUCKET).expect("bucket url"),
            Arc::clone(&self.store) as Arc<dyn ::object_store::ObjectStore>,
        );
    }
}

pub(super) async fn run_scan(spec: &ScanSpec, files: &MemoryParquet) -> Vec<RecordBatch> {
    try_run_scan(spec, files)
        .await
        .expect("scan must read the assigned files")
}

pub(super) async fn try_run_scan(
    spec: &ScanSpec,
    files: &MemoryParquet,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new_with_config(session_config_for_spec(spec));
    files.register(&ctx);
    register_files(&ctx, "scan_target", spec, &inline_resolved(spec))
        .await
        .expect("register_files must succeed with a logical schema");
    let sql = build_scan_sql(&ctx, "scan_target", spec)
        .await
        .expect("build_scan_sql");
    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    df.collect().await
}

/// A non-nullable logical field bound by its own name.
pub(crate) fn logical_field(name: &str, arrow_type: &str) -> LogicalField {
    LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable: false,
        initial_default: None,
        nested: None,
        physical_name: None,
    }
}

/// What the raw scan installs for an unpartitioned table with no name mapping.
pub(crate) fn column_binding_for(
    logical_schema: &[LogicalField],
    table_root: &str,
) -> (SchemaRef, FieldIdExprAdapterFactory) {
    let resolution = FieldIdResolution::for_logical_schema(logical_schema, &[])
        .expect("the logical schema's initial defaults reconstruct");
    (
        build_logical_arrow_schema(logical_schema),
        FieldIdExprAdapterFactory {
            resolution,
            table_root: table_root.to_string(),
        },
    )
}

pub(super) fn minimal_spec() -> ScanSpec {
    CommonScanSpec {
        storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
            endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            access_key: "testkey".into(),
            secret_key: "testsecret".into(),
            allow_http: true,
            ..Default::default()
        })),
        ..Default::default()
    }
    .with_files(vec![FileEntry::new(
        "s3://test-bucket/data/part-0.parquet",
        1024,
    )])
}

pub(crate) trait WithFiles {
    fn with_files(self, files: Vec<FileEntry>) -> ScanSpec;
}

impl WithFiles for CommonScanSpec {
    fn with_files(self, files: Vec<FileEntry>) -> ScanSpec {
        ScanSpec {
            common: self,
            files,
        }
    }
}

pub(super) fn join_spec(storage: ScanStorage) -> JoinSpec {
    JoinSpec {
        table_root: String::new(),
        files: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        fact_alias: "LHS_T1".into(),
        dimension_alias: "LHS_T0".into(),
        condition: r#""LHS_T0"."D_KEY" = "LHS_T1"."F_KEY""#.into(),
        post_join_limit: None,
        post_join_order_by: Vec::new(),
        partition_columns: Vec::new(),
        storage,
    }
}

/// The production resolution of a spec whose storage values are all inline.
pub(super) fn inline_resolved(spec: &ScanSpec) -> ResolvedScanStorage {
    resolve_scan_storage(&spec.common, &exasol_udf_sdk::test_support::DefaultsCtx)
        .expect("inline storage resolves without a CONNECTION")
}

impl ResolvedScanStorage {
    /// The production resolution of `primary` and an optional dimension-side `join` backend.
    pub(crate) fn from_backends(primary: StorageBackend, join: Option<StorageBackend>) -> Self {
        let common = CommonScanSpec {
            storage: ScanStorage::Inline(primary),
            join: join.map(|backend| join_spec(ScanStorage::Inline(backend))),
            ..Default::default()
        };
        inline_resolved(&common.with_files(Vec::new()))
    }
}

pub(super) const TEST_CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";

pub(super) fn refusing_endpoint(message: &str) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let body =
        format!("<Error><Code>SignatureDoesNotMatch</Code><Message>{message}</Message></Error>");
    let resp = format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            let _ = std::io::Read::read(&mut s, &mut [0u8; 4096]);
            let _ = std::io::Write::write_all(&mut s, resp.as_bytes());
        }
    });
    url
}

pub(super) fn refusing_backend(endpoint: &str, secret: &str) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: endpoint.into(),
        region: "us-east-1".into(),
        access_key: "testkey".into(),
        secret_key: secret.into(),
        allow_http: true,
        ..Default::default()
    })
}

pub(super) struct SinkCtx;

impl exasol_udf_sdk::context::UdfContext for SinkCtx {
    fn input_column_count(&self) -> usize {
        0
    }
    fn get(
        &self,
        _: usize,
    ) -> Result<&exasol_udf_sdk::value::Value, exasol_udf_sdk::error::UdfError> {
        unimplemented!()
    }
    fn input_column(
        &self,
        idx: usize,
    ) -> Result<&exasol_udf_sdk::value::ColumnInfo, exasol_udf_sdk::error::UdfError> {
        Err(exasol_udf_sdk::error::UdfError::Type(format!(
            "input column {idx} out of range"
        )))
    }
    fn output_column_count(&self) -> usize {
        0
    }
    fn output_column(
        &self,
        idx: usize,
    ) -> Result<&exasol_udf_sdk::value::ColumnInfo, exasol_udf_sdk::error::UdfError> {
        Err(exasol_udf_sdk::error::UdfError::Type(format!(
            "output column {idx} out of range"
        )))
    }
    fn emit(
        &mut self,
        _: Vec<exasol_udf_sdk::value::Value>,
    ) -> Result<(), exasol_udf_sdk::error::UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, exasol_udf_sdk::error::UdfError> {
        Ok(false)
    }
    fn emit_record_batch_ffi(
        &mut self,
        _: *mut core::ffi::c_void,
        _: *mut core::ffi::c_void,
    ) -> Result<(), exasol_udf_sdk::error::UdfError> {
        Ok(())
    }
}
