//! Fixtures shared by the engine's host test binaries: a test runtime, file URLs, storage
//! backends and their resolution, logical fields, join specs, declared output columns, and a
//! batch-capturing UDF context.
#![allow(dead_code)]

pub mod concurrency_probe;

use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, LogicalField, ScanSpec, ScanStorage,
    StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{ResolvedScanStorage, resolve_scan_storage};

pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// An inline S3 backend whose secret is distinctive, so redacting it never rewrites the rest of
/// an error's text.
pub fn dummy_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "SECRETKEY".into(),
        allow_http: true,
        ..Default::default()
    })
}

/// Writes `batch` as the Parquet file `path`, creating its parent directories, and returns its
/// file URL.
pub fn write_parquet(
    path: &std::path::Path,
    batch: &RecordBatch,
    props: Option<parquet::file::properties::WriterProperties>,
) -> String {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(path).expect("create parquet file");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), props).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
    file_url(path)
}

pub fn file_url(path: &std::path::Path) -> String {
    url::Url::from_file_path(path)
        .expect("absolute path")
        .to_string()
}

/// A directory URL with the trailing slash a table root needs.
pub fn dir_url(path: &std::path::Path) -> String {
    let mut url = file_url(path);
    if !url.ends_with('/') {
        url.push('/');
    }
    url
}

/// The production resolution of a spec whose storage values are all inline.
pub fn resolved_storage(spec: &ScanSpec) -> ResolvedScanStorage {
    resolve_scan_storage(&spec.common, &exasol_udf_sdk::test_support::DefaultsCtx)
        .expect("inline storage resolves without a CONNECTION")
}

/// A non-nullable logical field bound by its own name.
pub fn logical_field(name: &str, arrow_type: &str) -> LogicalField {
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

pub fn join_spec(storage: ScanStorage) -> JoinSpec {
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

use arrow::record_batch::RecordBatch;
use exasol_udf_sdk::ConnectionObject;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::{ColumnInfo, ExaType, Value};

/// `type_name` is only for readable assertion failures; the emit boundary reads `typ`.
/// Integer bins render at their widest declaration since `ExaType` has no precision.
pub fn output_columns(types: &[ExaType]) -> Vec<ColumnInfo> {
    types
        .iter()
        .enumerate()
        .map(|(idx, typ)| ColumnInfo {
            name: format!("C{idx}"),
            type_name: declared_type_name(typ),
            size: declared_size(typ),
            precision: declared_precision(typ),
            scale: declared_scale(typ),
            typ: typ.clone(),
        })
        .collect()
}

pub fn varchar() -> ExaType {
    ExaType::String { size: 2_000_000 }
}

pub fn decimal(precision: u32, scale: u32) -> ExaType {
    ExaType::Numeric { precision, scale }
}

fn declared_type_name(typ: &ExaType) -> String {
    match typ {
        ExaType::Boolean => "BOOLEAN".to_string(),
        ExaType::Double => "DOUBLE PRECISION".to_string(),
        ExaType::Int32 => "DECIMAL(9,0)".to_string(),
        ExaType::Int64 => "DECIMAL(18,0)".to_string(),
        ExaType::Numeric { precision, scale } => format!("DECIMAL({precision},{scale})"),
        ExaType::String { size } => format!("VARCHAR({size})"),
        ExaType::Char { size } => format!("CHAR({size})"),
        ExaType::Date => "DATE".to_string(),
        ExaType::Timestamp { precision } => format!("TIMESTAMP({precision})"),
        ExaType::Unsupported => "UNSUPPORTED".to_string(),
    }
}

fn declared_size(typ: &ExaType) -> Option<u32> {
    match typ {
        ExaType::String { size } | ExaType::Char { size } => Some(*size),
        _ => None,
    }
}

fn declared_precision(typ: &ExaType) -> Option<u32> {
    match typ {
        ExaType::Numeric { precision, .. } => Some(*precision),
        _ => None,
    }
}

fn declared_scale(typ: &ExaType) -> Option<u32> {
    match typ {
        ExaType::Numeric { scale, .. } => Some(*scale),
        _ => None,
    }
}

/// `TestContext` leaves `emit_record_batch_ffi` unimplemented; this captures the raw scan's
/// batch emits, imported through the C Data Interface as the SLC host does.
pub struct BatchCapturingCtx {
    inner: TestContext,
    calls: Vec<Vec<RecordBatch>>,
}

impl BatchCapturingCtx {
    pub fn new(inner: TestContext) -> Self {
        Self {
            inner,
            calls: Vec::new(),
        }
    }

    pub fn declaring(inner: TestContext, types: &[ExaType]) -> Self {
        Self::new(inner.with_output_columns(output_columns(types)))
    }

    pub fn batches(&self) -> Vec<&RecordBatch> {
        self.calls.iter().flat_map(|c| c.iter()).collect()
    }

    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.calls.into_iter().flatten().collect()
    }

    pub fn call_count(&self) -> usize {
        self.calls.len()
    }

    pub fn total_rows(&self) -> u64 {
        self.calls
            .iter()
            .flat_map(|c| c.iter())
            .map(|b| b.num_rows() as u64)
            .sum()
    }
}

impl UdfContext for BatchCapturingCtx {
    fn input_column_count(&self) -> usize {
        self.inner.input_column_count()
    }
    fn input_column(&self, idx: usize) -> Result<&ColumnInfo, UdfError> {
        self.inner.input_column(idx)
    }
    fn output_column_count(&self) -> usize {
        self.inner.output_column_count()
    }
    fn output_column(&self, idx: usize) -> Result<&ColumnInfo, UdfError> {
        self.inner.output_column(idx)
    }
    fn get(&self, col: usize) -> Result<&Value, UdfError> {
        self.inner.get(col)
    }
    fn emit(&mut self, values: Vec<Value>) -> Result<(), UdfError> {
        self.inner.emit(values)
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        self.inner.next()
    }
    fn set_return(&mut self, value: Option<Value>) -> Result<(), UdfError> {
        self.inner.set_return(value)
    }
    fn memory_limit(&self) -> u64 {
        self.inner.memory_limit()
    }
    fn session_id(&self) -> u64 {
        self.inner.session_id()
    }
    fn statement_id(&self) -> u32 {
        self.inner.statement_id()
    }
    fn node_id(&self) -> u32 {
        self.inner.node_id()
    }
    fn node_count(&self) -> u32 {
        self.inner.node_count()
    }
    fn vm_id(&self) -> u64 {
        self.inner.vm_id()
    }
    fn database_name(&self) -> String {
        self.inner.database_name()
    }
    fn database_version(&self) -> String {
        self.inner.database_version()
    }
    fn script_name(&self) -> String {
        self.inner.script_name()
    }
    fn script_schema(&self) -> String {
        self.inner.script_schema()
    }
    fn current_user(&self) -> Option<String> {
        self.inner.current_user()
    }
    fn current_schema(&self) -> Option<String> {
        self.inner.current_schema()
    }
    fn scope_user(&self) -> Option<String> {
        self.inner.scope_user()
    }
    fn debug_level(&self) -> tracing::Level {
        self.inner.debug_level()
    }
    fn cluster_ip(&self) -> Result<String, UdfError> {
        self.inner.cluster_ip()
    }
    fn connection(&self, name: &str) -> Result<ConnectionObject, UdfError> {
        self.inner.connection(name)
    }
    fn emit_record_batch_ffi(
        &mut self,
        array: *mut core::ffi::c_void,
        schema: *mut core::ffi::c_void,
    ) -> Result<(), UdfError> {
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
        let array =
            unsafe { std::ptr::replace(array.cast::<FFI_ArrowArray>(), FFI_ArrowArray::empty()) };
        let schema = unsafe { &*schema.cast::<FFI_ArrowSchema>() };
        let data = unsafe { arrow::ffi::from_ffi(array, schema) }
            .map_err(|e| UdfError::User(format!("ffi import: {e}")))?;
        self.calls
            .push(vec![RecordBatch::from(arrow::array::StructArray::from(
                data,
            ))]);
        Ok(())
    }
}

pub trait WithFiles {
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
