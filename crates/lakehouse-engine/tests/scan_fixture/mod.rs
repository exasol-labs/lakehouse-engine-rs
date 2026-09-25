#![allow(dead_code)]

use lakehouse_engine::scan::ResolvedScanStorage;
use lakehouse_engine::scan::spec::{ScanSpec, ScanStorage, StorageBackend};

pub fn resolved_storage(spec: &ScanSpec) -> ResolvedScanStorage {
    ResolvedScanStorage::from_backends(
        inline_backend(&spec.common.storage),
        spec.common
            .join
            .as_ref()
            .map(|join| inline_backend(&join.storage)),
    )
}

fn inline_backend(storage: &ScanStorage) -> StorageBackend {
    match storage {
        ScanStorage::Inline(backend) => backend.clone(),
        other => panic!("this fixture builds an inline storage value, not {other:?}"),
    }
}

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use exasol_udf_sdk::connect_back::{ConnectionObject, ExaConnection};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::{ColumnInfo, ExaType, Value};
use std::io::Cursor;

/// The declared output columns of a scan call, one per generated `EMITS` item,
/// in the shape `UdfContext::output_column` reports them.
///
/// `typ` is the authority the emit boundary reads; `type_name` carries the
/// Exasol spelling of that variant purely so a failing assertion is readable.
/// The two integer bins render at the widest declaration that bins to them,
/// because `ExaType` does not carry the declared precision.
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

/// The `ExaType` Exasol reports for a `VARCHAR(2000000)` declaration.
pub fn varchar() -> ExaType {
    ExaType::String { size: 2_000_000 }
}

/// The `ExaType` Exasol reports for a `DECIMAL(precision, scale)` declaration
/// it binned to NUMERIC rather than to one of the two integer bins.
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

/// Wraps [`TestContext`] and overrides [`UdfContext::emit_record_batch_ipc`] to
/// decode Arrow IPC bytes into captured `RecordBatch` values.
///
/// `TestContext` leaves `emit_record_batch_ipc` at the trait default, which
/// returns `UdfError::Unimplemented`. The raw scan path emits through that
/// method (via the `EmitBatch::emit_batch` blanket), so scan integration tests
/// need this wrapper to capture the emitted batches. Every other `UdfContext`
/// method delegates to the inner `TestContext`.
pub struct BatchCapturingCtx {
    inner: TestContext,
    /// One entry per `emit_record_batch_ipc` call; each entry holds the batches
    /// decoded from that call's IPC payload.
    calls: Vec<Vec<RecordBatch>>,
}

impl BatchCapturingCtx {
    pub fn new(inner: TestContext) -> Self {
        Self {
            inner,
            calls: Vec::new(),
        }
    }

    /// A capturing context whose call site declared `types` as its `EMITS` list,
    /// so the emit boundary reads those columns back through `output_column`.
    pub fn declaring(inner: TestContext, types: &[ExaType]) -> Self {
        Self::new(inner.with_output_columns(output_columns(types)))
    }

    /// All decoded batches across every `emit_record_batch_ipc` call, flattened.
    pub fn batches(&self) -> Vec<&RecordBatch> {
        self.calls.iter().flat_map(|c| c.iter()).collect()
    }

    /// All decoded batches across every `emit_record_batch_ipc` call, flattened
    /// and consumed.
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.calls.into_iter().flatten().collect()
    }

    /// Number of `emit_record_batch_ipc` calls received.
    pub fn call_count(&self) -> usize {
        self.calls.len()
    }

    /// Total row count across all decoded batches in all calls.
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
    fn connect_back(
        &mut self,
        conn: &ConnectionObject,
    ) -> Result<Box<dyn ExaConnection>, UdfError> {
        self.inner.connect_back(conn)
    }
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), UdfError> {
        let reader = StreamReader::try_new(Cursor::new(ipc), None)
            .map_err(|e| UdfError::User(format!("ipc decode: {e}")))?;
        let mut batches = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|e| UdfError::User(format!("ipc batch: {e}")))?;
            batches.push(batch);
        }
        self.calls.push(batches);
        Ok(())
    }
}
