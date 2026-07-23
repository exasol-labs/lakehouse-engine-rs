//! Host integration test for `schema.name-mapping.default` field-id resolution
//! (task 4.1, `specs/_plans/change-name-mapping-fallback/plan.md`).
//!
//! Docker-free: drives the production raw-scan pipeline
//! (`run_raw_scan_with_session` -> `build_dataframe` -> `register_files` ->
//! `rename_physical_to_logical`) against a local `file://` Parquet written via
//! `ArrowWriter`, exactly mirroring the harness in `scan_no_head_test.rs`.
//!
//! Two scenarios (see plan.md "Verification" table):
//!
//! 1. `name_mapping_resolves_no_field_id_column` — a Parquet file whose column
//!    carries NO embedded `PARQUET:field_id` and whose physical name
//!    (`old_col`) differs from the CURRENT logical name (`new_col`) still
//!    resolves to real, non-NULL values under the logical name when
//!    `ScanSpec::name_mapping` maps `old_col` -> the `new_col` field-id. Without
//!    the name-mapping resolution step this would NULL-fill the column instead
//!    (the logical field is nullable specifically so the unresolved case is a
//!    silent NULL, not a hard error, making "never NULL" the meaningful proof).
//! 2. `empty_name_mapping_preserves_physical_name_binding` — the pre-existing
//!    physical-name-identity fallback still binds correctly when
//!    `name_mapping` is empty and the physical name already equals the current
//!    logical name.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, ScanSpec, StorageProps,
};
use lakehouse_engine::scan::{run_raw_scan_with_session, session_config_for_spec};
use object_store::local::LocalFileSystem;
use parquet::arrow::ArrowWriter;

/// A fake `UdfContext` serving one input row and decoding every emitted Arrow
/// IPC batch — copied from `scan_no_head_test.rs` (private to that file, so
/// this integration test binary needs its own copy).
struct FakeCtx {
    served: bool,
    emitted: Vec<RecordBatch>,
}

impl FakeCtx {
    fn new() -> Self {
        Self {
            served: false,
            emitted: Vec::new(),
        }
    }
}

impl UdfContext for FakeCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::User("FakeCtx has no input columns".into()))
    }
    fn get_string(&self, _col: usize) -> Result<Option<&str>, UdfError> {
        Ok(None)
    }
    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        Err(UdfError::User("raw path must use emit_batch".into()))
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        if self.served {
            Ok(false)
        } else {
            self.served = true;
            Ok(true)
        }
    }
    fn debug_level(&self) -> tracing::Level {
        tracing::Level::INFO
    }
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), UdfError> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let reader = StreamReader::try_new(Cursor::new(ipc), None)
            .map_err(|e| UdfError::User(format!("ipc decode: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| UdfError::User(format!("ipc batch: {e}")))?;
            self.emitted.push(batch);
        }
        Ok(())
    }
}

/// Storage props are never dialed for a local `file://` scan; a placeholder
/// keeps the spec well-formed (copied from `scan_no_head_test.rs`).
fn dummy_storage() -> StorageProps {
    StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "s".into(),
        session_token: None,
        allow_http: true,
        path_style: true,
    }
}

/// Write a local Parquet at `dir/relative` with two Int64 columns
/// (`id_col_name`, `other_col_name`), NEITHER carrying `PARQUET:field_id`
/// metadata — the "file written before field-id support" shape both scenarios
/// need. `id` takes values `0..rows`; the other column takes `10 * id`.
/// Returns the file's absolute `file://` URL.
fn write_local_parquet_two_int_cols(
    dir: &std::path::Path,
    relative: &str,
    id_col_name: &str,
    other_col_name: &str,
    rows: i64,
) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new(id_col_name, DataType::Int64, false),
        Field::new(other_col_name, DataType::Int64, true),
    ]));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let ids: Vec<i64> = (0..rows).collect();
    let others: Vec<i64> = (0..rows).map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(others)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

/// Build a raw-scan `ScanSpec` over one local file, carrying `logical_schema`
/// (so the field-id adapter is installed, `use_field_id_adapter = true`) and
/// `name_mapping`. Projects `ID` + `NEW_COL` (uppercase, matching the
/// adapter's Exasol-identifier-casing convention exercised by
/// `scan_no_head_test.rs`'s `raw_spec`).
fn name_mapping_spec(
    file_url: String,
    file_size: u64,
    logical_schema: Vec<LogicalField>,
    name_mapping: Vec<NameMappingEntry>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root: String::new(),
            projection: vec!["ID".into(), "NEW_COL".into()],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            emit_exa_types: Vec::new(),
            logical_schema,
            name_mapping,
            join: None,
            storage: dummy_storage(),
            df_target_partitions: 1,
            df_batch_size: 64,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        },
        files: vec![FileEntry::new(file_url, file_size)],
    }
}

/// The logical schema shared by both scenarios: `id` (field-id 1, required)
/// and `new_col` (field-id 2, the CURRENT logical name, nullable). Nullable so
/// an unresolved binding would silently NULL-fill rather than hard-error —
/// the meaningful "never NULL" proof for scenario 1.
fn logical_schema() -> Vec<LogicalField> {
    vec![
        LogicalField {
            field_id: 1,
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
        },
        LogicalField {
            field_id: 2,
            name: "new_col".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: None,
        },
    ]
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

/// Run the production raw scan for `spec` against a session whose `file://`
/// object store is a plain `LocalFileSystem` (no HEAD interception needed —
/// this test proves name-mapping resolution, not the no-HEAD size mechanism
/// `scan_no_head_test.rs` already covers). Returns the decoded emitted batches.
async fn run_scan(spec: &ScanSpec, register_url: &str) -> Vec<RecordBatch> {
    let session = datafusion::execution::context::SessionContext::new_with_config(
        session_config_for_spec(spec),
    );
    session.runtime_env().register_object_store(
        &url::Url::parse(register_url).expect("register url"),
        Arc::new(LocalFileSystem::new()),
    );
    let mut ctx = FakeCtx::new();
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers)
        .await
        .expect("raw scan must succeed");
    ctx.emitted
}

/// Extract `(id, new_col)` pairs from the emitted batches, asserting the
/// second column is named `NEW_COL` (the CURRENT logical name, uppercased —
/// never `OLD_COL`) and recording whether each `new_col` value was NULL.
fn id_to_new_col(batches: &[RecordBatch]) -> HashMap<i64, Option<i64>> {
    let mut out = HashMap::new();
    for b in batches {
        assert_eq!(
            b.schema().field(1).name(),
            "NEW_COL",
            "the renamed column must be emitted under its CURRENT logical name"
        );
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id col");
        let values = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("new_col col");
        for i in 0..b.num_rows() {
            let value = if values.is_null(i) {
                None
            } else {
                Some(values.value(i))
            };
            out.insert(ids.value(i), value);
        }
    }
    out
}

/// A Parquet file whose column carries NO embedded `PARQUET:field_id` and
/// whose physical name (`old_col`) differs from the current logical name
/// (`new_col`, field-id 2). A `ScanSpec::name_mapping` entry mapping
/// `old_col` -> field-id 2 must resolve the column: every row's `NEW_COL`
/// value is the REAL value from the file (`10 * id`), never NULL — proving
/// the rename was resolved via the name-mapping, not a physical-name-identity
/// match (which would fail here since the names differ).
#[test]
fn name_mapping_resolves_no_field_id_column() {
    let dir = std::env::temp_dir().join(format!("lh_name_mapping_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let rows = 30;
    let file_url = write_local_parquet_two_int_cols(&dir, "renamed.parquet", "id", "old_col", rows);
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let spec = name_mapping_spec(
        file_url.clone(),
        file_size,
        logical_schema(),
        vec![NameMappingEntry {
            name: "old_col".to_string(),
            field_id: 2,
        }],
    );

    let batches = block_on(run_scan(&spec, &file_url));
    let by_id = id_to_new_col(&batches);
    assert_eq!(by_id.len(), rows as usize, "row count");

    for (id, value) in &by_id {
        assert_eq!(
            *value,
            Some(id * 10),
            "row {id}: NEW_COL must carry the real file value via name-mapping resolution, never NULL"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Companion case: `name_mapping` is empty AND the Parquet column's physical
/// name already equals the current logical name (`new_col`), so the
/// PRE-EXISTING physical-name-identity fallback is what resolves the column —
/// proving the new name-mapping step did not regress that fallback.
#[test]
fn empty_name_mapping_preserves_physical_name_binding() {
    let dir = std::env::temp_dir().join(format!("lh_name_mapping_empty_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let rows = 30;
    let file_url =
        write_local_parquet_two_int_cols(&dir, "identity.parquet", "id", "new_col", rows);
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let spec = name_mapping_spec(file_url.clone(), file_size, logical_schema(), vec![]);

    let batches = block_on(run_scan(&spec, &file_url));
    let by_id = id_to_new_col(&batches);
    assert_eq!(by_id.len(), rows as usize, "row count");

    for (id, value) in &by_id {
        assert_eq!(
            *value,
            Some(id * 10),
            "row {id}: physical-name fallback must still bind NEW_COL correctly with an empty name_mapping"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
