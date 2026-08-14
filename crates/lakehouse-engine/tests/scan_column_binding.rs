//! Host integration test for the column-binding resolution order in
//! `crates/lakehouse-engine/src/scan/field_id_projection.rs::bind_columns`:
//! embedded `PARQUET:field_id` -> declared `physical_name` -> `name_mapping`
//! -> identity. Docker-free: drives the production raw-scan pipeline
//! (`run_raw_scan_with_session` -> `build_dataframe` -> `register_files` ->
//! `bind_columns`) against local `file://` Parquet written via `ArrowWriter`,
//! mirroring the harness in `scan_name_mapping.rs` / `scan_no_head_test.rs`.
//!
//! Two scenarios:
//!
//! 1. `declared_physical_name_binds_the_renamed_physical_column` — a
//!    `LogicalField` declaring `physical_name` binds the matching physical
//!    column, and wins over a `name_mapping` entry that covers the same
//!    physical name for a DIFFERENT logical field.
//! 2. `identity_bound_fields_bind_by_name_and_keep_the_default_fill_semantics`
//!    — a `LogicalField` with neither `field_id` nor `physical_name` binds by
//!    its own name: absent-nullable NULL-fills, absent-with-`initial_default`
//!    substitutes the default, and absent-required-without-default errors
//!    cleanly rather than panicking.

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, ProjectionItem, ScanSpec,
    StorageBackend, StorageProps,
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
fn dummy_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "s".into(),
        allow_http: true,
        ..Default::default()
    })
}

/// Write a local Parquet at `dir/relative` with the given Int64 columns
/// (name, nullable) pairs, NONE carrying `PARQUET:field_id` metadata — every
/// binding under test here resolves without an embedded field-id. `id`
/// (assumed to be the first column) takes values `0..rows`; every other
/// column takes `10 * id`. Returns the file's absolute `file://` URL.
fn write_local_parquet(
    dir: &std::path::Path,
    relative: &str,
    columns: &[(&str, bool)],
    rows: i64,
) -> String {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, nullable)| Field::new(*name, DataType::Int64, *nullable))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let ids: Vec<i64> = (0..rows).collect();
    let arrays: Vec<Arc<dyn Array>> = columns
        .iter()
        .enumerate()
        .map(|(index, _)| {
            // The first column is always `id`, carrying the identity values,
            // every other column carries `10 * id`.
            let values: Vec<i64> = if index == 0 {
                ids.clone()
            } else {
                ids.iter().map(|i| i * 10).collect()
            };
            Arc::new(Int64Array::from(values)) as _
        })
        .collect();
    let batch = RecordBatch::try_new(schema, arrays).expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn raw_scan_spec(
    file_url: String,
    file_size: u64,
    projection: Vec<&str>,
    logical_schema: Vec<LogicalField>,
    name_mapping: Vec<NameMappingEntry>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: projection
                .into_iter()
                .map(|name| ProjectionItem::Column(name.to_string()))
                .collect(),
            logical_schema,
            name_mapping,
            storage: dummy_storage(),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, file_size)],
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

/// Run the production raw scan for `spec` against a session whose `file://`
/// object store is a plain `LocalFileSystem` (no HEAD interception needed).
async fn run_scan(spec: &ScanSpec, register_url: &str) -> Result<Vec<RecordBatch>, UdfError> {
    let session = datafusion::execution::context::SessionContext::new_with_config(
        session_config_for_spec(spec),
    );
    session.runtime_env().register_object_store(
        &url::Url::parse(register_url).expect("register url"),
        Arc::new(LocalFileSystem::new()),
    );
    let mut ctx = FakeCtx::new();
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers).await?;
    Ok(ctx.emitted)
}

fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column(batch.schema().index_of(name).expect("column present"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column")
}

/// A `LogicalField` declaring `physical_name: Some("col-abc")` binds the
/// physical column of that name (`AMOUNT` carries the real `10 * id`
/// values), and that declared binding wins even though a `name_mapping`
/// entry ALSO covers `col-abc` for a DIFFERENT logical field (`OTHER`,
/// bound by field-id 7): had `name_mapping` won instead, `OTHER` would carry
/// the real values and `AMOUNT` would be the one left NULL — the reverse of
/// what this test asserts.
#[test]
fn declared_physical_name_binds_the_renamed_physical_column() {
    let dir = std::env::temp_dir().join(format!(
        "lh_column_binding_physical_name_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let rows = 20;
    let file_url = write_local_parquet(
        &dir,
        "physical_name.parquet",
        &[("id", false), ("col-abc", true)],
        rows,
    );
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let logical_schema = vec![
        LogicalField {
            field_id: None,
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "amount".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: None,
            physical_name: Some("col-abc".to_string()),
        },
        LogicalField {
            field_id: Some(7),
            name: "other".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: None,
            physical_name: None,
        },
    ];
    let name_mapping = vec![NameMappingEntry {
        name: "col-abc".to_string(),
        field_id: 7,
    }];
    let spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec!["ID", "AMOUNT", "OTHER"],
        logical_schema,
        name_mapping,
    );

    let batches = block_on(run_scan(&spec, &file_url)).expect("raw scan must succeed");
    let mut row_count = 0;
    for batch in &batches {
        let id_values = int64_column(batch, "ID");
        let amounts = int64_column(batch, "AMOUNT");
        let others = int64_column(batch, "OTHER");
        for i in 0..batch.num_rows() {
            let id = id_values.value(i);
            assert!(
                !amounts.is_null(i),
                "row {id}: AMOUNT must bind to col-abc via the declared physical_name, never NULL"
            );
            assert_eq!(
                amounts.value(i),
                id * 10,
                "row {id}: AMOUNT must carry the real physical value"
            );
            assert!(
                others.is_null(i),
                "row {id}: OTHER must stay unbound — name_mapping must NOT also claim col-abc"
            );
        }
        row_count += id_values.len();
    }
    assert_eq!(row_count, rows as usize, "row count");

    let _ = std::fs::remove_dir_all(&dir);
}

/// A `LogicalField` with neither `field_id` nor `physical_name` set binds by
/// its own name (identity): present -> real value, absent-nullable ->
/// NULL-fill, absent-with-`initial_default` -> the default value, and
/// absent-required-without-default -> a clean scan error, never a panic.
#[test]
fn identity_bound_fields_bind_by_name_and_keep_the_default_fill_semantics() {
    let dir =
        std::env::temp_dir().join(format!("lh_column_binding_identity_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let rows = 15;
    let file_url = write_local_parquet(&dir, "identity.parquet", &[("id", false)], rows);
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let id_field = || LogicalField {
        field_id: None,
        name: "id".to_string(),
        arrow_type: "int64".to_string(),
        nullable: false,
        initial_default: None,
        physical_name: None,
    };

    // Absent nullable field (no default) NULL-fills; absent field with an
    // `initial_default` substitutes it instead of NULL.
    let logical_schema = vec![
        id_field(),
        LogicalField {
            field_id: None,
            name: "extra_nullable".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "extra_default".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: Some("42".to_string()),
            physical_name: None,
        },
    ];
    let spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec!["ID", "EXTRA_NULLABLE", "EXTRA_DEFAULT"],
        logical_schema,
        vec![],
    );
    let batches = block_on(run_scan(&spec, &file_url)).expect("raw scan must succeed");
    let mut row_count = 0;
    for batch in &batches {
        let ids = int64_column(batch, "ID");
        let nullable = int64_column(batch, "EXTRA_NULLABLE");
        let defaulted = int64_column(batch, "EXTRA_DEFAULT");
        for i in 0..batch.num_rows() {
            let id = ids.value(i);
            assert!(
                nullable.is_null(i),
                "row {id}: an absent nullable identity-bound field with no default must NULL-fill"
            );
            assert!(
                !defaulted.is_null(i),
                "row {id}: an absent identity-bound field with an initial_default must not NULL-fill"
            );
            assert_eq!(
                defaulted.value(i),
                42,
                "row {id}: an absent identity-bound field must substitute its initial_default"
            );
        }
        row_count += ids.len();
    }
    assert_eq!(row_count, rows as usize, "row count");

    // Absent REQUIRED identity-bound field with no default must error
    // cleanly, naming the column, never panic.
    let required_missing_schema = vec![
        id_field(),
        LogicalField {
            field_id: None,
            name: "mandatory".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            physical_name: None,
        },
    ];
    let error_spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec!["ID", "MANDATORY"],
        required_missing_schema,
        vec![],
    );
    let err = block_on(run_scan(&error_spec, &file_url))
        .expect_err("an absent required identity-bound field with no default must error");
    let text = err.to_string();
    assert!(
        text.contains("mandatory"),
        "error must name the missing required column: {text}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
