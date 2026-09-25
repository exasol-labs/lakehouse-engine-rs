use arrow::datatypes::DataType;
use datafusion::common::config::TableParquetOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::scan::emit::{classify_scan_error, emit_stream};
use crate::scan::render_nested_column_as_json;
use crate::scan::spec::{
    FileEntry, NameMappingEntry, ProjectionItem, ScanSpec, reconstruct_abs_uri,
    render_order_by_clause,
};
use crate::scan::storage_ref::ResolvedScanStorage;
use crate::scan::{diagnostics, emit_phase_telemetry};
use crate::types::mapping::{needs_json_fallback, needs_nested_json_rendering};

use super::field_id_projection::{
    FieldIdResolution, build_logical_arrow_schema, index_declared_physical_names,
    index_nested_members, reconstruct_initial_defaults,
};
use super::object_store::validate_uniform_object_store_files;
use super::partition_values::PartitionedScanSchema;
use super::sql_support::{build_alias_items, quote_ident};

/// Exposed so a host integration test can drive the production streaming + telemetry path
/// against a local Parquet file.
pub async fn run_raw_scan_with_session(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
    timers: &mut diagnostics::PhaseTimers,
) -> Result<(), UdfError> {
    let secrets = storage.all_secret_values();
    let df = build_dataframe(session_ctx, spec, storage).await?;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| classify_scan_error(e, &secrets))?;
    emit_stream(ctx, stream, &secrets, timers).await?;
    // Best-effort: a telemetry failure never fails an already-successful scan.
    emit_phase_telemetry(ctx, timers);
    Ok(())
}

/// Bounds every object-store read the delete path issues for one invocation (delete-file bodies
/// and data-file footers alike), shared across all its providers. Sized per call, never at
/// process scope. Clamped to 1 because `Semaphore::new(0)` would deadlock every read.
pub(super) fn delete_path_read_limiter(spec: &ScanSpec) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(spec.common.s3_max_connections.max(1)))
}

async fn build_dataframe(
    ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<datafusion::dataframe::DataFrame, UdfError> {
    let table_name = "scan_target";
    register_files(ctx, table_name, spec, storage).await?;

    let sql = build_scan_sql(ctx, table_name, spec).await?;
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("DataFusion SQL error: {e}")))?;
    // COUNT(DISTINCT) fan-out: each shard streams its distinct projected values and the outer
    // wrapper counts the union. The fan-out spec carries no LIMIT/ORDER BY, so this is exact.
    if spec.common.distinct {
        df.distinct()
            .map_err(|e| UdfError::User(format!("DataFusion distinct error: {e}")))
    } else {
        Ok(df)
    }
}

/// A `ListingTable` cannot attach the per-file base `ParquetAccessPlan` that applies positional
/// deletes, hence the custom provider, used for delete-free files too. Public so integration
/// tests can register the exact production provider as `scan_target`.
pub async fn register_files(
    ctx: &SessionContext,
    table_name: &str,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<(), UdfError> {
    let delete_path_read_limiter = delete_path_read_limiter(spec);
    register_file_list(
        ctx,
        table_name,
        &spec.files,
        &spec.common.table_root,
        &spec.common.logical_schema,
        &spec.common.name_mapping,
        &spec.common.partition_columns,
        storage.primary(),
        delete_path_read_limiter,
    )
    .await
}

/// A non-empty `logical_schema` installs the column-binding adapter (binding by field-id,
/// declared physical name, or own name across schema evolution); otherwise the schema is
/// inferred from the first file. `partition_columns` leave the file schema and are materialized
/// per file (see [`PartitionedScanSchema`]). Each side of a join registers through this same
/// provider, so both apply their own deletes.
///
/// `delete_path_read_limiter` must be the SAME `Arc` for every call in one invocation, including
/// both join sides, so the instance stays within one N-permit budget.
///
/// [`PositionalDeleteScanTable`]: crate::scan::positional_deletes::PositionalDeleteScanTable
#[allow(clippy::too_many_arguments)]
pub(super) async fn register_file_list(
    ctx: &SessionContext,
    table_name: &str,
    files: &[FileEntry],
    table_root: &str,
    logical_schema: &[crate::scan::spec::LogicalField],
    name_mapping: &[NameMappingEntry],
    partition_columns: &[String],
    storage: &crate::scan::spec::StorageBackend,
    delete_path_read_limiter: Arc<Semaphore>,
) -> Result<(), UdfError> {
    let first = files.first().ok_or_else(|| {
        UdfError::User(format!(
            "cannot register '{table_name}': the assigned file list is empty"
        ))
    })?;
    let first_abs = reconstruct_abs_uri(&first.path, table_root);

    // One object store is registered per table, keyed by the first file's scheme+host, so a
    // file under a different root would be read through the wrong store.
    validate_uniform_object_store_files(files, table_root, &first_abs)?;

    let object_store_url = ListingTableUrl::parse(&first_abs)
        .map_err(|e| UdfError::User(format!("invalid listing URL '{first_abs}': {e}")))?
        .object_store();

    // The adapter is installed whenever a logical schema is present, even if every field binds by
    // identity: only it provides per-file NULL-fill, `initial-default` substitution, and the
    // required-absent error.
    let secrets = storage.secret_values();
    let use_field_id_adapter = !logical_schema.is_empty();
    let table_schema = if use_field_id_adapter {
        build_logical_arrow_schema(logical_schema)
    } else {
        let listing_options = ListingOptions::new(Arc::new(int96_coerced_parquet_format()))
            .with_file_extension(".parquet")
            .with_collect_stat(false);
        let first_url = ListingTableUrl::parse(&first_abs)
            .map_err(|e| UdfError::User(format!("invalid listing URL '{first_abs}': {e}")))?;
        listing_options
            .infer_schema(&ctx.state(), &first_url)
            .await
            .map_err(|e| classify_scan_error(e, &secrets))?
    };

    // Built once per registration; a malformed encoded default surfaces as a user error.
    let field_id_resolution = FieldIdResolution {
        name_mapping: name_mapping.to_vec(),
        declared_physical_names: index_declared_physical_names(logical_schema),
        defaults: reconstruct_initial_defaults(logical_schema).map_err(UdfError::User)?,
        nested_members: index_nested_members(logical_schema),
    };

    // Partition columns have no physical counterpart in any data file.
    let schema = PartitionedScanSchema::split(table_schema, partition_columns)
        .map_err(|e| UdfError::User(format!("cannot register '{table_name}': {e}")))?;

    let table = crate::scan::positional_deletes::PositionalDeleteScanTable::new(
        object_store_url,
        schema,
        use_field_id_adapter,
        field_id_resolution,
        files.to_vec(),
        table_root.to_string(),
        storage,
        delete_path_read_limiter,
    );

    ctx.register_table(table_name, Arc::new(table))
        .map_err(|e| UdfError::User(format!("failed to register table: {e}")))?;

    Ok(())
}

/// Matches Iceberg's own readers, which read INT96 as microseconds.
const INT96_COERCE_TIME_UNIT: &str = "us";
/// An INT96 instant is UTC.
const INT96_COERCE_TZ: &str = "UTC";

/// arrow-rs otherwise decodes INT96 as `Timestamp(Nanosecond)`, which overflows on far-future
/// values legacy writers such as Fivetran emit (#143). The single source of truth for both
/// schema inference ([`register_file_list`]) and decode ([`scan_table_parquet_format`]), since a
/// divergence would be a schema mismatch.
pub fn int96_coerced_parquet_format() -> ParquetFormat {
    let mut options = TableParquetOptions::default();
    options.global.coerce_int96 = Some(INT96_COERCE_TIME_UNIT.to_string());
    options.global.coerce_int96_tz = Some(INT96_COERCE_TZ.to_string());
    ParquetFormat::default().with_options(options)
}

/// Read from the nested member descriptor because the logical schema tags every nested column
/// `utf8`, which is exactly why DataFusion approves a filter pushdown it then cannot honour.
pub(super) fn renders_nested_json(logical_schema: &[crate::scan::spec::LogicalField]) -> bool {
    logical_schema.iter().any(|field| field.nested.is_some())
}

pub(super) fn scan_renders_nested_json(spec: &ScanSpec) -> bool {
    renders_nested_json(&spec.common.logical_schema)
        || spec
            .common
            .join
            .as_ref()
            .is_some_and(|join| renders_nested_json(&join.logical_schema))
}

/// Set on the table's own options because `ParquetSource::try_pushdown_filters` ORs session and
/// table flags: a table-level `true` can widen per table, a session-level one cannot narrow.
pub(super) fn row_filter_pushdown_parquet_format() -> ParquetFormat {
    let mut options = int96_coerced_parquet_format().options().clone();
    options.global.pushdown_filters = true;
    ParquetFormat::default().with_options(options)
}

/// A table with a JSON-rendered nested column reads WITHOUT row-filter pushdown: DataFusion
/// approves it against the logical `Utf8` column, removes the `FilterExec`, then drops the
/// conjunct at file open against the physical nested schema, so the predicate is applied nowhere.
/// The cost is losing row-level pushdown for all of that table's columns.
///
/// Statistics, page-index, and bloom-filter pruning stay ON by measured decision: leaf min/max
/// of a nested column would falsely exclude rendered documents, but the pruning predicate is
/// built from the adapted (rendered) expression, finding no `Column` leaf, and row-group
/// pruning cannot map a nested field to a leaf. `tests/scan_parquet_pruning.rs` pins this.
pub(super) fn scan_table_parquet_format(resolution: &FieldIdResolution) -> ParquetFormat {
    if resolution.nested_members.is_empty() {
        row_filter_pushdown_parquet_format()
    } else {
        int96_coerced_parquet_format()
    }
}

/// Exposed so plan-shape and pruning-parity integration tests can inspect the production
/// pipeline without an S3 store.
pub async fn build_raw_scan_physical_plan(
    ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, UdfError> {
    let sql = build_scan_sql(ctx, "scan_target", spec).await?;
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("DataFusion SQL error: {e}")))?;
    df.create_physical_plan()
        .await
        .map_err(|e| UdfError::User(format!("physical plan error: {e}")))
}

/// The legacy SQL paths build plain SQL text and cannot substitute a `PhysicalExpr` as
/// `FieldIdExprAdapter` does, so they call the encoder by this function name.
pub(super) const NESTED_JSON_RENDER_UDF_NAME: &str = "lakehouse_render_nested_json";

/// `Signature::any` because the nested types' inner types vary; the encoder rejects bad input.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NestedJsonRenderUdf {
    signature: Signature,
}

impl NestedJsonRenderUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NestedJsonRenderUdf {
    fn name(&self) -> &str {
        NESTED_JSON_RENDER_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let array = args.args[0].to_array(args.number_rows)?;
        let rendered = render_nested_column_as_json(&array)?;
        Ok(ColumnarValue::Array(Arc::new(rendered)))
    }
}

/// Registered once per session at context construction.
pub(super) fn register_nested_json_render_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(NestedJsonRenderUdf::new()));
}

/// Nested columns call [`NESTED_JSON_RENDER_UDF_NAME`]; other incompatible columns get
/// `CAST(col AS VARCHAR)` so they arrive as Utf8.
pub(super) async fn build_scan_sql(
    ctx: &SessionContext,
    table_name: &str,
    spec: &ScanSpec,
) -> Result<String, UdfError> {
    let table = ctx
        .table(table_name)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
    let schema = table.schema();

    // DataFusion matches quoted identifiers case-sensitively, while the adapter speaks Exasol's
    // uppercase and Parquet keeps Iceberg casing. All columns are aliased, since the filter may
    // reference an unprojected one.
    let alias_items = build_alias_items(schema);
    let inner = format!("SELECT {} FROM {table_name}", alias_items.join(", "));

    let proj_items: Vec<ProjectionItem> = if spec.common.projection.is_empty() {
        schema
            .fields()
            .iter()
            .map(|f| ProjectionItem::Column(f.name().to_uppercase()))
            .collect()
    } else {
        spec.common.projection.clone()
    };

    // A rendered expression is spliced verbatim; quoting it as an identifier would name a
    // phantom column. Emission is positional, so only projection order reaches EMITS.
    //
    // Every `Expr` gets a unique positional alias: otherwise a bare column and a CAST of the
    // same column can derive equal names (`ID` vs `CAST(ID AS Utf8View)`), which DataFusion
    // rejects as duplicates (#136). Exasol already de-duplicates bare column references.
    let select_items: Vec<String> = proj_items
        .iter()
        .enumerate()
        .map(|(i, item)| match item {
            ProjectionItem::Expr { expr } => {
                format!("{expr} AS {}", quote_ident(&format!("_LH_PROJ_{i}")))
            }
            ProjectionItem::Column(col_name) => {
                let col_lower = col_name.to_lowercase();
                let data_type = schema
                    .fields()
                    .iter()
                    .find(|f| f.name().to_lowercase() == col_lower)
                    .map(|f| f.data_type().clone());
                let upper = col_name.to_uppercase();
                match data_type {
                    Some(dt) if needs_nested_json_rendering(&dt) => {
                        format!("{NESTED_JSON_RENDER_UDF_NAME}({})", quote_ident(&upper))
                    }
                    Some(dt) if needs_json_fallback(&dt) => {
                        format!("CAST({} AS VARCHAR)", quote_ident(&upper))
                    }
                    _ => quote_ident(&upper),
                }
            }
        })
        .collect();

    let select_clause = select_items.join(", ");
    let mut sql = format!("SELECT {select_clause} FROM ({inner})");

    if let Some(filter) = &spec.common.filter
        && !filter.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }

    // Rendered by the SAME `render_order_by_clause` as the adapter's outer merge SQL, so shard
    // and merge sorts rank identically, including NULL placement. Before LIMIT so DataFusion
    // folds it into a bounded TopK.
    if !spec.common.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&render_order_by_clause(&spec.common.order_by));
    }

    if let Some(limit) = spec.common.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    Ok(sql)
}

#[cfg(test)]
#[path = "raw_scan_tests.rs"]
mod tests;
