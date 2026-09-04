//! Raw-row scan path: registers the assigned files as a positional-delete-aware
//! table, builds the projection / filter / ORDER BY / LIMIT SQL, executes it,
//! and streams the resulting batches back through `ctx.emit`.
//!
//! Also houses the shared file-registration seam (`register_files` /
//! `register_file_list`), the per-invocation delete-read concurrency limiter,
//! and the single-source-of-truth INT96-coercing `ParquetFormat` used at every
//! scan data-file read (shared with `positional_deletes`).

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

/// Stream the raw-row scan over an already-built session and emit phase telemetry.
///
/// Registers the assigned files as `scan_target`, builds the projection/filter/
/// LIMIT DataFrame, executes it, and streams batches through [`emit_stream`]
/// (one fetched, emitted, dropped before the next). On completion it emits the
/// single gated per-VM phase-telemetry record. Exposed so a host integration
/// test can drive the exact production streaming + telemetry path against a
/// local Parquet file (no S3 store), feeding its own `SessionContext`.
///
/// The redaction secret set is read from `storage` — the RESOLVED backends —
/// never from the spec, which carries a reference rather than a credential. A
/// host test builds the argument with [`ResolvedScanStorage::from_backends`].
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
    emit_stream(ctx, stream, &secrets, &spec.common.emit_exa_types, timers).await?;
    // One per-VM telemetry record at completion. Gated + best-effort: a
    // logging/sink failure NEVER fails the scan (the scan already succeeded).
    emit_phase_telemetry(ctx, timers);
    Ok(())
}

/// One shared instance-level bound on every object-store read the delete path
/// issues while preparing a scan — Phase A delete-file bodies and Phase B
/// data-file footers alike, one permit per read — sized from THIS invocation's
/// connection-concurrency budget (never at process scope — it depends on the
/// per-call `s3_max_connections`) and shared across every provider registered
/// for one invocation.
///
/// Clamped to at least 1: `s3_max_connections` is normally clamped upstream, but
/// a syntactically valid `ScanSpec` JSON (e.g. hand-crafted or malformed) can
/// still carry an explicit `0`, and `Semaphore::new(0)` would deadlock every
/// read rather than degrade gracefully.
pub(super) fn delete_path_read_limiter(spec: &ScanSpec) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(spec.common.s3_max_connections.max(1)))
}

/// Build the DataFrame: register files as a ListingTable, then apply
/// projection/filter/limit SQL.
async fn build_dataframe(
    ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<datafusion::dataframe::DataFrame, UdfError> {
    let table_name = "scan_target";
    register_files(ctx, table_name, spec, storage).await?;

    // Build the SELECT SQL applying projection, filter, and limit.
    let sql = build_scan_sql(ctx, table_name, spec).await?;
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("DataFusion SQL error: {e}")))?;
    // Single-group COUNT(DISTINCT) fan-out: dedup the single-column projection so
    // this shard streams one row per shard-local distinct projected value; the outer
    // wrapper counts the union with a native `COUNT(DISTINCT "V")`. The fan-out spec
    // carries no LIMIT/ORDER BY, so `.distinct()` over the WHERE-filtered projection
    // is exactly the shard-local distinct set. Inert on every other scan.
    if spec.common.distinct {
        df.distinct()
            .map_err(|e| UdfError::User(format!("DataFusion distinct error: {e}")))
    } else {
        Ok(df)
    }
}

/// Register the assigned Parquet files as `table_name`, backed by the custom
/// [`PositionalDeleteScanTable`] provider over DataFusion's `ParquetSource`.
///
/// This replaces the previous `ListingTable`: a `ListingTable` cannot build a
/// `FileScanConfig` directly and therefore cannot attach the per-data-file base
/// `ParquetAccessPlan` that applies Iceberg positional deletes. The custom
/// provider is unified across ALL scans — delete-free files take the identical
/// path (no access plan attached) — and preserves exactly: the logical schema,
/// the `FieldIdExprAdapter` (field-id-first column binding), and the lean
/// single-partition plan.
///
/// Public so plan-shape / pruning-preservation integration tests can register
/// the exact production provider (with per-file base `ParquetAccessPlan`s) as
/// `scan_target` before asking [`build_raw_scan_physical_plan`] for the committed
/// pipeline — the built-in `SessionContext::register_parquet` shortcut never
/// attaches an access plan and so cannot exercise the delete-carrying path.
///
/// Registers the FACT side, so it reads `storage.primary()`: the spec's own
/// `storage` field names a CONNECTION rather than carrying a credential, and only
/// the resolved pair holds the backend this registration reads through.
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

/// Register an explicit [`FileEntry`] list as `table_name`, backed by the custom
/// [`PositionalDeleteScanTable`] provider over DataFusion's `ParquetSource`.
///
/// The lower-level seam shared by the single-table scan ([`register_files`]) and
/// the broadcast-join path ([`register_join_tables`]), which registers a fact and
/// a dimension file list into the SAME session. `table_root` reconstructs relative
/// paths; a non-empty `logical_schema` registers that schema and installs the
/// column-binding expression adapter (each logical field binds by the key it
/// declares — a field-id, a declared physical name, or its own name — which is
/// correct across schema evolution), otherwise one Arrow schema is inferred from
/// the first file. `name_mapping` is threaded alongside `logical_schema` into the
/// [`PositionalDeleteScanTable`]/[`FieldIdExprAdapterFactory`] for the same side,
/// shard-invariant like the logical schema itself. `partition_columns` names, in
/// partition order, the logical columns no data file carries; they leave the file
/// schema here and are materialized per file from each entry's logged partition
/// value (see [`PartitionedScanSchema`]). Read/inference errors route
/// through [`classify_scan_error`] so no credential value can leak, whichever
/// side's file list is unreadable.
///
/// Delete correctness: each side registers through the SAME
/// [`PositionalDeleteScanTable`] provider, so a data file carrying Iceberg
/// positional deletes has them applied to ITS OWN registration. The fact side's
/// per-shard files and the dimension side's full file list each apply their own
/// deletes — exactly as the single-table raw-scan path does — so a join over a
/// table with merge-on-read deletes joins on post-delete rows on both sides,
/// never silently reintroducing deleted rows through the join path.
///
/// `delete_path_read_limiter` is the shared instance-level semaphore bounding
/// every object-store read the delete path issues while preparing this scan —
/// Phase A delete-file bodies and Phase B data-file footers alike, one permit
/// per read — for this scan invocation; callers construct it ONCE per
/// invocation and pass the SAME `Arc` to every `register_file_list` call for
/// that invocation (including both sides of a join), so the whole instance
/// stays within one N-permit budget rather than each side getting its own.
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

    // The scan registers exactly ONE object store for this table, keyed by the
    // first file's scheme+host (`object_store_url` below and the store registered
    // in `build_session_context`). Every data file and every associated delete
    // file in THIS list must resolve to that same root; a file under a different
    // bucket/host would be read through the wrong (or an unregistered) store — a
    // confusing failure or, worse, a wrong-key read. Fail loud on a mixed-root
    // list (e.g. an Iceberg `write.data.path` or a delete file in a different
    // bucket). A join's fact and dimension sides are validated independently:
    // each may legitimately live in its own bucket, with its own registered store.
    validate_uniform_object_store_files(files, table_root, &first_abs)?;

    let object_store_url = ListingTableUrl::parse(&first_abs)
        .map_err(|e| UdfError::User(format!("invalid listing URL '{first_abs}': {e}")))?
        .object_store();

    // Prefer the query-time logical schema when the adapter supplied one: use it as
    // the table schema and install the column-binding expression adapter so each
    // column binds by the key its logical field declares — correct across schema
    // evolution. The decision is the PRESENCE of a logical schema alone: a schema
    // whose fields all bind by identity still installs the adapter, because only
    // the adapter provides per-file NULL-fill, `initial-default` substitution, and
    // the required-absent error. When it is absent (legacy specs), fall back to
    // inferring one Arrow schema from the first file.
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

    // Build the binding tables ONCE per registration from the logical schema —
    // the declared-physical-name index, the logical-name → ScalarValue
    // initial-default map, and the nested member trees — and thread them into the
    // column-binding adapter factory, which uses them per file for the
    // declared-physical-name binding, the absent-with-default fill (Iceberg rule 3),
    // and the nested resolution. A malformed encoded default surfaces as a clean
    // user error, never a panic.
    let field_id_resolution = FieldIdResolution {
        name_mapping: name_mapping.to_vec(),
        declared_physical_names: index_declared_physical_names(logical_schema),
        defaults: reconstruct_initial_defaults(logical_schema).map_err(UdfError::User)?,
        nested_members: index_nested_members(logical_schema),
    };

    // The partition columns leave the file schema here: they have no physical
    // counterpart in any data file, and each is instead materialized per file from
    // that file's logged partition value.
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

/// Time unit INT96 timestamps are coerced to: microseconds. Matches Iceberg's own
/// readers, which always read the legacy INT96 physical type as microseconds.
const INT96_COERCE_TIME_UNIT: &str = "us";
/// Timezone applied to coerced INT96 timestamps: an INT96 instant is UTC.
const INT96_COERCE_TZ: &str = "UTC";

/// The base [`ParquetFormat`] every scan data-file read is built from, coercing
/// Parquet INT96 timestamps to microsecond resolution as UTC instants
/// (`coerce_int96 = "us"`, `coerce_int96_tz = "UTC"`).
///
/// arrow-rs otherwise decodes INT96 as `Timestamp(Nanosecond)`, whose i64 range
/// (1677..=2262) overflows on the far-future values legacy writers such as
/// Fivetran emit — the plain-`SELECT *` overflow of issue #143. This is the
/// single source of truth for that coercion so the schema-inference site
/// ([`register_file_list`]) and the decode site
/// ([`scan_table_parquet_format`], reached through
/// [`crate::scan::positional_deletes::PositionalDeleteScanTable`]) cannot drift:
/// a divergence between inferred `Timestamp(Nanosecond)` and decoded
/// `Timestamp(Microsecond)` would be a schema mismatch. Every decode format
/// differs from this base ONLY in its predicate-driven read optimizations.
pub fn int96_coerced_parquet_format() -> ParquetFormat {
    let mut options = TableParquetOptions::default();
    options.global.coerce_int96 = Some(INT96_COERCE_TIME_UNIT.to_string());
    options.global.coerce_int96_tz = Some(INT96_COERCE_TZ.to_string());
    ParquetFormat::default().with_options(options)
}

/// Whether one registered side's logical schema declares a column the scan
/// renders to JSON — the question both the session-level and the per-table
/// Parquet filter-pushdown decisions are taken from.
///
/// Read from the nested member descriptor each list, struct, and map column
/// carries, because the logical schema erases the type itself: every nested
/// column is tagged `utf8`, which is exactly why DataFusion approves a filter
/// pushdown it then cannot honour. A spec carrying NO logical schema (the legacy
/// first-file inference path) answers `false` and needs no answer: there the
/// registered schema declares the column at its real nested type, so DataFusion
/// rejects the pushdown itself.
pub(super) fn renders_nested_json(logical_schema: &[crate::scan::spec::LogicalField]) -> bool {
    logical_schema.iter().any(|field| field.nested.is_some())
}

/// Whether any side of this scan — the fact table or a broadcast join's
/// dimension table — renders a column to JSON.
pub(super) fn scan_renders_nested_json(spec: &ScanSpec) -> bool {
    renders_nested_json(&spec.common.logical_schema)
        || spec
            .common
            .join
            .as_ref()
            .is_some_and(|join| renders_nested_json(&join.logical_schema))
}

/// The [`ParquetFormat`] a table with no JSON-rendered nested column reads its
/// data files through: [`int96_coerced_parquet_format`] plus Parquet row-filter
/// pushdown, which DataFusion leaves off by default.
///
/// The flag is set on the table's own Parquet options rather than only on the
/// session config because `ParquetSource::try_pushdown_filters` ORs the two, so a
/// session-level `true` cannot be narrowed per table while a table-level one can
/// be widened per table.
pub(super) fn row_filter_pushdown_parquet_format() -> ParquetFormat {
    let mut options = int96_coerced_parquet_format().options().clone();
    options.global.pushdown_filters = true;
    ParquetFormat::default().with_options(options)
}

/// The [`ParquetFormat`] a scan table reads its data files through, decided from
/// the nested member trees its logical schema produced.
///
/// A table carrying a JSON-rendered nested column reads WITHOUT Parquet
/// row-filter pushdown, and every other table reads WITH it
/// ([`row_filter_pushdown_parquet_format`]).
///
/// Row-filter pushdown is withheld because DataFusion approves it against the
/// logical schema — where a nested column is `Utf8`, a primitive, so "supported"
/// — removes the `FilterExec`, and then drops the conjunct at file open because it
/// does not match the PHYSICAL nested schema: the predicate is applied NOWHERE and
/// every row comes back. With the pushdown withheld the optimizer keeps a
/// `FilterExec` that evaluates the predicate over the RENDERED `Utf8` column,
/// which is correct for every comparison shape. The accepted cost is that such a
/// table loses row-level pushdown for ALL its columns, since DataFusion scopes the
/// flag to the Parquet source and never to one column; late materialization no
/// longer skips rows within a row group.
///
/// Statistics pruning, page-index pruning, and bloom-filter probing stay ON, and
/// that is a measured decision rather than an omission. Parquet does keep
/// statistics for a nested column's LEAF values — a `list<string>` holding
/// `["hello","world"]` writes leaf `min = "hello"`, `max = "world"`, which a
/// min/max range check against the RENDERED document would falsely exclude,
/// because `[` sorts below `h`. That comparison never happens: the per-file
/// pruning predicate is built from the ADAPTED predicate, where
/// [`crate::scan::render_nested_column_as_json`]'s expression wraps the column, so
/// the pruning-predicate builder finds no `Column` leaf to derive `tags_min` from;
/// and row-group pruning resolves statistics against the PHYSICAL file schema,
/// where parquet-rs declines to map a nested field to a leaf at all. Turning the
/// stages off would therefore cost every PRIMITIVE column of such a table its
/// row-group pruning while removing no hazard. `tests/scan_parquet_pruning.rs`
/// proves it positively against a multi-row-group file carrying exactly those
/// falsely-excluding leaf statistics, and fails loudly if a future release ever
/// does resolve them.
pub(super) fn scan_table_parquet_format(resolution: &FieldIdResolution) -> ParquetFormat {
    if resolution.nested_members.is_empty() {
        row_filter_pushdown_parquet_format()
    } else {
        int96_coerced_parquet_format()
    }
}

/// Build the raw-row-path DataFusion physical plan for a session whose scan
/// table is already registered as `scan_target`.
///
/// This is the exact production raw-scan pipeline: it reuses [`build_scan_sql`]
/// (the same projection / filter / LIMIT SQL the UDF runs) and DataFusion's
/// `create_physical_plan`. Exposed so plan-shape and pruning-parity integration
/// tests can inspect the committed pipeline without standing up an S3 store —
/// the caller registers a local Parquet file as `scan_target`, then asks for
/// the plan this function produces.
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

/// Name of the DataFusion scalar function the legacy (no-logical-schema) SQL
/// paths call to render a nested column as JSON. `build_scan_sql` and
/// `join_scan::render_join_select_item` cannot substitute a `PhysicalExpr` the
/// way `FieldIdExprAdapter` does — they build plain SQL text — so the encoder
/// is reached as a callable function name instead, registered once per session
/// by [`register_nested_json_render_udf`].
pub(super) const NESTED_JSON_RENDER_UDF_NAME: &str = "lakehouse_render_nested_json";

/// [`ScalarUDFImpl`] wrapping [`render_nested_column_as_json`] so generated SQL
/// text can call it under [`NESTED_JSON_RENDER_UDF_NAME`].
///
/// Accepts any single argument type (`Signature::any`) because the five nested
/// Arrow types it serves vary in their inner element/field/key/value types; the
/// encoder itself, not this signature, is what rejects an unsupported input.
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

/// Register [`NESTED_JSON_RENDER_UDF_NAME`] on `ctx` so generated SQL text can
/// call it. Called ONCE per session, from
/// [`build_session_context`](crate::scan::object_store::build_session_context)
/// at context construction, rather than from each SQL-build entry point that
/// merely inspects the session's schema.
pub(super) fn register_nested_json_render_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(NestedJsonRenderUdf::new()));
}

/// Build the SQL string for the scan query.
///
/// For a nested column (`needs_nested_json_rendering`), calls
/// [`NESTED_JSON_RENDER_UDF_NAME`] so it renders as strict JSON. For every
/// other incompatible column, `CAST(col AS VARCHAR)` so it arrives as Utf8 and
/// the convert module's JSON fallback just passes it through as Value::String.
pub(super) async fn build_scan_sql(
    ctx: &SessionContext,
    table_name: &str,
    spec: &ScanSpec,
) -> Result<String, UdfError> {
    // Get the registered table's schema so we can check which columns need casting.
    let table = ctx
        .table(table_name)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
    let schema = table.schema();

    // The adapter speaks Exasol identifier casing (uppercase) for projection,
    // filter, and EMITS, while the Parquet/Arrow columns keep the Iceberg field
    // casing (typically lowercase). DataFusion matches quoted identifiers
    // case-sensitively, so first wrap the listing table in an inner SELECT that
    // aliases every Arrow column to its uppercase name. The outer projection and
    // the pushed-down WHERE then both resolve against those uppercase aliases.
    // All columns are aliased (not just projected ones) because the filter may
    // reference a column that is not projected.
    let alias_items = build_alias_items(schema);
    let inner = format!("SELECT {} FROM {table_name}", alias_items.join(", "));

    // Determine the items to project (already uppercase from the adapter). An
    // empty projection means "all columns"; each is a bare column reference.
    let proj_items: Vec<ProjectionItem> = if spec.common.projection.is_empty() {
        schema
            .fields()
            .iter()
            .map(|f| ProjectionItem::Column(f.name().to_uppercase()))
            .collect()
    } else {
        spec.common.projection.clone()
    };

    // Build outer SELECT items. A bare column is quoted as an identifier, with a
    // CAST to VARCHAR for incompatible types so the convert module receives them
    // as Utf8 and emits Value::String. A rendered scalar expression (e.g.
    // `("SCORE" * 2)`) is spliced VERBATIM — it is already valid DataFusion SQL
    // resolved against the uppercase-aliased inner scan, exactly like `filter`
    // and the aggregate `arg_expr`; quoting it as an identifier would build a
    // phantom column name that has no matching field. Emission is positional, so
    // projection order — not name — carries through to EMITS.
    //
    // Every `Expr` item gets an explicit positional alias. Without one,
    // DataFusion derives its schema name from the expression text, and a bare
    // column projected alongside an unaliased CAST/EXTRACT/CASE of that SAME
    // column can derive an equal name (e.g. `ID` and `CAST(ID AS Utf8View)`),
    // which DataFusion's planner rejects as a duplicate projection name (issue
    // #136 follow-up). The alias text is never read — only position carries
    // through to EMITS — so any unique-per-position identifier is safe. `Column`
    // items keep their un-aliased form: a bare column's derived name is always
    // its own quoted identifier, and Exasol itself de-duplicates repeated bare
    // column references in the select list before the pushdown request ever
    // reaches the adapter, so two `Column` items can never collide with each
    // other — only an `Expr` wrapping a projected column can collide with it.
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

    // Append WHERE clause if a translated filter is present.
    if let Some(filter) = &spec.common.filter
        && !filter.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }

    // Append ORDER BY clause for a pushed-down ordered top-N scan. The keys are
    // rendered through the SAME shared `render_order_by_clause` the adapter's
    // outer merge SQL uses, so the per-shard bounded sort and the merge sort
    // induce the IDENTICAL ranking — key order, direction (ASC/DESC), and explicit
    // NULL placement (NULLS FIRST/LAST). That structural reuse is what makes the
    // distributed top-N provably equal to single-node evaluation regardless of any
    // engine's default NULL ordering. Placed after WHERE and before LIMIT so
    // DataFusion folds `ORDER BY <keys> LIMIT n` into a bounded, fetch-limited
    // TopK (not a full global sort). When `order_by` is empty this block emits
    // nothing, leaving pre-ordering-feature SQL byte-identical.
    if !spec.common.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&render_order_by_clause(&spec.common.order_by));
    }

    // Append LIMIT clause.
    if let Some(limit) = spec.common.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    Ok(sql)
}

#[cfg(test)]
#[path = "raw_scan_tests.rs"]
mod tests;
