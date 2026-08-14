//! Raw-row scan path: registers the assigned files as a positional-delete-aware
//! table, builds the projection / filter / ORDER BY / LIMIT SQL, executes it,
//! and streams the resulting batches back through `ctx.emit`.
//!
//! Also houses the shared file-registration seam (`register_files` /
//! `register_file_list`), the per-invocation delete-read concurrency limiter,
//! and the single-source-of-truth INT96-coercing `ParquetFormat` used at every
//! scan data-file read (shared with `positional_deletes`).

use datafusion::common::config::TableParquetOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::scan::emit::{classify_scan_error, emit_stream};
use crate::scan::spec::{
    FileEntry, NameMappingEntry, ProjectionItem, ScanSpec, reconstruct_abs_uri,
    render_order_by_clause,
};
use crate::scan::{diagnostics, emit_phase_telemetry};
use crate::types::mapping::needs_json_fallback;

use super::field_id_projection::{
    FieldIdResolution, build_logical_arrow_schema, index_declared_physical_names,
    reconstruct_initial_defaults,
};
use super::object_store::validate_uniform_object_store_files;
use super::sql_support::{build_alias_items, quote_ident};

/// Stream the raw-row scan over an already-built session and emit phase telemetry.
///
/// Registers the assigned files as `scan_target`, builds the projection/filter/
/// LIMIT DataFrame, executes it, and streams batches through [`emit_stream`]
/// (one fetched, emitted, dropped before the next). On completion it emits the
/// single gated per-VM phase-telemetry record. Exposed so a host integration
/// test can drive the exact production streaming + telemetry path against a
/// local Parquet file (no S3 store), feeding its own `SessionContext`.
pub async fn run_raw_scan_with_session(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    timers: &mut diagnostics::PhaseTimers,
) -> Result<(), UdfError> {
    let secrets = spec.common.storage.secret_values();
    let df = build_dataframe(session_ctx, spec).await?;
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
) -> Result<datafusion::dataframe::DataFrame, UdfError> {
    let table_name = "scan_target";
    register_files(ctx, table_name, spec).await?;

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
pub async fn register_files(
    ctx: &SessionContext,
    table_name: &str,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    let delete_path_read_limiter = delete_path_read_limiter(spec);
    register_file_list(
        ctx,
        table_name,
        &spec.files,
        &spec.common.table_root,
        &spec.common.logical_schema,
        &spec.common.name_mapping,
        &spec.common.storage,
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
/// shard-invariant like the logical schema itself. Read/inference errors route
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
    // the declared-physical-name index and the logical-name → ScalarValue
    // initial-default map — and thread them into the column-binding adapter
    // factory, which uses them per file for the declared-physical-name binding and
    // the absent-with-default fill (Iceberg rule 3). A malformed encoded default
    // surfaces as a clean user error, never a panic.
    let field_id_resolution = FieldIdResolution {
        name_mapping: name_mapping.to_vec(),
        declared_physical_names: index_declared_physical_names(logical_schema),
        defaults: reconstruct_initial_defaults(logical_schema).map_err(UdfError::User)?,
    };

    let table = crate::scan::positional_deletes::PositionalDeleteScanTable::new(
        object_store_url,
        table_schema,
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

/// The [`ParquetFormat`] every scan data-file read uses, coercing Parquet INT96
/// timestamps to microsecond resolution as UTC instants (`coerce_int96 = "us"`,
/// `coerce_int96_tz = "UTC"`).
///
/// arrow-rs otherwise decodes INT96 as `Timestamp(Nanosecond)`, whose i64 range
/// (1677..=2262) overflows on the far-future values legacy writers such as
/// Fivetran emit — the plain-`SELECT *` overflow of issue #143. This is the
/// single source of truth for that coercion so the schema-inference site
/// ([`register_file_list`]) and the decode site
/// ([`crate::scan::positional_deletes::PositionalDeleteScanTable`]) cannot drift:
/// a divergence between inferred `Timestamp(Nanosecond)` and decoded
/// `Timestamp(Microsecond)` would be a schema mismatch.
pub fn int96_coerced_parquet_format() -> ParquetFormat {
    let mut options = TableParquetOptions::default();
    options.global.coerce_int96 = Some(INT96_COERCE_TIME_UNIT.to_string());
    options.global.coerce_int96_tz = Some(INT96_COERCE_TZ.to_string());
    ParquetFormat::default().with_options(options)
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

/// Build the SQL string for the scan query.
///
/// For incompatible columns, CAST(col AS VARCHAR) so they arrive as Utf8 and
/// the convert module's JSON fallback just passes them through as Value::String.
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
                let needs_cast = schema
                    .fields()
                    .iter()
                    .find(|f| f.name().to_lowercase() == col_lower)
                    .map(|f| needs_json_fallback(f.data_type()))
                    .unwrap_or(false);
                let upper = col_name.to_uppercase();
                if needs_cast {
                    format!("CAST({} AS VARCHAR)", quote_ident(&upper))
                } else {
                    quote_ident(&upper)
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
