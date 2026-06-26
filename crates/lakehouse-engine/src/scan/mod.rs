/// DataFusion scan SET UDF — reads a ScanSpec from the input row, builds a
/// DataFusion SessionContext, registers ONLY the assigned files over MinIO,
/// applies projection/filter/limit, and streams rows back via ctx.emit.
pub mod convert;
pub mod emit;
pub mod runtime;
pub mod spec;

use crate::scan::convert::arrow_value_at;
use crate::scan::emit::{emit_stream, redact_storage_error};
use crate::scan::runtime::{build_runtime_env, probe_tmp_spill};
use crate::scan::spec::{AggKind, AggregatePlan, ScanSpec};
use crate::types::mapping::needs_json_fallback;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::Url;

/// Build the Tokio runtime for the scan UDF.
///
/// When `threads` is 1 (the default), a current-thread runtime is created —
/// one OS thread, matching Exasol's per-instance model and the
/// `NR_OF_CORES`-bound VM pool. When `threads` exceeds 1, a multi-thread
/// runtime is created with exactly `threads` worker threads, which is only
/// correct when the operator has explicitly widened the thread budget via the
/// `DATAFUSION_THREADS_PER_UDF` VS property.
fn build_scan_runtime(threads: usize) -> Result<tokio::runtime::Runtime, String> {
    if threads <= 1 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build current-thread tokio runtime: {e}"))
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build multi-thread tokio runtime: {e}"))
    }
}

/// Build the DataFusion `SessionConfig` for the given scan spec.
///
/// Sets `target_partitions` from `spec.df_target_partitions` (clamped to ≥1),
/// which controls how many logical partitions DataFusion creates internally.
/// With the default of 1 and a current-thread Tokio runtime, each UDF instance
/// uses exactly one core; cluster-level shard fan-out provides all parallelism.
fn session_config_for_spec(spec: &ScanSpec) -> SessionConfig {
    SessionConfig::new()
        .with_information_schema(false)
        .with_target_partitions(spec.df_target_partitions.max(1))
}

/// Entry point for the LAKEHOUSE_SCAN SET UDF.
///
/// Reads the scan spec from the first input column (VARCHAR JSON), builds a
/// DataFusion session, scans the assigned files, and emits rows.
pub fn run_scan(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    // Advance to the first (and only) input row.
    let has_row = ctx.next()?;
    if !has_row {
        // No input row — nothing to scan.
        return Ok(());
    }

    let spec_json = ctx
        .get_string(0)?
        .ok_or_else(|| UdfError::User("scan spec input is NULL".into()))?;

    // Parse spec BEFORE building the runtime: the runtime kind depends on
    // spec.df_threads_per_udf, so we must deserialize first.
    let spec = ScanSpec::from_json(spec_json).map_err(UdfError::User)?;

    // Build the Tokio runtime according to the spec's thread configuration.
    // A fresh runtime per call is correct for a stateless disposable UDF.
    let rt = build_scan_runtime(spec.df_threads_per_udf).map_err(UdfError::User)?;

    rt.block_on(async { run_scan_async(ctx, &spec).await })
}

async fn run_scan_async(ctx: &mut dyn UdfContext, spec: &ScanSpec) -> Result<(), UdfError> {
    let memory_limit_bytes = ctx.memory_limit();
    let session_ctx = build_session_context(spec, memory_limit_bytes)?;
    if spec.aggregates.is_some() {
        run_partial_aggregate(ctx, &session_ctx, spec).await
    } else {
        let secrets = spec.storage.secret_values();
        let df = build_dataframe(&session_ctx, spec).await?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| redact_storage_error(e.to_string(), &secrets))?;
        emit_stream(ctx, stream, &secrets).await?;
        Ok(())
    }
}

/// Run a node-local partial aggregate and emit exactly one row per shard.
///
/// Dispatches to `run_grouped_partial_aggregate` when the spec carries non-empty
/// `group_keys`; otherwise executes the single-group (ungrouped) path which
/// always emits exactly one partial-aggregate row.
///
/// The column layout follows the COLUMN CONTRACT (see `build_partial_agg_sql`
/// and `build_grouped_partial_agg_sql`).
async fn run_partial_aggregate(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    // Dispatch: grouped path when group_keys is Some and non-empty.
    if let Some(group_keys) = &spec.group_keys
        && !group_keys.is_empty()
    {
        return run_grouped_partial_aggregate(ctx, session_ctx, spec).await;
    }

    let secrets = spec.storage.secret_values();
    let aggregates = spec
        .aggregates
        .as_deref()
        .expect("run_partial_aggregate called without aggregates");

    // Register the assigned files so we can query them.
    let table_name = "scan_target";
    register_files(session_ctx, table_name, spec).await?;

    // Build the alias inner SELECT (uppercase column names).
    let table = session_ctx
        .table(table_name)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
    let alias_items = build_alias_items(table.schema());
    let aliased_table = format!("SELECT {} FROM {table_name}", alias_items.join(", "));

    let sql = build_partial_agg_sql_filtered(aggregates, &aliased_table, spec.filter.as_deref());

    let df = session_ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("partial aggregate SQL error: {e}")))?;

    // Execute and collect the single partial-aggregate row.
    let batches = df
        .collect()
        .await
        .map_err(|e| redact_storage_error(e.to_string(), &secrets))?;

    // The aggregate always produces exactly one row (even over an empty table).
    // Emit that row; if the query produced no batches at all (should not happen
    // for a well-formed aggregate), emit a row of NULLs.
    let row = if let Some(batch) = batches.first() {
        if batch.num_rows() > 0 {
            // Convert the single row from the batch.
            (0..batch.num_columns())
                .map(|col_idx| arrow_value_at(batch.column(col_idx), 0))
                .collect::<Vec<_>>()
        } else {
            emit_null_partial_row(aggregates)
        }
    } else {
        emit_null_partial_row(aggregates)
    };

    ctx.emit(&row)?;
    Ok(())
}

/// Execute a grouped partial aggregate for the assigned shard files.
///
/// DataFusion runs the GROUP BY query and streams one row per distinct group.
/// Each emitted row carries:
///   - one `Value::String` per group key (GK_0 … GK_{n-1}), stringified via
///     `arrow_value_at` then `to_string()` — the adapter declares all GK columns
///     as `VARCHAR(2000000)` in the EMITS clause.
///   - the PARTIAL_* values in the same order produced by the single-group path.
///
/// An empty result (no matching rows in this shard) emits zero rows, NOT a null
/// fallback row.  This matches the COLUMN CONTRACT: the outer wrapper re-groups
/// partial rows from all shards, so zero rows from one shard is correct.
///
/// Streaming rule: fetch one `RecordBatch` at a time, convert → emit → drop
/// before fetching the next.  Never collect all batches in memory at once.
async fn run_grouped_partial_aggregate(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    let secrets = spec.storage.secret_values();
    let group_keys = spec
        .group_keys
        .as_deref()
        .expect("run_grouped_partial_aggregate called without group_keys");
    let aggregates = spec
        .aggregates
        .as_deref()
        .expect("run_grouped_partial_aggregate called without aggregates");

    // Register the assigned files so we can query them.
    let table_name = "scan_target";
    register_files(session_ctx, table_name, spec).await?;

    // Build the alias inner SELECT (uppercase column names) — same pattern as
    // the single-group path so group-key expressions reference uppercase names.
    let table = session_ctx
        .table(table_name)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
    let alias_items = build_alias_items(table.schema());
    let aliased_table = format!("SELECT {} FROM {table_name}", alias_items.join(", "));

    let sql = build_grouped_partial_agg_sql(
        group_keys,
        aggregates,
        &aliased_table,
        spec.filter.as_deref(),
    );

    let df = session_ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("grouped partial aggregate SQL error: {e}")))?;

    // Stream result batches — fetch one RecordBatch at a time, convert → emit → drop.
    let mut stream = df
        .execute_stream()
        .await
        .map_err(|e| redact_storage_error(e.to_string(), &secrets))?;

    let n_group_keys = group_keys.len();

    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| redact_storage_error(e.to_string(), &secrets))?;

        for row_idx in 0..batch.num_rows() {
            // Group-key columns come first (columns 0 .. n_group_keys - 1).
            // They are emitted as VARCHAR strings regardless of the DataFusion type.
            let mut row_values: Vec<Value> = Vec::with_capacity(batch.num_columns());

            for col_idx in 0..n_group_keys {
                let raw = arrow_value_at(batch.column(col_idx), row_idx);
                // Stringify for GK_i VARCHAR(2000000) column.
                // Value has no Display; format each variant explicitly.
                let gk_str = value_to_gk_string(raw);
                row_values.push(gk_str);
            }

            // Partial aggregate columns follow.
            for col_idx in n_group_keys..batch.num_columns() {
                row_values.push(arrow_value_at(batch.column(col_idx), row_idx));
            }

            ctx.emit(&row_values)?;
        }
        // Drop the batch before fetching the next — never hold two batches at once.
        drop(batch);
    }

    Ok(())
}

/// Build the DataFusion SQL for a grouped partial aggregate.
///
/// Produces:
/// ```sql
/// SELECT <gk_0>, ..., <gk_{n-1}>, <partial_agg_0>, ...
/// FROM (<aliased_table>)
/// [WHERE <filter>]
/// GROUP BY <gk_0>, ..., <gk_{n-1}>
/// ```
///
/// Group-key expressions are inserted verbatim — they are already-rendered
/// DataFusion SQL fragments from the adapter (e.g. `"REGION"` or `YEAR("DATE")`).
/// No LIMIT is applied (the adapter never pushes LIMIT into grouped shard specs;
/// the outer wrapper applies LIMIT after re-grouping the partials).
pub fn build_grouped_partial_agg_sql(
    group_keys: &[String],
    aggregates: &[AggregatePlan],
    aliased_table: &str,
    filter: Option<&str>,
) -> String {
    // SELECT list: group keys first (verbatim), then partial aggregate items.
    let mut select_items: Vec<String> = group_keys.to_vec();
    let partial_items: Vec<String> = aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| partial_select_items(plan, i))
        .collect();
    select_items.extend(partial_items);

    let mut sql = format!(
        "SELECT {} FROM ({})",
        select_items.join(", "),
        aliased_table
    );

    if let Some(f) = filter
        && !f.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(f);
    }

    // GROUP BY the group-key expressions (same verbatim fragments as in SELECT).
    sql.push_str(" GROUP BY ");
    sql.push_str(&group_keys.join(", "));

    sql
}

/// Stringify a group-key `Value` for the `GK_i VARCHAR(2000000)` EMITS column.
///
/// NULL group keys stay NULL (the outer wrapper groups them together consistently).
/// String values pass through unchanged. All other types are converted to their
/// canonical string representation so the adapter's VARCHAR column accepts them.
fn value_to_gk_string(v: Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::String(s) => Value::String(s),
        Value::Bool(b) => Value::String(if b { "true" } else { "false" }.to_string()),
        Value::Int32(n) => Value::String(n.to_string()),
        Value::Int64(n) => Value::String(n.to_string()),
        Value::Double(f) => Value::String(f.to_string()),
        Value::Numeric(d) => Value::String(d.to_string()),
        Value::Date(nd) => Value::String(nd.to_string()),
        Value::Timestamp(ndt) => Value::String(ndt.to_string()),
    }
}

/// Build the fallback null partial row for an empty aggregate result.
///
/// COUNT/CountCol -> 0 (not NULL); SUM/Min/Max/Avg parts -> NULL.
/// Stat family: cnt -> 0, sum -> NULL, sumsq -> NULL.
fn emit_null_partial_row(aggregates: &[AggregatePlan]) -> Vec<exasol_udf_sdk::value::Value> {
    use exasol_udf_sdk::value::Value;
    let mut row = Vec::new();
    for plan in aggregates {
        match plan.kind {
            AggKind::Count | AggKind::CountCol => row.push(Value::Int64(0)),
            AggKind::Sum | AggKind::Min | AggKind::Max => row.push(Value::Null),
            AggKind::Avg => {
                row.push(Value::Null); // partial_avg_sum
                row.push(Value::Int64(0)); // partial_avg_cnt
            }
            AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => {
                row.push(Value::Int64(0)); // partial_stat_cnt
                row.push(Value::Null); // partial_stat_sum
                row.push(Value::Null); // partial_stat_sumsq
            }
        }
    }
    row
}

/// COLUMN CONTRACT:
///
/// Iterating `aggregates` in order, each plan item at index `i` contributes:
/// - `Count`    -> 1 column: `"PARTIAL_count_{i}"`   (DECIMAL(20,0), summable)
/// - `CountCol` -> 1 column: `"PARTIAL_count_{i}"`   (DECIMAL(20,0), summable)
/// - `Sum`      -> 1 column: `"PARTIAL_sum_{i}"`     (type from `partial_emits_items`: DOUBLE
///   PRECISION for float columns, DECIMAL(36,s) for DECIMAL(p,s) columns)
/// - `Min`      -> 1 column: `"PARTIAL_min_{i}"`     (type from `partial_emits_items`: the
///   column's real Exasol type, e.g. DATE, TIMESTAMP, or DECIMAL)
/// - `Max`      -> 1 column: `"PARTIAL_max_{i}"`     (type from `partial_emits_items`: same
///   as Min — the column's real Exasol type)
/// - `Avg`      -> 2 columns: `"PARTIAL_avg_sum_{i}"` (DOUBLE PRECISION) then
///   `"PARTIAL_avg_cnt_{i}"` (DECIMAL(20,0))
///
/// For the exact EMITS types, defer to `partial_emits_items` in `adapter::pushdown` as the
/// single source of truth — this DataFusion SELECT list produces the values; the EMITS clause
/// declares the Exasol types that receive them.
///
/// The scan UDF aggregate SELECT list, the EMITS clause in the fan-out SQL, and
/// the outer merge SELECT MUST all agree on this order and column count.
///
/// `aliased_table` is a subquery string: `SELECT ... FROM scan_target` with
/// uppercase aliases already applied. No filter applied.
#[cfg(test)]
pub fn build_partial_agg_sql(aggregates: &[AggregatePlan], aliased_table: &str) -> String {
    build_partial_agg_sql_filtered(aggregates, aliased_table, None)
}

/// Build the partial-aggregate SQL, optionally with a WHERE clause.
pub fn build_partial_agg_sql_filtered(
    aggregates: &[AggregatePlan],
    aliased_table: &str,
    filter: Option<&str>,
) -> String {
    let select_items: Vec<String> = aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| partial_select_items(plan, i))
        .collect();

    let mut sql = format!(
        "SELECT {} FROM ({})",
        select_items.join(", "),
        aliased_table
    );

    if let Some(f) = filter
        && !f.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(f);
    }

    sql
}

/// Produce the SELECT list items for one aggregate plan entry at index `i`.
fn partial_select_items(plan: &AggregatePlan, i: usize) -> Vec<String> {
    match plan.kind {
        AggKind::Count => {
            vec![format!(r#"COUNT(*) AS "PARTIAL_count_{i}""#)]
        }
        AggKind::CountCol => {
            let col = plan.column.as_deref().unwrap_or("");
            vec![format!(
                r#"COUNT({}) AS "PARTIAL_count_{i}""#,
                quote_ident(col)
            )]
        }
        AggKind::Sum => {
            let col = plan.column.as_deref().unwrap_or("");
            vec![format!(r#"SUM({}) AS "PARTIAL_sum_{i}""#, quote_ident(col))]
        }
        AggKind::Min => {
            let col = plan.column.as_deref().unwrap_or("");
            vec![format!(r#"MIN({}) AS "PARTIAL_min_{i}""#, quote_ident(col))]
        }
        AggKind::Max => {
            let col = plan.column.as_deref().unwrap_or("");
            vec![format!(r#"MAX({}) AS "PARTIAL_max_{i}""#, quote_ident(col))]
        }
        AggKind::Avg => {
            let col = plan.column.as_deref().unwrap_or("");
            vec![
                format!(r#"SUM({}) AS "PARTIAL_avg_sum_{i}""#, quote_ident(col)),
                format!(r#"COUNT({}) AS "PARTIAL_avg_cnt_{i}""#, quote_ident(col)),
            ]
        }
        // STDDEV/VARIANCE family: emit (cnt, sum, sum_sq) sufficient statistics.
        // COUNT(col) excludes NULLs, matching single-node semantics.
        AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => {
            let col = plan.column.as_deref().unwrap_or("");
            let qcol = quote_ident(col);
            vec![
                format!(r#"COUNT({qcol}) AS "PARTIAL_stat_cnt_{i}""#),
                format!(r#"SUM({qcol}) AS "PARTIAL_stat_sum_{i}""#),
                format!(r#"SUM({qcol} * {qcol}) AS "PARTIAL_stat_sumsq_{i}""#),
            ]
        }
    }
}

/// Build a DataFusion SessionContext with the MinIO object store registered.
///
/// Sizes the DataFusion memory pool from `memory_limit_bytes` (UDF per-instance
/// limit in bytes; `0` = unknown sentinel → conservative 1024 MB default) and
/// probes `/tmp` for disk-spill eligibility.
fn build_session_context(
    spec: &ScanSpec,
    memory_limit_bytes: u64,
) -> Result<SessionContext, UdfError> {
    let config = session_config_for_spec(spec);

    // Memory pool + spill config.
    let spill = probe_tmp_spill();
    let runtime_env = build_runtime_env(
        memory_limit_bytes,
        spec.memory_pool_fraction,
        spec.instance_overhead_mb * 1024 * 1024,
        spill,
    )
    .map_err(|e| UdfError::User(format!("failed to build DataFusion runtime env: {e}")))?;

    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

    // Register the MinIO object store for the S3 URL scheme.
    let bucket = extract_bucket(spec)?;
    let s3 = build_s3_store(&spec.storage, &bucket)?;
    let store_url = Url::parse(&format!("s3://{bucket}"))
        .map_err(|e| UdfError::User(format!("invalid bucket URL: {e}")))?;
    ctx.runtime_env()
        .register_object_store(&store_url, Arc::new(s3));

    Ok(ctx)
}

/// Build an AmazonS3 (MinIO-compatible) object store from StorageProps.
fn build_s3_store(
    storage: &crate::scan::spec::StorageProps,
    bucket: &str,
) -> Result<impl ObjectStore, UdfError> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_allow_http(storage.allow_http);

    // Path-style stores (MinIO and other S3-compatibles) need the explicit endpoint
    // and path-style addressing. For real AWS S3 (virtual-hosted) we must NOT set an
    // endpoint: object_store derives https://<bucket>.s3.<region>.amazonaws.com from
    // the region. Setting a regional endpoint without the bucket sends requests to
    // the account root -> S3 returns 403 (s3:ListAllMyBuckets).
    if storage.path_style {
        builder = builder
            .with_endpoint(&storage.endpoint)
            .with_virtual_hosted_style_request(false);
    }

    if let Some(token) = &storage.session_token {
        builder = builder.with_token(token);
    }

    let secrets = storage.secret_values();
    builder.build().map_err(|e| {
        // Do not echo the error directly — it might contain credential fragments.
        let stripped = crate::scan::emit::redact_secret_values(&e.to_string(), &secrets);
        UdfError::User(format!(
            "failed to configure S3 object store: {}",
            crate::scan::emit::redact_credentials(&stripped)
        ))
    })
}

/// Extract the S3 bucket name from the first file URI in the spec.
fn extract_bucket(spec: &ScanSpec) -> Result<String, UdfError> {
    let first = spec
        .files
        .first()
        .ok_or_else(|| UdfError::User("scan spec has no files".into()))?;
    let url = Url::parse(first).map_err(|e| UdfError::User(format!("invalid file URI: {e}")))?;
    url.host_str()
        .map(|h| h.to_string())
        .ok_or_else(|| UdfError::User(format!("file URI has no bucket/host: {first}")))
}

/// Build the DataFrame: register files as a ListingTable, then apply
/// projection/filter/limit SQL.
async fn build_dataframe(
    ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<datafusion::dataframe::DataFrame, UdfError> {
    // Register only the assigned files as a listing table.
    let table_name = "scan_target";
    register_files(ctx, table_name, spec).await?;

    // Build the SELECT SQL applying projection, filter, and limit.
    let sql = build_scan_sql(ctx, table_name, spec).await?;
    ctx.sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("DataFusion SQL error: {e}")))
}

/// Register the assigned Parquet files as a ListingTable named `table_name`.
async fn register_files(
    ctx: &SessionContext,
    table_name: &str,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    let file_format = Arc::new(ParquetFormat::default());
    let listing_options = ListingOptions::new(file_format)
        .with_file_extension(".parquet")
        // Disable glob — we supply exact paths.
        .with_collect_stat(false);

    let table_paths: Vec<ListingTableUrl> = spec
        .files
        .iter()
        .map(|f| {
            ListingTableUrl::parse(f)
                .map_err(|e| UdfError::User(format!("invalid listing URL '{f}': {e}")))
        })
        .collect::<Result<_, _>>()?;

    // Resolve the schema from the first file so we know column types.
    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_paths[0])
        .await
        .map_err(|e| redact_storage_error(e.to_string(), &spec.storage.secret_values()))?;

    let config = ListingTableConfig::new_with_multi_paths(table_paths)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let table = ListingTable::try_new(config)
        .map_err(|e| UdfError::User(format!("failed to create listing table: {e}")))?;

    ctx.register_table(table_name, Arc::new(table))
        .map_err(|e| UdfError::User(format!("failed to register table: {e}")))?;

    Ok(())
}

/// Build the SQL string for the scan query.
///
/// For incompatible columns, CAST(col AS VARCHAR) so they arrive as Utf8 and
/// the convert module's JSON fallback just passes them through as Value::String.
async fn build_scan_sql(
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
    let alias_items: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let arrow_name = f.name();
            format!(
                "{} AS {}",
                quote_ident(arrow_name),
                quote_ident(&arrow_name.to_uppercase())
            )
        })
        .collect();
    let inner = format!("SELECT {} FROM {table_name}", alias_items.join(", "));

    // Determine the columns to project (already uppercase from the adapter).
    let proj_cols: Vec<String> = if spec.projection.is_empty() {
        schema
            .fields()
            .iter()
            .map(|f| f.name().to_uppercase())
            .collect()
    } else {
        spec.projection.clone()
    };

    // Build outer SELECT items: CAST incompatible types to VARCHAR so the
    // convert module receives them as Utf8 and emits Value::String. Emission is
    // positional, so projection order — not name — carries through to EMITS.
    let select_items: Vec<String> = proj_cols
        .iter()
        .map(|col_name| {
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
        })
        .collect();

    let select_clause = select_items.join(", ");
    let mut sql = format!("SELECT {select_clause} FROM ({inner})");

    // Append WHERE clause if a translated filter is present.
    if let Some(filter) = &spec.filter
        && !filter.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }

    // Append LIMIT clause.
    if let Some(limit) = spec.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    Ok(sql)
}

/// Build `"col" AS "COL"` alias items for all fields in `schema`.
///
/// Used to wrap a listing table in an inner SELECT that exposes uppercase column
/// names, so projection/filter expressions resolved against uppercase identifiers
/// find a match regardless of the Parquet field casing.
fn build_alias_items(schema: &datafusion::common::DFSchema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let arrow_name = f.name();
            format!(
                "{} AS {}",
                quote_ident(arrow_name),
                quote_ident(&arrow_name.to_uppercase())
            )
        })
        .collect()
}

/// Double-quote an identifier (SQL-safe column name).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::runtime::{DEFAULT_BUDGET_BYTES, MIN_POOL_FLOOR_BYTES};
    use crate::scan::spec::{AggKind, AggregatePlan, CatalogProps, StorageProps};
    use datafusion::execution::memory_pool::MemoryLimit;

    // ---------------------------------------------------------------------------
    // build_session_context memory pool sizing — seam tests for task 1.3
    // ---------------------------------------------------------------------------

    /// Minimal ScanSpec with a valid-looking S3 URI for build_session_context tests.
    fn minimal_spec() -> ScanSpec {
        ScanSpec {
            files: vec!["s3://test-bucket/data/part-0.parquet".into()],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: None,
            group_keys: None,
            storage: StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "testkey".into(),
                secret_key: "testsecret".into(),
                session_token: None,
                allow_http: true,
                path_style: true,
            },
            catalog: CatalogProps {
                uri: "http://localhost:8181".into(),
                warehouse: "wh".into(),
                table: "db.tbl".into(),
            },
            df_target_partitions: 1,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        }
    }

    /// A positive memory limit causes the DataFusion pool to be sized at fraction × (limit − overhead).
    /// Uses minimal_spec defaults: fraction=0.6, overhead=200 MiB.
    #[test]
    fn session_context_sizes_pool_from_ctx_limit() {
        let limit: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB
        let spec = minimal_spec();
        let overhead_bytes = spec.instance_overhead_mb * 1024 * 1024;
        let net = limit - overhead_bytes;
        let expected_budget = (net as f64 * spec.memory_pool_fraction) as usize;
        let ctx = build_session_context(&spec, limit).expect("build must succeed");
        match ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(actual) => assert_eq!(
                actual, expected_budget,
                "pool budget must be fraction × (limit − overhead)"
            ),
            _ => panic!("expected Finite pool limit"),
        }
    }

    /// A zero memory limit causes the DataFusion pool to use the conservative default budget.
    #[test]
    fn session_context_uses_default_budget_on_zero_limit() {
        let ctx = build_session_context(&minimal_spec(), 0).expect("build must succeed");
        match ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(actual) => assert_eq!(
                actual, DEFAULT_BUDGET_BYTES as usize,
                "pool budget must equal the 1 GiB default when limit is unknown (0)"
            ),
            _ => panic!("expected Finite pool limit"),
        }
    }

    /// Task 5.2: explicit non-default fraction/overhead in spec flow through to pool sizing.
    ///
    /// Builds a spec with fraction=0.5 and overhead=256 MiB, calls build_session_context
    /// with a known limit (4 GiB), and asserts the pool equals 0.5 × (4 GiB − 256 MiB).
    /// This proves the values are read from the spec, not from hardcoded constants.
    #[test]
    fn memory_budget_round_trips_into_scan_spec() {
        let mut spec = minimal_spec();
        spec.memory_pool_fraction = 0.5;
        spec.instance_overhead_mb = 256;
        let limit: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB
        let overhead_bytes = 256_u64 * 1024 * 1024;
        let net = limit - overhead_bytes;
        let expected = (net as f64 * 0.5_f64) as usize;
        let ctx = build_session_context(&spec, limit).expect("build must succeed");
        match ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(actual) => assert_eq!(
                actual, expected,
                "pool budget must be 0.5 × (4 GiB − 256 MiB); got {actual}, expected {expected}"
            ),
            _ => panic!("expected Finite pool limit"),
        }
        // Verify this is NOT the MIN_POOL_FLOOR_BYTES (it should be much larger).
        assert!(
            expected > MIN_POOL_FLOOR_BYTES as usize,
            "expected budget must exceed the floor"
        );
    }

    // ---------------------------------------------------------------------------
    // build_partial_agg_sql — host-runnable unit tests
    // ---------------------------------------------------------------------------

    fn sample_plans_count_sum_min_max() -> Vec<AggregatePlan> {
        vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
            },
        ]
    }

    /// Column order: COUNT(*) first, then SUM, MIN, MAX — each one column.
    #[test]
    fn partial_agg_sql_count_star_uses_count_star() {
        let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
        assert!(
            sql.contains("COUNT(*) AS"),
            "COUNT(*) plan must use COUNT(*): {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "COUNT(*) partial column must be PARTIAL_count_0: {sql}"
        );
    }

    /// COUNT(col) plan uses COUNT("COL"), not COUNT(*).
    #[test]
    fn partial_agg_sql_count_col_uses_count_col() {
        let plans = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("ID".into()),
        }];
        let sql = build_partial_agg_sql(&plans, "aliased");
        assert!(
            sql.contains(r#"COUNT("ID")"#),
            "COUNT(col) must use COUNT(\"ID\"): {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "COUNT(col) partial must be PARTIAL_count_0: {sql}"
        );
        assert!(
            !sql.contains("COUNT(*)"),
            "COUNT(col) must not use COUNT(*): {sql}"
        );
    }

    /// SUM plan uses SUM("COL") at index 1.
    #[test]
    fn partial_agg_sql_sum_uses_sum_col() {
        let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
        assert!(
            sql.contains(r#"SUM("AMOUNT") AS "PARTIAL_sum_1""#),
            "SUM plan must use SUM(\"AMOUNT\") as PARTIAL_sum_1: {sql}"
        );
    }

    /// MIN/MAX plans use MIN/MAX("COL").
    #[test]
    fn partial_agg_sql_min_max_use_min_max_col() {
        let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
        assert!(
            sql.contains(r#"MIN("TS") AS "PARTIAL_min_2""#),
            "MIN plan must use MIN at index 2: {sql}"
        );
        assert!(
            sql.contains(r#"MAX("TS") AS "PARTIAL_max_3""#),
            "MAX plan must use MAX at index 3: {sql}"
        );
    }

    /// AVG plan emits TWO columns: sum first, count second.
    #[test]
    fn partial_agg_sql_avg_emits_sum_count_pair() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
        }];
        let sql = build_partial_agg_sql(&plans, "aliased");
        // Must NOT emit an AVG() function.
        assert!(
            !sql.contains("AVG("),
            "must not use AVG() for partial avg: {sql}"
        );
        // Must emit SUM for the sum part.
        assert!(
            sql.contains(r#"SUM("SCORE") AS "PARTIAL_avg_sum_0""#),
            "AVG plan must emit SUM as PARTIAL_avg_sum_0: {sql}"
        );
        // Must emit COUNT(col) for the count part (not COUNT(*)).
        assert!(
            sql.contains(r#"COUNT("SCORE") AS "PARTIAL_avg_cnt_0""#),
            "AVG plan must emit COUNT(col) as PARTIAL_avg_cnt_0: {sql}"
        );
    }

    /// Mixed: COUNT/SUM/AVG — AVG contributes two columns at indices 2 (sum) and 2 (cnt),
    /// i.e., each plan item is indexed by its position in the aggregates vec.
    #[test]
    fn partial_agg_sql_mixed_column_order_and_indices() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("SCORE".into()),
            },
        ];
        let sql = build_partial_agg_sql(&plans, "aliased");
        // COUNT at index 0.
        assert!(sql.contains("PARTIAL_count_0"), "count at index 0: {sql}");
        // SUM at index 1.
        assert!(sql.contains("PARTIAL_sum_1"), "sum at index 1: {sql}");
        // AVG at index 2 -> both sum and cnt use index 2.
        assert!(
            sql.contains("PARTIAL_avg_sum_2"),
            "avg sum at index 2: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_2"),
            "avg cnt at index 2: {sql}"
        );
    }

    /// Filter is applied when present.
    #[test]
    fn partial_agg_sql_applies_filter() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
        }];
        let sql = build_partial_agg_sql_filtered(&plans, "aliased", Some("\"ID\" > 5"));
        assert!(
            sql.contains("WHERE"),
            "filter must produce WHERE clause: {sql}"
        );
        assert!(
            sql.contains("\"ID\" > 5"),
            "filter expression must appear: {sql}"
        );
    }

    /// No filter: no WHERE clause.
    #[test]
    fn partial_agg_sql_no_filter_no_where() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
        }];
        let sql = build_partial_agg_sql(&plans, "aliased");
        assert!(
            !sql.contains("WHERE"),
            "no filter must produce no WHERE: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // build_grouped_partial_agg_sql — GROUP BY key emission and partial columns
    // ---------------------------------------------------------------------------

    /// Single group key with COUNT(*): SELECT includes the key and COUNT(*).
    #[test]
    fn grouped_partial_agg_sql_single_key_count() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
        }];
        let sql =
            build_grouped_partial_agg_sql(&[r#""REGION""#.to_string()], &plans, "aliased", None);
        assert!(
            sql.contains(r#""REGION""#),
            "group key must appear in SQL: {sql}"
        );
        assert!(sql.contains("COUNT(*) AS"), "COUNT(*) must appear: {sql}");
        assert!(
            sql.contains("PARTIAL_count_0"),
            "partial count column at index 0: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "must have GROUP BY clause: {sql}");
    }

    /// The emitted SELECT layout matches the GK_* then PARTIAL_* adapter contract:
    /// group keys appear before partial aggregate columns in the SELECT list.
    #[test]
    fn grouped_partial_agg_sql_layout_matches_emits() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            },
        ];
        let sql = build_grouped_partial_agg_sql(
            &[r#""REGION""#.to_string(), r#""CATEGORY""#.to_string()],
            &plans,
            "aliased",
            None,
        );
        // Verify ordering: group key positions come before partial aggregate positions.
        let region_pos = sql.find(r#""REGION""#).expect("REGION must appear");
        let partial_count_pos = sql
            .find("PARTIAL_count_0")
            .expect("PARTIAL_count_0 must appear");
        assert!(
            region_pos < partial_count_pos,
            "group key must precede partial columns: {sql}"
        );
        let category_pos = sql.find(r#""CATEGORY""#).expect("CATEGORY must appear");
        assert!(
            category_pos < partial_count_pos,
            "second group key must precede partial columns: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "SUM at index 1 must appear: {sql}"
        );
    }

    /// No LIMIT is ever added to a grouped partial aggregate SQL.
    #[test]
    fn grouped_partial_agg_sql_no_limit() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
        }];
        let sql =
            build_grouped_partial_agg_sql(&[r#""REGION""#.to_string()], &plans, "aliased", None);
        assert!(
            !sql.contains("LIMIT"),
            "grouped partial SQL must not contain LIMIT: {sql}"
        );
    }

    /// Expression group keys (e.g. YEAR("DATE")) are inserted verbatim into the
    /// DataFusion GROUP BY clause without any quoting or transformation.
    #[test]
    fn grouped_partial_agg_sql_expression_key_verbatim() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
        }];
        let expr_key = r#"YEAR("ORDER_DATE")"#.to_string();
        let sql =
            build_grouped_partial_agg_sql(std::slice::from_ref(&expr_key), &plans, "aliased", None);
        assert!(
            sql.contains(&expr_key),
            "expression key must appear verbatim in SQL: {sql}"
        );
        // Must appear in both SELECT and GROUP BY.
        let first_pos = sql.find(&expr_key).unwrap();
        let second_pos = sql[first_pos + 1..]
            .find(&expr_key)
            .map(|p| p + first_pos + 1);
        assert!(
            second_pos.is_some(),
            "expression key must appear in both SELECT and GROUP BY: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Stat aggregate partial SQL — COUNT(col), SUM(col), SUM(col*col) triple
    // ---------------------------------------------------------------------------

    /// Stat aggregate partial emits COUNT(col), SUM(col), SUM(col*col) at index 0.
    #[test]
    fn partial_agg_sql_stat_emits_cnt_sum_sumsq() {
        for kind in &[
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            let plans = vec![AggregatePlan {
                kind: kind.clone(),
                column: Some("SCORE".into()),
            }];
            let sql = build_partial_agg_sql(&plans, "aliased");
            assert!(
                sql.contains(r#"COUNT("SCORE") AS "PARTIAL_stat_cnt_0""#),
                "{kind:?} must emit COUNT(col) as PARTIAL_stat_cnt_0: {sql}"
            );
            assert!(
                sql.contains(r#"SUM("SCORE") AS "PARTIAL_stat_sum_0""#),
                "{kind:?} must emit SUM(col) as PARTIAL_stat_sum_0: {sql}"
            );
            assert!(
                sql.contains(r#"SUM("SCORE" * "SCORE") AS "PARTIAL_stat_sumsq_0""#),
                "{kind:?} must emit SUM(col*col) as PARTIAL_stat_sumsq_0: {sql}"
            );
            // Must NOT use AVG or STDDEV directly — only sufficient statistics
            assert!(
                !sql.contains("STDDEV"),
                "{kind:?} must not emit STDDEV: {sql}"
            );
            assert!(
                !sql.contains("VARIANCE"),
                "{kind:?} must not emit VARIANCE: {sql}"
            );
        }
    }

    /// Stat aggregate null fallback row has 3 values: cnt=0, sum=NULL, sumsq=NULL.
    #[test]
    fn stat_aggregate_null_fallback_row_has_three_values() {
        use exasol_udf_sdk::value::Value;
        for kind in &[
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            let plans = vec![AggregatePlan {
                kind: kind.clone(),
                column: Some("X".into()),
            }];
            let row = emit_null_partial_row(&plans);
            assert_eq!(row.len(), 3, "{kind:?} fallback row must have 3 values");
            assert_eq!(row[0], Value::Int64(0), "{kind:?} cnt must be 0");
            assert_eq!(row[1], Value::Null, "{kind:?} sum must be NULL");
            assert_eq!(row[2], Value::Null, "{kind:?} sumsq must be NULL");
        }
    }

    // ---------------------------------------------------------------------------
    // T7 — runtime selection and session config from ScanSpec
    // ---------------------------------------------------------------------------

    /// SessionConfig applies target_partitions from the spec.
    ///
    /// Scenario: session_config_uses_spec_target_partitions
    #[test]
    fn session_config_uses_spec_target_partitions() {
        let mut spec = minimal_spec();
        spec.df_target_partitions = 4;
        let config = session_config_for_spec(&spec);
        assert_eq!(
            config.target_partitions(),
            4,
            "SessionConfig must use df_target_partitions from spec"
        );
    }

    /// A spec with df_threads_per_udf == 1 selects the current-thread runtime.
    ///
    /// Scenario: runtime_is_current_thread_when_threads_is_one
    #[test]
    fn runtime_is_current_thread_when_threads_is_one() {
        let rt = build_scan_runtime(1).expect("runtime must build");
        assert_eq!(
            rt.handle().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread,
            "df_threads_per_udf == 1 must yield a current-thread runtime"
        );
    }

    /// A spec with df_threads_per_udf > 1 selects the multi-thread runtime.
    ///
    /// Scenario: runtime_is_multi_thread_when_threads_exceeds_one
    #[test]
    fn runtime_is_multi_thread_when_threads_exceeds_one() {
        let rt = build_scan_runtime(4).expect("runtime must build");
        assert_eq!(
            rt.handle().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread,
            "df_threads_per_udf > 1 must yield a multi-thread runtime"
        );
    }

    /// Mixed stat + count: stat at index 1 uses PARTIAL_stat_*_1 names.
    #[test]
    fn stat_aggregate_index_follows_plan_order() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::VarPop,
                column: Some("X".into()),
            },
        ];
        let sql = build_partial_agg_sql(&plans, "aliased");
        assert!(sql.contains("PARTIAL_count_0"), "count at index 0: {sql}");
        assert!(
            sql.contains("PARTIAL_stat_cnt_1"),
            "stat at index 1 must use suffix _1: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_stat_sum_1"),
            "stat sum at index 1: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_stat_sumsq_1"),
            "stat sumsq at index 1: {sql}"
        );
    }
}
