/// DataFusion scan SET UDF — reads a ScanSpec from the input row, builds a
/// DataFusion SessionContext, registers ONLY the assigned files over MinIO,
/// applies projection/filter/limit, and streams rows back via ctx.emit.
pub mod convert;
pub mod emit;
pub mod spec;

use crate::scan::emit::{emit_stream, redact_storage_error};
use crate::scan::spec::ScanSpec;
use crate::types::mapping::needs_json_fallback;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::Url;

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

    let spec = ScanSpec::from_json(spec_json).map_err(UdfError::User)?;

    // Run async DataFusion scan on a current-thread tokio runtime.
    // A fresh runtime per call is correct for a stateless disposable UDF.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;

    rt.block_on(async { run_scan_async(ctx, &spec).await })
}

async fn run_scan_async(ctx: &mut dyn UdfContext, spec: &ScanSpec) -> Result<(), UdfError> {
    let secrets = spec.storage.secret_values();
    let session_ctx = build_session_context(spec)?;
    let df = build_dataframe(&session_ctx, spec).await?;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| redact_storage_error(e.to_string(), &secrets))?;
    emit_stream(ctx, stream, &secrets).await?;
    Ok(())
}

/// Build a DataFusion SessionContext with the MinIO object store registered.
fn build_session_context(spec: &ScanSpec) -> Result<SessionContext, UdfError> {
    let config = SessionConfig::new().with_information_schema(false);
    let ctx = SessionContext::new_with_config(config);

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
        .with_endpoint(&storage.endpoint)
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_virtual_hosted_style_request(!storage.path_style)
        .with_allow_http(storage.allow_http);

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

/// Double-quote an identifier (SQL-safe column name).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
