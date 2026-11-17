//! Credential values never appear in error messages.

pub mod capabilities;
pub mod catalog_kind;
pub mod column_notes;
pub mod connection;
pub mod direct_storage;
pub mod direct_storage_properties;
pub mod parquet_directory;
mod permission;
mod properties;
pub mod pushdown;
#[cfg(test)]
#[path = "pushdown_surface_probe_tests.rs"]
mod pushdown_surface_probe;
pub mod scoped_refresh;
pub mod sharding;
pub mod skipped_tables;
mod statement_state;
mod table_filter;
pub mod table_identity;
pub mod tables;
mod tuning;

use crate::adapter::capabilities::get_capabilities_response;
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::column_notes::checked_note;
use crate::adapter::connection::ConnectionCreds;
use crate::adapter::connection::{catalog_block, read_connection, storage_block};
use crate::adapter::direct_storage::DirectStorageCatalogClient;
use crate::adapter::permission::PermissionCheck;
use crate::adapter::properties::{Check, PROP_CATALOG_CONNECTION, PROP_NAMESPACE, VsProperties};
use crate::adapter::pushdown::{
    ConnectionStorage, PushdownContext, ScanCatalog, ScanUdfs, declare_columns, handle_pushdown,
};
use crate::adapter::scoped_refresh::{RequestedTables, ScopedRefresh};
use crate::adapter::skipped_tables::{SkippedTables, insert_skipped_tables, skip_kind_and_reason};
use crate::adapter::statement_state::{STATEMENT_STATES, StatementCatalog, VirtualSchemaState};
use crate::adapter::table_filter::TableFilter;
use crate::adapter::table_identity::{encode_identifier_note, read_identifier};
use crate::adapter::tables::{
    catalog_identifier_string, flatten_table_name, reject_name_collisions,
};
use crate::adapter::tuning::{Tuning, metadata_read_concurrency, resolve_nr_of_cores};
use crate::scan::sealed::SealedStorageKey;
use crate::scan::spec::StorageBackend;
use crate::types::exasol_type::exasol_type_to_json;
use crate::types::mapping::{EngineTimestampSupport, column_source_type_to_exasol};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::udf_log;
use lakehouse_catalog::{
    CatalogClient, CatalogListing, CatalogTableIdent, GlueCatalogSession, IcebergRestCatalogClient,
    SkippedTable, UnityCatalogSession, resolve_aws_identity,
};
use serde_json::{Value as Json, json};
use std::num::NonZeroUsize;
use vs_expression::ExasolVersion;

const NOTE_CATALOG_PREFIX: &str = "CATALOG_PREFIX";

pub fn adapter_call(ctx: &mut dyn UdfContext, json_arg: &str) -> Result<String, UdfError> {
    let request: Json = serde_json::from_str(json_arg)
        .map_err(|e| UdfError::User(format!("VS request is not valid JSON: {e}")))?;
    let response = dispatch(ctx, &request)?;
    Ok(response.to_string())
}

fn dispatch(ctx: &mut dyn UdfContext, request: &Json) -> Result<Json, UdfError> {
    match request.get("type").and_then(|t| t.as_str()) {
        Some("getCapabilities") => {
            let props = VsProperties::parse(&get_properties(request), Check::Lenient)?;
            Ok(get_capabilities_response(
                props.excluded_capabilities.as_deref(),
            ))
        }
        Some("createVirtualSchema") => handle_create_virtual_schema(ctx, request),
        Some("refresh") => {
            // Stateless: re-resolve the schema, same as create.
            handle_create_virtual_schema(ctx, request)
        }
        Some("setProperties") => {
            // Stateless: re-resolve the schema with the new properties, same as create.
            handle_create_virtual_schema(ctx, request)
        }
        Some("dropVirtualSchema") => Ok(json!({"type": "dropVirtualSchema"})),
        Some("pushdown") => {
            let props = VsProperties::parse(&get_properties(request), Check::Lenient)?;
            // The user is mapped before the CONNECTION is read, so a refused user costs no request.
            let check = props.permission.check_for(|| ctx.current_user())?;
            let tuning = Tuning::of(&props);
            let state = STATEMENT_STATES.get_or_build(virtual_schema_name(request)?, || {
                build_virtual_schema_state(&*ctx, &props, stored_catalog_prefix(request))
            })?;
            let udfs = ScanUdfs::in_schema(Some(&ctx.script_schema()));
            let cluster_nodes = cluster_nodes_from_context(ctx);
            let version = ExasolVersion::of(&ctx.database_version());
            let binary_as_string = props.checked.binary_as_string;

            state.block_on(|statement| async move {
                let context = PushdownContext {
                    connection: statement.connection(),
                    udfs,
                    cluster_nodes,
                    tuning,
                    binary_as_string,
                    version,
                };
                handle_pushdown_request(request, statement, &check, &context).await
            })
        }
        other => Err(UdfError::User(format!(
            "unsupported VS request type: {}",
            other.unwrap_or("(none)")
        ))),
    }
}

/// `schemaMetadataInfo.name`, the key of the virtual schema's statement state. Exasol sends it
/// with every pushdown.
fn virtual_schema_name(request: &Json) -> Result<&str, UdfError> {
    request
        .pointer("/schemaMetadataInfo/name")
        .and_then(Json::as_str)
        .ok_or_else(|| UdfError::User("pushdown: the request names no virtual schema".to_string()))
}

fn build_virtual_schema_state(
    ctx: &dyn UdfContext,
    props: &VsProperties,
    catalog_prefix: Option<String>,
) -> Result<VirtualSchemaState, UdfError> {
    // Built before `resolve_connection_config`, which blocks on it to assume the role.
    let rt = build_runtime()?;
    let config = resolve_connection_config(ctx, props, &rt)?;
    let catalog = StatementCatalog::open(config, props, catalog_prefix)?;
    Ok(VirtualSchemaState::new(rt, catalog))
}

/// `creds` and `storage` carry the AWS identity the request acts as — a named
/// role's session, not the stated key pair.
pub struct ResolvedConnectionConfig {
    pub(crate) catalog_uri: String,
    pub(crate) storage: StorageBackend,
    pub(crate) creds: ConnectionCreds,
    pub(crate) allow_http: bool,
    pub(crate) catalog_kind: CatalogKind,
    pub(crate) connection_name: String,
    pub(crate) sealed_storage_key: Option<SealedStorageKey>,
    pub(crate) metadata_read_concurrency: NonZeroUsize,
}

impl ResolvedConnectionConfig {
    pub(crate) fn connection_storage(&self) -> ConnectionStorage<'_> {
        ConnectionStorage {
            storage: &self.storage,
            creds: &self.creds,
            allow_http: self.allow_http,
        }
    }
}

/// iceberg-rust spawns its per-manifest-entry planning work, which then spreads over the cores.
fn build_runtime() -> Result<tokio::runtime::Runtime, UdfError> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(resolve_nr_of_cores() as usize)
        .enable_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))
}

/// Both entry points call this, so neither reaches the catalog or storage before
/// a named role is assumed; `rt` then assumes the role once per call.
fn resolve_connection_config(
    ctx: &dyn UdfContext,
    props: &VsProperties,
    rt: &tokio::runtime::Runtime,
) -> Result<ResolvedConnectionConfig, UdfError> {
    let kind = props.catalog_kind;
    props.permission.require_supported_kind(kind)?;
    let connection_name = props
        .catalog_connection
        .as_deref()
        .ok_or_else(|| UdfError::User(format!("{PROP_CATALOG_CONNECTION} is required")))?;
    let resolved = read_connection(ctx, Some(connection_name), kind)?;
    props.permission.require_management_url(&resolved.uri)?;
    let allow_http = props.checked.allow_http;
    // A scan finishes well within the session's lifetime, so it needs no renewing role source.
    let creds = rt
        .block_on(resolve_aws_identity(
            resolved.creds,
            &resolved.uri,
            allow_http,
        ))?
        .creds;
    let storage = storage_block(&creds, allow_http);
    Ok(ResolvedConnectionConfig {
        catalog_uri: resolved.uri,
        storage,
        creds,
        allow_http,
        catalog_kind: kind,
        connection_name: connection_name.to_string(),
        sealed_storage_key: resolved.sealed_storage_key,
        metadata_read_concurrency: metadata_read_concurrency(&props.checked.tuning),
    })
}

fn handle_create_virtual_schema(
    ctx: &mut dyn UdfContext,
    request: &Json,
) -> Result<Json, UdfError> {
    // `setProperties` carries ALTER ... SET values, which must win over the persisted ones.
    let raw_props = if request.get("type").and_then(|t| t.as_str()) == Some("setProperties") {
        merge_set_properties(request)
    } else {
        get_properties(request)
    };
    // The permission settings are validated only: listing runs as the CONNECTION's identity.
    let props = VsProperties::parse(&raw_props, Check::Strict)?;
    let rt = build_runtime()?;
    let config = resolve_connection_config(ctx, &props, &rt)?;

    // Optional under direct storage: the CONNECTION address alone denotes the storage subtree.
    let configured_ns: Vec<String> = match config.catalog_kind {
        CatalogKind::DirectStorage => Vec::new(),
        CatalogKind::IcebergRest | CatalogKind::UnityCatalogNative | CatalogKind::Glue => {
            let namespace = props.namespace.as_deref().ok_or_else(|| {
                UdfError::User(format!("property '{PROP_NAMESPACE}' is required"))
            })?;
            namespace.split('.').map(|s| s.to_string()).collect()
        }
    };

    let client = construct_catalog_client(&config, &props, None)?;
    let (listing, skipped_tables) = rt
        .block_on(list_source(
            client.as_ref(),
            &configured_ns,
            &props.checked.table_filter,
            request,
        ))
        .map_err(|e| redact_error(&config.storage, e))?;

    let engine_timestamps =
        EngineTimestampSupport::from(ExasolVersion::of(&ctx.database_version()));

    let tables_json = build_listing_virtual_tables(&configured_ns, &listing, engine_timestamps)
        .map_err(|e| redact_error(&config.storage, e))?;

    for entry in &listing.skipped {
        udf_log!(ctx, warn, "{}", skip_warning(entry));
    }

    let adapter_notes = build_adapter_notes(request, &skipped_tables, client.catalog_prefix());

    let schema_metadata = json!({
        "tables": tables_json,
        "adapterNotes": adapter_notes,
    });

    Ok(build_schema_response(request, schema_metadata))
}

/// A partial refresh lists identifiers only and loads just the requested tables, so a broken
/// unrequested table cannot fail it. It reads no identifier note: each name resolves afresh.
/// A table the filter hides is never loaded or skipped: a partial refresh drops it from the
/// listing, so a requested hidden name resolves like a name the source does not serve.
async fn list_source(
    client: &dyn CatalogClient,
    configured_ns: &[String],
    table_filter: &TableFilter,
    request: &Json,
) -> Result<(CatalogListing, SkippedTables), UdfError> {
    let exposes =
        |ident: &CatalogTableIdent| table_filter.exposes(&flatten_table_name(configured_ns, ident));
    let Some(requested) = RequestedTables::from_request(request)? else {
        let listing = client.list_tables(configured_ns, &exposes).await?;
        let skipped_tables = SkippedTables::listed(&listing.skipped);
        return Ok((listing, skipped_tables));
    };
    let mut listed = client.list_table_idents(configured_ns).await?;
    listed.retain(|ident| exposes(ident));
    let scope = ScopedRefresh::new(&requested, configured_ns, &listed);
    let listing = client.load_listed_tables(&scope.selection()).await?;
    let persisted = SkippedTables::persisted(&parse_adapter_notes(request));
    let skipped_tables = scope.merge_skipped_tables(&listing.skipped, &persisted);
    Ok((listing, skipped_tables))
}

fn skip_warning(entry: &SkippedTable) -> String {
    let (skipped_kind, reason) = skip_kind_and_reason(entry);
    format!(
        "createVirtualSchema: skipping {skipped_kind} '{}' ({reason})",
        catalog_identifier_string(&entry.ident),
    )
}

/// The `requestedTables` echo marks a partial refresh, which Exasol scopes to the requested names:
/// it replaces or creates each returned table, drops a requested one the response omits, and keeps
/// every other virtual table (verified live, #425).
fn build_schema_response(request: &Json, schema_metadata: Json) -> Json {
    let response_type = request
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("createVirtualSchema");
    let mut response = json!({
        "type": response_type,
        "schemaMetadata": schema_metadata,
    });
    if let Some(requested_tables) = request.get("requestedTables") {
        response["requestedTables"] = requested_tables.clone();
    }
    response
}

async fn handle_pushdown_request(
    request: &Json,
    statement: &StatementCatalog,
    permission: &PermissionCheck,
    context: &PushdownContext<'_>,
) -> Result<Json, UdfError> {
    let config = statement.connection();
    let iceberg_identifier = resolve_pushdown_identifier(request)?;
    let catalog = catalog_block(&config.creds, &iceberg_identifier);

    handle_pushdown(request, statement, permission, &catalog, context)
        .await
        .map_err(|e| redact_error(&config.storage, e))
}

/// `schemaMetadataInfo.properties` wins on conflict.
fn get_properties(request: &Json) -> Json {
    let mut merged = match request.get("properties") {
        Some(Json::Object(m)) => m.clone(),
        _ => serde_json::Map::new(),
    };
    if let Some(Json::Object(smi)) = request.get("schemaMetadataInfo")
        && let Some(Json::Object(props)) = smi.get("properties")
    {
        for (k, v) in props {
            merged.insert(k.clone(), v.clone());
        }
    }
    Json::Object(merged)
}

/// Inverse precedence of [`get_properties`]: request values win, and a `null` removes the
/// property, so a null-unset required property fails its check instead of keeping its old value.
fn merge_set_properties(request: &Json) -> Json {
    let mut merged = match request
        .get("schemaMetadataInfo")
        .and_then(|smi| smi.get("properties"))
    {
        Some(Json::Object(m)) => m.clone(),
        _ => serde_json::Map::new(),
    };
    if let Some(Json::Object(props)) = request.get("properties") {
        for (k, v) in props {
            if v.is_null() {
                merged.remove(k);
            } else {
                merged.insert(k.clone(), v.clone());
            }
        }
    }
    Json::Object(merged)
}

/// adapterNotes arrives as a JSON-encoded string, not an object.
fn parse_adapter_notes(request: &Json) -> serde_json::Map<String, Json> {
    request
        .get("schemaMetadataInfo")
        .and_then(|smi| smi.get("adapterNotes"))
        .and_then(|n| n.as_str())
        .filter(|s| !s.is_empty())
        .and_then(|s| serde_json::from_str::<Json>(s).ok())
        .and_then(|v| match v {
            Json::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default()
}

/// An empty catalog prefix is valid, so an empty value is kept.
fn stored_catalog_prefix(request: &Json) -> Option<String> {
    parse_adapter_notes(request)
        .get(NOTE_CATALOG_PREFIX)
        .and_then(Json::as_str)
        .map(str::to_string)
}

/// The only site that matches a [`CatalogKind`] to build a client or load a table; the listing
/// pipeline and the pushdown scan load after it never re-match the kind. Issues no request, and
/// redacts a construction error against the CONNECTION's storage secrets.
fn construct_catalog_client(
    config: &ResolvedConnectionConfig,
    props: &VsProperties,
    catalog_prefix: Option<String>,
) -> Result<Box<dyn ScanCatalog>, UdfError> {
    client_of_kind(config, props, catalog_prefix).map_err(|e| redact_error(&config.storage, e))
}

fn client_of_kind(
    config: &ResolvedConnectionConfig,
    props: &VsProperties,
    catalog_prefix: Option<String>,
) -> Result<Box<dyn ScanCatalog>, UdfError> {
    let (catalog_uri, storage, creds) = (&config.catalog_uri, &config.storage, &config.creds);
    let metadata_read_concurrency = config.metadata_read_concurrency;
    match config.catalog_kind {
        CatalogKind::IcebergRest => Ok(Box::new(IcebergRestCatalogClient::new(
            catalog_uri.clone(),
            creds.clone(),
            catalog_prefix,
            metadata_read_concurrency,
        ))),
        CatalogKind::UnityCatalogNative => Ok(Box::new(UnityCatalogSession::new(
            catalog_uri,
            creds.clone(),
        ))),
        CatalogKind::DirectStorage => {
            let resolved =
                direct_storage_properties::resolve_direct_storage_properties(props, catalog_uri)?;
            let secrets = storage.secret_values();
            let client = DirectStorageCatalogClient::new(
                storage,
                &resolved.base_path,
                resolved.directory_options(),
                metadata_read_concurrency,
                &secrets,
            )?;
            Ok(Box::new(client))
        }
        CatalogKind::Glue => Ok(Box::new(GlueCatalogSession::new(
            catalog_uri,
            storage.clone(),
            creds.clone(),
            metadata_read_concurrency,
        )?)),
    }
}

/// The full-Unicode `to_uppercase` fold is a deliberate Exasol-target trade-off: `ß` expands to
/// `SS`, so two columns differing only in that expansion collapse to one name with no check.
fn build_listing_virtual_tables(
    configured_ns: &[String],
    listing: &CatalogListing,
    engine_timestamps: EngineTimestampSupport,
) -> Result<Vec<Json>, UdfError> {
    reject_name_collisions(configured_ns, listing.tables.iter().map(|t| &t.ident))?;
    let mut tables_json: Vec<Json> = Vec::with_capacity(listing.tables.len());

    for table in &listing.tables {
        let exasol_name = flatten_table_name(configured_ns, &table.ident);
        let columns = table
            .columns
            .iter()
            .zip(declare_columns(table))
            .map(|(col, note)| {
                let column_name = col.name.to_uppercase();
                let mut column = json!({
                    "name": column_name,
                    "dataType": exasol_type_to_json(&column_source_type_to_exasol(
                        &col.source_type,
                        engine_timestamps,
                    )),
                });
                if let Some(note) = note {
                    column["adapterNotes"] =
                        Json::String(checked_note(&exasol_name, &column_name, note)?);
                }
                Ok(column)
            })
            .collect::<Result<Vec<Json>, UdfError>>()?;
        tables_json.push(json!({
            "name": exasol_name,
            "adapterNotes": encode_identifier_note(&table.ident),
            "columns": columns,
        }));
    }

    Ok(tables_json)
}

fn resolve_pushdown_identifier(request: &Json) -> Result<String, UdfError> {
    let involved_table_name = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|t| t.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| UdfError::User("pushdown request missing involvedTables[0].name".into()))?;

    read_identifier(request, involved_table_name)
}

/// Exasol rejects a raw-object adapterNotes, so it is a JSON string; pre-existing notes are merged.
fn build_adapter_notes(
    request: &Json,
    skipped: &SkippedTables,
    catalog_prefix: Option<String>,
) -> Json {
    let mut notes = parse_adapter_notes(request);
    match catalog_prefix {
        Some(prefix) => notes.insert(NOTE_CATALOG_PREFIX.to_string(), Json::String(prefix)),
        None => notes.remove(NOTE_CATALOG_PREFIX),
    };
    insert_skipped_tables(&mut notes, skipped);
    Json::String(Json::Object(notes).to_string())
}

/// `0` (stub or missing handshake) maps to `1`, the single-shard fallback.
fn cluster_nodes_from_context(ctx: &dyn UdfContext) -> usize {
    match ctx.node_count() {
        0 => 1,
        n => n as usize,
    }
}

/// Value-based stripping runs first to catch secrets the label-based heuristic misses.
fn redact_error(storage: &StorageBackend, e: UdfError) -> UdfError {
    match e {
        UdfError::User(msg) => {
            let stripped = crate::scan::emit::redact_secret_values(&msg, &storage.secret_values());
            UdfError::User(crate::scan::emit::redact_credentials(&stripped))
        }
        other => other,
    }
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "catalog_client_tests.rs"]
mod catalog_client_tests;

#[cfg(test)]
#[path = "unity_schema_tests.rs"]
mod unity_schema_tests;
