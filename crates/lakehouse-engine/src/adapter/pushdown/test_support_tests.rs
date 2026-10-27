use super::*;
use crate::adapter::column_notes::{DeclaredSchema, TableNotes};
use crate::adapter::table_identity::encode_identifier_note;
use crate::scan::sealed::{SealedStorageKey, derive_sealed_storage_key};
use crate::scan::spec::{DeleteMechanism, ScanStorage, StorageProps};
use lakehouse_catalog::{
    CatalogColumn, CatalogTable, CatalogTableIdent, CatalogTableType, ColumnSourceType, TableFormat,
};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type RecordedRequests = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// Every response closes its connection, so one accept loop serves a pooled client's sequential
/// requests in arrival order. `responder` maps a request head to a status, content type, and body;
/// the validators make a body readable as an object.
async fn spawn_fake_http_server<F>(responder: F) -> (String, RecordedRequests)
where
    F: Fn(&str) -> (u16, &'static str, String) + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let address = format!("http://{}", listener.local_addr().expect("local_addr"));
    let requests: RecordedRequests = Arc::default();

    let recorded = requests.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let Some((head, body)) = read_http_request(&mut stream).await else {
                continue;
            };
            let (status, content_type, response_body) = responder(&head);
            recorded
                .lock()
                .expect("recorded requests")
                .push((head, body));
            let reason = if (200..300).contains(&status) {
                "OK"
            } else {
                "ERROR"
            };
            let length = response_body.len();
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n\
                 Content-Length: {length}\r\nETag: \"e{length}\"\r\n\
                 Last-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\nAccept-Ranges: bytes\r\n\
                 Connection: close\r\n\r\n{response_body}"
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    (address, requests)
}

fn request_target(head: &str) -> &str {
    head.split_whitespace().nth(1).unwrap_or("")
}

pub(crate) struct RecordingCatalog {
    pub(super) uri: String,
    requests: RecordedRequests,
}

impl RecordingCatalog {
    /// `responder` maps a request target to a status and body.
    pub(super) async fn spawn<F>(responder: F) -> Self
    where
        F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
    {
        let (uri, requests) = spawn_fake_http_server(move |head| {
            let (status, body) = responder(request_target(head));
            (status, "application/json", body)
        })
        .await;
        Self { uri, requests }
    }

    pub(super) fn targets(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("recorded requests")
            .iter()
            .map(|(head, _)| request_target(head).to_string())
            .collect()
    }
}
/// No catalog auth, so a session issues no token-grant request.
pub(crate) fn unauthenticated_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "wh".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: None,
        path_style: Some(true),
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
        ..Default::default()
    }
}

pub(crate) const ICEBERG_CONFIG_TARGET: &str = "/v1/config?warehouse=wh";

pub(crate) const ICEBERG_LOAD_TABLE_TARGET: &str = "/v1/namespaces/db/tables/t?snapshots=refs";

pub(crate) const UNITY_TABLE_TARGET: &str = "/api/2.1/unity-catalog/tables/cat.sch.orders";

/// SigV4 mode lets `CatalogSession::resolve` build a session without contacting the catalog, so
/// `loadTable` is a loopback catalog's only request.
pub(crate) fn sigv4_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "123456789012".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "signing-access-key".into(),
        secret_key: SIGV4_SECRET_KEY.into(),
        path_style: Some(true),
        use_sigv4: true,
        ..Default::default()
    }
}

pub(crate) const SIGV4_SECRET_KEY: &str = "signing-secret-key";

/// Closed port: a stray request fails with a transport error, distinct from every asserted refusal.
pub(crate) const UNREACHABLE_CATALOG: &str = "http://127.0.0.1:1";

/// A `loadTable` body for a one-column table at `s3://bucket/db/t`, each `metadata` field
/// replacing its default. No snapshot, so the empty scan reads no manifest and resolution stays
/// offline.
pub(crate) fn load_table_body(metadata: Json) -> Json {
    let mut fields = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000003",
        "location": "s3://bucket/db/t",
        "last-sequence-number": 0,
        "last-updated-ms": 0,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]
        }],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "snapshots": []
    });
    let Json::Object(overrides) = metadata else {
        panic!("the metadata overrides are an object: {metadata}");
    };
    fields.as_object_mut().expect("an object").extend(overrides);
    let location = fields["location"].as_str().unwrap_or_default().to_string();
    serde_json::json!({
        "metadata-location": format!("{location}/metadata/v1.json"),
        "metadata": fields,
    })
}

pub(crate) fn snapshotless_load_table_body(location: &str) -> String {
    load_table_body(serde_json::json!({ "location": location })).to_string()
}

pub(crate) fn load_table_body_with_columns(fields: Json, last_column_id: i32) -> String {
    load_table_body(serde_json::json!({
        "last-column-id": last_column_id,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": fields}],
    }))
    .to_string()
}

/// Binary, fixed, and uuid columns at the top level and below a struct, list, and map, between
/// the mappable `id` and `name`; the last column id is [`BINARY_ICEBERG_LAST_COLUMN_ID`].
pub(crate) fn binary_iceberg_fields() -> serde_json::Value {
    serde_json::json!([
        {"id": 1, "name": "id", "required": false, "type": "int"},
        {"id": 2, "name": "b", "required": false, "type": "binary"},
        {"id": 3, "name": "f", "required": false, "type": "fixed[16]"},
        {"id": 4, "name": "u", "required": false, "type": "uuid"},
        {"id": 5, "name": "s", "required": false, "type": {"type": "struct", "fields": [
            {"id": 6, "name": "ok", "required": false, "type": "string"},
            {"id": 7, "name": "x", "required": false, "type": "binary"}
        ]}},
        {"id": 8, "name": "l", "required": false, "type": {
            "type": "list", "element-id": 9, "element": "fixed[4]", "element-required": false
        }},
        {"id": 10, "name": "m", "required": false, "type": {
            "type": "map", "key-id": 11, "key": "string",
            "value-id": 12, "value": "uuid", "value-required": false
        }},
        {"id": 13, "name": "name", "required": false, "type": "string"}
    ])
}

pub(crate) const BINARY_ICEBERG_LAST_COLUMN_ID: i32 = 13;

pub(crate) fn user_message(error: UdfError) -> String {
    match error {
        UdfError::User(message) => message,
        other => panic!("expected UdfError::User, got {other:?}"),
    }
}

pub(crate) fn assert_mentions(message: &str, fragments: &[&str]) {
    for fragment in fragments {
        assert!(
            message.contains(fragment),
            "'{fragment}' missing: {message}"
        );
    }
}

pub(crate) fn assert_refuses_binary_col(error: UdfError) {
    let message = user_message(error);
    assert!(
        message.contains("binary_col") && message.contains("#351"),
        "the refusal must be the gate's own message, naming the column and its reason: \
         {message}"
    );
}

pub(crate) fn glue_column(name: &str, hive_type: &str) -> CatalogColumn {
    CatalogColumn::new(
        name,
        ColumnSourceType::Glue {
            hive_type: hive_type.to_string(),
        },
    )
}

/// A base for `..` struct update: the unpartitioned, columnless table `db.t` at `location`.
pub(crate) fn catalog_table(format: TableFormat, location: &str) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["db".to_string()],
            name: "t".to_string(),
        },
        table_type: CatalogTableType::Table,
        storage_location: Some(location.to_string()),
        format,
        vended_credential_key: None,
        partition_columns: Vec::new(),
        columns: Vec::new(),
        metadata_location: None,
        properties: Default::default(),
    }
}

/// The table `namespace.name` holding `columns`, at no storage location.
pub(crate) fn named_table(
    namespace: &[&str],
    name: &str,
    format: TableFormat,
    columns: Vec<CatalogColumn>,
) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: namespace.iter().map(|part| part.to_string()).collect(),
            name: name.to_string(),
        },
        storage_location: None,
        columns,
        ..catalog_table(format, "")
    }
}

pub(crate) fn iceberg_column(field: &iceberg::spec::NestedFieldRef) -> CatalogColumn {
    CatalogColumn::new(
        field.name.clone(),
        ColumnSourceType::Iceberg(Arc::clone(field)),
    )
}

/// The source of an optional Iceberg column `c` of the primitive type `primitive`.
pub(crate) fn iceberg_source(primitive: iceberg::spec::PrimitiveType) -> ColumnSourceType {
    let field =
        iceberg::spec::NestedField::optional(1, "c", iceberg::spec::Type::Primitive(primitive));
    ColumnSourceType::Iceberg(field.into())
}

pub(crate) fn unity_column(name: &str, type_json: Option<String>) -> CatalogColumn {
    CatalogColumn::new(name, ColumnSourceType::Unity { type_json })
}

pub(crate) fn glue_catalog_table(
    database: &str,
    name: &str,
    format: TableFormat,
    location: &str,
    columns: &[(&str, &str)],
) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec![database.to_string()],
            name: name.to_string(),
        },
        columns: columns
            .iter()
            .map(|(name, hive_type)| glue_column(name, hive_type))
            .collect(),
        ..catalog_table(format, location)
    }
}

pub(crate) fn iceberg_notes(fields: &[iceberg::spec::NestedFieldRef]) -> Vec<(String, String)> {
    declared_notes(&CatalogTable {
        columns: fields.iter().map(iceberg_column).collect(),
        ..catalog_table(TableFormat::Iceberg, "s3://bucket/db/t")
    })
}

/// What CREATE records for every column of `table`: its Exasol name and note, through the real
/// `declare_columns`.
pub(crate) fn declared_notes(table: &CatalogTable) -> Vec<(String, String)> {
    let notes = declare_columns(table);
    table
        .columns
        .iter()
        .zip(notes)
        .map(|(column, note)| {
            let note = note.expect("CREATE declares every column");
            (column.name.to_uppercase(), note)
        })
        .collect()
}

/// The notes CREATE records for the current schema of a `loadTable` `body`.
pub(crate) fn load_table_body_notes(body: &Json) -> Vec<(String, String)> {
    let metadata: iceberg::spec::TableMetadata =
        serde_json::from_value(body["metadata"].clone()).expect("the body's metadata decodes");
    iceberg_notes(metadata.current_schema().as_struct().fields())
}

pub(crate) fn declared_logical_schema(schema: &iceberg::spec::Schema) -> Vec<LogicalField> {
    let notes = iceberg_notes(schema.as_struct().fields());
    decoded_notes(&notes)
        .into_iter()
        .map(|declaration| declaration.source_type)
        .collect()
}

/// Each of `notes` decoded back into the declaration CREATE encoded.
pub(crate) fn decoded_notes(
    notes: &[(String, String)],
) -> Vec<crate::adapter::column_notes::ColumnDeclaration> {
    notes
        .iter()
        .map(|(_, note)| serde_json::from_str(note).expect("a declared note decodes"))
        .collect()
}

/// The declaration CREATE records for each column of `table`.
pub(crate) fn declared_columns(
    table: &CatalogTable,
) -> Vec<crate::adapter::column_notes::ColumnDeclaration> {
    decoded_notes(&declared_notes(table))
}

/// A nullable, unbound field of the given Arrow tag.
pub(crate) fn nullable_field(name: &str, arrow_type: &str) -> LogicalField {
    LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    }
}

/// The refusal a `label` table records for the binary `column`, at `member_path` when nested.
pub(crate) fn binary_refused(
    label: &str,
    column: &str,
    member_path: Option<&str>,
    declared: &str,
) -> RefusedColumn {
    RefusedColumn {
        column_name: column.to_string(),
        reason: crate::adapter::column_notes::binary_refusal(label, column, member_path, declared),
    }
}

/// `notes` as the columns of the involved table `table` in a pushdown request.
pub(crate) fn involved_notes<'a>(table: &'a str, notes: &'a [(String, String)]) -> TableNotes<'a> {
    TableNotes {
        table,
        columns: notes
            .iter()
            .map(|(column, note)| (column.as_str(), Some(note.as_str())))
            .collect(),
    }
}

/// What the resolver answers for `reader` over the table `notes` declare: the one parse, the
/// reader's files for that declaration, and the one assembly.
pub(crate) async fn resolve_from_notes(
    reader: &dyn FormatReader,
    notes: &TableNotes<'_>,
    filter_json: Option<&Json>,
) -> Result<ResolvedScan, UdfError> {
    ResolvedScan::resolve(reader, notes.declared_schema()?, filter_json).await
}

/// [`resolve_from_notes`] over the notes CREATE records for `table`, involved as `name`.
pub(crate) async fn resolve_declared(
    reader: &dyn FormatReader,
    name: &str,
    table: &CatalogTable,
    filter_json: Option<&Json>,
) -> Result<ResolvedScan, UdfError> {
    let notes = declared_notes(table);
    resolve_from_notes(reader, &involved_notes(name, &notes), filter_json).await
}

/// The declaration the pushdown parses from the notes CREATE records for `table`.
pub(crate) fn declared_schema_of(table: &CatalogTable) -> DeclaredSchema {
    let notes = declared_notes(table);
    involved_notes(&table.ident.name.to_uppercase(), &notes)
        .declared_schema()
        .expect("CREATE's notes parse")
}

/// The declaration of a table whose involved columns carry no note.
pub(crate) fn no_notes() -> DeclaredSchema {
    TableNotes::default()
        .declared_schema()
        .expect("an empty declaration parses")
}

/// `request` with the note CREATE writes on every involved column for an optional Iceberg `long`
/// field numbered by its position, for requests that plan no data file.
pub(crate) fn with_iceberg_notes(mut request: Json) -> Json {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    let tables = request["involvedTables"]
        .as_array_mut()
        .into_iter()
        .flatten();
    for table in tables {
        let columns = table["columns"].as_array_mut().into_iter().flatten();
        for (position, column) in (1..).zip(columns) {
            let name = column["name"].as_str().unwrap_or_default().to_lowercase();
            let field = NestedField::optional(position, name, Type::Primitive(PrimitiveType::Long));
            column["adapterNotes"] = iceberg_notes(&[field.into()]).remove(0).1.into();
        }
    }
    request
}

pub(crate) fn unity_delta_table(
    fields: &[delta_kernel::schema::StructField],
    mode: Option<&str>,
    partitions: &[&str],
) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["cat".into(), "sch".into()],
            name: "t".into(),
        },
        partition_columns: partitions.iter().map(|name| name.to_string()).collect(),
        columns: fields
            .iter()
            .map(|field| {
                let type_json = serde_json::to_string(field).expect("a field serializes");
                unity_column(field.name(), Some(type_json))
            })
            .collect(),
        properties: mode
            .map(|mode| ("delta.columnMapping.mode".to_string(), mode.to_string()))
            .into_iter()
            .collect(),
        ..catalog_table(TableFormat::Delta, "s3://bucket/t")
    }
}

/// The pairs a reader hands the Delta translator for a table without column mapping: every
/// column of `schema` under its own name.
pub(crate) fn by_name(
    schema: &delta_kernel::schema::StructType,
) -> Vec<(&str, &delta_kernel::schema::StructField)> {
    schema
        .fields()
        .map(|field| (field.name().as_str(), field))
        .collect()
}

/// What CREATE records for a Delta table without column mapping whose nullable columns are the
/// `(name, delta_type)` pairs [`fileless_delta_commit`] logs, partitioned by `partitions`.
pub(crate) fn delta_notes(columns: &[(&str, &str)], partitions: &[&str]) -> Vec<(String, String)> {
    let fields: Vec<delta_kernel::schema::StructField> = columns
        .iter()
        .map(|(name, delta_type)| {
            serde_json::from_value(serde_json::json!({
                "name": name, "type": delta_type, "nullable": true, "metadata": {},
            }))
            .expect("a Delta field decodes")
        })
        .collect();
    declared_notes(&unity_delta_table(&fields, None, partitions))
}

/// `request` with each column of its involved table `table` carrying the note `notes` holds
/// under the column's name.
pub(crate) fn with_notes(mut request: Json, table: &str, notes: &[(String, String)]) -> Json {
    let tables = request["involvedTables"]
        .as_array_mut()
        .into_iter()
        .flatten();
    let columns = tables
        .filter(|involved| involved["name"] == table)
        .flat_map(|involved| involved["columns"].as_array_mut().into_iter().flatten());
    for column in columns {
        let name = column["name"].as_str().unwrap_or_default().to_string();
        if let Some((_, note)) = notes.iter().find(|(declared, _)| *declared == name) {
            column["adapterNotes"] = note.clone().into();
        }
    }
    request
}

/// [`with_notes`] carrying the [`delta_notes`] of `columns` partitioned by `partitions`.
pub(crate) fn with_delta_notes(
    request: Json,
    table: &str,
    columns: &[(&str, &str)],
    partitions: &[&str],
) -> Json {
    with_notes(request, table, &delta_notes(columns, partitions))
}

/// `request` with each involved table named in `identifiers` carrying the identifier note CREATE
/// writes for its dotted catalog identifier, `(exasol name, dotted identifier)`.
pub(crate) fn with_identifier_notes(mut request: Json, identifiers: &[(&str, &str)]) -> Json {
    let tables = request["involvedTables"]
        .as_array_mut()
        .into_iter()
        .flatten();
    for involved in tables {
        let table = involved["name"].as_str().unwrap_or_default();
        if let Some((_, dotted)) = identifiers.iter().find(|(name, _)| *name == table) {
            let mut segments: Vec<String> = dotted.split('.').map(str::to_string).collect();
            let ident = CatalogTableIdent {
                name: segments.pop().unwrap_or_default(),
                namespace: segments,
            };
            involved["adapterNotes"] = encode_identifier_note(&ident).into();
        }
    }
    request
}

/// No storage location, so the Delta reader refuses before reaching any object store.
pub(crate) fn locationless_delta_table_body() -> String {
    serde_json::json!({
        "name": "orders",
        "table_type": "MANAGED",
        "data_source_format": "DELTA",
        "table_id": "table-1",
        "columns": []
    })
    .to_string()
}

pub(crate) async fn unity_delta_catalog() -> RecordingCatalog {
    RecordingCatalog::spawn(|target| match target.rsplit_once('.') {
        Some((_, name)) if !name.is_empty() => (
            200,
            serde_json::json!({
                "name": name,
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "storage_location": format!("s3://bucket/{name}"),
                "table_id": format!("table-{name}"),
                "columns": [],
            })
            .to_string(),
        ),
        _ => (404, r#"{"message":"no such table"}"#.to_string()),
    })
    .await
}

pub(crate) fn fileless_delta_commit(id: &str, columns: &[(&str, &str)]) -> String {
    fileless_delta_commit_with_protocol(
        id,
        columns,
        serde_json::json!({"minReaderVersion": 1, "minWriterVersion": 2}),
    )
}

pub(crate) fn fileless_delta_commit_with_protocol(
    id: &str,
    columns: &[(&str, &str)],
    protocol: Json,
) -> String {
    let fields: Vec<Json> = columns
        .iter()
        .map(|(name, delta_type)| {
            serde_json::json!({
                "name": name, "type": delta_type, "nullable": true, "metadata": {},
            })
        })
        .collect();
    let protocol = serde_json::json!({"protocol": protocol});
    let metadata = serde_json::json!({"metaData": {
        "id": id,
        "format": {"provider": "parquet", "options": {}},
        "schemaString": serde_json::json!({"type": "struct", "fields": fields}).to_string(),
        "partitionColumns": [],
        "configuration": {},
        "createdTime": 1,
    }});
    format!("{protocol}\n{metadata}\n")
}

pub(crate) fn delta_commit_zero_key(name: &str) -> String {
    format!("{name}/_delta_log/00000000000000000000.json")
}

/// Served over HTTP because `read_delta_log` builds its own store from the
/// storage backend, so no injected store is reachable. Data-file reads 404;
/// plan-time resolution performs none.
pub(crate) async fn delta_object_endpoint(objects: Vec<(String, String)>) -> StorageBackend {
    object_endpoint("bucket", objects).await
}

pub(crate) async fn object_endpoint(
    bucket: &str,
    objects: Vec<(String, String)>,
) -> StorageBackend {
    ObjectEndpoint::spawn(bucket, objects).await.storage
}

/// Serves each `(key, body)` of `bucket` path-style, listing as S3's `ListObjectsV2` does; any
/// other key is absent.
pub(crate) struct ObjectEndpoint {
    pub(super) storage: StorageBackend,
    requests: RecordedRequests,
}

impl ObjectEndpoint {
    pub(super) async fn spawn(bucket: &str, objects: Vec<(String, String)>) -> Self {
        let bucket = bucket.to_string();
        let (endpoint, requests) = spawn_fake_http_server(move |head| {
            let target = request_target(head);
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            if query.contains("list-type=2") {
                let listing = list_bucket_result(&bucket, query, &objects);
                return (200, "application/xml", listing);
            }
            let key = path
                .strip_prefix(&format!("/{bucket}/"))
                .map(|key| percent_encoding::percent_decode_str(key).decode_utf8_lossy());
            match objects
                .iter()
                .find(|(served, _)| key.as_deref() == Some(served.as_str()))
            {
                Some((_, body)) => (200, "application/octet-stream", body.clone()),
                None => {
                    let error = r#"<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>"#;
                    (404, "application/xml", error.to_string())
                }
            }
        })
        .await;
        let storage = StorageBackend::S3(StorageProps {
            endpoint,
            region: "us-east-1".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            allow_http: true,
            path_style: true,
            ..Default::default()
        });
        Self { storage, requests }
    }

    /// The prefix of every listing request, sorted.
    pub(super) fn listed_prefixes(&self) -> Vec<String> {
        let mut prefixes: Vec<String> = self
            .requests
            .lock()
            .expect("recorded requests")
            .iter()
            .filter_map(|(head, _)| {
                let (_, query) = request_target(head).split_once('?')?;
                query
                    .contains("list-type=2")
                    .then(|| query_param(query, "prefix"))
            })
            .collect();
        prefixes.sort();
        prefixes
    }
}

fn query_param(query: &str, key: &str) -> String {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
        .unwrap_or_default()
}

pub(crate) struct GlueEndpoint {
    pub(super) address: String,
    requests: RecordedRequests,
}

impl GlueEndpoint {
    /// `responder` maps a Glue operation name, such as `GetTable`, to a status and JSON body.
    pub(super) async fn spawn<F>(responder: F) -> Self
    where
        F: Fn(&str) -> (u16, serde_json::Value) + Send + Sync + 'static,
    {
        let (address, requests) = spawn_fake_http_server(move |head| {
            let (status, body) = match glue_operation(head) {
                Some(operation) => responder(operation),
                None => (400, serde_json::Value::Null),
            };
            (status, "application/x-amz-json-1.1", body.to_string())
        })
        .await;
        Self { address, requests }
    }

    pub(super) fn operations(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("recorded requests")
            .iter()
            .filter_map(|(head, _)| glue_operation(head).map(str::to_string))
            .collect()
    }

    pub(super) fn bodies_of(&self, operation: &str) -> Vec<serde_json::Value> {
        self.requests
            .lock()
            .expect("recorded requests")
            .iter()
            .filter(|(head, _)| glue_operation(head) == Some(operation))
            .map(|(_, body)| serde_json::from_slice(body).expect("a Glue request body is JSON"))
            .collect()
    }
}

fn glue_operation(head: &str) -> Option<&str> {
    header(head, "x-amz-target")?.strip_prefix("AWSGlue.")
}

/// The request head and its whole body: closing a socket with unread bytes resets it before the
/// client reads the response.
async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Option<(String, Vec<u8>)> {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        raw.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
    let content_length: usize = header(&head, "content-length")
        .and_then(|length| length.parse().ok())
        .unwrap_or(0);
    let mut body = raw[head_end + 4..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Some((head, body))
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.trim().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
}

/// Honors `delimiter` as S3 does: a key with the delimiter below `prefix` rolls up into its
/// common prefix, so a delimiter listing returns only the prefix's direct children as objects.
fn list_bucket_result(bucket: &str, query: &str, objects: &[(String, String)]) -> String {
    let param = |key: &str| query_param(query, key);
    let (prefix, after, delimiter) = (param("prefix"), param("start-after"), param("delimiter"));
    let mut contents = String::new();
    let mut common_prefixes = std::collections::BTreeSet::new();
    for (key, body) in objects
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix) && key > &after)
    {
        let rolled_up = (!delimiter.is_empty())
            .then(|| key[prefix.len()..].find(&delimiter))
            .flatten();
        match rolled_up {
            Some(end) => {
                common_prefixes.insert(key[..prefix.len() + end + delimiter.len()].to_string());
            }
            None => contents.push_str(&format!(
                "<Contents><Key>{key}</Key>\
                 <LastModified>2024-01-01T00:00:00.000Z</LastModified>\
                 <ETag>&quot;e{}&quot;</ETag><Size>{}</Size>\
                 <StorageClass>STANDARD</StorageClass></Contents>",
                body.len(),
                body.len()
            )),
        }
    }
    let common_prefixes: String = common_prefixes
        .iter()
        .map(|common| format!("<CommonPrefixes><Prefix>{common}</Prefix></CommonPrefixes>"))
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>{bucket}</Name><Prefix>{prefix}</Prefix><MaxKeys>1000</MaxKeys>\
         <IsTruncated>false</IsTruncated>{contents}{common_prefixes}</ListBucketResult>"
    )
}

pub(crate) async fn delta_pushdown(
    request: &Json,
    catalog_uri: &str,
    storage: StorageBackend,
    table: &str,
) -> Result<Json, UdfError> {
    let conn = ResolvedConnectionConfig {
        catalog_uri: catalog_uri.to_string(),
        storage,
        creds: unauthenticated_creds(),
        allow_http: true,
        catalog_kind: CatalogKind::UnityCatalogNative,
        connection_name: TEST_CONNECTION_NAME.to_string(),
        sealed_storage_key: Some(test_sealing_key()),
        permission_check: crate::adapter::permission::PermissionCheck::Off,
    };
    let catalog = CatalogProps {
        warehouse: "wh".into(),
        table: table.into(),
    };
    handle_pushdown(
        request, &conn, &catalog, None, 1, 1, 1, 1024, 1, 0.6, 200, 4, 1024,
    )
    .await
}

pub(crate) async fn iceberg_catalog() -> RecordingCatalog {
    RecordingCatalog::spawn(|target| {
        if target.starts_with("/v1/config") {
            return (200, "{}".to_string());
        }
        match target.rsplit_once('/') {
            Some((_, table)) => (
                200,
                snapshotless_load_table_body(&format!("s3://bucket/{table}")),
            ),
            None => (404, r#"{"message":"no such table"}"#.to_string()),
        }
    })
    .await
}

pub(crate) const TEST_CONNECTION_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

pub(crate) const TEST_CONNECTION_PASSWORD: &str =
    r#"{"warehouse":"wh","secret_key":"FIXTURESECRET"}"#;

pub(crate) fn test_sealing_key() -> SealedStorageKey {
    derive_sealed_storage_key(TEST_CONNECTION_PASSWORD)
}

fn test_connection(creds: ConnectionCreds) -> ResolvedConnectionConfig {
    ResolvedConnectionConfig {
        catalog_uri: "http://catalog.example.com".to_string(),
        storage: sample_storage(),
        creds,
        allow_http: true,
        catalog_kind: CatalogKind::IcebergRest,
        connection_name: TEST_CONNECTION_NAME.to_string(),
        sealed_storage_key: Some(test_sealing_key()),
        permission_check: crate::adapter::permission::PermissionCheck::Off,
    }
}

pub(crate) static TEST_CONNECTION: std::sync::LazyLock<ResolvedConnectionConfig> =
    std::sync::LazyLock::new(|| test_connection(unauthenticated_creds()));

pub(crate) static TEST_VENDED_CONNECTION: std::sync::LazyLock<ResolvedConnectionConfig> =
    std::sync::LazyLock::new(|| {
        test_connection(ConnectionCreds {
            use_vended_credentials: true,
            ..unauthenticated_creds()
        })
    });

pub(crate) fn sample_scan_storage() -> ScanStorage {
    ScanStorage::Connection {
        name: TEST_CONNECTION_NAME.to_string(),
        allow_http: true,
    }
}

pub(crate) fn sample_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        allow_http: true,
        ..Default::default()
    })
}

pub(crate) const SENTINEL_ACCESS_KEY: &str = "AKIA-SENTINEL-ACCESS-0001";
pub(crate) const SENTINEL_SECRET_KEY: &str = "sentinel-secret-value-0002";

/// Sentinel credentials on a closed loopback port; `path_style` keeps `object_store` off real AWS.
pub(crate) fn closed_port_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://127.0.0.1:1".into(),
        region: "us-east-1".into(),
        access_key: SENTINEL_ACCESS_KEY.into(),
        secret_key: SENTINEL_SECRET_KEY.into(),
        allow_http: true,
        path_style: true,
        ..Default::default()
    })
}

/// Mirrors `handle_pushdown`'s post-resolution SQL assembly, single shard.
pub(crate) fn build_sql_for_fixture(
    files: Vec<String>,
    proj_cols: Vec<String>,
    proj_types: Vec<String>,
    filter: Option<String>,
    limit: Option<u64>,
) -> String {
    build_sql_for_fixture_n(files, proj_cols, proj_types, filter, limit, 1)
}

pub(crate) fn build_sql_for_fixture_n(
    files: Vec<String>,
    proj_cols: Vec<String>,
    proj_types: Vec<String>,
    filter: Option<String>,
    limit: Option<u64>,
    cluster_nodes: usize,
) -> String {
    let col_types: Vec<(String, String)> = proj_cols
        .iter()
        .cloned()
        .zip(proj_types.iter().cloned())
        .collect();
    let proj_items: Vec<ProjectionItem> = proj_cols
        .iter()
        .cloned()
        .map(ProjectionItem::Column)
        .collect();
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: proj_items.clone(),
            filter,
            limit,
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let files_with_sizes: Vec<FileEntry> =
        files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
    let shards =
        crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &proj_items,
        &proj_types,
        limit,
        &col_types,
        None,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
}

/// Assumes the common JSON contains no single quote, true for these fixtures.
pub(crate) fn common_arg_literal(sql: &str) -> &str {
    let start = sql.find('\'').expect("SQL must contain a literal") + 1;
    let rest = &sql[start..];
    let end = rest.find('\'').expect("common literal must be closed");
    &rest[..end]
}

/// Paren-balanced: declared types like `DECIMAL(20,0)` carry their own parentheses.
pub(crate) fn emits_clause(sql: &str) -> &str {
    let open = sql.find("EMITS (").expect("SQL must carry an EMITS clause") + "EMITS ".len();
    let mut depth = 0usize;
    for (offset, ch) in sql[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &sql[open + 1..open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("EMITS clause must be closed: {sql}");
}

/// A visible select list never contains ` FROM (`, so the first occurrence is the
/// wrapper's own. Panics when the SQL carries no wrapper.
pub(crate) fn outer_select_list(sql: &str) -> &str {
    let list = sql
        .strip_prefix("SELECT ")
        .expect("SQL must start with SELECT");
    let end = list
        .find(" FROM (")
        .expect("SQL must carry a wrapping outer SELECT … FROM (");
    &list[..end]
}

pub(crate) fn nq4_request() -> Json {
    serde_json::json!({
        "involvedTables": [{
            "name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
            ],
        }],
        "pushdownRequest": {
            "type": "select",
            "selectList": [
                {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
                {"type": "column", "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 18, "scale": 2},
            ],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                "isAscending": false,
                "nullsLast": true
            }],
            "limit": {"numElements": 20}
        }
    })
}

pub(crate) fn pd(request: &Json) -> Json {
    request.get("pushdownRequest").cloned().unwrap()
}

/// Runs the same `relativize_shards_to_root` stripping as `handle_pushdown`, so
/// per-shard paths match production.
pub(crate) fn build_row_sql_with_root(
    files: Vec<(String, u64)>,
    table_root: &str,
    proj_cols: Vec<String>,
    proj_types: Vec<String>,
    cluster_nodes: usize,
) -> String {
    let col_types: Vec<(String, String)> = proj_cols
        .iter()
        .cloned()
        .zip(proj_types.iter().cloned())
        .collect();
    let proj_items: Vec<ProjectionItem> = proj_cols
        .iter()
        .cloned()
        .map(ProjectionItem::Column)
        .collect();
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            table_root: table_root.to_string(),
            projection: proj_items.clone(),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let files: Vec<FileEntry> = files.into_iter().map(FileEntry::from).collect();
    let g = shard_count(cluster_nodes, 1, files.len());
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let shards = relativize_shards_to_root(shards, table_root);
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &proj_items,
        &proj_types,
        None,
        &col_types,
        None,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
}

pub(crate) fn agg_item(name: &str, col: Option<&str>, distinct: bool) -> serde_json::Value {
    let mut args = serde_json::json!([]);
    if let Some(c) = col {
        args = serde_json::json!([{"type": "column", "name": c}]);
    }
    serde_json::json!({
        "type": "function_aggregate",
        "name": name,
        "arguments": args,
        "distinct": distinct,
    })
}

pub(crate) fn pos_delete(path: &str, size: u64) -> DeleteMechanism {
    DeleteMechanism::IcebergPositionalDelete {
        path: path.into(),
        size,
    }
}

pub(crate) fn agg_item_expr(
    name: &str,
    arg: serde_json::Value,
    distinct: bool,
) -> serde_json::Value {
    serde_json::json!({
        "type": "function_aggregate",
        "name": name,
        "arguments": [arg],
        "distinct": distinct,
    })
}

pub(crate) mod filter_json {
    use serde_json::{Value as Json, json};

    pub(crate) fn column(name: &str) -> Json {
        json!({"type": "column", "name": name, "tableName": "SALES"})
    }

    pub(crate) fn string(value: &str) -> Json {
        json!({"type": "literal_string", "value": value})
    }

    pub(crate) fn number(value: &str) -> Json {
        json!({"type": "literal_exactnumeric", "value": value})
    }

    pub(crate) fn compare(kind: &str, left: Json, right: Json) -> Json {
        json!({"type": kind, "left": left, "right": right})
    }

    pub(crate) fn equal(name: &str, value: &str) -> Json {
        compare("predicate_equal", column(name), string(value))
    }

    pub(crate) fn not(expression: Json) -> Json {
        json!({"type": "predicate_not", "expression": expression})
    }

    pub(crate) fn and(expressions: Vec<Json>) -> Json {
        json!({"type": "predicate_and", "expressions": expressions})
    }

    pub(crate) fn or(expressions: Vec<Json>) -> Json {
        json!({"type": "predicate_or", "expressions": expressions})
    }

    pub(crate) fn is_null(name: &str) -> Json {
        json!({"type": "predicate_is_null", "expression": column(name)})
    }

    pub(crate) fn is_not_null(name: &str) -> Json {
        json!({"type": "predicate_is_not_null", "expression": column(name)})
    }

    pub(crate) fn in_list(name: &str, arguments: Vec<Json>) -> Json {
        json!({"type": "predicate_in_constlist", "expression": column(name), "arguments": arguments})
    }

    pub(crate) fn between(name: &str, low: Json, high: Json) -> Json {
        json!({"type": "predicate_between", "expression": column(name), "left": low, "right": high})
    }
}
