//! Adapter pushdown fixtures: request JSON and column-type builders, Iceberg, Delta, and Glue
//! catalog and AdapterNotes fixtures, fake catalog and object-store endpoints, test CONNECTIONs.

use super::*;
use crate::adapter::ResolvedConnectionConfig;
use crate::adapter::column_notes::TableNotes;
use crate::adapter::table_identity::encode_identifier_note;
use crate::scan::sealed::{SealedStorageKey, derive_sealed_storage_key};
use crate::scan::spec::ProjectionItem;
use crate::scan::spec::{DeleteMechanism, ScanStorage, StorageProps};
use crate::scan::test_support::WithFiles;
use crate::types::exasol_type::{ExasolType, exasol_type_to_json};
use futures::FutureExt;
use futures::future::{self, BoxFuture};
use lakehouse_catalog::{
    CatalogColumn, CatalogTable, CatalogTableIdent, CatalogTableType, ColumnSourceType, TableFormat,
};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
// Named, not globbed: a local helper of the same name would silently shadow a glob import.
pub(crate) use vs_expression::test_support::{
    agg_item, agg_item_expr, alias_col, aliased_col, and, between, binary_lit, bool_lit, case_when,
    cast_json, col, compare, count_star, date_lit, extract, fixed_binary_lit, in_list, is_not_null,
    is_null, like, lit, not, null_lit, num_lit, numbered_col, or, predicate_equal, scalar_fn,
    str_lit, string_equal, table_col, table_column, timestamp_lit,
};

/// The table a single-table filter fixture qualifies its columns with, as Exasol does.
pub(crate) const SALES: &str = "SALES";

/// A column node of [`SALES`].
pub(crate) fn sales_col(name: &str) -> Json {
    table_col(SALES, name)
}

/// `SALES.name = 'value'`, the single-table string filter most pushdown tests send.
pub(crate) fn sales_equal(name: &str, value: &str) -> Json {
    string_equal(SALES, name, value)
}

/// `SECOND(argument, 3) > 1`: DataFusion declines it on arity while Exasol renders it.
pub(crate) fn declined_second_predicate(argument: Json) -> Json {
    compare(
        "predicate_greater",
        scalar_fn("SECOND", [argument, num_lit(3)]),
        num_lit(1),
    )
}

/// `ROUND(inner, digits)`.
pub(crate) fn round_of(inner: Json, digits: i64) -> Json {
    scalar_fn("ROUND", [inner, num_lit(digits)])
}

/// `dividend / divisor`, which Exasol sends as `FLOAT_DIV`.
pub(crate) fn float_div(dividend: Json, divisor: Json) -> Json {
    scalar_fn("FLOAT_DIV", [dividend, divisor])
}

/// An `ORDER BY` element; Exasol always sends both `isAscending` and `nullsLast`.
pub(crate) fn order_by_element(expression: Json, ascending: bool, nulls_last: bool) -> Json {
    serde_json::json!({
        "type": "order_by_element",
        "expression": expression,
        "isAscending": ascending,
        "nullsLast": nulls_last,
    })
}

/// An involved-table column entry, typed as Exasol declares it.
pub(crate) fn involved_column_json(name: &str, exa_type: ExasolType) -> Json {
    serde_json::json!({"name": name, "dataType": exasol_type_to_json(&exa_type)})
}

/// An `involvedTables` entry with its typed `columns`.
pub(crate) fn involved_table_json<N: AsRef<str>>(name: &str, columns: &[(N, ExasolType)]) -> Json {
    let columns: Vec<Json> = columns
        .iter()
        .map(|(column, exa_type)| involved_column_json(column.as_ref(), *exa_type))
        .collect();
    serde_json::json!({"name": name, "columns": columns})
}

type RecordedRequests = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// The one fake HTTP server: it records each request's head and body, then answers with what
/// `responder` maps the head to: a status, a content type, and a body. Each connection runs on its
/// own task, so a responder may hold one request until others arrive.
pub(super) async fn spawn_fake_http_server<F>(responder: F) -> (String, RecordedRequests)
where
    F: Fn(String) -> BoxFuture<'static, (u16, &'static str, String)> + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let address = format!("http://{}", listener.local_addr().expect("local_addr"));
    let requests: RecordedRequests = Arc::default();
    let responder = Arc::new(responder);

    let recorded = requests.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let (responder, recorded) = (Arc::clone(&responder), Arc::clone(&recorded));
            tokio::spawn(async move {
                let Some((head, body)) = read_http_request(&mut stream).await else {
                    return;
                };
                recorded
                    .lock()
                    .expect("recorded requests")
                    .push((head.clone(), body));
                let (status, content_type, response_body) = responder(head).await;
                let response = http_response(status, content_type, &response_body);
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });

    (address, requests)
}

/// [`spawn_fake_http_server`] with a responder that answers at once.
async fn spawn_answering_http_server<F>(responder: F) -> (String, RecordedRequests)
where
    F: Fn(&str) -> (u16, &'static str, String) + Send + Sync + 'static,
{
    spawn_fake_http_server(move |head| future::ready(responder(&head)).boxed()).await
}

/// A whole response that closes its connection, with the validators that make a body readable
/// as an object.
fn http_response(status: u16, content_type: &str, body: &str) -> String {
    let reason = if (200..300).contains(&status) {
        "OK"
    } else {
        "ERROR"
    };
    let length = body.len();
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n\
         Content-Length: {length}\r\nETag: \"e{length}\"\r\n\
         Last-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\nAccept-Ranges: bytes\r\n\
         Connection: close\r\n\r\n{body}"
    )
}

pub(super) fn request_target(head: &str) -> &str {
    head.split_whitespace().nth(1).unwrap_or("")
}

/// A fake catalog that answers JSON.
pub(crate) struct FakeCatalog {
    pub(super) uri: String,
}

impl FakeCatalog {
    /// `responder` maps a request target to a status and body.
    pub(super) async fn spawn<F>(responder: F) -> Self
    where
        F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
    {
        let (uri, _) = spawn_answering_http_server(move |head| {
            let (status, body) = responder(request_target(head));
            (status, "application/json", body)
        })
        .await;
        Self { uri }
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

pub(crate) fn assert_refuses_variant_col(error: UdfError) {
    let message = user_message(error);
    assert!(
        message.contains("variant_col") && message.contains("type 'variant'"),
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

/// The per-column type map the planner builds for one table that declares no binary column.
pub(crate) fn column_types<N: AsRef<str>>(columns: &[(N, ExasolType)]) -> ColumnTypes {
    declared_column_types(columns, &[])
}

/// [`column_types`], binary where a column's `declared` field is.
pub(crate) fn declared_column_types<N: AsRef<str>>(
    columns: &[(N, ExasolType)],
    declared: &[LogicalField],
) -> ColumnTypes {
    column_types_under(columns, declared, false)
}

/// [`column_types`] for a virtual schema that sets `BINARY_AS_STRING` as given.
pub(crate) fn column_types_under<N: AsRef<str>>(
    columns: &[(N, ExasolType)],
    declared: &[LogicalField],
    binary_as_string: bool,
) -> ColumnTypes {
    let involved = support::InvolvedColumns {
        types: columns
            .iter()
            .map(|(name, exa_type)| (name.as_ref().to_string(), *exa_type))
            .collect(),
        notes: TableNotes::default(),
    };
    ColumnTypes::of(&involved, declared, binary_as_string)
}

/// The map `handle_pushdown` builds for `request`'s first involved table, which declares no binary
/// column.
pub(crate) fn first_table_column_types(request: &Json) -> ColumnTypes {
    column_types(
        &support::involved_columns(request, |tables| tables.first())
            .unwrap()
            .types,
    )
}

/// `sides` with each side's map built, as the resolver builds it, from the columns `request`
/// declares for the side's table and the side's declared schema.
pub(crate) fn request_typed(
    request: &Json,
    sides: &[joins::ResolvedJoinSide],
) -> Vec<joins::ResolvedJoinSide> {
    sides
        .iter()
        .map(|side| {
            let involved = support::involved_columns(request, |tables| {
                tables
                    .iter()
                    .find(|table| table["name"].as_str() == Some(side.table_name.as_str()))
            })
            .unwrap();
            joins::ResolvedJoinSide {
                column_types: ColumnTypes::of(&involved, &side.logical_schema, false),
                ..side.clone()
            }
        })
        .collect()
}

/// One fileless side per leg of `join`, typed by the columns `request` declares for its table: the
/// only side state the broadcast render reads.
pub(crate) fn request_sides(
    request: &Json,
    join: &joins::DetectedJoin,
) -> Vec<joins::ResolvedJoinSide> {
    let sides: Vec<_> = join
        .tables
        .iter()
        .map(|leaf| joins::ResolvedJoinSide {
            table_name: leaf.table_name.clone(),
            table_identifier: leaf.table_identifier.clone(),
            table_root: String::new(),
            files: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            effective_storage: sample_storage(),
            partition_columns: Vec::new(),
            total_bytes: 0,
            refused_columns: Vec::new(),
            column_types: ColumnTypes::default(),
        })
        .collect();
    request_typed(request, &sides)
}

pub(crate) fn table_ref(name: &str) -> Json {
    serde_json::json!({"name": name, "type": "table"})
}

pub(crate) fn inner_join(left: Json, right: Json, condition: Json) -> Json {
    serde_json::json!({
        "type": "join", "join_type": "inner", "left": left, "right": right, "condition": condition
    })
}

/// Exasol's `CAST(arg AS target)` node.
pub(crate) fn cast_to(target: ExasolType, arg: Json) -> Json {
    cast_json(exasol_type_to_json(&target), arg)
}

pub(crate) fn customer_orders_join() -> Json {
    inner_join(
        table_ref("CUSTOMER"),
        table_ref("ORDERS"),
        predicate_equal(
            table_col("CUSTOMER", "C_CUSTKEY"),
            table_col("ORDERS", "O_CUSTKEY"),
        ),
    )
}

pub(crate) fn broadcast_sides(
    fact: joins::ResolvedJoinSide,
    dimension: joins::ResolvedJoinSide,
    dimension_leg: usize,
) -> joins::JoinSides {
    joins::JoinSides {
        fact,
        dimension,
        broadcast_eligible: true,
        dimension_leg,
    }
}

pub(crate) fn rendered_join(
    condition: &str,
    projection: &[ProjectedColumn],
) -> joins::RenderedJoinPushdown {
    joins::RenderedJoinPushdown {
        condition: condition.to_string(),
        filter: None,
        projection: projection.to_vec(),
        window: joins::JoinWindowPlan::Unbounded,
    }
}

/// Each item paired with the type at its position, for fixtures that list the two apart.
pub(crate) fn projected_columns(
    items: &[ProjectionItem],
    types: &[ExasolType],
) -> Vec<ProjectedColumn> {
    assert_eq!(items.len(), types.len(), "one type per projected item");
    items
        .iter()
        .zip(types)
        .map(|(item, exa_type)| ProjectedColumn {
            item: item.clone(),
            exa_type: *exa_type,
        })
        .collect()
}

/// The items and the types of `projection`, for assertions on one of the two.
pub(crate) fn split_projection(
    projection: &[ProjectedColumn],
) -> (Vec<ProjectionItem>, Vec<ExasolType>) {
    projection
        .iter()
        .map(|column| (column.item.clone(), column.exa_type))
        .unzip()
}

/// `project_columns` split into its items, its types, and its widening flag.
pub(crate) fn project_split(
    pushdown: &PushdownRequest,
    all_cols: &ColumnTypes,
) -> Result<(Vec<ProjectionItem>, Vec<ExasolType>, bool), UdfError> {
    let (projection, widened) = support::project_columns(pushdown, all_cols)?;
    let (items, types) = split_projection(&projection);
    Ok((items, types, widened))
}

/// `request`'s `pushdownRequest`, parsed as `handle_pushdown` parses it, for a fixture that
/// returns only its JSON or whose select list a test replaced in place.
pub(crate) fn parsed_pushdown(request: &Json) -> PushdownRequest {
    PushdownRequest::parse(pd(request)).expect("the fixture declares a valid selectListDataTypes")
}

/// [`project_split`] over `request`'s first involved table, as `handle_pushdown` projects it.
pub(crate) fn project_first_table(
    request: &Json,
    pushdown_req: &Json,
) -> Result<(Vec<ProjectionItem>, Vec<ExasolType>, bool), UdfError> {
    project_split(
        &PushdownRequest::parse(pushdown_req.clone())?,
        &first_table_column_types(request),
    )
}

/// One value per `ExasolType` variant and per spelling its SQL rendering takes, so a consumer's
/// pin covers every type a column, `EMITS` item, or `CAST` target can carry.
pub(crate) const EXASOL_TYPE_SPELLINGS: [ExasolType; 17] = [
    ExasolType::Boolean,
    ExasolType::decimal(18, 2),
    ExasolType::DoublePrecision,
    ExasolType::Date,
    ExasolType::BARE_TIMESTAMP,
    ExasolType::timestamp(6),
    ExasolType::TimestampWithLocalTimeZone { precision: 3 },
    ExasolType::TimestampWithLocalTimeZone { precision: 6 },
    ExasolType::char(20),
    ExasolType::Char {
        size: 3,
        ascii: true,
    },
    ExasolType::WIDEST_VARCHAR,
    ExasolType::varchar(10),
    ExasolType::Varchar {
        size: 2_000_000,
        ascii: true,
    },
    ExasolType::IntervalYearToMonth { precision: 2 },
    ExasolType::IntervalDayToSecond {
        precision: 2,
        fraction: 3,
    },
    ExasolType::Geometry { srid: 0 },
    ExasolType::Hashtype { bytes: 16 },
];

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

/// The refusal a `label` table records for the geospatial `column`, at `member_path` when nested.
pub(crate) fn unread_refused(
    label: &str,
    column: &str,
    member_path: Option<&str>,
    declared: &str,
) -> RefusedColumn {
    RefusedColumn {
        column_name: column.to_string(),
        reason: crate::adapter::column_notes::column_refusal(
            label,
            column,
            member_path,
            &crate::adapter::column_notes::unread_type_cause(declared),
        ),
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

/// What the resolver answers over the table `notes` declare, in the resolver's order: the one
/// parse, then `load`, then the loaded table's files pruned for that declaration, then the one
/// assembly.
pub(crate) async fn resolve_from_notes<L: LoadedTable>(
    load: impl Future<Output = Result<L, UdfError>>,
    notes: &TableNotes<'_>,
    filter_json: Option<&Json>,
) -> Result<ResolvedScan, UdfError> {
    let declared = notes.declared_schema()?;
    ResolvedScan::resolve(&load.await?, declared, filter_json).await
}

/// [`resolve_from_notes`] over the notes CREATE records for `table`, involved as `name`.
pub(crate) async fn resolve_declared<L: LoadedTable>(
    load: impl Future<Output = Result<L, UdfError>>,
    name: &str,
    table: &CatalogTable,
    filter_json: Option<&Json>,
) -> Result<ResolvedScan, UdfError> {
    let notes = declared_notes(table);
    resolve_from_notes(load, &involved_notes(name, &notes), filter_json).await
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

pub(crate) async fn unity_delta_catalog() -> FakeCatalog {
    FakeCatalog::spawn(|target| match target.rsplit_once('.') {
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
        let (endpoint, requests) = spawn_answering_http_server(move |head| {
            object_response(&bucket, request_target(head), &objects)
        })
        .await;
        Self {
            storage: endpoint_storage(&endpoint),
            requests,
        }
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

/// The answer of a path-style S3 endpoint serving `objects` of `bucket` to a request for `target`.
pub(super) fn object_response(
    bucket: &str,
    target: &str,
    objects: &[(String, String)],
) -> (u16, &'static str, String) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    if query.contains("list-type=2") {
        let listing = list_bucket_result(bucket, query, objects);
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
}

/// Path-style S3 storage at a fake object endpoint.
pub(crate) fn endpoint_storage(endpoint: &str) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: endpoint.to_string(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        allow_http: true,
        path_style: true,
        ..Default::default()
    })
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
        let (address, requests) = spawn_answering_http_server(move |head| {
            let (status, body) = match glue_operation(head) {
                Some(operation) => responder(operation),
                None => (400, serde_json::Value::Null),
            };
            (status, "application/x-amz-json-1.1", body.to_string())
        })
        .await;
        Self { address, requests }
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
pub(super) async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
) -> Option<(String, Vec<u8>)> {
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
    pushdown_over(
        request,
        CatalogKind::UnityCatalogNative,
        catalog_uri,
        storage,
        table,
        1024,
    )
    .await
}

/// Plans `request` as the first pushdown of its statement, over an unauthenticated `kind`
/// CONNECTION at `catalog_uri`, and over `table` when it is no join.
pub(crate) async fn pushdown_over(
    request: &Json,
    kind: CatalogKind,
    catalog_uri: &str,
    storage: StorageBackend,
    table: &str,
    join_broadcast_max_bytes: u64,
) -> Result<Json, UdfError> {
    let connection = resolved_connection(kind, catalog_uri, storage, unauthenticated_creds());
    let statement = StatementCatalog::open(connection, &get_properties(request), None)?;
    let catalog = CatalogProps {
        warehouse: "wh".into(),
        table: table.into(),
    };
    handle_pushdown(
        request,
        &statement,
        &PermissionCheck::Off,
        &catalog,
        None,
        1,
        1,
        1,
        1024,
        1,
        0.6,
        200,
        4,
        join_broadcast_max_bytes,
    )
    .await
}

/// The state of a statement's first pushdown over `catalog_uri`, with an unauthenticated
/// CONNECTION and no properties.
pub(crate) fn statement_over(
    kind: CatalogKind,
    catalog_uri: &str,
    storage: StorageBackend,
) -> StatementCatalog {
    let connection = resolved_connection(kind, catalog_uri, storage, unauthenticated_creds());
    StatementCatalog::open(connection, &Json::Null, None)
        .expect("the catalog client issues no request")
}

/// A CONNECTION's storage view that allows HTTP.
pub(crate) fn connection_storage<'a>(
    storage: &'a StorageBackend,
    creds: &'a ConnectionCreds,
) -> ConnectionStorage<'a> {
    ConnectionStorage {
        storage,
        creds,
        allow_http: true,
    }
}

pub(crate) const TEST_CONNECTION_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

pub(crate) const TEST_CONNECTION_PASSWORD: &str =
    r#"{"warehouse":"wh","secret_key":"FIXTURESECRET"}"#;

pub(crate) fn test_sealing_key() -> SealedStorageKey {
    derive_sealed_storage_key(TEST_CONNECTION_PASSWORD)
}

fn test_connection(creds: ConnectionCreds) -> ResolvedConnectionConfig {
    resolved_connection(
        CatalogKind::IcebergRest,
        "http://catalog.example.com",
        sample_storage(),
        creds,
    )
}

/// A CONNECTION of `kind` at `catalog_uri` that allows HTTP and seals under the test key.
pub(crate) fn resolved_connection(
    kind: CatalogKind,
    catalog_uri: &str,
    storage: StorageBackend,
    creds: ConnectionCreds,
) -> ResolvedConnectionConfig {
    ResolvedConnectionConfig {
        catalog_uri: catalog_uri.to_string(),
        storage,
        creds,
        allow_http: true,
        catalog_kind: kind,
        connection_name: TEST_CONNECTION_NAME.to_string(),
        sealed_storage_key: Some(test_sealing_key()),
        metadata_read_concurrency: crate::adapter::DEFAULT_METADATA_READ_CONCURRENCY,
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
    proj_types: Vec<ExasolType>,
    filter: Option<String>,
    limit: Option<u64>,
) -> String {
    build_sql_for_fixture_n(files, proj_cols, proj_types, filter, limit, 1)
}

pub(crate) fn build_sql_for_fixture_n(
    files: Vec<String>,
    proj_cols: Vec<String>,
    proj_types: Vec<ExasolType>,
    filter: Option<String>,
    limit: Option<u64>,
    cluster_nodes: usize,
) -> String {
    let col_types: Vec<(String, ExasolType)> = proj_cols
        .iter()
        .cloned()
        .zip(proj_types.iter().cloned())
        .collect();
    let proj_items: Vec<ProjectionItem> = proj_cols
        .iter()
        .cloned()
        .map(ProjectionItem::from)
        .collect();
    let spec_template = CommonScanSpec {
        projection: proj_items.clone(),
        filter,
        limit,
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
    let files_with_sizes: Vec<FileEntry> =
        files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
    let shards =
        crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&proj_items, &proj_types),
        limit,
        &column_types(&col_types),
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
    pushdown_request(
        "LINEITEM",
        &[
            ("L_ORDERKEY", ExasolType::decimal(20, 0)),
            ("L_EXTENDEDPRICE", ExasolType::decimal(18, 2)),
        ],
    )
    .field("type", "select")
    .column(table_col("LINEITEM", "L_ORDERKEY"))
    .column(table_col("LINEITEM", "L_EXTENDEDPRICE"))
    .order_by(serde_json::json!([order_by_element(
        numbered_col("LINEITEM", 1, "L_EXTENDEDPRICE"),
        false,
        true
    )]))
    .limit(20)
    .build()
    .0
}

pub(crate) fn pd(request: &Json) -> Json {
    request.get("pushdownRequest").cloned().unwrap()
}

/// A pushdown request as Exasol sends it, started by [`pushdown_request`]: each involved table with
/// its typed columns, and one `selectListDataTypes` entry per select-list item, written through
/// `exasol_type_to_json` from the types [`build`](Self::build) also returns.
#[derive(Debug, Clone)]
pub(crate) struct PushdownRequestBuilder {
    tables: Vec<(String, Vec<(String, ExasolType)>)>,
    select_list: Option<Vec<(Json, ExasolType)>>,
    fields: serde_json::Map<String, Json>,
    request_fields: serde_json::Map<String, Json>,
    raw_select_list_data_types: Option<Option<Json>>,
}

/// `request` selecting `items` instead, each beside the type Exasol declares for it, for a test
/// that varies one fixture's select list.
pub(crate) fn selecting(mut request: Json, items: &[(Json, ExasolType)]) -> Json {
    let (nodes, data_types): (Vec<Json>, Vec<Json>) = items
        .iter()
        .map(|(node, exa_type)| (node.clone(), exasol_type_to_json(exa_type)))
        .unzip();
    request["pushdownRequest"]["selectList"] = Json::Array(nodes);
    request["pushdownRequest"]["selectListDataTypes"] = Json::Array(data_types);
    request
}

/// The columns of a table whose select list projects no bare column.
pub(crate) const NO_COLUMNS: &[(&str, ExasolType)] = &[];

/// A request over `table` and its `columns` with no select list yet, as for `SELECT *`.
pub(crate) fn pushdown_request<N: AsRef<str>>(
    table: &str,
    columns: &[(N, ExasolType)],
) -> PushdownRequestBuilder {
    PushdownRequestBuilder {
        tables: Vec::new(),
        select_list: None,
        fields: serde_json::Map::new(),
        request_fields: serde_json::Map::new(),
        raw_select_list_data_types: None,
    }
    .table(table, columns)
}

impl PushdownRequestBuilder {
    /// Another involved table, as a join involves.
    pub(crate) fn table<N: AsRef<str>>(mut self, name: &str, columns: &[(N, ExasolType)]) -> Self {
        let columns = columns
            .iter()
            .map(|(column, exa_type)| (column.as_ref().to_string(), *exa_type))
            .collect();
        self.tables.push((name.to_string(), columns));
        self
    }

    /// A bare `column` select-list item, typed as its involved table declares the column.
    pub(crate) fn column(self, node: Json) -> Self {
        let exa_type = self.declared_column_type(&node).unwrap_or_else(|| {
            panic!("a bare column item must name a column of an involved table: {node}")
        });
        self.item_typed(node, exa_type)
    }

    /// A select-list item that is no bare column of an involved table, typed as Exasol declares it.
    pub(crate) fn item(self, node: Json, exa_type: ExasolType) -> Self {
        assert!(
            self.declared_column_type(&node).is_none(),
            "a bare column item takes its column's type through `column`: {node}"
        );
        self.item_typed(node, exa_type)
    }

    /// A `COUNT(*)` select-list item, typed `DECIMAL(18,0)` as Exasol declares it.
    pub(crate) fn count_star(self) -> Self {
        self.item(count_star(), ExasolType::decimal(18, 0))
    }

    /// An empty `selectList`, as Exasol sends for `SELECT RANDOM() FROM <table>`.
    pub(crate) fn empty_select_list(mut self) -> Self {
        self.select_list = Some(Vec::new());
        self
    }

    pub(crate) fn filter(self, filter: Json) -> Self {
        self.field("filter", filter)
    }

    /// `aggregationType` `group_by` with these `groupBy` keys.
    pub(crate) fn group_by(self, keys: Vec<Json>) -> Self {
        self.field("aggregationType", "group_by")
            .field("groupBy", keys)
    }

    /// `aggregationType` `single_group`, as Exasol sends for an aggregate with no GROUP BY.
    pub(crate) fn single_group(self) -> Self {
        self.field("aggregationType", "single_group")
    }

    pub(crate) fn order_by(self, elements: Json) -> Self {
        self.field("orderBy", elements)
    }

    pub(crate) fn limit(self, num_elements: u64) -> Self {
        self.field("limit", serde_json::json!({"numElements": num_elements}))
    }

    /// `selectListDataTypes` as `raw` spells it, or absent for `None`, in place of the list the
    /// item types derive: for a test that pins an absent, invalid, or miscounted list.
    pub(crate) fn select_list_data_types(mut self, raw: Option<Json>) -> Self {
        self.raw_select_list_data_types = Some(raw);
        self
    }

    /// Any other `pushdownRequest` key, such as `having`, `from`, or `type`.
    pub(crate) fn field(mut self, key: &str, value: impl Into<Json>) -> Self {
        self.fields.insert(key.to_string(), value.into());
        self
    }

    /// A top-level request key beside `involvedTables` and `pushdownRequest`, such as
    /// `schemaMetadataInfo`.
    pub(crate) fn request_field(mut self, key: &str, value: impl Into<Json>) -> Self {
        self.request_fields.insert(key.to_string(), value.into());
        self
    }

    /// The request JSON and the type of each select-list item, or of each column of every
    /// involved table when the request carries no select list.
    pub(crate) fn build(self) -> (Json, Vec<ExasolType>) {
        let item_types: Vec<ExasolType> = match &self.select_list {
            Some(items) => items.iter().map(|(_, exa_type)| *exa_type).collect(),
            None => self
                .tables
                .iter()
                .flat_map(|(_, columns)| columns.iter().map(|(_, exa_type)| *exa_type))
                .collect(),
        };
        let mut pushdown = self.fields;
        if let Some(items) = self.select_list {
            let nodes: Vec<Json> = items.into_iter().map(|(node, _)| node).collect();
            pushdown.insert("selectList".into(), Json::Array(nodes));
        }
        let data_types = self
            .raw_select_list_data_types
            .unwrap_or_else(|| Some(item_types.iter().map(exasol_type_to_json).collect()));
        if let Some(data_types) = data_types {
            pushdown.insert("selectListDataTypes".into(), data_types);
        }
        let involved_tables: Vec<Json> = self
            .tables
            .iter()
            .map(|(name, columns)| involved_table_json(name, columns))
            .collect();
        let mut request = self.request_fields;
        request.insert("involvedTables".into(), Json::Array(involved_tables));
        request.insert("pushdownRequest".into(), Json::Object(pushdown));
        (Json::Object(request), item_types)
    }

    /// [`build`](Self::build)'s `pushdownRequest`, parsed as `handle_pushdown` parses it.
    pub(crate) fn build_pushdown(self) -> PushdownRequest {
        parsed_pushdown(&self.build().0)
    }

    fn item_typed(mut self, node: Json, exa_type: ExasolType) -> Self {
        self.select_list
            .get_or_insert_with(Vec::new)
            .push((node, exa_type));
        self
    }

    fn declared_column_type(&self, node: &Json) -> Option<ExasolType> {
        if node.get("type").and_then(Json::as_str) != Some("column") {
            return None;
        }
        let name = node.get("name").and_then(Json::as_str)?;
        let table = ["tableName", "tableAlias"]
            .iter()
            .find_map(|key| node.get(*key).and_then(Json::as_str));
        self.tables
            .iter()
            .filter(|(table_name, _)| table.is_none_or(|t| t.eq_ignore_ascii_case(table_name)))
            .flat_map(|(_, columns)| columns)
            .find(|(column, _)| column.eq_ignore_ascii_case(name))
            .map(|(_, exa_type)| *exa_type)
    }
}

/// Runs the same `relativize_shards_to_root` stripping as `handle_pushdown`, so
/// per-shard paths match production.
pub(crate) fn build_row_sql_with_root(
    files: Vec<(String, u64)>,
    table_root: &str,
    proj_cols: Vec<String>,
    proj_types: Vec<ExasolType>,
    cluster_nodes: usize,
) -> String {
    let col_types: Vec<(String, ExasolType)> = proj_cols
        .iter()
        .cloned()
        .zip(proj_types.iter().cloned())
        .collect();
    let proj_items: Vec<ProjectionItem> = proj_cols
        .iter()
        .cloned()
        .map(ProjectionItem::from)
        .collect();
    let spec_template = CommonScanSpec {
        table_root: table_root.to_string(),
        projection: proj_items.clone(),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
    let files: Vec<FileEntry> = files.into_iter().map(FileEntry::from).collect();
    let g = shard_count(cluster_nodes, 1, files.len());
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let shards = relativize_shards_to_root(shards, table_root);
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&proj_items, &proj_types),
        None,
        &column_types(&col_types),
        None,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
}

pub(crate) fn pos_delete(path: &str, size: u64) -> DeleteMechanism {
    DeleteMechanism::IcebergPositionalDelete {
        path: path.into(),
        size,
    }
}
