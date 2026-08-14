use super::super::super::test_support::{RecordingCatalog, sample_storage};
use super::*;
use iceberg::spec::{DataContentType, DataFileFormat};
use lakehouse_catalog::{ConnectionCreds, StorageBackend};

/// Credentials whose SigV4 mode makes the `loadTable` GET the only request the
/// resolution issues, so a single-shot loopback catalog answers the whole run.
fn one_request_sigv4_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "123456789012".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "signing-access-key".into(),
        secret_key: "signing-secret-key".into(),
        session_token: None,
        path_style: true,
        use_sigv4: true,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

/// A `loadTable` response for a table with a location, a two-field schema, a
/// name-mapping property, and NO snapshot.
///
/// The absent snapshot is what keeps this a unit test: `TableScanBuilder::build`
/// answers an empty `TableScan` when `current_snapshot()` is `None`, so the
/// resolution returns its schema, root, and name mapping without reading a single
/// object from the store the location names.
fn name_mapped_load_table_body() -> String {
    serde_json::json!({
        "metadata-location": "s3://bucket/db/t/metadata/v1.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000003",
            "location": "s3://bucket/db/t",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "label", "required": false, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "snapshots": [],
            "properties": {
                "schema.name-mapping.default":
                    "[{\"field-id\":1,\"names\":[\"id\"]},{\"field-id\":2,\"names\":[\"label\"]}]"
            }
        }
    })
    .to_string()
}

/// Scenario: Iceberg planning is byte-identical through the new seam — the
/// reader now OWNS resolution outright.
///
/// Driven once through the reader against a loopback catalog serving a
/// two-field, name-mapped, snapshotless table, this asserts the reader
/// resolves the fixture's files, table root, effective storage,
/// field-id-bound logical schema, and name mapping, and adds no partition
/// columns. `dispatch_golden_tests.rs` is where encoding byte-identity itself
/// is pinned.
#[tokio::test]
async fn iceberg_reader_owns_resolution_and_keeps_its_encoding() {
    let creds = one_request_sigv4_creds();
    let storage = sample_storage();
    let catalog_props = CatalogProps {
        warehouse: creds.warehouse.clone(),
        table: "db.t".into(),
    };

    let body = name_mapped_load_table_body();
    let catalog = RecordingCatalog::spawn(move |_target| (200, body.clone())).await;
    let session = CatalogSession::resolve(&catalog.uri, &creds.warehouse, &creds)
        .await
        .expect("the SigV4 path resolves a session without contacting the catalog");
    let reader = IcebergFormatReader {
        session: &session,
        catalog_props: &catalog_props,
        connection: ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
    };
    let resolved = reader
        .resolve_scan(None)
        .await
        .expect("the reader must resolve the loopback catalog's table");

    assert_eq!(resolved.table_root, "s3://bucket/db/t");
    assert!(
        resolved.files.is_empty(),
        "a snapshotless table has no files"
    );
    assert_eq!(
        resolved.effective_storage, storage,
        "no vending is requested, so the effective storage is the static one"
    );
    assert_eq!(
        resolved.logical_schema.len(),
        2,
        "the fixture's two-field schema must reach the resolved scan"
    );
    assert!(
        resolved
            .logical_schema
            .iter()
            .all(|field| field.field_id.is_some() && field.physical_name.is_none()),
        "every Iceberg logical field must carry a field-id and no declared physical name, \
         so its encoding gains no key: {:?}",
        resolved.logical_schema
    );
    assert_eq!(
        resolved.name_mapping,
        vec![
            NameMappingEntry {
                name: "id".to_string(),
                field_id: 1,
            },
            NameMappingEntry {
                name: "label".to_string(),
                field_id: 2,
            },
        ],
        "the fixture's name-mapping property must flatten into the resolved scan"
    );
    assert!(
        resolved.partition_columns.is_empty(),
        "an Iceberg scan must carry no partition columns, so its encoding stays byte-identical"
    );
}

// ---------------------------------------------------------------------------
// Fail-loud on unsupported delete/data mechanisms (manifest level)
// ---------------------------------------------------------------------------

/// The two mechanisms this engine CAN apply — a Parquet data file and a
/// Parquet positional-delete file — classify as supported (`Ok`).
#[test]
fn classify_accepts_parquet_data_and_parquet_positional_delete() {
    assert!(
        classify_manifest_file(DataContentType::Data, DataFileFormat::Parquet).is_ok(),
        "Parquet data file must be supported"
    );
    assert!(
        classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Parquet).is_ok(),
        "Parquet positional delete must be supported"
    );
}

/// Equality deletes fail loud regardless of file format.
#[test]
fn classify_rejects_equality_deletes() {
    for fmt in [
        DataFileFormat::Parquet,
        DataFileFormat::Avro,
        DataFileFormat::Orc,
    ] {
        assert_eq!(
            classify_manifest_file(DataContentType::EqualityDeletes, fmt),
            Err(UnsupportedDeleteMechanism::EqualityDelete),
            "equality delete ({fmt:?}) must fail loud"
        );
    }
}

/// A position delete stored as a Puffin blob is a v3 deletion vector — the
/// exact case indistinguishable from a Parquet positional delete once
/// `plan_files` has dropped the format discriminator, so it MUST be caught at
/// the manifest level.
#[test]
fn classify_rejects_puffin_deletion_vector() {
    assert_eq!(
        classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Puffin),
        Err(UnsupportedDeleteMechanism::DeletionVector),
        "Puffin position delete (deletion vector) must fail loud"
    );
}

/// ORC/Avro data and delete files fail loud.
#[test]
fn classify_rejects_orc_and_avro_data_and_delete_files() {
    assert_eq!(
        classify_manifest_file(DataContentType::Data, DataFileFormat::Orc),
        Err(UnsupportedDeleteMechanism::OrcDataFile),
    );
    assert_eq!(
        classify_manifest_file(DataContentType::Data, DataFileFormat::Avro),
        Err(UnsupportedDeleteMechanism::AvroDataFile),
    );
    assert_eq!(
        classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Orc),
        Err(UnsupportedDeleteMechanism::OrcDeleteFile),
    );
    assert_eq!(
        classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Avro),
        Err(UnsupportedDeleteMechanism::AvroDeleteFile),
    );
}

/// The fail-loud error names the mechanism, names the table, and leaks no
/// credential (defensively redacted).
#[test]
fn unsupported_delete_error_names_mechanism_and_redacts() {
    let err = unsupported_delete_error(
        UnsupportedDeleteMechanism::DeletionVector,
        "db.mor_dv_table",
    );
    let msg = match err {
        UdfError::User(m) => m,
        other => panic!("expected UdfError::User, got {other:?}"),
    };
    assert!(
        msg.contains("Iceberg v3 Puffin deletion vectors"),
        "error must name the mechanism: {msg}"
    );
    assert!(
        msg.contains("db.mor_dv_table"),
        "error must name the offending table: {msg}"
    );
    // No credential label may survive the defensive redaction.
    assert!(
        !msg.contains("access_key"),
        "must not leak access_key: {msg}"
    );
    assert!(
        !msg.contains("secret_key"),
        "must not leak secret_key: {msg}"
    );
}

/// A manifest-read error that echoes Azure static credentials verbatim has
/// BOTH literal values stripped — not merely their labels.
///
/// The two credentials fail the label heuristic in different ways, so each
/// independently requires the value-based pass:
///   - the account key is echoed bare inside a string-to-sign, with no
///     recognizable label anywhere near it;
///   - the SAS token carries its OWN `sig=` label, so a label-only pass
///     rewrites the middle of the token and leaves its permission and expiry
///     fields verbatim.
#[test]
fn manifest_read_errors_redact_the_literal_azure_secret_values() {
    let account_key = "Zm9vYmFyYmF6cXV1eGNvcmdlc2VjcmV0QUNDT1VOVEtFWT09";
    let sas_permissions = "sp=racwdlmeop";
    let sas_token = format!(
        "sv=2024-11-04&ss=bf&srt=sco&{sas_permissions}&se=2026-12-31T23:59:59Z&sig=aB3%2FxQ7"
    );
    let raw = format!(
        "AuthenticationFailed: Server failed to authenticate the request. \
         String to sign used was: {account_key}. \
         Request URL: https://acct.dfs.core.windows.net/c/meta/snap.avro?{sas_token}"
    );
    let secrets = [account_key, sas_token.as_str()];

    let surfaced = format!(
        "failed to read Iceberg manifest list for 'ns.tbl': {}",
        redact_error_text(&raw, &secrets)
    );

    assert!(
        !surfaced.contains(account_key),
        "account key value must not survive: {surfaced}"
    );
    assert!(
        !surfaced.contains(&sas_token),
        "SAS token value must not survive: {surfaced}"
    );
    assert!(
        !surfaced.contains(sas_permissions),
        "the SAS token's permission field must not survive either: {surfaced}"
    );
    assert!(
        surfaced.contains("failed to read Iceberg manifest list for 'ns.tbl'"),
        "the actionable context must be preserved: {surfaced}"
    );
}

/// `iceberg_delete_mechanism` maps the iceberg task-level content type onto the
/// mechanism honestly: position → positional, equality → equality, and the `Data`
/// sentinel (which never appears in a task's delete list) → the non-positional
/// mechanism the scan's read-time backstop rejects rather than applies.
#[test]
fn iceberg_delete_mechanism_maps_position_equality_and_the_data_sentinel() {
    use iceberg::spec::DataContentType;
    assert_eq!(
        iceberg_delete_mechanism("d0.parquet".into(), 50, DataContentType::PositionDeletes),
        DeleteMechanism::IcebergPositionalDelete {
            path: "d0.parquet".into(),
            size: 50,
        }
    );
    assert_eq!(
        iceberg_delete_mechanism("d1.parquet".into(), 60, DataContentType::EqualityDeletes),
        DeleteMechanism::IcebergEqualityDelete {
            path: "d1.parquet".into(),
            size: 60,
        }
    );
    assert_eq!(
        iceberg_delete_mechanism("d2.parquet".into(), 70, DataContentType::Data),
        DeleteMechanism::IcebergEqualityDelete {
            path: "d2.parquet".into(),
            size: 70,
        },
        "the Data sentinel must map to a non-positional mechanism"
    );
}

// ---------------------------------------------------------------------------
// Iceberg `schema.name-mapping.default` parsing
// ---------------------------------------------------------------------------

/// A representative `schema.name-mapping.default` payload — mirroring the
/// Iceberg spec's own example shape — flattens to one `NameMappingEntry` per
/// TOP-LEVEL name. Multi-name entries expand to one entry per name (Avro field
/// aliases); an entry's nested `fields` children are excluded, but the entry's
/// OWN top-level name(s) are still included; an entry with no `field-id` at
/// all (schema-only, not present in imported files) is fully excluded.
#[test]
fn resolves_name_mapping_flat_entries_once() {
    let raw = r#"
    [
        { "field-id": 1, "names": ["id", "record_id"] },
        {
            "field-id": 3,
            "names": ["location"],
            "fields": [
                { "field-id": 4, "names": ["latitude", "lat"] },
                { "field-id": 5, "names": ["longitude", "long"] }
            ]
        },
        { "names": ["schema_only_no_field_id"] }
    ]
    "#;

    let entries = parse_name_mapping(Some(raw)).expect("valid name-mapping JSON must parse");

    assert_eq!(
        entries,
        vec![
            NameMappingEntry {
                name: "id".to_string(),
                field_id: 1,
            },
            NameMappingEntry {
                name: "record_id".to_string(),
                field_id: 1,
            },
            NameMappingEntry {
                name: "location".to_string(),
                field_id: 3,
            },
        ],
        "multi-name entry expands per name; nested `fields` children (lat/lat, \
         long/long) are excluded while the parent's own top-level name is kept; \
         the id-less entry is fully excluded"
    );
}

/// An absent `schema.name-mapping.default` property (`None`) yields an empty
/// mapping, not an error — a table with no name-mapping is the common,
/// fully-supported case.
#[test]
fn absent_name_mapping_is_empty() {
    assert_eq!(
        parse_name_mapping(None).expect("absent property must not error"),
        Vec::new()
    );
}

/// A present-but-malformed `schema.name-mapping.default` value fails loud with
/// a clean, credential-free plan-time error that names the offending property.
#[test]
fn malformed_name_mapping_errors_cleanly() {
    let err = parse_name_mapping(Some("{ not valid json mapping shape"))
        .expect_err("malformed name-mapping JSON must error");

    let msg = match err {
        UdfError::User(m) => m,
        other => panic!("expected UdfError::User, got {other:?}"),
    };
    assert!(
        msg.contains(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING),
        "error must name the offending property: {msg}"
    );
    assert!(
        !msg.contains("access_key") && !msg.contains("secret_key"),
        "error must not leak credentials: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Effective storage: vended vs. static, and the empty-location guard
// ---------------------------------------------------------------------------

/// A `loadTable` response body with `location` present but empty — an omitted
/// key fails deserialization earlier and never reaches the guard under test.
fn load_table_body_with_empty_location() -> String {
    serde_json::json!({
        "metadata-location": "s3://bucket/db/t/metadata/v1.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 0,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0
        }
    })
    .to_string()
}

async fn effective_storage_from_loopback_catalog(
    creds: &ConnectionCreds,
    body: String,
) -> Result<StorageBackend, UdfError> {
    let catalog = RecordingCatalog::spawn(move |_target| (200, body.clone())).await;
    let session = CatalogSession::resolve(&catalog.uri, &creds.warehouse, creds)
        .await
        .expect("the SigV4 path resolves a session without contacting the catalog");
    let catalog_props = CatalogProps {
        warehouse: creds.warehouse.clone(),
        table: "db.t".into(),
    };
    let storage = sample_storage();
    let reader = IcebergFormatReader {
        session: &session,
        catalog_props: &catalog_props,
        connection: ConnectionStorage {
            storage: &storage,
            creds,
            allow_http: true,
        },
    };

    reader
        .resolve_scan(None)
        .await
        .map(|resolved| resolved.effective_storage)
}

/// Drive the reader against a single-shot loopback catalog that answers
/// `loadTable` with an empty table location.
async fn resolve_against_locationless_catalog(creds: &ConnectionCreds) -> Result<(), UdfError> {
    effective_storage_from_loopback_catalog(creds, load_table_body_with_empty_location())
        .await
        .map(|_| ())
}

/// A `loadTable` response with an empty table `location` is rejected as a
/// `UdfError::User`, with the identical message whether or not vended credentials
/// are requested.
#[tokio::test]
async fn absent_table_location_errors_on_both_vended_and_static_paths() {
    let static_creds = one_request_sigv4_creds();
    let mut vended_creds = one_request_sigv4_creds();
    vended_creds.use_vended_credentials = true;

    let vended_err = resolve_against_locationless_catalog(&vended_creds)
        .await
        .expect_err("vended path must reject a loadTable response with an empty location");
    let static_err = resolve_against_locationless_catalog(&static_creds)
        .await
        .expect_err("static path must reject a loadTable response with an empty location");

    let vended_message = match vended_err {
        UdfError::User(m) => m,
        other => panic!("vended path must fail as a user error, got {other:?}"),
    };
    let static_message = match static_err {
        UdfError::User(m) => m,
        other => panic!("static path must fail as a user error, got {other:?}"),
    };

    for (path, message) in [("vended", &vended_message), ("static", &static_message)] {
        assert!(
            message.contains("loadTable"),
            "{path}-path error must name the response the location is absent from: {message}"
        );
        assert!(
            message.contains("location"),
            "{path}-path error must name the absent location field: {message}"
        );
        assert!(
            message.contains("db.t"),
            "{path}-path error must name the table whose loadTable response was malformed: \
             {message}"
        );
        assert!(
            !message.contains("storage backend cannot be resolved"),
            "{path}-path error must not frame the failure as a vended-storage-backend \
             resolution failure: {message}"
        );
    }
    assert_eq!(
        vended_message, static_message,
        "both paths must surface the SAME absent-location error — the vended-credential \
         flag must not change how a malformed catalog response is diagnosed"
    );
}

/// The store address the CONNECTION configures, and the DIFFERENT one the catalog
/// vends for the very same table. Every value is distinct, so which source placed
/// the resolved store is readable off the resolved value alone.
const CONNECTION_ENDPOINT: &str = "https://connection-store.example.com";
const CONNECTION_REGION: &str = "eu-central-1";
const VENDED_ENDPOINT: &str = "https://vended-store.example.com";
const VENDED_REGION: &str = "us-west-2";
const VENDED_ACCESS_KEY: &str = "vended-access-key";
const VENDED_SECRET_KEY: &str = "vended-secret-key";
const VENDED_SESSION_TOKEN: &str = "vended-session-token";

/// A `loadTable` response vending a complete S3 credential set AND a store address
/// of its own, for a table whose metadata carries NO snapshot.
///
/// The absent snapshot is what keeps this a pure unit test: `TableScanBuilder::build`
/// answers an empty `TableScan` when `current_snapshot()` is `None`, so the reader
/// reaches its effective-storage decision and returns without reading a single
/// object from the store that address names.
fn load_table_body_vending_its_own_store_address() -> String {
    serde_json::json!({
        "metadata-location": "s3://bucket/db/t/metadata/v1.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000002",
            "location": "s3://bucket/db/t",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 0,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "snapshots": []
        },
        "config": {
            "s3.access-key-id": VENDED_ACCESS_KEY,
            "s3.secret-access-key": VENDED_SECRET_KEY,
            "s3.session-token": VENDED_SESSION_TOKEN,
            "client.region": VENDED_REGION,
            "s3.endpoint": VENDED_ENDPOINT
        }
    })
    .to_string()
}

/// Under vending, a CONNECTION-configured `endpoint` and `region` place the store
/// while the credentials still come from the catalog alone.
///
/// This is the only test at the layer that PERFORMS the split: the reader is what
/// narrows the CONNECTION down to a `StaticStoreAddress` before handing it to the
/// vended selector. Both vended E2E fixtures carry CONNECTIONs with an empty
/// `endpoint` and `region`, so neither can tell an address that came from the
/// CONNECTION from one that came from nowhere — substituting
/// `&StaticStoreAddress::default()` at that call site fails HERE and nowhere else.
#[tokio::test]
async fn vended_addressing_prefers_the_connection_endpoint_and_region() {
    let mut creds = one_request_sigv4_creds();
    creds.use_vended_credentials = true;
    creds.endpoint = CONNECTION_ENDPOINT.into();
    creds.region = CONNECTION_REGION.into();

    let storage = effective_storage_from_loopback_catalog(
        &creds,
        load_table_body_vending_its_own_store_address(),
    )
    .await
    .expect("a vended key pair over a snapshotless s3:// table must resolve a backend");

    let StorageBackend::S3(props) = storage else {
        panic!("an s3:// table location must resolve an S3 backend");
    };

    assert_eq!(
        props.endpoint, CONNECTION_ENDPOINT,
        "the CONNECTION's endpoint must place the store, not the vended {VENDED_ENDPOINT}"
    );
    assert_eq!(
        props.region, CONNECTION_REGION,
        "the CONNECTION's region must place the store, not the vended {VENDED_REGION}"
    );
    assert_eq!(
        props.access_key, VENDED_ACCESS_KEY,
        "the access key must come from the catalog alone: the CONNECTION reaches this \
         resolution as an ADDRESS, and must never supply a storage credential"
    );
    assert_eq!(
        props.secret_key, VENDED_SECRET_KEY,
        "the secret key must come from the catalog alone: the CONNECTION reaches this \
         resolution as an ADDRESS, and must never supply a storage credential"
    );
    assert_eq!(
        props.session_token.as_deref(),
        Some(VENDED_SESSION_TOKEN),
        "the vended session token must reach the effective storage the scan reads with"
    );
}
