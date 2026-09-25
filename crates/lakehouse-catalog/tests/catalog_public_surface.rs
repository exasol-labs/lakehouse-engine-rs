//! External-crate reachability probe: narrowing any item used here below `pub`, or
//! dropping its `lib.rs` re-export, fails to compile.
#![allow(unused_imports)]

use std::collections::BTreeSet;

use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;
use iceberg_catalog_rest::{LoadTableResult, StorageCredential};
use lakehouse_catalog::{
    AdlsCred, CatalogClient, CatalogColumn, CatalogListing, CatalogProps, CatalogSession,
    CatalogTable, CatalogTableIdent, CatalogTableType, ColumnSourceType, ConnectionCreds,
    IcebergRestCatalogClient, SkipReason, SkippedTable, StaticStoreAddress, StorageBackend,
    StorageCreds, StorageProps, TableFormat, TemporaryTableCredentials, UnityCatalogSession,
    load_table_any_auth, parse_table_ident, redact_credentials, redact_secret_values,
    resolve_uc_vended_storage, resolve_vended_storage,
};

const CATALOG_SOURCES: &[(&str, &str)] = &[
    ("auth.rs", include_str!("../src/auth.rs")),
    ("client.rs", include_str!("../src/client.rs")),
    ("creds.rs", include_str!("../src/creds.rs")),
    ("iceberg_io.rs", include_str!("../src/iceberg_io.rs")),
    ("lib.rs", include_str!("../src/lib.rs")),
    ("namespace.rs", include_str!("../src/namespace.rs")),
    ("redaction.rs", include_str!("../src/redaction.rs")),
    ("session.rs", include_str!("../src/session.rs")),
    ("sigv4.rs", include_str!("../src/sigv4.rs")),
    ("storage.rs", include_str!("../src/storage.rs")),
    ("vended.rs", include_str!("../src/vended.rs")),
    ("unity/mod.rs", include_str!("../src/unity/mod.rs")),
    ("unity/auth.rs", include_str!("../src/unity/auth.rs")),
    ("unity/client.rs", include_str!("../src/unity/client.rs")),
    ("unity/vended.rs", include_str!("../src/unity/vended.rs")),
];

fn source(name: &str) -> &'static str {
    CATALOG_SOURCES
        .iter()
        .find_map(|(file, src)| (*file == name).then_some(*src))
        .unwrap_or_else(|| panic!("{name} must be present in CATALOG_SOURCES"))
}

fn connection_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "warehouse".into(),
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
    }
}

/// Boundary-checked so `pub fn s3_backend` does not match `pub fn s3_backend_from_vended`.
fn declares(source: &str, declaration: &str) -> bool {
    source.match_indices(declaration).any(|(index, matched)| {
        source[index + matched.len()..]
            .chars()
            .next()
            .is_none_or(|next| !next.is_alphanumeric() && next != '_')
    })
}

/// Scenario: Vended-storage mechanism steps are never declared public
#[test]
fn demoted_and_deleted_functions_are_not_declared_public() {
    for (name, source) in CATALOG_SOURCES {
        for mechanism in [
            "pub fn merge_vended_into_storage",
            "pub fn select_credential_source",
            "pub fn extract_vended_keys",
            "pub fn build_s3_file_io",
            "pub fn s3_backend_from_vended",
            "pub fn adls_backend_from_vended",
        ] {
            assert!(
                !declares(source, mechanism),
                "{name} must not declare `{mechanism}` — it is a demoted or deleted \
                 function behind `resolve_vended_storage`/`StorageBackend::file_io` \
                 that the crate must keep private or removed"
            );
        }
    }
}

/// Scenario: StorageBackend's secret_values and file_io are callable from outside the crate
#[test]
fn storage_backend_secret_values_and_file_io_are_reachable() {
    let backend = StorageBackend::S3(StorageProps::default());
    let _: Vec<&str> = backend.secret_values();
    let _: iceberg::io::FileIO = backend.file_io();
}

/// Scenario: Both catalog clients are usable as Box<dyn CatalogClient>
#[test]
fn both_clients_are_catalog_client_trait_objects() {
    let iceberg: Box<dyn CatalogClient> = Box::new(IcebergRestCatalogClient::new(
        "http://catalog".into(),
        StorageBackend::S3(StorageProps::default()),
        connection_creds(),
    ));
    let unity: Box<dyn CatalogClient> =
        Box::new(UnityCatalogSession::new("http://unity", connection_creds()));

    let clients: Vec<Box<dyn CatalogClient>> = vec![iceberg, unity];
    assert_eq!(clients.len(), 2);
}

/// Scenario: Neutral catalog types are constructible outside the crate while Unity wire types stay hidden
#[test]
fn catalog_client_trait_and_neutral_types_are_reachable() {
    let ident = CatalogTableIdent {
        namespace: vec!["ns".into()],
        name: "t".into(),
    };
    let column = CatalogColumn {
        name: "c".into(),
        source_type: ColumnSourceType::Unity {
            type_name: "int".into(),
            precision: 0,
            scale: 0,
        },
    };
    let table = CatalogTable {
        ident: ident.clone(),
        table_type: CatalogTableType::Table,
        storage_location: None,
        format: TableFormat::Delta,
        vended_credential_key: Some("opaque-vending-key".into()),
        columns: vec![column],
    };
    assert_eq!(table.format, TableFormat::Delta);
    assert_ne!(
        TableFormat::Iceberg,
        TableFormat::Delta,
        "both formats the engine can plan are reachable from outside the crate and distinct"
    );
    let listing = CatalogListing {
        tables: vec![table],
        skipped: vec![SkippedTable {
            ident,
            reason: SkipReason::NotLoadableIcebergTable,
        }],
    };
    assert_eq!(listing.tables.len(), 1);
    assert_eq!(listing.skipped.len(), 1);

    let unity_client = source("unity/client.rs");
    let unity_mod = source("unity/mod.rs");
    let lib = source("lib.rs");
    for wire in [
        "CatalogsPage",
        "SchemasPage",
        "TablesPage",
        "CatalogInfo",
        "SchemaInfo",
        "TableInfo",
        "ColumnInfo",
    ] {
        assert!(
            !unity_client.contains(&format!("pub struct {wire}")),
            "unity/client.rs must not declare the Unity wire type `{wire}` public"
        );
        assert!(
            !unity_mod.contains(wire),
            "unity/mod.rs must not re-export the Unity wire type `{wire}`"
        );
        assert!(
            !lib.contains(wire),
            "lib.rs must not re-export the Unity wire type `{wire}`"
        );
    }
}

/// Scenario: The direct-storage neutral variants are reachable from outside the crate
#[test]
fn added_neutral_variants_are_reachable_from_outside_the_crate() {
    assert_eq!(TableFormat::Parquet, TableFormat::Parquet);
    assert_ne!(TableFormat::Parquet, TableFormat::Iceberg);
    assert_ne!(TableFormat::Parquet, TableFormat::Delta);

    let column = CatalogColumn {
        name: "c".into(),
        source_type: ColumnSourceType::Parquet("int64".into()),
    };
    match &column.source_type {
        ColumnSourceType::Parquet(tag) => assert_eq!(tag, "int64"),
        other => panic!("expected a Parquet source type, got {other:?}"),
    }

    let ident = CatalogTableIdent {
        namespace: vec!["ns".into()],
        name: "t".into(),
    };
    let listing = CatalogListing {
        tables: vec![CatalogTable {
            ident: ident.clone(),
            table_type: CatalogTableType::Table,
            storage_location: None,
            format: TableFormat::Parquet,
            vended_credential_key: None,
            columns: vec![column],
        }],
        skipped: vec![SkippedTable {
            ident,
            reason: SkipReason::NoDataFile,
        }],
    };
    assert_eq!(listing.tables[0].format, TableFormat::Parquet);
    assert_eq!(listing.skipped[0].reason, SkipReason::NoDataFile);
}

/// Scenario: Raw Unity wire fields do not appear in the neutral types
#[test]
fn raw_unity_wire_fields_do_not_appear_in_the_neutral_types() {
    let neutral = production_code(source("client.rs"));

    for wire_field in ["data_source_format", "table_id"] {
        assert!(
            !declares(&neutral, wire_field),
            "client.rs's PRODUCTION code must not name the raw Unity Catalog wire field \
             `{wire_field}` — the neutral table carries its projection (a closed format tag, an \
             opaque vending key), never the wire field itself"
        );
    }
}

/// Scenario: list_namespace_tables stays crate-private and is not re-exported
#[test]
fn list_namespace_tables_is_no_longer_public() {
    let namespace = source("namespace.rs");
    assert!(
        !namespace.contains("pub fn list_namespace_tables")
            && !namespace.contains("pub async fn list_namespace_tables"),
        "namespace.rs must not declare `list_namespace_tables` public — the CatalogClient \
         trait is its only caller"
    );
    assert!(
        namespace.contains("pub(crate) async fn list_namespace_tables"),
        "list_namespace_tables must remain crate-private"
    );
    assert!(
        !source("lib.rs").contains("list_namespace_tables"),
        "lib.rs must not re-export `list_namespace_tables`"
    );
}

/// Scenario: ConnectionCreds::sigv4_signing_region is callable from outside the crate
#[test]
fn connection_creds_sigv4_signing_region_is_reachable() {
    let creds = ConnectionCreds {
        region: String::new(),
        ..connection_creds()
    };

    let region: Option<String> =
        creds.sigv4_signing_region("https://glue.eu-west-1.amazonaws.com/iceberg");

    assert_eq!(region.as_deref(), Some("eu-west-1"));
}

/// Scenario: The native Unity Catalog public items are reachable from outside the crate
#[test]
fn unity_catalog_public_items_are_reachable() {
    let _session = UnityCatalogSession::new("http://unity", connection_creds());
    let vended = TemporaryTableCredentials {
        aws_temp_credentials: None,
        azure_user_delegation_sas: None,
        gcp_oauth_token: None,
    };
    let _resolved: Result<StorageBackend, UdfError> = resolve_uc_vended_storage(
        &vended,
        "s3://bucket/db/t",
        true,
        &StaticStoreAddress::default(),
    );
}

fn minimal_load_table_result(config: Vec<(&str, &str)>) -> LoadTableResult {
    let meta_json = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000001",
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
        "default-sort-order-id": 0
    });
    let metadata: TableMetadata = serde_json::from_value(meta_json).expect("valid metadata");

    LoadTableResult {
        metadata_location: Some("s3://bucket/db/t/metadata/v1.json".into()),
        metadata,
        config: config
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        storage_credentials: None,
    }
}

/// Scenario: resolve_vended_storage takes only a credential-free store address, never a backend
#[test]
fn resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend() {
    let result = minimal_load_table_result(vec![
        ("s3.access-key-id", "AKIAEXAMPLE"),
        ("s3.secret-access-key", "secret-value"),
        ("client.region", "us-east-1"),
    ]);

    let backend: Result<StorageBackend, UdfError> = resolve_vended_storage(
        &result,
        "s3://bucket/db/t",
        true,
        &StaticStoreAddress::default(),
    );

    match backend.expect("scheme-selected S3 arm must succeed") {
        StorageBackend::S3(props) => {
            assert_eq!(props.access_key, "AKIAEXAMPLE");
            assert_eq!(props.region, "us-east-1");
        }
        StorageBackend::Adls { .. } => panic!("an s3:// anchor must select the S3 variant"),
    }

    assert_static_store_address_declares_no_credential_field();
}

/// Scenario: resolve_uc_vended_storage takes only a credential-free store address
#[test]
fn resolve_uc_vended_storage_signature_takes_only_a_credential_free_store_address() {
    let vended = TemporaryTableCredentials {
        aws_temp_credentials: None,
        azure_user_delegation_sas: None,
        gcp_oauth_token: None,
    };

    let resolved: Result<StorageBackend, UdfError> = resolve_uc_vended_storage(
        &vended,
        "s3://bucket/db/t",
        true,
        &StaticStoreAddress::default(),
    );

    assert!(
        resolved.is_err(),
        "an s3:// location with no vended aws credential must surface a clear error"
    );

    assert_static_store_address_declares_no_credential_field();
}

/// Comment lines are dropped so a name a doc comment merely mentions satisfies no probe.
fn production_code(source: &str) -> String {
    let production = &source[..source.find("#[cfg(test)]").unwrap_or(source.len())];
    production
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn declaration_body<'a>(source: &'a str, declaration: &str) -> &'a str {
    let start = source
        .find(declaration)
        .unwrap_or_else(|| panic!("the probed source must declare `{declaration}`"));
    let body_start = source[start..]
        .find('{')
        .map(|offset| start + offset + 1)
        .unwrap_or_else(|| panic!("`{declaration}` must have a `{{ ... }}` body"));

    let mut depth = 1usize;
    let mut body_end = None;
    for (offset, ch) in source[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    body_end = Some(body_start + offset);
                    break;
                }
            }
            _ => {}
        }
    }

    let body_end = body_end.unwrap_or_else(|| {
        panic!("failed to find the matching closing brace for `{declaration}`'s body")
    });
    &source[body_start..body_end]
}

fn enum_variant_names<'a>(source: &'a str, enum_name: &str) -> Vec<&'a str> {
    let body = declaration_body(source, &format!("enum {enum_name}"));

    let variant_names: Vec<&str> = body
        .lines()
        .map(|line| line.split("///").next().unwrap_or(line).trim())
        .filter_map(|code| {
            let name = code
                .split(['(', '{', ','])
                .next()
                .map(str::trim)
                .unwrap_or("");
            let is_variant_declaration =
                !name.is_empty() && name.chars().next().is_some_and(char::is_uppercase);
            is_variant_declaration.then_some(name)
        })
        .collect();

    assert!(
        !variant_names.is_empty(),
        "extracted no variant names from `enum {enum_name}`'s body — the probe's own \
         parsing is broken, not just failing to find a match"
    );
    variant_names
}

/// Scenario: storage.rs constructs every StorageBackend variant
#[test]
fn shared_vended_home_constructs_every_storage_backend_variant() {
    let storage = source("storage.rs");
    let code = production_code(storage);

    for variant in enum_variant_names(storage, "StorageBackend") {
        let constructed = format!("StorageBackend::{variant}");
        assert!(
            code.contains(&constructed),
            "storage.rs's PRODUCTION code must construct `{constructed}` — the shared vended \
             home builds every backend variant, and a variant named only in a comment or only \
             inside `#[cfg(test)]` does not satisfy that"
        );
    }
}

/// Scenario: Each vended selector dispatches on every VendedBackendKind
#[test]
fn each_vended_selector_dispatches_every_vended_backend_kind() {
    let kinds = enum_variant_names(source("storage.rs"), "VendedBackendKind");

    for selector in ["vended.rs", "unity/vended.rs"] {
        let code = production_code(source(selector));
        for kind in &kinds {
            let dispatched = format!("VendedBackendKind::{kind}");
            assert!(
                code.contains(&dispatched),
                "{selector}'s PRODUCTION code must dispatch on `{dispatched}` before calling \
                 the shared home, but that literal appears in no code line outside \
                 `#[cfg(test)]`"
            );
        }
    }
}

/// Scenario: VendedBackendKind and StorageBackend declare the same variant set
#[test]
fn vended_kind_and_storage_backend_variant_sets_are_equal() {
    let storage = source("storage.rs");
    let variants: BTreeSet<&str> = enum_variant_names(storage, "StorageBackend")
        .into_iter()
        .collect();
    let kinds: BTreeSet<&str> = enum_variant_names(storage, "VendedBackendKind")
        .into_iter()
        .collect();

    assert_eq!(
        variants, kinds,
        "`StorageBackend`'s variant names and `VendedBackendKind`'s must be the same set — a \
         kind with no variant dispatches nowhere, and a variant with no kind is unreachable \
         from a vended location's scheme"
    );
}

fn static_store_address_field_declarations() -> Vec<&'static str> {
    let body = declaration_body(source("storage.rs"), "struct StaticStoreAddress");
    let fields: Vec<&str> = body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("//"))
        .map(|line| line.trim_end_matches(','))
        .collect();

    assert!(
        !fields.is_empty(),
        "extracted no fields from `struct StaticStoreAddress`'s body — the probe's own \
         parsing is broken, not just failing to find a match"
    );
    fields
}

fn assert_static_store_address_declares_no_credential_field() {
    for declaration in static_store_address_field_declarations() {
        let name = declaration
            .split(':')
            .next()
            .unwrap_or(declaration)
            .split_whitespace()
            .next_back()
            .unwrap_or("");
        for credential in [
            "access_key",
            "secret_key",
            "session_token",
            "token",
            "account_key",
            "sas_token",
            "password",
        ] {
            assert!(
                !name.contains(credential),
                "`struct StaticStoreAddress` must declare no credential field, but it names \
                 `{name}`, which spells `{credential}` — the vended selectors take this type \
                 precisely because it CANNOT carry a credential"
            );
        }
    }
}

/// Scenario: StaticStoreAddress is reachable via Default and From<&ConnectionCreds> and declares no credential field
#[test]
fn static_store_address_is_reachable_and_declares_no_credential_field() {
    let unset = StaticStoreAddress::default();
    assert_eq!(unset.endpoint(), "");
    assert_eq!(unset.region(), "");
    assert_eq!(unset.path_style(), None);

    let creds = connection_creds();
    let configured = StaticStoreAddress::from(&creds);
    assert_eq!(configured.endpoint(), creds.endpoint);
    assert_eq!(configured.region(), creds.region);
    assert_eq!(configured.path_style(), creds.path_style);

    assert_static_store_address_declares_no_credential_field();
}

/// Scenario: Shared vended policy steps stay crate-private (`scheme_of` is reused by the engine)
#[test]
fn shared_vended_policy_steps_are_not_public() {
    const SHARED_STEPS: [(&str, &str); 5] = [
        ("pub fn s3_backend", "s3_backend"),
        ("pub fn adls_backend", "adls_backend"),
        ("pub fn location_host", "location_host"),
        ("pub fn adls_account_name", "adls_account_name"),
        ("pub struct VendedS3", "VendedS3"),
    ];

    for (name, source) in CATALOG_SOURCES {
        for (declaration, _) in SHARED_STEPS {
            assert!(
                !declares(source, declaration),
                "{name} must not declare `{declaration}` — the shared vended policy step \
                 behind `resolve_vended_storage`/`resolve_uc_vended_storage` stays \
                 crate-private"
            );
        }
    }

    let lib = source("lib.rs");
    for (_, item) in SHARED_STEPS {
        assert!(
            !lib.contains(item),
            "lib.rs must not re-export the shared vended policy step `{item}` — the crate \
             exposes the two concept-level selectors, not the policy they share"
        );
    }
}

/// Scenario: StaticStoreAddress fields stay non-public
#[test]
fn static_store_address_fields_are_not_public() {
    for declaration in static_store_address_field_declarations() {
        let visibility = declaration.split_whitespace().next().unwrap_or("");
        assert!(
            visibility != "pub" && !visibility.starts_with("pub("),
            "`struct StaticStoreAddress` must keep every field non-`pub`, but declares \
             `{declaration}` — a public field is a second construction path around the one \
             reviewed `From<&ConnectionCreds>` conversion"
        );
    }
}
