//! Compile-time reachability probe for `lakehouse-catalog`'s public surface,
//! from an external-crate vantage.
//!
//! This is a `use` list plus minimal behavioral pins with almost no logic. It
//! lives in the `tests/` crate so it only sees items that are actually `pub`
//! (and re-exported at the crate root) — not the elevated visibility a
//! descendant `mod tests` would see. Every module in `src/` (`auth`, `client`,
//! `creds`, `iceberg_io`, `namespace`, `redaction`, `session`, `sigv4`,
//! `storage`, `unity`, `vended`) is private (`mod`, not `pub mod`); the only
//! externally reachable items are the ones `src/lib.rs` re-exports with
//! `pub use`. If any of the items below is narrowed below `pub` or its
//! re-export is removed, this file fails to compile — turning an effective
//! visibility regression into a build failure rather than a silent gap that
//! only a `pub use` text diff would miss.
//!
//! Covers the Verification > Scenario Coverage row "The crate exposes the
//! concept-level API and hides every mechanism step"
//! (vs-adapter/catalog-crate-structure).
#![allow(unused_imports)]

use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;
use iceberg_catalog_rest::{LoadTableResult, StorageCredential};
use lakehouse_catalog::{
    AdlsCred, CatalogClient, CatalogColumn, CatalogListing, CatalogProps, CatalogSession,
    CatalogTable, CatalogTableIdent, CatalogTableType, ColumnSourceType, ConnectionCreds,
    IcebergRestCatalogClient, StorageBackend, StorageProps, TemporaryTableCredentials,
    UnityCatalogSession, load_table_any_auth, parse_table_ident, redact_credentials,
    redact_secret_values, resolve_uc_vended_storage, resolve_vended_storage,
};

/// Every production `.rs` source file under `crates/lakehouse-catalog/src/`
/// (`*_tests.rs` siblings hold no production surface), embedded at compile time
/// via `include_str!`. Paths are relative to this test file:
/// `crates/lakehouse-catalog/tests` -> `crates/lakehouse-catalog/src`.
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
        path_style: true,
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

/// `resolve_vended_storage` is the crate's only vended entry point. Selecting
/// the credential source and merging it into the storage props are two
/// mechanism steps behind it, demoted from `pub`; `extract_vended_keys` names
/// the four `extract_vended_*` readers the consolidation inlined. `build_s3_file_io` is
/// the deleted predecessor of `StorageBackend::file_io` — its reappearance as
/// a free function would be the same kind of surface regression.
/// `s3_backend_from_vended` replaced the deleted `merge_vended_into_storage` for the
/// S3 arm. A `pub` on any of these five is how that demotion or deletion could be
/// silently reversed.
#[test]
fn demoted_and_deleted_functions_are_not_declared_public() {
    for (name, source) in CATALOG_SOURCES {
        for mechanism in [
            "pub fn merge_vended_into_storage",
            "pub fn select_credential_source",
            "pub fn extract_vended_keys",
            "pub fn build_s3_file_io",
            "pub fn s3_backend_from_vended",
        ] {
            assert!(
                !source.contains(mechanism),
                "{name} must not declare `{mechanism}` — it is a demoted or deleted \
                 function behind `resolve_vended_storage`/`StorageBackend::file_io` \
                 that the crate must keep private or removed"
            );
        }
    }
}

/// `StorageBackend::secret_values` and `StorageBackend::file_io` are called
/// directly here — not just the `StorageBackend` type named in the `use` list
/// above — so narrowing either method below `pub` is a compile failure in
/// this external-crate probe, rather than a silent gap the type-only import
/// would miss. `catalog_storage_props` is deliberately NOT referenced here:
/// it is `pub(crate)`, not part of the public surface.
#[test]
fn storage_backend_secret_values_and_file_io_are_reachable() {
    let backend = StorageBackend::S3(StorageProps::default());
    let _: Vec<&str> = backend.secret_values();
    let _: iceberg::io::FileIO = backend.file_io();
}

/// The Iceberg REST client and the Unity Catalog session are both usable as
/// `Box<dyn CatalogClient>`: the trait is dyn-compatible and each type
/// implements it, so the engine's single construction site can hold either
/// behind one boxed trait object.
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

/// The shared trait and its catalog-neutral metadata types are constructible
/// from outside the crate, while the Unity Catalog wire types stay hidden —
/// never re-exported and never `pub`-declared — so the engine consumes only the
/// neutral shape.
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
        columns: vec![column],
    };
    let listing = CatalogListing {
        tables: vec![table],
        skipped: vec![ident],
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

/// `list_namespace_tables` was demoted `pub` -> `pub(crate)` now that
/// `IcebergRestCatalogClient::list_tables` is its only caller, and its `lib.rs`
/// re-export was removed. Naming it in the `use` list above would already fail
/// to compile; this pins the demotion at the source so a re-widening to `pub`
/// or a re-added re-export fails here too.
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

/// The native Unity Catalog public items — the session, the temporary-table-
/// credentials response type, and the vended selector — are reachable from
/// outside the crate through the `unity` re-export.
#[test]
fn unity_catalog_public_items_are_reachable() {
    let _session = UnityCatalogSession::new("http://unity", connection_creds());
    let vended = TemporaryTableCredentials {
        aws_temp_credentials: None,
        azure_user_delegation_sas: None,
        gcp_oauth_token: None,
    };
    let _resolved: Result<StorageBackend, UdfError> =
        resolve_uc_vended_storage(&vended, "s3://bucket/db/t", true);
}

/// Minimal `LoadTableResult` fixture; `vended.rs`'s own helper of the same shape is
/// `#[cfg(test)]`-private to that crate and unreachable from here.
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

/// Pins `resolve_vended_storage`'s arity and return type from OUTSIDE the crate:
/// `(&LoadTableResult, anchor: &str, allow_http: bool) -> Result<StorageBackend,
/// UdfError>`, with no CONNECTION-derived parameter. Reintroducing a
/// `base: &StorageBackend` would fail to compile here rather than only in the crate's
/// own `#[cfg(test)]`-private unit tests.
#[test]
fn resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend() {
    let result = minimal_load_table_result(vec![
        ("s3.access-key-id", "AKIAEXAMPLE"),
        ("s3.secret-access-key", "secret-value"),
        ("client.region", "us-east-1"),
    ]);

    let backend: Result<StorageBackend, UdfError> =
        resolve_vended_storage(&result, "s3://bucket/db/t", true);

    match backend.expect("scheme-selected S3 arm must succeed") {
        StorageBackend::S3(props) => {
            assert_eq!(props.access_key, "AKIAEXAMPLE");
            assert_eq!(props.region, "us-east-1");
        }
        StorageBackend::Adls { .. } => panic!("an s3:// anchor must select the S3 variant"),
    }
}

/// Pins `resolve_uc_vended_storage`'s arity and return type from OUTSIDE the
/// crate: `(&TemporaryTableCredentials, storage_location: &str, allow_http: bool)
/// -> Result<StorageBackend, UdfError>`, carrying no `warehouse`, `region`, or
/// existing `StorageBackend` — the three vended selectors' input disjointness is
/// enforced by this signature. A response with no usable S3 credential for an
/// `s3://` location is a clear error, not a fabricated backend.
#[test]
fn resolve_uc_vended_storage_signature_takes_no_connection_value() {
    let vended = TemporaryTableCredentials {
        aws_temp_credentials: None,
        azure_user_delegation_sas: None,
        gcp_oauth_token: None,
    };

    let resolved: Result<StorageBackend, UdfError> =
        resolve_uc_vended_storage(&vended, "s3://bucket/db/t", true);

    assert!(
        resolved.is_err(),
        "an s3:// location with no vended aws credential must surface a clear error"
    );
}

/// Every variant name declared in `storage.rs`'s `enum StorageBackend` source,
/// extracted generically rather than hardcoding `["S3", "Adls"]`, so a third
/// variant added to the enum propagates into every selector probe below.
fn storage_backend_variant_names() -> Vec<&'static str> {
    let storage_source = source("storage.rs");

    let enum_start = storage_source
        .find("enum StorageBackend")
        .expect("storage.rs must declare `enum StorageBackend`");
    let body_start = storage_source[enum_start..]
        .find('{')
        .map(|offset| enum_start + offset + 1)
        .expect("`enum StorageBackend` must have a `{ ... }` body");

    let mut depth = 1usize;
    let mut body_end = body_start;
    for (offset, ch) in storage_source[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    body_end = body_start + offset;
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(
        depth == 0,
        "failed to find the matching closing brace for `enum StorageBackend`'s body"
    );
    let body = &storage_source[body_start..body_end];

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
        "extracted no variant names from `enum StorageBackend`'s body — the probe's own \
         parsing is broken, not just failing to find a match"
    );
    variant_names
}

/// Asserts a vended selector's PRODUCTION source names every `StorageBackend`
/// variant in its scheme-to-variant mapping — a variant named only inside a
/// `#[cfg(test)]` module must not satisfy the probe — so a third variant fails
/// here until the selector maps it too.
fn assert_source_names_every_storage_backend_variant(vended_source: &str) {
    let production = &vended_source[..vended_source
        .find("#[cfg(test)]")
        .unwrap_or(vended_source.len())];

    for variant in storage_backend_variant_names() {
        let qualified = format!("StorageBackend::{variant}");
        assert!(
            production.contains(&qualified),
            "the vended selector's PRODUCTION source must name `{qualified}` somewhere \
             (its scheme-to-variant mapping), but that literal does not appear outside \
             `#[cfg(test)]`"
        );
    }
}

/// The Iceberg vended selector (`vended.rs`) names every `StorageBackend`
/// variant in its scheme-to-variant mapping.
#[test]
fn vended_selector_source_names_every_storage_backend_variant() {
    assert_source_names_every_storage_backend_variant(source("vended.rs"));
}

/// The Unity Catalog vended selector (`unity/vended.rs`) — the third
/// backend-selection site — names every `StorageBackend` variant, so the
/// single-home and every-variant guarantees both hold as the enum grows.
#[test]
fn uc_vended_selector_source_names_every_storage_backend_variant() {
    assert_source_names_every_storage_backend_variant(source("unity/vended.rs"));
}
