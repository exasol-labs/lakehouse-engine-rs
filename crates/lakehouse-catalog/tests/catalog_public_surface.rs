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

/// True when `source` declares `declaration` — e.g. `pub fn s3_backend` — as that
/// item ITSELF rather than as the prefix of a longer name. Without the boundary
/// check `pub fn s3_backend` would also fire on `pub fn s3_backend_from_vended`,
/// and the shared policy steps could not be asserted in exactly one probe apiece.
fn declares(source: &str, declaration: &str) -> bool {
    source.match_indices(declaration).any(|(index, matched)| {
        source[index + matched.len()..]
            .chars()
            .next()
            .is_none_or(|next| !next.is_alphanumeric() && next != '_')
    })
}

/// Mechanism steps behind `resolve_vended_storage`/`StorageBackend::file_io` that
/// were demoted from `pub` or deleted outright. `merge_vended_into_storage` and
/// `select_credential_source` are the two demoted steps; `extract_vended_keys`
/// names the four `extract_vended_*` readers the consolidation inlined;
/// `build_s3_file_io` is the deleted predecessor of `StorageBackend::file_io`.
/// `s3_backend_from_vended` and `adls_backend_from_vended` are deleted
/// predecessors too — the shared `s3_backend`/`adls_backend` in `storage.rs`
/// replaced BOTH, one construction per backend for both catalog kinds — and it is
/// `shared_vended_policy_steps_are_not_public`, not this test, that asserts those
/// replacements stay crate-private. A `pub` on any name here is how a demotion or
/// deletion could be silently reversed.
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
/// neutral shape. `CatalogTable` is constructed with its FORMAT tag and its
/// credential-vending key named explicitly, so dropping either field or narrowing
/// `TableFormat` below `pub` is a build failure here rather than a silent gap.
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

/// The raw Unity Catalog wire fields behind the neutral format tag and the
/// credential-vending key stay inside the Unity client: only their neutral
/// PROJECTIONS cross the boundary. `client.rs` — the module declaring every
/// neutral type — must name neither field in production code, so re-exposing a
/// raw wire field on a neutral type fails here rather than putting a Unity
/// Catalog concept on the crate's surface for every consumer to match on.
/// Matched through `declares`, whose trailing-boundary check is what keeps the
/// pre-existing local `table_ident` from reading as `table_id`.
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
/// credentials response type, the vended selector, and the store address that
/// selector takes — are reachable from outside the crate through the `unity` and
/// `storage` re-exports.
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
/// `(&LoadTableResult, anchor: &str, allow_http: bool, address: &StaticStoreAddress)
/// -> Result<StorageBackend, UdfError>`. The store address is the ONLY
/// CONNECTION-derived parameter, and it is a type that cannot carry a credential —
/// asserted here as part of the arity pin, because the arity is only worth pinning
/// while the added parameter stays credential-free. Reintroducing a
/// `base: &StorageBackend`, or widening the address to `&ConnectionCreds`, would
/// fail here rather than only in the crate's own `#[cfg(test)]`-private unit tests.
/// Called with an UNSET address, so the assertions below still read the vended
/// values.
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

/// Pins `resolve_uc_vended_storage`'s arity and return type from OUTSIDE the
/// crate: `(&TemporaryTableCredentials, storage_location: &str, allow_http: bool,
/// address: &StaticStoreAddress) -> Result<StorageBackend, UdfError>`. It carries
/// no `warehouse`, no static credential, and no existing `StorageBackend` — the
/// three vended selectors' input disjointness is enforced by this signature. The
/// one CONNECTION-derived value it does take is a store ADDRESS whose type cannot
/// carry a credential, asserted here as part of the arity pin so the added
/// parameter cannot widen back into a credential-bearing one. A response with no
/// usable S3 credential for an `s3://` location is a clear error, not a fabricated
/// backend.
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

/// The production half of a crate source file, with whole-line comments removed:
/// everything before its `#[cfg(test)]` sibling-module declaration, minus every
/// `//`-prefixed line. A name a doc comment merely MENTIONS must not satisfy a
/// probe that asks where a value is CONSTRUCTED or DISPATCHED on.
fn production_code(source: &str) -> String {
    let production = &source[..source.find("#[cfg(test)]").unwrap_or(source.len())];
    production
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The `{ ... }` body of the named declaration in `source` — `enum StorageBackend`,
/// `struct StaticStoreAddress` — brace-matched, so a nested body cannot end it early.
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

/// Every variant name the named enum declares in `source`, extracted generically
/// rather than hardcoded, so a variant added to `StorageBackend` or a kind added
/// to `VendedBackendKind` propagates into every probe below.
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

/// `storage.rs` — the enum's own module — is the shared home the vended policy
/// moved into, so every `StorageBackend` variant is CONSTRUCTED there. Neither
/// vended selector names a variant any more: each classifies a location's scheme
/// into a `VendedBackendKind` and hands neutral values to this home, which is the
/// same relocation the dispatch probe below pins from the selectors' side.
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

/// Each vended selector dispatches on EVERY `VendedBackendKind` before calling
/// into the shared home, so a kind added to the enum fails here until BOTH
/// selectors map it. Leaving this to the construction probe alone would stop
/// forcing that per-selector — and a kind one selector handled and the other did
/// not is exactly the drift that let a plaintext `abfs://` location through
/// ungated.
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

/// The two enums stay in step: every kind a selector can dispatch to has a
/// constructible `StorageBackend` variant, and every variant has a kind that
/// selects it. Without this binding, growing one enum alone would leave the
/// construction probe and the dispatch probe each passing over a different set.
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

/// Every field declaration `storage.rs`'s own `struct StaticStoreAddress`
/// declaration carries, comment lines dropped and trailing commas removed, so the
/// probes below read what the struct DECLARES rather than what its doc comment
/// mentions.
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

/// `StaticStoreAddress` is the capability-narrowed parameter both vended selectors
/// take instead of `&ConnectionCreds`: it can carry a store address and nothing
/// else. The guarantee is the TYPE's, so it is asserted against that type's own
/// declaration — a field added there is the single edit that would put a static
/// credential back within a vended resolution's reach, and it must fail a test
/// rather than depend on review.
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

/// The store address both vended selectors take is reachable from OUTSIDE the
/// crate through exactly the two constructions its private fields leave open —
/// `Default` and the single `From<&ConnectionCreds>` conversion — and its
/// declaration names no credential field. Together those decide WHICH CONNECTION
/// values may cross into a vended resolution in one reviewed conversion rather
/// than at each call site.
#[test]
fn static_store_address_is_reachable_and_declares_no_credential_field() {
    let unset = StaticStoreAddress::default();
    assert_eq!(unset.endpoint(), "");
    assert_eq!(unset.region(), "");

    let creds = connection_creds();
    let configured = StaticStoreAddress::from(&creds);
    assert_eq!(configured.endpoint(), creds.endpoint);
    assert_eq!(configured.region(), creds.region);

    assert_static_store_address_declares_no_credential_field();
}

/// The vended policy steps `storage.rs` now owns are mechanism, not surface: the
/// two construction functions, the three derivations they read a location
/// through, and the neutral `VendedS3` both selectors reduce their own wire shape
/// to. Moving them into one shared home widened nothing — a `pub` on any of them,
/// or a `lib.rs` re-export, would turn an internal refactor into a permanent API
/// obligation. Each is asserted here and nowhere else:
/// `demoted_and_deleted_functions_are_not_declared_public` covers their deleted
/// predecessors instead.
#[test]
fn shared_vended_policy_steps_are_not_public() {
    const SHARED_STEPS: [(&str, &str); 6] = [
        ("pub fn s3_backend", "s3_backend"),
        ("pub fn adls_backend", "adls_backend"),
        ("pub fn scheme_of", "scheme_of"),
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

/// Both `StaticStoreAddress` fields stay non-`pub`. Their privacy is the whole
/// mechanism: it leaves `Default` and the single `From<&ConnectionCreds>`
/// conversion as the only constructions reachable outside `storage.rs`, so WHICH
/// CONNECTION values cross into a vended resolution is one reviewed edit rather
/// than a field any call site can set. Widening either field to `pub` restores
/// field-by-field construction at a distance and must fail here rather than pass
/// silently — `static_store_address_is_reachable_and_declares_no_credential_field`
/// would not notice, since it constrains which fields EXIST, not who may set them.
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

/// Every field declaration `creds.rs`'s own `struct StorageCreds` declaration
/// carries, comment lines dropped and trailing commas removed — mirrors
/// `static_store_address_field_declarations`, applied to the projection that
/// crosses the SAME crate boundary in the opposite direction: `parse_creds`
/// reads these nine fields out of it rather than the vended selectors reading
/// a store address into it.
fn storage_creds_field_declarations() -> Vec<&'static str> {
    let body = declaration_body(source("creds.rs"), "struct StorageCreds");
    let fields: Vec<&str> = body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("//"))
        .map(|line| line.trim_end_matches(','))
        .collect();

    assert!(
        !fields.is_empty(),
        "extracted no fields from `struct StorageCreds`'s body — the probe's own \
         parsing is broken, not just failing to find a match"
    );
    fields
}

/// `StorageCreds` is the storage-only projection `parse_creds` reads across the
/// crate boundary: it must declare exactly the nine storage fields, each `pub`
/// (mirroring `StorageProps`, because `parse_creds` constructs it from outside
/// this crate), and none of the eight catalog-auth fields `ConnectionCreds`
/// also carries — a catalog-auth field added here is the one edit that would
/// let a secret meant to stay adapter-side reach the scan UDF through this
/// projection instead.
#[test]
fn storage_creds_declares_exactly_the_nine_pub_storage_fields() {
    let declarations = storage_creds_field_declarations();

    let names: BTreeSet<&str> = declarations
        .iter()
        .map(|declaration| {
            let visibility = declaration.split_whitespace().next().unwrap_or("");
            assert_eq!(
                visibility, "pub",
                "`struct StorageCreds` must declare every storage field `pub`, but `{declaration}` \
                 is not — `parse_creds` constructs this type across the crate boundary"
            );
            declaration
                .split(':')
                .next()
                .unwrap_or(declaration)
                .split_whitespace()
                .next_back()
                .unwrap_or("")
        })
        .collect();

    let expected: BTreeSet<&str> = [
        "endpoint",
        "region",
        "access_key",
        "secret_key",
        "session_token",
        "path_style",
        "account_name",
        "account_key",
        "sas_token",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        names, expected,
        "`struct StorageCreds` must declare exactly the nine storage fields, no more and no fewer"
    );

    for credential in [
        "token",
        "client_id",
        "client_secret",
        "oauth2_server_uri",
        "scope",
        "warehouse",
        "use_sigv4",
        "use_vended_credentials",
    ] {
        assert!(
            !names.contains(credential),
            "`struct StorageCreds` must declare no catalog-auth field, but it names \
             `{credential}` — this projection crosses into the scan UDF and must carry no \
             catalog secret"
        );
    }
}

/// `StorageCreds::from_json` and `StorageCreds::backend` are called directly
/// here — not just the `StorageCreds` type named in the `use` list above — so
/// narrowing either method below `pub` is a compile failure in this
/// external-crate probe, rather than a silent gap the type-only import would
/// miss.
#[test]
fn storage_creds_from_json_and_backend_are_reachable() {
    let creds = StorageCreds::from_json(&serde_json::json!({
        "endpoint": "http://minio:9000",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET",
    }));
    let _: StorageBackend = creds.backend(true);
}
