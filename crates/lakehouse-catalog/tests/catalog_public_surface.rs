//! Compile-time reachability probe for `lakehouse-catalog`'s public surface,
//! from an external-crate vantage.
//!
//! This is a pure `use` list with no behavior. It lives in the `tests/` crate
//! so it only sees items that are actually `pub` (and re-exported at the
//! crate root) — not the elevated visibility a descendant `mod tests` would
//! see. Every module in `src/` (`creds`, `iceberg_io`, `namespace`,
//! `redaction`, `session`, `storage`, `vended`, plus `auth` and `sigv4`) is
//! private (`mod`, not `pub mod`); the only externally reachable items are the
//! ones `src/lib.rs` re-exports with `pub use`. If any of the items below is
//! narrowed below `pub` or its re-export is removed, this file fails to
//! compile — turning an effective visibility regression into a build failure
//! rather than a silent gap that only a `pub use` text diff would miss.
//!
//! Covers the Verification > Scenario Coverage row "The crate exposes the
//! concept-level API and hides every mechanism step"
//! (vs-adapter/catalog-crate-structure).
#![allow(unused_imports)]

use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;
use iceberg_catalog_rest::{LoadTableResult, StorageCredential};
use lakehouse_catalog::{
    AdlsCred, CatalogProps, CatalogSession, ConnectionCreds, StorageBackend, StorageProps,
    list_namespace_tables, load_table_any_auth, parse_table_ident, redact_credentials,
    redact_secret_values, resolve_vended_storage,
};

/// Every `.rs` source file under `crates/lakehouse-catalog/src/`, embedded at
/// compile time via `include_str!`. Paths are relative to this test file:
/// `crates/lakehouse-catalog/tests` -> `crates/lakehouse-catalog/src`.
const CATALOG_SOURCES: &[(&str, &str)] = &[
    ("auth.rs", include_str!("../src/auth.rs")),
    ("creds.rs", include_str!("../src/creds.rs")),
    ("iceberg_io.rs", include_str!("../src/iceberg_io.rs")),
    ("lib.rs", include_str!("../src/lib.rs")),
    ("namespace.rs", include_str!("../src/namespace.rs")),
    ("redaction.rs", include_str!("../src/redaction.rs")),
    ("session.rs", include_str!("../src/session.rs")),
    ("sigv4.rs", include_str!("../src/sigv4.rs")),
    ("storage.rs", include_str!("../src/storage.rs")),
    ("test_support.rs", include_str!("../src/test_support.rs")),
    ("vended.rs", include_str!("../src/vended.rs")),
];

/// `resolve_vended_storage` is the crate's only vended entry point. Selecting
/// the credential source and merging it into the storage props are two
/// mechanism steps behind it, demoted from `pub`; `extract_vended_keys` names
/// the four `extract_vended_*` readers the consolidation inlined. `build_s3_file_io` is
/// the deleted predecessor of `StorageBackend::file_io` — its reappearance as
/// a free function would be the same kind of surface regression.
/// `s3_backend_from_vended` is the construct-from-vended reader that replaced
/// the deleted `merge_vended_into_storage` for the S3 arm — it must stay
/// exactly as demoted as the function it replaced. A `pub` on any of these
/// five is how that demotion or deletion could be silently reversed.
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

/// Build a minimal `LoadTableResult` for `resolve_vended_storage` fixtures —
/// the external-crate equivalent of `vended.rs`'s own `mod tests` helper of
/// the same shape, since that one is `#[cfg(test)]`-private to the crate and
/// unreachable from here.
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

/// Pins `resolve_vended_storage`'s arity and return type from outside the
/// crate: exactly three positional parameters — `&LoadTableResult`, an anchor
/// `&str`, and an `allow_http: bool` — and a `Result<StorageBackend,
/// UdfError>` return, with no `&StorageBackend` (or any other
/// CONNECTION-derived value) among them. A future regression that
/// reintroduced a `base: &StorageBackend` parameter, or changed the return
/// type, would fail to compile here rather than only in the crate's own
/// (`#[cfg(test)]`-private) unit tests.
///
/// Also exercises the scheme-driven selection this arity change exists for:
/// the anchor's OWN scheme picks the variant (here S3, for an `s3://`
/// anchor), with nothing else in the call carrying a pre-existing backend to
/// select from.
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

/// Extracts every variant name declared in `storage.rs`'s `enum
/// StorageBackend` source — generically, by scanning the enum body rather
/// than hardcoding `["S3", "Adls"]` — and asserts each one appears as a
/// literal in `vended.rs`'s source text.
///
/// A hardcoded list would keep passing silently after a third variant is
/// added to the enum; extracting the names from `storage.rs` itself is what
/// makes this probe notice a new variant automatically and fail until
/// `vended.rs`'s scheme-to-variant mapping is updated to name it too.
#[test]
fn vended_selector_source_names_every_storage_backend_variant() {
    let storage_source = CATALOG_SOURCES
        .iter()
        .find_map(|(name, source)| (*name == "storage.rs").then_some(*source))
        .expect("storage.rs must be present in CATALOG_SOURCES");
    let vended_source = CATALOG_SOURCES
        .iter()
        .find_map(|(name, source)| (*name == "vended.rs").then_some(*source))
        .expect("vended.rs must be present in CATALOG_SOURCES");

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

    // Restrict the search to the PRODUCTION region of vended.rs: a variant name
    // that appears only inside its `#[cfg(test)] mod tests` (e.g. a fixture
    // import or an assertion string) must not satisfy this probe, or a third
    // variant with no production scheme-mapping arm at all could still pass.
    let vended_production_source = &vended_source[..vended_source
        .find("#[cfg(test)]")
        .unwrap_or(vended_source.len())];

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

    for variant in variant_names {
        let qualified = format!("StorageBackend::{variant}");
        assert!(
            vended_production_source.contains(&qualified),
            "vended.rs's PRODUCTION source must name `{qualified}` somewhere (its \
             scheme-to-variant mapping), but that literal does not appear outside \
             `#[cfg(test)] mod tests`"
        );
    }
}
