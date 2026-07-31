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

use lakehouse_catalog::{
    CatalogProps, CatalogSession, ConnectionCreds, StorageBackend, StorageProps,
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
/// a free function would be the same kind of surface regression. A `pub` on
/// any of these four is how that demotion or deletion could be silently reversed.
#[test]
fn demoted_and_deleted_functions_are_not_declared_public() {
    for (name, source) in CATALOG_SOURCES {
        for mechanism in [
            "pub fn merge_vended_into_storage",
            "pub fn select_credential_source",
            "pub fn extract_vended_keys",
            "pub fn build_s3_file_io",
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
