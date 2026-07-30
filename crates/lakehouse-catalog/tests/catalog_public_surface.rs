//! Compile-time reachability probe for `lakehouse-catalog`'s public surface,
//! from an external-crate vantage.
//!
//! This is a pure `use` list with no behavior. It lives in the `tests/` crate
//! so it only sees items that are actually `pub` (and re-exported at the
//! crate root) — not the elevated visibility a descendant `mod tests` would
//! see. Every module in `src/` (`creds`, `iceberg_io`, `namespace`,
//! `redaction`, `session`, `vended`, plus `auth` and `sigv4`) is private
//! (`mod`, not `pub mod`); the only externally reachable items are the ones
//! `src/lib.rs` re-exports with `pub use`. If any of the items below is
//! narrowed below `pub` or its re-export is removed, this file fails to
//! compile — turning an effective visibility regression into a build failure
//! rather than a silent gap that only a `pub use` text diff would miss.
//!
//! Covers the Verification > Scenario Coverage row "The crate exposes the
//! concept-level API and hides every mechanism step"
//! (vs-adapter/catalog-crate-structure).
#![allow(unused_imports)]

use lakehouse_catalog::{
    CatalogProps, CatalogSession, ConnectionCreds, StorageProps, build_s3_file_io,
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
    ("test_support.rs", include_str!("../src/test_support.rs")),
    ("vended.rs", include_str!("../src/vended.rs")),
];

/// `resolve_vended_storage` is the crate's only vended entry point. Selecting the
/// credential source and merging it into the storage props are the two mechanism
/// steps left after the consolidation inlined the four `extract_vended_*` readers,
/// so a `pub` on either is how that demotion could be silently reversed.
#[test]
fn vended_mechanism_functions_are_not_declared_public() {
    for (name, source) in CATALOG_SOURCES {
        for mechanism in [
            "pub fn merge_vended_into_storage",
            "pub fn select_credential_source",
        ] {
            assert!(
                !source.contains(mechanism),
                "{name} must not declare `{mechanism}` — it is a mechanism step \
                 behind `resolve_vended_storage` that the crate must keep private"
            );
        }
    }
}
