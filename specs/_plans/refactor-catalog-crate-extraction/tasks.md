# Tasks: refactor-catalog-crate-extraction

## Phase 2: Implementation (Group A)
- [x] 1.1 Create crates/lakehouse-catalog with manifest, empty src/lib.rs, workspace member, path dep in lakehouse-engine; confirm cargo test --workspace and clippy green
- [x] 1.2 Repair Makefile VS_SRCS staleness guard before any catalog code moves

## Phase 2: Implementation (Group B)
- [x] 2.1 Move StorageProps/CatalogProps/ConnectionCreds into lakehouse-catalog, re-export at pre-move paths [expert]
- [x] 3.1 Move adapter/sigv4.rs into lakehouse-catalog::sigv4 with its four tests
- [x] 7.1 Update specs/mission.md and CLAUDE.md for the two-crate layout

## Phase 2: Implementation (Group C)
- [x] 2.2 Move redact_credentials/redact_secret_values/redact_catalog_error into lakehouse-catalog
- [x] 3.0 Add crates/lakehouse-catalog/src/test_support.rs with all 15 shared test helpers

## Phase 2: Implementation (Group D)
- [x] 3.2 Move credentials.rs catalog code into lakehouse-catalog as auth/session/iceberg_io/vended modules [expert]

## Phase 2: Implementation (Group E)
- [x] 3.3 Move namespace.rs into lakehouse-catalog::namespace; delete engine base_creds/static_storage [expert]

## Phase 2: Implementation (Group F)
- [x] 4.1 Introduce resolve_vended_storage, demote seven mechanism functions to crate-private [expert]
- [x] 4.2 Add behavior-parity unit tests for resolve_vended_storage (six absence/precedence cases)

## Phase 2: Implementation (Group G)
- [x] 5.1 Rename resolve_file_list_with_session to resolve_file_list(&CatalogSession, ...) [expert]
- [x] 5.2 Change resolve_table_schema to take &CatalogSession; hoist one session build in adapter/mod.rs [expert]
- [x] 5.3 Update the four external resolve_file_list call sites in tests/
- [x] 6.1 Update pushdown_surface_probe.rs and pushdown_public_surface.rs (25->22, 15->12 items) [expert]
- [x] 6.2 Add crates/lakehouse-catalog/tests/catalog_public_surface.rs
- [x] 6.3 Add crates/lakehouse-catalog/tests/catalog_crate_boundary.rs
- [x] 6.4 Add crates/lakehouse-engine/tests/shared_type_reexports.rs
- [x] 7.2 Add malformed_table_ident_fails_before_any_catalog_contact test

## Phase 2: Implementation (Group H)
- [x] 6.5 Add crates/lakehouse-engine/tests/catalog_session_signatures.rs
- [x] 7.3 Run the full gate (fmt, clippy, test --workspace, cross-musl-udf-build, test-e2e, test-e2e-lakekeeper)

## Phase 4: Review Fixes
- [x] 4.1 Make the hoisted CatalogSession build in adapter/mod.rs lazy so an empty table_idents performs no catalog contact; add create_virtual_schema_over_empty_namespace_contacts_no_catalog_session [expert]
- [x] 4.2 Delete redact_catalog_error from lakehouse-catalog and repoint every call site to redact_credentials [expert]
- [x] 4.3 Rewrite vended.rs mechanism-level tests against resolve_vended_storage; inline and delete the four now-callerless extract_* helpers [expert]
- [x] 4.4 Delete the unread CatalogProps.uri field and every producer's uri: initializer [expert]
- [x] 4.5 Drop the userless direct reqwest dependency from crates/lakehouse-engine/Cargo.toml [expert]
- [x] 4.6 Move the four literal aws-*/reqwest pins in lakehouse-catalog/Cargo.toml into [workspace.dependencies] [expert]
- [x] 4.7 Rewrite lakehouse-catalog/src/lib.rs's crate doc to present tense; drop plan-path citations in catalog_public_surface.rs and catalog_session_signatures.rs, citing permanent spec feature names instead
- [x] 4.8 Rewrite test_support.rs's module doc to drop task numbers, deleted-line citations, and the stale "thirteen items" count
- [x] 4.9 Relocate the nine single-module test_support.rs helpers (AUTH_PROP_KEYS to auth.rs, make_load_table_result/vended_result_flat_config/VENDED_AK/VENDED_SK/VENDED_TOK/VENDED_REGION to vended.rs); narrow the four REST_CATALOG_PROP_* constants in auth.rs from pub(crate) to private
- [x] 4.10 Reword vended.rs's vended_creds_override_static_in_spec doc comment and its "Static infrastructure fields must be preserved" inline comment to describe fixture-driven fallthrough, not a function guarantee
- [x] 4.11 Convert sigv4.rs's misattached /// header to a //! module doc comment naming its place in the crate
- [x] 4.12 Narrow the redact_credentials/redact_secret_values re-export in scan/emit.rs from pub to pub(crate)
- [x] 4.13 Replace the eight-line inline rationale block at pushdown/mod.rs's parse-before-config guard with the single guard line (adapter/mod.rs's half already fixed by the expert pass)
- [x] 4.14 Rewrite the eight plan-task-number/issue-number comments across pushdown/mod.rs, vended.rs, catalog_session_signatures.rs, and shared_type_reexports.rs to name behavior instead
- [x] 4.15 Delete the four unreachable sentinel constants and the vacuous assertion loop in catalog_auth_secrets_never_in_scan_spec_with_vending

## Phase 3: Verification
- [x] V.1 Automated checks (build/test/lint/format)
- [x] V.2 Scenario coverage audit
- [x] V.3 Manual verification steps
