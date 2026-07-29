# Plan: refactor-catalog-crate-extraction

## Summary

Extract the Iceberg REST catalog access layer out of `lakehouse-engine` into a new `crates/lakehouse-catalog` workspace member, so `CatalogSession` becomes genuinely `pub`, `resolve_file_list` and `resolve_table_schema` take `&CatalogSession` directly, and the `refactor-catalog-http-session` wrapper retires. The same change redraws the vended-credentials boundary: one concept-level `resolve_vended_storage` replaces the five-step mechanism sequence, its seven helper functions become crate-private, and the frozen pushdown façade releases exactly three items.

Tracked in issue [#204](https://github.com/exasol-labs/lakehouse-engine-rs/issues/204). Absorbs issue #214 (see `decision-log.md` decision [2]).

## Context

`CatalogSession` is `pub(crate)` in `crates/lakehouse-engine/src/adapter/pushdown/credentials.rs`. Because a `pub(crate)` type cannot appear in a `pub` signature, plan `refactor-catalog-http-session` (#185) shipped a public `resolve_file_list(catalog_uri: &str, …)` wrapper that builds a single-use session and delegates to a `pub(crate)` core `resolve_file_list_with_session(&CatalogSession, …)`. The wrapper is correct but permanent: no amount of module reshuffling inside one crate makes the type public without also exposing it on the frozen `crate::adapter::pushdown::<name>` façade. `specs/_decision/024-refactor-catalog-http-session.md` records the crate extraction as the rejected-and-deferred option, naming this issue.

The same freeze pins a second problem. `extract_vended_keys` and `merge_vended_into_storage` are `pub` on the façade for one reason: two probe files name them. They are the mechanism steps of vended-credential resolution, not the concept, and they have exactly one production caller each. Their sibling extractors disagree on how absence is spelled — empty string for the key pair, `Option<String>` for region and endpoint, `Option<bool>` for path-style — and the same longest-prefix credential-source selection runs four times over one `LoadTableResult`. None of that can be fixed while a probe asserts `extract_vended_keys`' signature.

The issue defers one decision to this plan: the shared-type boundary. The catalog code depends on `ConnectionCreds`, `CatalogProps`, and `StorageProps`, and the issue's own framing is that `scan/spec.rs` types are "used engine-wide". Reading the code splits the three apart. `CatalogProps` is planning-only and mis-homed: `vs-adapter/rest-catalog-oauth-auth` records that `ScanSpec` carries no catalog block, and no `scan/*.rs` module names it. `ConnectionCreds` appears in 11 files, none of them in `scan/`. Only `StorageProps` is genuinely two-owner — 28 files, and it crosses the UDF boundary inside `CommonScanSpec` — and the catalog is the side that produces it.

- **Goals** — `CatalogSession` genuinely `pub`; one `resolve_file_list` taking `&CatalogSession`; the wrapper and `resolve_file_list_with_session` gone; `resolve_vended_storage` as the crate's public vended API with uniform `Option` absence; the mechanism steps crate-private; the façade baseline redrawn once and re-frozen; behavior and generated SQL unchanged.
- **Non-Goals** — moving Iceberg file planning or the scan-spec wire types into the new crate; folding namespace enumeration onto `CatalogSession`; publishing either crate; a crate-local error type; changing the CONNECTION password schema, the VS protocol, or the `ScanSpec` JSON encoding; fixing `datafusion-scan/scan-module-structure`'s separate dangling probe citation.

## Design

### Context

Four forces shape the boundary.

1. **A crate boundary is the only construct that gives the required visibility.** Rust has no "public to another module but not to the crate's consumers" beyond `pub(crate)`, which is exactly the constraint blocking #185's wrapper. Extraction is not the cheapest way to make `CatalogSession` public — it is the only way that also keeps `CatalogAuth`, the OAuth grant, and the prefix lookup unreachable.
2. **The dependency must point one way.** `lakehouse-engine` → `lakehouse-catalog`. The reverse edge is a cycle, so any type both crates name must be declared in the catalog crate.
3. **`StorageProps` is a wire type.** It is a `CommonScanSpec` field serialized into the scan-driving SQL literal. Its serde encoding must not move a byte, which rules out any design that re-encodes it across the boundary.
4. **The characterization gate is the unedited suite.** ~37,600 lines of source with co-located tests, plus four E2E suites. "Behavior unchanged" is only falsifiable if the tests that prove it are not themselves rewritten. Every design choice below is biased toward leaving consumer code untouched.

### Decision

Two crates. `lakehouse-catalog` owns catalog access AND the credential material that access needs; `lakehouse-engine` keeps Iceberg file planning, the scan-spec wire format, and the Exasol CONNECTION parser. The three shared types and the two redaction primitives are declared once in the catalog crate and re-exported at their pre-move engine paths, so no consumer's `use` line changes.

#### Architecture

```
                    crates/lakehouse-catalog                crates/lakehouse-engine
                    ────────────────────────                ───────────────────────
  pub  ──────────▶  CatalogSession {client,uri,auth,prefix}
                    CatalogSession::resolve                 adapter/pushdown/mod.rs
                    load_table_any_auth          ◀────────  adapter/pushdown/file_resolution.rs
                    build_s3_file_io             ◀────────    resolve_file_list(&CatalogSession, …)
                    resolve_vended_storage       ◀────────    resolve_table_schema(&CatalogSession, …)
                    list_namespace_tables        ◀────────  adapter/mod.rs  (createVirtualSchema)
                    parse_table_ident            ◀────────  adapter/pushdown/{mod,joins}
                    redact_catalog_error         ◀────────  adapter/pushdown/{support,file_resolution}
                    ConnectionCreds  ─ re-export ─────────▶ adapter::connection::ConnectionCreds
                    StorageProps     ─ re-export ─────────▶ scan::spec::StorageProps
                    CatalogProps     ─ re-export ─────────▶ scan::spec::CatalogProps
                    redact_credentials     ─ re-export ───▶ scan::emit::redact_credentials
                    redact_secret_values   ─ re-export ───▶ scan::emit::redact_secret_values

  crate-private ─▶  CatalogAuth · resolve_catalog_auth · oauth2_client_credentials_grant
                    authed_get_json · resolve_load_table_prefix · prefix_from_config
                    build_load_table_url · glue_catalog_prefix · build_rest_catalog
                    inject_catalog_auth_props · non_empty · redact_catalog_auth_error
                    sign_request · extract_vended_keys · merge_vended_into_storage
                    extract_vended_{region,endpoint,path_style} · vended_config_value
                    extract_s3_keys_from_config

  NOT in the crate: FileEntry · LogicalField · NameMappingEntry · DeleteFileRef · ScanSpec
                    CommonScanSpec · plan_files_from_table · build_logical_schema
                    read_connection · parse_creds · validate_creds · storage_block · catalog_block
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Producer owns the type, consumer re-exports the path | `StorageProps` in `lakehouse-catalog`, `pub use` at `scan::spec` | One definition, one serde contract, zero conversion code, and 28 consuming files unedited |
| Concept-level façade over a private mechanism | `resolve_vended_storage` public, its seven steps crate-private | The public surface states intent (resolve effective storage), not the recipe (extract keys, merge keys) |
| Uniform absence convention | `Option` for all six vended values, `None` = absent-or-empty | Callers stop having to know which two of six spell absence as `""` |
| Crate-boundary visibility as the enforcement | `CatalogSession` fields private, `CatalogAuth` crate-private | The loader structurally cannot re-derive auth — the property `vs-adapter/pushdown-catalog-session` already asserts, now enforced across a crate edge |
| Compile-time reachability probe per boundary | `catalog_public_surface.rs` mirroring `pushdown_public_surface.rs` | The new boundary is guarded the same way the one it left is |
| Delivery mechanism stays outside the domain crate | `read_connection` and friends stay in the engine | The catalog crate must not name Exasol's CONNECTION object |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Two crates; the catalog crate declares `StorageProps`, `CatalogProps`, `ConnectionCreds` | A third `lakehouse-types` crate; leave them in the engine (cycle); parallel structs plus conversions | A `-types` crate groups by technical role and splits `ConnectionCreds` from its interpreters; parallel structs duplicate an S3 shape that must stay wire-stable, and force a rewrite of 1,805 moved test lines. See decision-log [1] |
| Absorb #214, deliver the consolidation once in final shape | Block on #214; execute #214's intermediate `pub(super)` form first | #214's "zero public-surface change" constraint exists only because the façade is frozen, and this plan unfreezes it. Ordering (verbatim move, then consolidate) preserves both parity checkpoints without a throwaway shape. See decision-log [2] |
| Boundary at catalog access, not file planning | Move `resolve_file_list` into the crate | Would drag `FileEntry`, `LogicalField`, `NameMappingEntry`, `DeleteFileRef`, and the Arrow type-tag mapping across, making the catalog crate own the scan-spec wire format |
| Redaction and SigV4 move with the credentials | Duplicate a small redactor in the crate; inject a redaction closure | Duplicating a security-relevant pattern list is the worst back-door duplication available; a closure is a parameter every caller would fill identically |
| `list_namespace_tables` moves | Leave it on the pushdown façade | Moving one caller makes `build_rest_catalog`, `glue_catalog_prefix`, and `sign_request` crate-private |
| `resolve_table_schema` also takes `&CatalogSession` | Defer the createVirtualSchema loop to a follow-up | Two sibling entry points with contradictory contracts is the inconsistency this plan removes; the fix is ~10 lines and collapses N OAuth grants to 1 |
| Exactly three items leave the façade; it re-freezes at the new baseline | Re-export `resolve_vended_storage` / `CatalogSession` on the façade | An engine-side probe asserting a catalog-crate concept through an alias re-creates the coupling the redraw removes |
| The probes become their own baseline | Write a fresh baseline `.txt` under this plan | `/speq:record` archives it into gitignored `_recorded/`, reproducing the dangling reference on a two-plan cycle |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `vs-adapter/catalog-crate-structure` | NEW | `specs/_plans/refactor-catalog-crate-extraction/vs-adapter/catalog-crate-structure/spec.md` |
| `vs-adapter/pushdown-catalog-session` | CHANGED | `specs/_plans/refactor-catalog-crate-extraction/vs-adapter/pushdown-catalog-session/spec.md` |
| `vs-adapter/pushdown-planning-cloud-credentials` | CHANGED | `specs/_plans/refactor-catalog-crate-extraction/vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| `vs-adapter/pushdown-module-structure` | CHANGED | `specs/_plans/refactor-catalog-crate-extraction/vs-adapter/pushdown-module-structure/spec.md` |

Checked and deliberately left unedited: `vs-adapter/connection-credentials`, `vs-adapter/rest-catalog-oauth-auth`, `vs-adapter/adapter-module-structure`, `packaging/single-so-two-entry-points`. None names a module path the extraction invalidates, and all four remain part of the characterization gate. See decision-log [10].

## Impact

No change reaches an Exasol user. The SQL the adapter returns, the VS protocol, the CONNECTION password schema, and the `ScanSpec` JSON encoding are all unchanged, so a deployed virtual schema behaves identically without redeployment.

One operator-visible improvement, stated precisely: `createVirtualSchema` over a namespace of N tables on an OAuth2 catalog runs ONE client-credentials grant and ONE `/v1/config` lookup FOR THE SCHEMA LOOP instead of N of each, alongside the namespace-listing catalog's own unchanged grant. `adapter/mod.rs:246` calls `list_namespace_tables` first, and when `client_id` and `client_secret` are both set that builds a `RestCatalog` whose `iceberg-catalog-rest` client performs its own `client_credentials` exchange (`client.rs:123`) and its own `/v1/config` handshake (`catalog.rs:430`), independent of any `CatalogSession`. The request therefore goes from N+1 grants to 2, not to 1. On the static-token and no-auth modes `iceberg-catalog-rest` runs no grant at all, so the saving is the schema loop's N `/v1/config` lookups collapsing to one; on the SigV4 mode `list_namespace_tables` builds no `RestCatalog`. Folding namespace enumeration onto the session is out of scope (see `vs-adapter/pushdown-catalog-session`'s out-of-scope Background bullet).

An OAuth2 grant failure in the schema loop surfaces before the loop rather than at the first table, with the same message.

**Breaking for in-repo Rust API consumers only.** `resolve_file_list` and `resolve_table_schema` change signature; `extract_vended_keys`, `merge_vended_into_storage`, and `list_namespace_tables` leave `lakehouse_engine::adapter::pushdown`. The only affected callers are this repository's own `tests/` crate, updated in tasks 5.3 and 6.1. `lakehouse-engine` is an unpublished private cdylib, so no external consumer exists.

## Requirements

| Requirement | Details |
|-------------|---------|
| Crate identity | `crates/lakehouse-catalog`, package `lakehouse-catalog`, `[lib] name = "lakehouse_catalog"`, version `0.1.0`, `edition = "2024"`, `license.workspace = true` — mirroring `crates/vs-expression` |
| Direct dependencies | `exasol-udf-sdk`, `iceberg`, `iceberg-catalog-rest`, `iceberg-storage-opendal`, `reqwest`, `serde`, `serde_json`, `url`, `aws-sigv4`, `aws-credential-types`, `aws-smithy-runtime-api`; dev-dependency `tokio` for the async tests |
| Forbidden direct dependencies | `arrow`, `parquet`, `datafusion`, `object_store`, `roaring`, `async-trait`, `tracing`, `exasol-udf-macros`, `lakehouse-engine` |
| Version bump | `lakehouse-engine` `0.30.12` → `0.30.13`, matching this repo's PATCH-per-refactor convention (#261, #264) |
| Iceberg compliance | Normative quotes for `StorageCredential.prefix`, the `LoadTableResult` storage-credentials rule, and the enumerated AWS config keys are recorded in the `catalog-crate-structure` and `pushdown-planning-cloud-credentials` Background sections. No deviation is introduced; the two deliberate readings are named and preserved (decision-log [11]) |
| CI | `cargo test --workspace` and `cargo clippy --workspace --all-targets` already cover a new member, so no `.github/workflows` edit is required. The `Makefile` DOES need one: `VS_SRCS` (line 24) hardcodes `crates/lakehouse-engine/src crates/vs-expression/src` as its `find` roots and lists only those two manifests, so without task 1.2 every catalog-only edit leaves `$(VS_SO)` considered up to date and both E2E suites run a stale `.so` |

## Dependencies

No new third-party dependency. Every crate the new manifest declares is already in `Cargo.lock` via `lakehouse-engine`. Issue #214 is a prerequisite in name only: this plan performs its work (decision-log [2]), so #214 closes as subsumed rather than blocking.

## Migration

| Current | New |
|---------|-----|
| `crate::adapter::pushdown::CatalogSession` (`pub(crate)`) | `lakehouse_catalog::CatalogSession` (`pub`) |
| `resolve_file_list(catalog_uri: &str, …)` + `resolve_file_list_with_session(&CatalogSession, …)` | one `resolve_file_list(&CatalogSession, …)` |
| `resolve_table_schema(catalog_uri: &str, …)` | `resolve_table_schema(&CatalogSession, …)` |
| `crate::adapter::pushdown::{extract_vended_keys, merge_vended_into_storage}` | crate-private; callers use `lakehouse_catalog::resolve_vended_storage` |
| `crate::adapter::pushdown::list_namespace_tables` | `lakehouse_catalog::list_namespace_tables` |
| `crate::adapter::sigv4::sign_request` | crate-private `lakehouse_catalog::sigv4::sign_request` |
| `crate::scan::spec::{StorageProps, CatalogProps}` (declared) | declared in `lakehouse-catalog`, re-exported at the same path |
| `crate::adapter::connection::ConnectionCreds` (declared) | declared in `lakehouse-catalog`, re-exported at the same path |
| `crate::scan::emit::{redact_credentials, redact_secret_values}` (declared) | declared in `lakehouse-catalog`, re-exported at the same path |
| `pushdown/support.rs::redact_catalog_error` (`pub(super)`) | `lakehouse_catalog::redact_catalog_error` (`pub`); engine declaration deleted |
| Probe baseline: `specs/_plans/refactor-adapter-pushdown-modules/public-surface-baseline.txt` | the two probe `use` lists themselves |

## Implementation Tasks

1. **Scaffold the crate**
   - [ ] 1.1 Create `crates/lakehouse-catalog` with its manifest and an empty `src/lib.rs`; add it to `[workspace] members`; add the path dependency to `crates/lakehouse-engine/Cargo.toml`. Confirm `cargo test --workspace` and `cargo clippy --workspace --all-targets -- -D warnings` are green before any code moves.
   - [ ] 1.2 Repair the `.so` staleness guard BEFORE any catalog code moves: add `crates/lakehouse-catalog/src` to the `find` roots in `Makefile`'s `VS_SRCS` (line 24) and add `crates/lakehouse-catalog/Cargo.toml` plus the root workspace `Cargo.toml` to its file list. Verify by touching a `crates/lakehouse-catalog/src/*.rs` file and confirming `make cross-musl-udf-build` rebuilds the `.so` rather than reporting it up to date. Without this, `make test-e2e` and `make test-e2e-lakekeeper` — this plan's named parity gates for `resolve_vended_storage` — run against a stale binary and pass vacuously.

2. **Move the shared types and redaction**
   - [ ] 2.1 Move `StorageProps` (with `Default`, `secret_values`, `default_true`) and `CatalogProps` out of `scan/spec.rs`, and `ConnectionCreds` (with its manual `Debug` impl and `has_catalog_auth`) out of `adapter/connection.rs`, into `lakehouse-catalog`. Re-export all three at their pre-move engine paths. Leave `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` in the engine. No consumer `use` path may change. [expert]
   - [ ] 2.2 Move `redact_credentials` and `redact_secret_values` out of `scan/emit.rs` and `redact_catalog_error` out of `pushdown/support.rs` into `lakehouse-catalog`; re-export the first two from `scan/emit.rs`; DELETE the `support.rs` declaration and repoint its four callers at the crate. Move the corresponding tests.

3. **Move the catalog access code**
   - [ ] 3.0 Add `crates/lakehouse-catalog/src/test_support.rs` (`#[cfg(test)]`) as the crate's SINGLE home for every test helper more than one moved test module reaches — 15 items, one declaration each, no per-module duplicate. Two come from the engine's `crates/lakehouse-engine/src/adapter/pushdown/test_support.rs`: `base_creds() -> ConnectionCreds` (`:23`) and `static_storage() -> StorageProps` (`:44`), reached today through `use super::super::test_support::*;` and called 17 times (`credentials.rs`: 9 `base_creds`, 6 `static_storage`; `namespace.rs:375-376`: one each). Thirteen more are declared inside `credentials.rs`'s own flat test module, where they are module-local today and become cross-module the moment task 3.2 splits that module four ways: the nine sentinel constants at `credentials.rs:1545-1553` (`STATIC_AK`, `STATIC_SK`, `VENDED_AK`, `VENDED_SK`, `VENDED_TOK`, `BEARER_TOK`, `CLIENT_SECRET`, `OAUTH_ACCESS_TOKEN`, `VENDED_REGION`), `AUTH_PROP_KEYS` (`:1356`), and the three builders `make_load_table_result` (`:911`), `creds_no_auth` (`:1556`), and `vended_result_flat_config` (`:1578`). The recorded `vs-adapter/pushdown-module-structure` rule — a test helper shared across submodules MUST live in one shared `#[cfg(test)]` support module — binds all 15 identically, so provisioning only the two named fixtures would leave an implementer inventing a home for the other 13. The engine's `test_support.rs` CANNOT move wholesale: it opens with `use super::*` and names `ProjectionItem`, `ScanSpec`, `CommonScanSpec`, `FileEntry`, `DeleteFileRef`, `build_scan_driving_sql`, `SCAN_UDF_NAME`, `relativize_shards_to_root`, and `crate::adapter::sharding::partition_files_by_bytes`. `base_creds` and `static_storage` MOVE rather than being copied: task 3.2 takes 15 of their 17 callers and task 3.3 the last 2, so task 3.3 DELETES both engine declarations, because a `pub(super)` fixture with no caller fails `cargo clippy --workspace --all-targets -- -D warnings` on `dead_code`. The other 12 engine helpers stay: `sample_storage`, `pd`, `pos_delete`, and the SQL-fixture builders still serve the pushdown modules that do not move. This task must land before 3.2, because the catalog crate does not compile until those 17 fixture calls resolve.
   - [ ] 3.1 Move `adapter/sigv4.rs` into `lakehouse-catalog::sigv4` with its four tests. `sign_request` stays crate-visible only. Its `mod tests` needs only `use super::*`, so it carries no fixture dependency.
   - [ ] 3.2 Move `adapter/pushdown/credentials.rs`'s catalog code into `lakehouse-catalog` as `auth`, `session`, `iceberg_io`, and `vended` modules, VERBATIM apart from `use` paths and visibility. `CatalogSession`, `CatalogSession::resolve`, `load_table_any_auth`, and `build_s3_file_io` become `pub`; everything else crate-private; `CatalogSession`'s fields stay private. Move the test module's 1,805 lines into the module that owns each subject, EXCEPT `catalog_auth_secrets_never_in_scan_spec_with_vending` (`credentials.rs:2094`): relocate that one test into an engine-side test module that can name `ScanSpec`, `CommonScanSpec`, and `FileEntry`, because it asserts a property of the engine's scan-spec serialization rather than of the catalog crate, and `credentials.rs:757`'s `use crate::scan::spec::{CommonScanSpec, FileEntry, ScanSpec};` moves with it rather than into the crate. Copy the four sentinel constants that test reads — `VENDED_AK`, `VENDED_SK`, `VENDED_TOK`, `VENDED_REGION` (`credentials.rs:1547-1553`) — into the engine-side test module alongside it, keeping the same literal values on both sides so the crate-side and engine-side assertions stay comparable; task 3.0 moves those four into the crate's support module, where they are test-module-private and unreachable from `lakehouse-engine`, and raising them to `pub` would put test sentinels on the crate's public surface. An audit of the remaining 1,805 lines found no other test naming an engine-only type: `LogicalField`, `NameMappingEntry`, `DeleteFileRef`, `ProjectionItem`, `build_scan_driving_sql`, `SCAN_UDF_NAME`, and `relativize_shards_to_root` appear nowhere in either moved test module. The parity gate for this task is the existing suite passing with no assertion edit. [expert]
   - [ ] 3.3 Move `adapter/pushdown/namespace.rs` into `lakehouse-catalog::namespace` with its tests; `list_namespace_tables` and `parse_table_ident` become `pub`; repoint `adapter/mod.rs`, `pushdown/mod.rs`, `pushdown/joins/*`, and `file_resolution.rs`. Drop the `pub use namespace::list_namespace_tables;` re-export. This task takes the last two callers of `base_creds` and `static_storage` (`namespace.rs:375-376`), so DELETE both declarations from `crates/lakehouse-engine/src/adapter/pushdown/test_support.rs` (`:23`, `:44`) here, leaving the other 12 helpers untouched. Confirm with `cargo clippy --workspace --all-targets -- -D warnings`: keeping either declaration fails on `dead_code`, and deleting a third fails on an unresolved call. [expert]

4. **Consolidate vended-storage resolution**
   - [ ] 4.1 Introduce `pub fn resolve_vended_storage(result, base, anchor) -> StorageProps` in `lakehouse-catalog::vended`: select the credential source ONCE (longest matching non-empty `storage_credentials.prefix` against the location, else the flat `config` map), read all six values from it into a uniform `Option` shape, and overlay each present value onto `base`. Demote `extract_vended_keys`, `merge_vended_into_storage`, `extract_vended_region`, `extract_vended_endpoint`, `extract_vended_path_style`, `vended_config_value`, and `extract_s3_keys_from_config` to crate-private. Replace the five-step sequence in `file_resolution.rs` with one call, keeping the `use_vended_credentials` gate at the call site. [expert]
   - [ ] 4.2 Add behavior-parity unit tests for `resolve_vended_storage` covering the six absence and precedence cases: empty vended access key, empty vended secret key, absent or empty session token, unparseable `s3.path-style-access`, a matched `storage_credentials` entry omitting a key (no fallback to the flat map), and `allow_http` always taken from the base.

5. **Retire the wrappers**
   - [ ] 5.1 Rename `resolve_file_list_with_session` to `resolve_file_list` taking `&CatalogSession`, delete the old public wrapper, and build the session on the single-table `handle_pushdown` path AFTER validating the involved-table identifier, so the parse-before-config guarantee moves to the caller rather than being dropped. [expert]
   - [ ] 5.2 Change `resolve_table_schema` to take `&CatalogSession`, and hoist one session build in `adapter/mod.rs` ahead of the createVirtualSchema table-enumeration loop, captured by the `build_virtual_tables` closure. Preserve the non-Iceberg-table skip behavior and its warning. [expert]
   - [ ] 5.3 Update the four external `resolve_file_list` call sites — `tests/common/e2e_harness.rs::resolve_fixture_files` and the three calls in `tests/e2e_scan_test.rs::e2e_range_filter_prunes_by_file_bounds` — to build a `lakehouse_catalog::CatalogSession` and pass it. Build it once per test and reuse it across the three pruning calls.

6. **Redraw the public surfaces**
   - [ ] 6.1 Update `src/adapter/pushdown_surface_probe.rs` (25 → 22 items) and `tests/pushdown_public_surface.rs` (15 → 12 items) by removing `extract_vended_keys`, `merge_vended_into_storage`, and `list_namespace_tables`; rewrite both doc comments to state that the probe's own `use` list is the baseline and to drop the archived `public-surface-baseline.txt` citation. [expert]
   - [ ] 6.2 Add `crates/lakehouse-catalog/tests/catalog_public_surface.rs`: an external-vantage `use` list naming every `pub` item of the new crate, plus an `include_str!`-based assertion that the crate's sources declare no `pub fn extract_vended_keys` and no `pub fn merge_vended_into_storage`.
   - [ ] 6.3 Add `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs`: an `include_str!` read of the crate's own `Cargo.toml` asserting that none of the forbidden direct dependencies appears (precedent: `tests/build_convention.rs`).
   - [ ] 6.4 Add `crates/lakehouse-engine/tests/shared_type_reexports.rs`: an external-vantage test proving each re-exported path resolves to the catalog crate's type, plus a golden-string assertion that `StorageProps`' serde encoding is unchanged.
   - [ ] 6.5 Add `crates/lakehouse-engine/tests/catalog_session_signatures.rs`: a compile-time probe that `resolve_file_list` and `resolve_table_schema` accept `&lakehouse_catalog::CatalogSession`.

7. **Documentation and the full gate**
   - [ ] 7.1 Update `specs/mission.md` (Project Structure tree and the sibling-crate note) and `CLAUDE.md` (Build section) to name the two library crates and restate that one `.so` still carries both entry points.
   - [ ] 7.2 Add `crates/lakehouse-engine/src/adapter/pushdown/mod.rs::tests::malformed_table_ident_fails_before_any_catalog_contact`: a malformed `catalog.table` against an unreachable `catalog_uri` must return the parse error, not a transport error.
   - [ ] 7.3 Run the full gate: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `make cross-musl-udf-build`, `make test-e2e`, `make test-e2e-lakekeeper`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, then 1.2 |
| Group B | 2.1, 3.1, 7.1 |
| Group C | 2.2, 3.0 |
| Group D | 3.2 |
| Group E | 3.3 |
| Group F | 4.1, then 4.2 |
| Group G | 5.1 → 5.2 → 5.3 (sequential), concurrently with 6.1, 6.2, 6.3, 6.4, 7.2 |
| Group H | 6.5, 7.3 |

Sequential dependencies:

- Group A → everything (the crate must exist). 1.2 follows 1.1 because it edits the `Makefile` only once the crate directory exists, and it must precede any catalog-only edit — otherwise the `.so` staleness guard skips the rebuild and both E2E gates go vacuous.
- Group B → Group C → Group D → Group E. `credentials.rs` needs the types (2.1), the redaction primitives (2.2), the two shared test fixtures (3.0), and `sign_request` (3.1) already in the crate before it can move; `namespace.rs` needs `build_rest_catalog` and `glue_catalog_prefix`, which 3.2 moves.
- Group D and Group E both depend on 3.0: without `crates/lakehouse-catalog/src/test_support.rs` the moved test modules leave 15 (`credentials.rs`) and 2 (`namespace.rs`) unresolved fixture calls, so the crate does not compile and 3.2's "existing suite passes unedited" gate is unreachable.
- 3.0 depends on 2.1 (it names `ConnectionCreds` and `StorageProps`) but not on 2.2, so it runs alongside 2.2 in Group C. Both Group C tasks add a `mod` line to `crates/lakehouse-catalog/src/lib.rs`, the same single conflict point already noted for 2.1 and 3.1 in Group B.
- The engine-side deletion of `base_creds` and `static_storage` happens in 3.3, after the last caller leaves. 3.2 takes 15 of the 17 calls and 3.3 the remaining 2, so deleting either declaration before 3.3 breaks the build and deleting neither leaves two `dead_code` findings at task 7.3's gate.
- 2.1 and 3.1 are independent: `sign_request` takes only `&str` arguments and names none of the three types. Both add a `mod` line to `lib.rs`, which is the only conflict point.
- Group E → Group F. `resolve_vended_storage` consolidates code that 3.2 moved and that 3.3's façade edit exposes.
- Group F → Group G. 5.1 rewrites the call site 4.1 collapsed; 6.1 removes façade items that 3.3 and 4.1 relocated and demoted.
- 5.3 depends on 5.1 (it calls the new signature). 5.1 and 5.2 both edit `file_resolution.rs`, so they stay sequential.
- 6.5 depends on 5.1 and 5.2 (it probes their new signatures).
- 7.3 is last.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs::resolve_file_list_with_session` | Merged into `resolve_file_list`, which now takes the session; keeping both would leave an alias with no caller |
| Function | `crates/lakehouse-engine/src/adapter/pushdown/support.rs::redact_catalog_error` | Whole body is a call to `redact_credentials` with the same argument — a pass-through. Moves to `lakehouse-catalog`, engine declaration deleted |
| Re-export | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs:32` | `pub use credentials::{extract_vended_keys, merge_vended_into_storage};` — both demoted to crate-private |
| Re-export | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs:35` | `pub use namespace::list_namespace_tables;` — relocated to `lakehouse_catalog` |
| Re-export | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs:31` | `use credentials::CatalogSession;` — the module is gone; the type is imported from the crate |
| Module | `crates/lakehouse-engine/src/adapter/sigv4.rs` | Moves wholesale to `lakehouse-catalog::sigv4`; the engine has no other caller |
| Module | `crates/lakehouse-engine/src/adapter/pushdown/credentials.rs` | Dissolved into the crate's `auth`, `session`, `iceberg_io`, and `vended` modules |
| Module | `crates/lakehouse-engine/src/adapter/pushdown/namespace.rs` | Moves wholesale to `lakehouse-catalog::namespace` |
| Test fixtures | `crates/lakehouse-engine/src/adapter/pushdown/test_support.rs::{base_creds, static_storage}` | Both fixtures' only callers move to `lakehouse-catalog` in tasks 3.2 and 3.3, and a `pub(super)` fixture with no caller fails `cargo clippy --workspace --all-targets -- -D warnings` on `dead_code`. Task 3.0 gives them one new home in the crate's support module; the other 12 helpers keep their engine callers and stay |
| Call sequence | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:272-294` | The five-step extract-and-merge block, replaced by one `resolve_vended_storage` call |
| Doc reference | `src/adapter/pushdown_surface_probe.rs` and `tests/pushdown_public_surface.rs` doc comments | Both cite `specs/_plans/refactor-adapter-pushdown-modules/public-surface-baseline.txt`, archived into gitignored `specs/_recorded/` and unreadable |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The catalog access layer lives in a standalone crate the engine depends on one way | Unit | `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs` | `catalog_manifest_declares_no_execution_engine_dependency` |
| One crate declares each shared credential type, re-exported at its pre-move engine path | Integration | `crates/lakehouse-engine/tests/shared_type_reexports.rs` | `reexported_paths_resolve_to_the_catalog_crate_types`, `storage_props_wire_encoding_unchanged` |
| The crate exposes the concept-level API and hides every mechanism step | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | compile-time `use` list, plus `vended_mechanism_functions_are_not_declared_public` |
| Every moved module keeps its own tests | Unit | `crates/lakehouse-catalog/src/{sigv4,session,auth,iceberg_io,vended,namespace,redaction}.rs` | the moved modules, e.g. `sigv4::tests::signed_request_carries_sigv4_header`, `session::tests::build_load_table_url_inserts_prefix_verbatim_without_encoding`, `namespace::tests::parse_table_ident_handles_multilevel_namespace` |
| Behavior is unchanged across the extraction | Integration | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | `grouped_aggregate_matches_golden`, `group_by_fallback_matches_golden`, `lone_count_distinct_matches_golden`, `multi_count_distinct_decline_matches_golden`, `single_group_row_scan_matches_golden`, `empty_grouped_matches_golden` |
| Behavior is unchanged across the extraction | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_partition_filter_prunes_and_returns_correct_rows`, `e2e_range_filter_prunes_by_file_bounds` |
| CatalogSession is public and every file-resolution entry point takes one | Integration | `crates/lakehouse-engine/tests/catalog_session_signatures.rs` | `file_resolution_entry_points_take_a_shared_session` |
| CatalogSession is public and every file-resolution entry point takes one | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `malformed_table_ident_fails_before_any_catalog_contact` |
| CatalogSession is public and every file-resolution entry point takes one | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_range_filter_prunes_by_file_bounds` (harness-built session resolves the same file lists) |
| createVirtualSchema resolves every table's schema on one shared session | Integration | `crates/lakehouse-engine/tests/catalog_session_signatures.rs` | `schema_resolution_entry_point_takes_a_shared_session` |
| createVirtualSchema resolves every table's schema on one shared session | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_create_virtual_schema_lists_tables_over_oidc` |
| One concept-level call resolves the effective scan storage from a loadTable response | Unit | `crates/lakehouse-catalog/src/vended.rs` | `resolve_vended_storage_{empty_access_key_preserves_static, empty_secret_key_preserves_static, absent_session_token_preserves_static, unparseable_path_style_preserves_static, matched_entry_missing_key_does_not_fall_back_to_config, allow_http_always_from_base, selects_credential_source_once_for_all_six_values}`, plus the source/precedence and endpoint/region/path-style set `vended_storage_{prefers_storage_credentials_over_flat_config, longest_matching_prefix_wins, falls_back_to_flat_config, uses_flat_config_when_no_storage_credentials, anchor_is_the_s3_table_location, adopts_endpoint_and_path_style_from_flat_config, adopts_endpoint_from_storage_credentials, keeps_static_endpoint_and_path_style_when_absent, adopts_region_from_flat_config, session_token_overrides_static}` |
| One concept-level call resolves the effective scan storage from a loadTable response | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_creds_projection_filter` |
| The pushdown façade releases exactly the three items the catalog extraction relocates | Integration | `crates/lakehouse-engine/tests/pushdown_public_surface.rs` and `crates/lakehouse-engine/src/adapter/pushdown_surface_probe.rs` | compile-time `use` lists (12 external items, 22 in-crate items) |

Coverage notes:

- The one-way dependency edge, and the absence of `FileEntry` / `LogicalField` / `NameMappingEntry` / `DeleteFileRef` / `ScanSpec` / `CommonScanSpec` from the catalog crate, are compiler-enforced: the reverse edge is a cycle, so no test can express the negative. `catalog_crate_boundary.rs` covers the manifest half, which is the part the compiler does not check.
- "No consumer `use` path changes" is proven by 4 `scan/*.rs` runtime modules, 10 `adapter/**` modules, and 13 files under `tests/` compiling unedited, not by a dedicated test.
- The one-grant-per-schema-loop claim is enforced STRUCTURALLY, not counted: `resolve_table_schema` takes a session by shared reference and holds no means to build one, so a per-table grant is inexpressible. This matches the precedent already set by `vs-adapter/pushdown-catalog-session`'s "The per-table loader cannot re-derive auth or prefix" scenario. Counting grants would need an HTTP mock server, which this workspace does not carry. The claim is scoped to the SCHEMA LOOP: `list_namespace_tables`' own `RestCatalog` grant is unchanged and out of scope, so a createVirtualSchema request still performs two grants in total.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `vs-adapter/catalog-crate-structure` | `cargo tree -p lakehouse-catalog --depth 1` | Lists `iceberg`, `iceberg-catalog-rest`, `iceberg-storage-opendal`, `reqwest`, `serde`, `serde_json`, `url`, `exasol-udf-sdk`, and the three `aws-*` crates; NO `arrow`, `parquet`, `datafusion`, `object_store`, `roaring`, or `lakehouse-engine` |
| `vs-adapter/catalog-crate-structure` | `cargo test -p lakehouse-catalog` | 0 failures, and a passing-test count at least equal to what `credentials.rs`, `sigv4.rs`, `namespace.rs`, and the moved redaction tests contributed before the move |
| `vs-adapter/pushdown-catalog-session` | `make test-e2e` | `e2e_range_filter_prunes_by_file_bounds` resolves 3, 1, and 2 files through a harness-built `CatalogSession`; every E2E suite passes |
| `vs-adapter/pushdown-planning-cloud-credentials` | `make test-e2e-lakekeeper` | `lakekeeper_vended_creds_projection_filter` passes: the vended STS keys, `s3.endpoint`, and `s3.path-style-access` still reach MinIO through `resolve_vended_storage` |
| `vs-adapter/pushdown-module-structure` | `cargo test -p lakehouse-engine --test pushdown_public_surface` | Compiles and passes with 12 named items; adding `extract_vended_keys` back to the `use` list fails to compile |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0; one `.so` emitted from `-p lakehouse-engine` |
| Test | `cargo test --workspace` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Test (Lakekeeper E2E) | `make test-e2e-lakekeeper` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors/warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Spec | `speq plan validate refactor-catalog-crate-extraction` | pass |
