# Verification Report: add-native-unity-catalog-client

Feature: native Unity Catalog client behind a shared `CatalogClient` trait, with a
`CATALOG_KIND` seam (default Iceberg REST), one createVirtualSchema listing pipeline for
both kinds, Unity PAT/OAuth-M2M auth + vended credentials, and a local-fixture E2E.

## Result: PASS

All implementation tasks (Phase 2), review fixes (Phase 4), and verification gates
(Phase 5) are complete and green. Every plan Verification-table scenario is backed by an
existing, passing test.

## Phase 5 gate results

| Gate | Command | Result |
|------|---------|--------|
| 5.1 Build | `make cross-musl-udf-build` | **exit 0** — `target/release/liblakehouse_engine.so` (167.4M) built in `rust:1.94-bookworm` (glibc 2.36, SLC-matching) |
| 5.2 Test  | `cargo test --workspace --lib` | **1056 passed, 0 failed** (132 lakehouse-catalog + 799 lakehouse-engine + 125 vs-expression) |
| 5.2 Test  | plan-pinned surface probes | catalog_public_surface 10/10, catalog_crate_boundary 1/1, catalog_session_signatures/pushdown_public_surface/shared_type_reexports/two_entry_points/build_convention/scan_facade_reachability all pass |
| 5.3 Lint  | `cargo clippy --workspace --all-targets -- -D warnings` | **0 warnings** (default features) |
| 5.3 Lint  | `cargo clippy -p lakehouse-engine --all-targets --features unity-e2e -- -D warnings` | **0 warnings** |
| 5.4 Format| `cargo fmt --check` | **clean** |
| 5.5 Audit | scenario coverage vs plan Verification table | **49/49 scenarios backed** (see below) |

## Scenario coverage audit (5.5)

Every one of the 49 rows in the plan's Verification table names a test that exists in the
tree and passes. Two gaps found during the audit and closed:

- The plan named five engine-level integration tests in
  `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs`, a file no implementation
  task had created. Implemented all five against an in-process mock Unity Catalog HTTP
  server driving the real `dispatch` → createVirtualSchema entry point (not the Docker
  stack, so they run in plain `cargo test`): `enumerates_unity_namespace_tables`,
  `listing_issues_no_per_table_get_table_call` (asserts zero `GET /tables/{full_name}`
  regardless of table count — the N+1-avoidance guarantee),
  `lists_view_with_columns_and_no_storage_location`,
  `records_table_map_and_rejects_collision` (TABLE_MAP + collision error), and
  `unreachable_unity_catalog_is_credential_safe_error` (refused-connection error, sentinel
  bearer token proven absent from the message).

E2E rows (`e2e_unity_test.rs`) are `unity-e2e`-feature-gated and fail-not-skip when the
stack is down; they were verified live during implementation against a local OSS Unity
Catalog + Delta fixture stack (`make unity-up`): 10 passed, 0 failed, and the OSS
inline-`columns[]` precondition was confirmed by direct `GET /tables` query. They run again
in the integration `make test-e2e` / `make test-e2e-unity` gate at record time.

## Code review (Phase 4)

Two adversarial reviewers (catalog crate, engine crate) returned 8 findings — all
confirmed, none invented. No correctness bug in the engine crate; all high-risk invariants
verified intact:

- **Single construction site** — `construct_catalog_client` is the sole `CatalogKind`→client
  match; `build_listing_virtual_tables` names neither the kind nor a client, so the listing
  pipeline is structurally incapable of branching on kind.
- **Byte-identical Iceberg listing** — same enumeration, same `iceberg_type_to_exasol`
  mapping, same full-Unicode `to_uppercase` fold (`straße`→`STRASSE`), same `TABLE_MAP`,
  same skipped-table warning text and error strings.
- **Pushdown refusal** — Unity kind is refused before any credential/catalog resolution and
  never routed to Iceberg file resolution.
- **Credential non-leak** — vended AWS/Azure/GCP credentials, the OAuth client secret, PAT,
  and bearer tokens are scrubbed from every error/SQL/log line and redacted in every
  credential struct's `Debug`.

Findings fixed:

| ID | Category | Fix |
|----|----------|-----|
| C1 | dead-flexibility | Deleted unspecified `list_catalog_names`/`list_schema_names` + orphaned Catalog/Schema wire types (were `pub` only to dodge the dead-code lint; the spec's client surface is the `CatalogClient` trait) |
| C2 | contextless-error | Percent-encode each `full_name` path segment in `get_table_info` (latent malformed-URL on reserved chars) |
| C3 | implementation-coupled-test | Extended the mock to serve the namespace-list endpoint; the three Iceberg guarantee tests now drive the public `list_tables` end-to-end |
| C4 | correctness (real bug) | A Unity token endpoint omitting/zeroing `expires_in` defeated the OAuth cache (grant per request) — now a clear grant error, matching the adjacent empty-token guard |
| E1 | outdated-comment | Re-anchored 5 stale `resolve_table_schema` doc refs → `build_listing_virtual_tables` (preserving the join-path ASCII-vs-Unicode fold-agreement premise verbatim) |
| E2 | outdated-comment | Rewrote the construction-site probe comment to state only what it enforces |
| E3 | implementation-in-name | Renamed `iceberg_identifier_string` → `catalog_identifier_string` (now catalog-neutral) |
| E4 | duplicate-operation | `resolve_connection_config` returns the resolved `CatalogKind`; parsed once per create request |

## Spec-accuracy reconciliation (empirical)

The C3 fix surfaced that the draft spec's "empty namespace performs no OAuth2 grant / a
virtual schema over an empty namespace with failing credentials keeps succeeding" claim was
imprecise. Verified empirically (mock grant-counting) and by git archaeology
(`list_namespace_tables` is unchanged vs `main`; the old empty-namespace test asserted no
`CatalogSession`, driven with a pre-enumerated empty ident batch):

- **The code is correct and byte-identical to `main`** on grant behavior. Empty namespace in
  OAuth mode = 1 grant (the enumeration `RestCatalog`'s own grant, which runs to discover the
  namespace is empty); N-table OAuth = 2 grants (enumeration + one shared load session);
  no-auth/static-token = 0 grants + one `/v1/config` lookup — all identical before and after.
- The "keeps succeeding with failing credentials" claim was **false for OAuth mode**
  (and already false pre-refactor — not a regression), true only for no-auth/static-token.

The draft spec deltas, plan prose, `tasks.md`, and `decision-log.md` decision [10]
(marked "Promotes to ADR: yes") were reconciled to distinguish the enumeration-phase grant
from the resolution-phase `CatalogSession` grant, so nothing false is recorded. No code or
test changed.

## Constraint compliance

- No `async-trait`, no new external dependency in `lakehouse-catalog` (trait is
  dyn-compatible via `Pin<Box<dyn Future + Send>>`); `catalog_crate_boundary` passes.
- Unity wire types stay crate/module-private.
- Arrow/Spark→Exasol mapping follows the project convention; incompatible/out-of-range types
  → `VARCHAR(2000000)`; one exhaustive type-mapping home for both kinds.
- Unit tests live in sibling `*_tests.rs` files per the project layout rule.
- Iceberg REST scan/pushdown path untouched.
