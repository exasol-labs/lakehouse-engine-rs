# Plan: add-lakekeeper-permission-check

## Summary

An opt-in `PERMISSION_CHECK = 'LAKEKEEPER'` virtual-schema property makes the adapter map the querying Exasol user through `USER_MAPPING` to a Lakekeeper principal and call `batch-check` once per query. The call goes out on the request's existing `CatalogSession`, after the session resolves and before any `loadTable`, and the adapter refuses the query on any denial or failure. Without the property the engine behaves as today and contacts no management API (#415).

## Design

### Context

#415 is increment 2 of 3 of the *Per-user permission enforcement via Lakekeeper* milestone. It depends on #414 and feeds #416. Today every query reads as the CONNECTION's service account, so every Exasol user sees every table that account can read.

Facts this design rests on:

- **The pushdown has one resolution seam.** `TableScanResolver::for_request` (`crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs`) is called by `handle_pushdown` for every single-table shape and by `plan_join` for every join. All aggregate, top-N, fallback-wrapper, and empty-result paths consume the shards of that one resolver (`build_dispatch_sql`, `pushdown/mod.rs`). No pushdown path loads table metadata around it.
- **The session already holds what the check needs.** `CatalogSession` (`crates/lakehouse-catalog/src/session.rs`) holds the pooled `reqwest::Client`, the resolved `CatalogAuth` (the OAuth2 grant runs once in `resolve`), the catalog URI, and the `/v1/config` prefix. `load_table_any_auth` is the precedent for a self-issued call on the session.
- **The prefix is the warehouse id on Lakekeeper.** Live check, 2026-10-02, Lakekeeper `v0.13.1`: `GET /catalog/v1/config?warehouse=lakehouse_authz` answered `defaults.prefix = 5c25f9c4-be40-11f1-a87d-17102559a460`, the id that `/management/v1/warehouse` lists for `lakehouse_authz`.
- **The 403 path is reachable live.** On the same stack, `lakehouse-reader-b` reads `/v1/config` (200) but holds no grant management, so its batch-check answers 403 `CannotInspectPermissions` (`cannot-inspect.json`). The operator's batch-check for the never-registered id `oidc~nobody_ever@corp` answered `allowed: false` for an existing table and for a missing table.
- **Pushdown requests carry the persisted properties.** `dispatch` already reads `CATALOG_CONNECTION` and `CATALOG_KIND` of a pushdown from `schemaMetadataInfo.properties` (`get_properties`), so the three new properties reach the pushdown the same way.
- **The current user reaches the adapter.** `exasol-udf-sdk` 0.30.0 declares `UdfContext::current_user() -> Option<String>` and `scope_user()`. The SDK's `TestContext` sets both (`with_current_user`, `with_scope_user`). `dispatch` receives `ctx: &mut dyn UdfContext`. #415 states that `language-container-rs` reports the user on the adapter path. If it does not, every checked query is refused, and the e2e test of this plan fails.
- **Only privileged users set VS properties.** `ALTER VIRTUAL SCHEMA ... SET` requires `ALTER ANY VIRTUAL SCHEMA`, `ALTER` on the schema, or ownership ([Exasol ALTER SCHEMA](https://docs.exasol.com/db/latest/sql/alter_schema.htm)).
- **The query cache skips virtual schemas.** Exasol documents that the query cache is not used for a statement that contains a virtual schema ([Exasol query cache](https://docs.exasol.com/db/7.1/database_concepts/query_cache.htm)). The e2e test checks this live by running one statement as an allowed user and then as a denied user.
- **Iceberg and Delta compliance.** `batch-check` is a Lakekeeper management API, not part of the Iceberg REST catalog spec. The Iceberg REST spec defines the `/v1/config` `prefix` only as "An optional prefix in the path" (`rest-catalog-open-api.yaml`, `components.parameters.prefix`). Reading it as a warehouse id is Lakekeeper-specific, and `vs-adapter/lakekeeper-permission-client` states that reading. The check changes no scan, file planning, schema, or type handling, so no section of the Iceberg table spec or the Delta protocol applies, and no deviation exists.
- **The library scenario threshold is more than 10 per spec.** `vs-adapter/catalog-crate-public-surface-extensions` holds 10 scenarios, so the client's surface addition goes into the new `vs-adapter/lakekeeper-permission-client` spec.

- **Goals**: the three properties, a total and injective user mapping, one batch-check per query on the request's session, check before load, fail closed on every failure and on every unsupported catalog kind, and live proof on the `lakekeeper-e2e` stack.
- **Non-Goals**: OPA or Cedar modes. Permission checks for the Unity Catalog or direct-storage kinds. Checks on createVirtualSchema, refresh, or setProperties (#416 asserts that limitation). Per-shape end-to-end tests, the bypass audit, and the trust-model documentation (#416). Row- or column-level authorization. Per-user storage credentials.

### Decision

#### Architecture

```
dispatch (adapter/mod.rs)
 ├─ createVirtualSchema / refresh / setProperties
 │    PermissionSettings::parse(props)              invalid value or template → error, CONNECTION unread
 │    kind.supports_lakekeeper_permission_check()   Unity / direct storage   → error, CONNECTION unread
 │    resolve_connection_config → management base   underivable or foreign origin → error
 │    list the namespace (unchanged, no batch-check)
 └─ pushdown
      PermissionSettings::parse(props), kind support
      principal = mapping(ctx.current_user())        unmappable user → refusal, no catalog request
      resolve_connection_config → management base → conn.permission_gate
      handle_pushdown(conn, ..) ──┬─ single-table shapes ─┐   signatures unchanged
                                  └─ plan_join(conn, ..) ─┤
                                                          ▼
      TableScanResolver::for_request(kind, uri, connection, table_identifiers, props,
                                     conn.permission_gate.as_ref())
        Iceberg:  CatalogSession::resolve            (one grant, one /v1/config)
                  gate.authorize(&session, ids) ──▶ lakehouse_catalog::batch_check
                                                     POST <management>/v1/action/batch-check
                  any denial or error              → refusal, no loadTable
        Unity / direct storage with a gate         → refusal, no request
      resolver.resolve(id)   id outside the checked set → refusal
        └─ loadTable, file planning, shape routing (unchanged)
```

`crates/lakehouse-catalog/src/lakekeeper.rs` owns everything Lakekeeper-specific: the wire types, the management-base derivation and origin rule, the warehouse-id reading of the session prefix, check-id mapping, and error mapping. `crates/lakehouse-engine/src/adapter/permission.rs` owns everything Exasol-specific: the three properties, the template grammar and charset, the principal, and the refusal messages. `CatalogSession` gains `pub(crate)` accessors only.

#### Key Interfaces

```rust
// lakehouse-catalog (pub, re-exported at the crate root)
pub struct LakekeeperManagementBase { /* private */ }
impl LakekeeperManagementBase {
    pub fn resolve(catalog_uri: &str, configured: Option<&str>) -> Result<Self, UdfError>;
}
pub struct TableReadDecision { pub table: String, pub allowed: bool }
pub async fn batch_check(
    session: &CatalogSession,
    management: &LakekeeperManagementBase,
    principal: &str,
    tables: &[&str],
    creds: &ConnectionCreds,
) -> Result<Vec<TableReadDecision>, UdfError>;

// lakehouse-engine adapter (crate-private)
pub(crate) enum PermissionSettings { Off, Lakekeeper { /* mapping, management override */ } }
pub(crate) struct PermissionGate { /* Exasol user, principal, management base */ }
impl PermissionGate {
    pub(crate) async fn authorize(
        &self, session: &CatalogSession, tables: &[&str], creds: &ConnectionCreds,
    ) -> Result<(), UdfError>;
}
pub struct ResolvedConnectionConfig {
    // existing pub(crate) fields unchanged
    pub(crate) permission_gate: Option<PermissionGate>,
}
impl<'a> TableScanResolver<'a> {
    pub(super) async fn for_request(
        kind: CatalogKind, catalog_uri: &str, connection: ConnectionStorage<'a>,
        table_identifiers: &[&str], props: &Json, gate: Option<&PermissionGate>,
    ) -> Result<Self, UdfError>;
}
```

The gate travels on `ResolvedConnectionConfig`, which `handle_pushdown` and `plan_join` already receive as `conn`. Their signatures stay unchanged (decision [1]).

#### Quick Diagnostic (new modules)

| Question | `lakehouse_catalog::lakekeeper` | `adapter/permission.rs` |
|----------|----------------------------------|--------------------------|
| One-sentence responsibility | Asks Lakekeeper on a query's session whether one principal is allowed to read each table of a set | Turns the permission properties and the querying user into a gate that authorizes a request's tables or refuses the query |
| Easier to call than to rebuild | Yes: one call hides the wire shape, deduplication, warehouse id, id mapping, origin check, and redaction | Yes: one gate hides the grammar, the charset, the base resolution, and the messages |
| Internal change forces an edit outside | No: the adapter sees only `TableReadDecision` and errors | No: the resolver sees only `authorize` |
| Doc comment states the reason | `batch_check` states why it is a free function (Lakekeeper-only, off the shared session) | `PermissionGate` states why the mapping is injective |
| One owner per decision | Wire and topology here only | Mapping and properties here only. The kind rule stays in `catalog_kind.rs` |
| Boundary visible without internals | The crate root re-exports three items | Two crate-private types |
| Tactical shortcut with follow-up | None. Unsupported kinds are a declared Non-Goal, not a shortcut | None |
| Business logic depends inward | The module names no Exasol concept (`vs-adapter/catalog-crate-public-surface-extensions`) | The current user arrives as an argument from `dispatch` |

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single enforcement chokepoint | `TableScanResolver::for_request` and `resolve` | Every pushdown path loads tables through it, so check-before-load is structural (decision [1]) |
| Free function on a shared session | `lakehouse_catalog::batch_check(&CatalogSession, ...)` | Same shape as `load_table_any_auth`. Keeps a Lakekeeper-only concern off the session that Glue and plain REST share (decision [2]) |
| Make illegal states unrepresentable | batch-check request type | `identity` is not optional, so an identity-less check cannot be built (#415 totality) |
| Fixture-pinned wire contract | `lakekeeper_tests.rs` over the #414 fixtures | The fixtures record live Lakekeeper `v0.13.1` exchanges, and #414's drift test keeps them current |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Check inside `for_request`, and `resolve` refuses unchecked ids (decision [1]) | Per-shape checks. Refusing every shape but the row scan | Every shape already resolves through the seam |
| Client in `lakehouse-catalog`, mapping in the adapter (decision [2]) | A session method. The whole client in the adapter | One owner per decision, and the session's fields stay private |
| Warehouse id from the session prefix, UUID only (decision [3]) | A management lookup call. A warehouse-id property | Live-verified, and no extra call |
| `/catalog` → `/management`, override on the same origin (decision [4]) | An override to any host | The request carries the session's bearer token |
| One placeholder, a per-placeholder charset (decision [5]) | One charset. Several placeholders | Injectivity: no two users share a principal |
| Unsupported kinds rejected at every request type (decision [6]) | Refusal at pushdown only | The real cause is named at create time |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| lakekeeper-permission-check | NEW | `specs/_plans/add-lakekeeper-permission-check/vs-adapter/lakekeeper-permission-check/spec.md` |
| lakekeeper-permission-client | NEW | `specs/_plans/add-lakekeeper-permission-check/vs-adapter/lakekeeper-permission-client/spec.md` |
| pushdown-catalog-session | CHANGED | `specs/_plans/add-lakekeeper-permission-check/vs-adapter/pushdown-catalog-session/spec.md` |
| pushdown-format-neutral-resolution | CHANGED | `specs/_plans/add-lakekeeper-permission-check/vs-adapter/pushdown-format-neutral-resolution/spec.md` |

## Impact

- With `PERMISSION_CHECK` absent, nothing changes: the same SQL, the same catalog requests, and no management API contact.
- To enable the check, an operator sets `PERMISSION_CHECK = 'LAKEKEEPER'` and `USER_MAPPING`, plus `LAKEKEEPER_MANAGEMENT_URL` behind a path-rewriting gateway. The CONNECTION's identity needs a grant that includes `can_read_assignments` on the checked tables, for example `manage_grants` on the warehouse or the namespace. Lakekeeper grants have to name the template-derived ids, or `LAKEKEEPER__OPENID_SUBJECT_CLAIM` has to name a claim that the template reproduces.
- With the check on, each query costs one more HTTP round trip to the catalog host, the batch-check. The request has no timeout of its own, the same as the existing `loadTable` GET.
- With the check on, a query by a user without a grant fails with an error that names the user, the principal, and the table. A user whose name falls outside the template's charset is refused. A catalog or management API outage refuses every query of that virtual schema.
- Breaking for one edge case only: a virtual schema that already carries an unrecognized `PERMISSION_CHECK` value, which the adapter ignored before, now fails refresh, setProperties, and every query until the value is fixed or unset.
- The listing still shows every table to every user with `SELECT` on the schema (#416 asserts this).

## Dependencies

- #414 fixtures and stack: `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/`, OpenFGA in `docker-compose.lakekeeper.yml`, and the `lakehouse-reader-b` client.
- No new crate: `reqwest`, `serde`, `serde_json`, and `url` are already dependencies of `lakehouse-catalog`.
- The implementing commit references `Closes #415`. #416 builds on decision [1].

## Implementation Tasks

### 1. Lakekeeper permission client (`lakehouse-catalog`)

- [ ] 1.1 In `crates/lakehouse-catalog/src/session.rs`, add `pub(crate)` read accessors for the client, the auth, the catalog URI, and the prefix, and keep every field private. Update the module doc comment so that it names `lakekeeper.rs` as the second reader. In `crates/lakehouse-catalog/src/iceberg_io.rs`, add a `pub(crate)` authed JSON POST that shares `authed_get_json`'s bearer, SigV4, and redaction handling, and returns the status and the redacted body of a non-2xx answer so that the caller can read `error.type`.
- [ ] 1.2 Add `crates/lakehouse-catalog/src/lakekeeper.rs` with crate-private serde wire types that follow the fixtures (`identity` is not optional), `TableReadDecision`, `LakekeeperManagementBase::resolve` (derivation and the same-origin override through `url::Url`), and `batch_check`: deduplicate in first-seen order, require a UUID prefix, split each identifier with `parse_table_ident`, re-check the base origin against the session's catalog URI, POST with `error-on-not-found: false`, map results by id, and turn 403 `CannotInspectPermissions` into the privilege error and every other failure into an error. Declare the module in `lib.rs` and re-export the three items.
- [ ] 1.3 Add `crates/lakehouse-catalog/src/lakekeeper_tests.rs`. Cover request equality with each fixture's `request` after placeholder substitution (`include_str!` the four fixtures), the decision of each fixture's response, malformed answers, every failure kind from the client spec with credential sentinels echoed in the answer body (extend `test_support_tests.rs`'s recording server to answer a POST and capture its body), deduplication through a recording server, and the management-base cases.
- [ ] 1.4 In `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, name the three items, add `lakekeeper.rs` to `CATALOG_SOURCES`, and assert that the wire types are not `pub` and that `lakekeeper.rs` names no Exasol user or virtual-schema property.

### 2. Permission check in the adapter

- [ ] 2.1 Add `crates/lakehouse-engine/src/adapter/permission.rs` with the sibling `permission_tests.rs`. Define the constants `PERMISSION_CHECK`, `USER_MAPPING`, and `LAKEKEEPER_MANAGEMENT_URL`. Add `PermissionSettings::parse`, which reads through `super::nonempty_str` (`vs-adapter/adapter-module-structure` keeps that the one accessor). Add the `USER_MAPPING` grammar and charset per decision [5], principal derivation, management-base resolution that adds the property name to the client's error, and `PermissionGate::authorize`, which calls `batch_check` and composes the denial and failure refusals. Unit-test the grammar rules, the injectivity of each placeholder over its accepted names, and the messages.
- [ ] 2.2 In `crates/lakehouse-engine/src/adapter/catalog_kind.rs`, add `CatalogKind::supports_lakekeeper_permission_check` as an exhaustive match. In `adapter/mod.rs`, have `handle_create_virtual_schema` (which serves createVirtualSchema, refresh, and setProperties) parse the settings and check kind support before `resolve_connection_config`, then resolve the management base after the CONNECTION is read. It sends no batch-check.
- [ ] 2.3 In the pushdown arm of `dispatch`, parse the settings and check kind support before `resolve_connection_config`. Read `ctx.current_user()` only when the check is on. Derive the principal from it. After the CONNECTION is read, resolve the management base and build the `PermissionGate`. Add the field `pub(crate) permission_gate: Option<PermissionGate>` to `ResolvedConnectionConfig`. Set it to `None` in `resolve_connection_config`. Store the gate in it in the pushdown arm. Keep the signatures of `handle_pushdown_request`, `handle_pushdown`, and `plan_join` unchanged. Add `permission_gate: None` to the six test struct literals of `ResolvedConnectionConfig` in `pushdown_tests.rs`, `test_support_tests.rs`, and `joins/joins_tests.rs`.
- [ ] 2.4 In `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs`, add the parameter `gate: Option<&PermissionGate>` to `for_request`. Pass `conn.permission_gate.as_ref()` from `handle_pushdown` and from `plan_join`. Pass `None` from the 11 existing `for_request` calls in `scan_resolution_tests.rs`. The Iceberg arm calls `gate.authorize(&session, table_identifiers, creds)` right after `CatalogSession::resolve`. The Unity and direct-storage arms refuse a gated request before they build anything. While gated, the resolver records the checked identifiers, and `resolve` refuses any other identifier. [expert]
- [ ] 2.5 In `crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs`, extend `RecordingCatalog` to record the method, the `Authorization` header, and the body. It reads each request through its `Content-Length`. Add a Lakekeeper responder that serves the token endpoint, `/v1/config` with a UUID prefix, a configurable batch-check answer, and a snapshotless `loadTable`.
- [ ] 2.6 Add the dispatch-level tests that the Scenario Coverage table names in `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, with a `TestContext` that sets `with_current_user` and `with_scope_user`: the off state, invalid properties, the management base, the principal, the unmappable user, the unsupported kinds, and the listing requests.
- [ ] 2.7 Add the resolver tests in `scan_resolution_tests.rs` and the seam tests in `pushdown_tests.rs` that the Scenario Coverage table names: every shape through the existing shape fixtures with byte-identical SQL, the denied and unverifiable answers, and the OAuth2 grant count with the check on. Keep every existing assertion.
- [ ] 2.8 In `specs/mission.md` § External Dependencies, add a row for the Lakekeeper management API. Purpose: the permission check when a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`. Failure impact: every query of such a virtual schema is refused.

### 3. Live end-to-end proof (`lakekeeper-e2e`)

- [ ] 3.1 In `crates/lakehouse-engine/tests/common/e2e_harness.rs`, add `VsProps::with_property(name, value)`. In `tests/common/lakekeeper_authz.rs`, seed the events table into `lakehouse_authz` and resolve its table id. Ensure `select` on it for `oidc~lk_perm_allowed@lakehouse.test` and `select` on `authz_alpha` for `oidc~lk_perm_denied@lakehouse.test`, and grant nothing to `oidc~lk_perm_unknown@lakehouse.test`. Keep the provisioning idempotent. In `tests/common/lakekeeper.rs`, correct the doc comment of `WarehouseProfile::authz`, which states that no scan reads the warehouse.
- [ ] 3.2 In `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`, create the Exasol users `LK_PERM_ALLOWED`, `LK_PERM_DENIED`, and `LK_PERM_UNKNOWN` with `CREATE SESSION`. Create virtual schema `LK_PERM_LAKEHOUSE` over `lakehouse_authz` through the operator CONNECTION, with the check on and `USER_MAPPING = 'oidc~$lower($1)@lakehouse.test'`. Create `LK_PERM_UNINSPECTING` the same way, then replace its CONNECTION with the client credentials of `lakehouse-reader-b`. Grant `SELECT` on both schemas to the three users.
- [ ] 3.3 Add `permission_check_grants_decide_each_mapped_users_query` and `permission_check_refuses_when_the_connection_cannot_inspect_permissions`. Assert the seeded rows for the granted user, then the denial for each other user on the byte-identical statement, and the `CannotInspectPermissions` refusal for the granted user on `LK_PERM_UNINSPECTING`. Assert that no error carries a client secret or a token.
- [ ] 3.4 Run `make test-e2e-lakekeeper` and `make test-e2e`, and record both results in the PR.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Lakekeeper permission client | 1.1-1.4 | — | spec delta `vs-adapter/lakekeeper-permission-client`; recorded `vs-adapter/catalog-crate-public-surface-extensions`; `crates/lakehouse-catalog/src/{session.rs,iceberg_io.rs,lakekeeper.rs,lakekeeper_tests.rs,lib.rs,test_support_tests.rs}`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/` |
| B: Permission check at the pushdown seam | 2.1-3.4 | A (consumes `batch_check`, `LakekeeperManagementBase`, `TableReadDecision`) | spec deltas `vs-adapter/lakekeeper-permission-check`, `vs-adapter/pushdown-catalog-session`, `vs-adapter/pushdown-format-neutral-resolution`; recorded `vs-adapter/catalog-kind-selection`, `lakekeeper-e2e/lakekeeper-e2e-harness`; `crates/lakehouse-engine/src/adapter/{permission.rs,permission_tests.rs,catalog_kind.rs,mod.rs,adapter_tests.rs}`, `crates/lakehouse-engine/src/adapter/pushdown/{mod.rs,scan_resolution.rs,scan_resolution_tests.rs,pushdown_tests.rs,test_support_tests.rs}`, `crates/lakehouse-engine/src/adapter/pushdown/joins/{mod.rs,joins_tests.rs}`, `crates/lakehouse-engine/tests/{e2e_lakekeeper_test.rs,common/lakekeeper_authz.rs,common/lakekeeper.rs,common/e2e_harness.rs}`, `specs/mission.md` |

Group B keeps the end-to-end tests with the adapter code that they prove, because they implement scenarios of `vs-adapter/lakekeeper-permission-check`. Task 2.4 carries `[expert]`: it is the security chokepoint, and its correctness depends on every pushdown path that reaches it.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | The plan adds a feature. No code becomes obsolete. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Absent PERMISSION_CHECK leaves every request unchanged and contacts no management API | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `absent_permission_check_sends_no_management_request_and_changes_no_output` |
| Invalid permission properties are rejected before the CONNECTION is read | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `invalid_permission_properties_are_rejected_before_the_connection_is_read` |
| Invalid permission properties are rejected before the CONNECTION is read | Unit | `crates/lakehouse-engine/src/adapter/permission_tests.rs` | `user_mapping_rejects_each_grammar_rule` |
| The management API base is derived from the catalog URI or set on the catalog's origin | Unit | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `management_base_is_derived_or_set_on_the_catalog_origin` |
| The management API base is derived from the catalog URI or set on the catalog's origin | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `an_unresolvable_management_base_is_rejected_naming_the_property` |
| The principal is derived from the querying user through USER_MAPPING | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `the_principal_is_derived_from_the_current_user_not_the_scope_user` |
| A user name the mapping cannot accept is refused before any catalog request | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `an_unmappable_user_is_refused_before_any_catalog_request` |
| A user name the mapping cannot accept is refused before any catalog request | Unit | `crates/lakehouse-engine/src/adapter/permission_tests.rs` | `each_placeholder_maps_its_accepted_names_injectively` |
| The check runs once per request at the resolution seam before any table load | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `every_iceberg_shape_is_checked_once_before_any_table_load` |
| The check runs once per request at the resolution seam before any table load | Integration | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `a_gated_resolver_refuses_an_unchecked_identifier` |
| A denied or unverifiable answer refuses the query before any table metadata is loaded | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_denied_or_unverifiable_answer_refuses_with_no_table_load` |
| PERMISSION_CHECK is rejected under the Unity Catalog and direct-storage kinds | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `permission_check_is_rejected_under_unity_and_direct_storage` |
| PERMISSION_CHECK is rejected under the Unity Catalog and direct-storage kinds | Integration | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `a_gated_resolver_refuses_unity_and_direct_storage_before_any_request` |
| Listing requests validate the permission properties and run no check | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `listing_requests_validate_permission_properties_and_send_no_check` |
| Grants on the template-derived principal decide a live Exasol user's query | E2E | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_grants_decide_each_mapped_users_query` |
| The batch-check request carries one identity-bearing read check per distinct table | Unit | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `batch_check_request_equals_each_fixture_request` |
| The batch-check request carries one identity-bearing read check per distinct table | Integration | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `batch_check_sends_one_check_per_distinct_table` |
| Each table's decision is read from the result that carries its check id | Unit | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `each_fixture_response_yields_its_decision` |
| Each table's decision is read from the result that carries its check id | Unit | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `an_answer_with_missing_unknown_or_duplicate_ids_is_malformed` |
| A failed or malformed batch-check is an error that carries no credential | Integration | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `every_batch_check_failure_is_a_credential_free_error` |
| A caller that cannot inspect permissions gets an error that names the missing privilege | Integration | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `a_cannot_inspect_answer_names_the_missing_privilege` |
| A caller that cannot inspect permissions gets an error that names the missing privilege | E2E | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_refuses_when_the_connection_cannot_inspect_permissions` |
| The Lakekeeper client extends the catalog crate's public surface through a reviewed probe edit | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `lakekeeper_permission_client_items_are_reachable_and_wire_types_are_not` |
| The Lakekeeper permission check reuses the request's one catalog session | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `permission_check_adds_one_request_and_no_grant_on_the_session` |
| Every pushdown request shape resolves through the one format-reader seam | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `every_request_shape_resolves_through_the_format_reader_seam` (existing, unchanged) |
| One catalog session per request serves every table the request resolves | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_two_leg_join_resolves_both_legs_on_one_catalog_session` (existing, unchanged) and `permission_check_adds_one_request_and_no_grant_on_the_session` |

The unit tests cover pure computation only: template parsing, name mapping, base derivation, and JSON serialization and parsing against the fixtures.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| lakekeeper-permission-check | `make test-e2e-lakekeeper` | Every test passes, and none is reported ignored |
| lakekeeper-permission-check | `exapump sql "SELECT COUNT(*) FROM LK_PERM_LAKEHOUSE.EVENTS" -d "exasol://LK_PERM_ALLOWED:<password from e2e_lakekeeper_test.rs>@localhost:28563?validateservercertificate=0"` | `20` |
| lakekeeper-permission-check | the same command with `LK_PERM_DENIED` and its password | An error that names `LK_PERM_DENIED`, `oidc~lk_perm_denied@lakehouse.test`, and `e2e_lakehouse.events` |
| lakekeeper-permission-check | `exapump sql "ALTER VIRTUAL SCHEMA LK_PERM_LAKEHOUSE SET PERMISSION_CHECK = 'OPA'" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | An error that names `OPA` and `LAKEKEEPER`, and the property stays `LAKEKEEPER` |
| lakekeeper-permission-client | `cargo test -p lakehouse-catalog lakekeeper` | All tests pass |
| lakekeeper-permission-client | the `exapump` SELECT as `LK_PERM_ALLOWED` against `LK_PERM_UNINSPECTING.EVENTS` | An error that names `CannotInspectPermissions` and `manage_grants`, with no secret |
| pushdown-catalog-session | `cargo test -p lakehouse-engine permission_check_adds_one_request_and_no_grant_on_the_session` | 1 passed |
| pushdown-format-neutral-resolution | `cargo test -p lakehouse-engine every_request_shape_resolves_through_the_format_reader_seam` | 1 passed |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e-lakekeeper` and `make test-e2e` | 0 failures, none ignored |
| Lint | `cargo clippy --all-targets -- -D warnings` and `cargo clippy -p lakehouse-engine --all-targets --features lakekeeper-e2e -- -D warnings` | 0 errors or warnings |
| Format | `cargo fmt --check` | No changes |
| Plan | `speq plan validate add-lakekeeper-permission-check` | Pass |
