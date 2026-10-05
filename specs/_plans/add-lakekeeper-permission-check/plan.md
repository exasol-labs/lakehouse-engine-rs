# Plan: add-lakekeeper-permission-check

## Summary

An opt-in `PERMISSION_CHECK = 'LAKEKEEPER'` virtual-schema property makes the adapter render the querying Exasol user's Lakekeeper principal from the `USER_MAPPING` MiniJinja template and call `batch-check` once per query. The call goes out on the request's existing `CatalogSession`, after the session resolves and before any `loadTable`. The adapter refuses the query on any denial or failure. The check works only under the Iceberg REST catalog kind. Without the property the engine behaves as today and contacts no management API (#415).

## Design

### Context

#415 is increment 2 of 3 of the *Per-user permission enforcement via Lakekeeper* milestone. It depends on #414 and feeds #416. Today every query reads as the CONNECTION's service account, so every Exasol user sees every table that account can read.

Facts this design rests on:

- **The pushdown has one resolution point.** `TableScanResolver::for_request` (`crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs`) serves `handle_pushdown` for every single-table shape and `plan_join` for every join. All aggregate, top-N, fallback-wrapper, and empty-result paths consume the shards of that one resolver (`build_dispatch_sql`, `pushdown/mod.rs`). No pushdown path loads table metadata around it.
- **The session already holds what the check needs.** `CatalogSession` (`crates/lakehouse-catalog/src/session.rs`) holds the pooled `reqwest::Client`, the resolved `CatalogAuth`, the catalog URI, and the `/v1/config` prefix. The OAuth2 grant runs once in `resolve`. `load_table_any_auth` is the precedent for a self-issued call on the session.
- **The prefix is the warehouse id on Lakekeeper.** Live check, 2026-10-02, Lakekeeper `v0.13.1`: `GET /catalog/v1/config?warehouse=lakehouse_authz` answered `defaults.prefix = 5c25f9c4-be40-11f1-a87d-17102559a460`. `/management/v1/warehouse` lists the same id for `lakehouse_authz`.
- **The 403 path is reachable live.** On the same stack, `lakehouse-reader-b` reads `/v1/config` (200) but holds no grant management. Its batch-check answers 403 `CannotInspectPermissions` (`cannot-inspect.json`). The operator's batch-check for the never-registered id `oidc~nobody_ever@corp` answered `allowed: false` for an existing table and for a missing table.
- **One function validates for both entry points.** `resolve_connection_config` (`adapter/mod.rs`) resolves the kind and reads the CONNECTION for createVirtualSchema, refresh, setProperties, and pushdown. A pushdown request carries the persisted properties (`get_properties`), so a virtual schema that carried `PERMISSION_CHECK` before this feature is validated on its next query.
- **Four catalog kinds exist.** `CatalogKind` holds `IcebergRest`, `UnityCatalogNative`, `DirectStorage`, and `Glue` (#455). The kind rule compares with `IcebergRest`, so it names none of the others.
- **The current user reaches the adapter.** `exasol-udf-sdk` 0.30.0 declares `UdfContext::current_user() -> Option<String>` and `scope_user()`. The SDK's `TestContext` sets both (`with_current_user`, `with_scope_user`). #415 states that `language-container-rs` reports the user on the adapter path. If it does not, every checked query is refused, and the e2e test of this plan fails.
- **Only privileged users set VS properties.** `ALTER VIRTUAL SCHEMA ... SET` requires `ALTER ANY VIRTUAL SCHEMA`, `ALTER` on the schema, or ownership ([Exasol ALTER SCHEMA](https://docs.exasol.com/db/latest/sql/alter_schema.htm)). The adapter therefore trusts the `USER_MAPPING` author and guards only against injection through a user name.
- **Exasol passes a template through unchanged.** On Exasol Personal 2026.2, a multi-line `USER_MAPPING` with `{% %}`, `{{ }}`, double quotes, and tabs reaches the adapter byte for byte through `CREATE VIRTUAL SCHEMA` and `ALTER VIRTUAL SCHEMA ... SET`. `EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` stores it unchanged (PR #457 review, 2026-10-05). The e2e test repeats this on the Docker Exasol, and task 3.7 measures the property-length limit.
- **MiniJinja 2.24.0 renders every example.** A scratch crate with the plan's engine settings rendered each template of the spec delta as stated on 2026-10-05. `{% include %}` and `{% macro %}` failed to compile. An unknown filter and a missing lookup failed the render. A 5,000-entry lookup table cost 8 fuel units. With the plan's feature set, `memo-map` 0.3.4 is the only new crate, and both crates are Apache-2.0.
- **The query cache skips virtual schemas.** Exasol does not use the query cache for a statement that contains a virtual schema ([Exasol query cache](https://docs.exasol.com/db/7.1/database_concepts/query_cache.htm)). The e2e test checks this live.
- **Iceberg and Delta compliance.** `batch-check` is a Lakekeeper management API, not part of the Iceberg REST catalog spec. The Iceberg REST spec defines the `/v1/config` `prefix` only as "An optional prefix in the path" (`rest-catalog-open-api.yaml`, `components.parameters.prefix`). Reading it as a warehouse id holds for Lakekeeper only, and the spec delta states that reading. The check changes no scan, file planning, schema, or type handling. No section of the Iceberg table spec or the Delta protocol applies, so no deviation exists.
- **The merged spec holds 10 scenarios.** The client scenarios merge into `vs-adapter/lakekeeper-permission-check`. Ten is the most that a spec holds below the library threshold of more than 10.

- **Goals**: the two properties, a template user mapping whose author the adapter trusts, with no injection through a user name, one batch-check per query on the request's session, check before load, fail closed on every failure and every kind other than Iceberg REST, and live proof on the `lakekeeper-e2e` stack.
- **Non-Goals**: OPA or Cedar modes. Permission checks for any kind other than Iceberg REST. A management URL override for path-rewriting gateways (decision [4]). Checks on createVirtualSchema, refresh, or setProperties (#416 asserts that limitation). Per-shape end-to-end tests, the bypass audit, and the trust-model documentation (#416). Row- or column-level authorization. Per-user storage credentials.

### Decision

#### Architecture

```
dispatch (adapter/mod.rs)
 ├─ createVirtualSchema / refresh / setProperties
 │    PermissionSettings::parse(props)              bad value or template syntax → error
 │    resolve_connection_config(.., &settings)
 │      kind != IcebergRest                         → "requires the Iceberg REST catalog kind"
 │      read the CONNECTION
 │      lakekeeper_management_url(catalog URI)      no trailing /catalog → error
 │    list the namespace (unchanged, no batch-check)
 └─ pushdown
      PermissionSettings::parse(props)
      mapping.principal_for(ctx.current_user())      no user, render error, bad principal → refusal
      resolve_connection_config(.., &settings)       same kind and URL checks
      conn.permission_gate = PermissionGate { user, principal }
      handle_pushdown(conn, ..) ──┬─ single-table shapes ─┐   signatures unchanged
                                  └─ plan_join(conn, ..) ─┤
                                                          ▼
      TableScanResolver::for_request(.., conn.permission_gate.as_ref())
        Iceberg:  CatalogSession::resolve            (one grant, one /v1/config)
                  gate.authorize(&session, ids) ──▶ lakekeeper_batch_check
                                                     POST <management>/v1/action/batch-check
                  any denial or error              → refusal, no loadTable
      resolver.resolve(id)   id the check did not cover → refusal (every id under other kinds)
        └─ loadTable, file planning, shape routing (unchanged)
```

`crates/lakehouse-catalog/src/lakekeeper.rs` owns everything Lakekeeper-specific: the wire types, the management URL derivation, the warehouse-id reading of the session prefix, check ids, and error mapping. `crates/lakehouse-engine/src/adapter/permission.rs` owns everything Exasol-specific: the two properties, the template engine and its settings, the principal checks, and the refusal messages. No other module names `minijinja`.

#### Key Interfaces

```rust
// lakehouse-catalog (pub, re-exported at the crate root)
pub fn lakekeeper_management_url(catalog_uri: &str) -> Result<String, UdfError>;
pub struct TableReadDecision { pub table: String, pub allowed: bool }
pub async fn lakekeeper_batch_check(
    session: &CatalogSession, principal: &str, tables: &[&str], creds: &ConnectionCreds,
) -> Result<Vec<TableReadDecision>, UdfError>;

// lakehouse-engine adapter (crate-private)
pub(crate) enum PermissionSettings { Off, Lakekeeper(UserMapping) }
pub(crate) struct UserMapping { /* the compiled template in a configured minijinja Environment */ }
impl UserMapping {
    pub(crate) fn principal_for(&self, user: Option<&str>) -> Result<String, UdfError>;
}
pub(crate) struct PermissionGate { /* Exasol user, principal */ }
impl PermissionGate {
    pub(crate) async fn authorize(
        &self, session: &CatalogSession, tables: &[&str], creds: &ConnectionCreds,
    ) -> Result<(), UdfError>;
}
```

`lakekeeper_batch_check` derives the management URL from the session's own catalog URI, so the bearer token goes only to the catalog's origin.

#### Quick Diagnostic (new modules)

| Question | `lakehouse_catalog::lakekeeper` | `adapter/permission.rs` |
|----------|----------------------------------|--------------------------|
| One-sentence responsibility | Asks Lakekeeper on a query's session whether one principal can read each table of a set | Turns the permission properties and the querying user into a gate that authorizes a request's tables or refuses the query |
| Easier to call than to rebuild | Yes: one call hides the wire shape, deduplication, the management URL, the warehouse id, check ids, and redaction | Yes: one gate hides the template engine, its settings, the principal checks, and the messages |
| Internal change forces an edit outside | No: the adapter sees only `TableReadDecision` and errors | No: the resolver sees only `authorize` |
| Doc comment states the reason | `lakekeeper_batch_check` states why it is a free function and why it derives the URL itself | `UserMapping` states why the user name is a template value only and why the engine runs strict, loader-free, and fuel-bounded |
| One owner per decision | Wire and topology here only | Mapping, engine settings, and properties here only. The kind rule sits in `resolve_connection_config`, where the kind is resolved |
| Boundary visible without internals | The crate root re-exports three items | Crate-private types only |
| Tactical shortcut with follow-up | None. The missing gateway override is a scope decision (decision [4]) | None |
| Business logic depends inward | The module names no Exasol concept (`vs-adapter/catalog-crate-public-surface-extensions`) | The current user arrives as an argument from `dispatch`. `minijinja` stays behind `UserMapping` |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Check where a pushdown first reads table metadata, and refuse unchecked tables (decision [1]) | Per-shape checks. Refusing every shape but the row scan | Every shape already loads its tables through that point |
| Lakekeeper calls are free functions on the shared session (decision [2]) | A session method. The whole client in the adapter | The session stays shared and closed |
| Warehouse id is the prefix, sent unchanged, with no detection call (decision [3]) | A UUID rule. A detection call | A non-Lakekeeper server fails the batch-check, with a clear error |
| `/catalog` → `/management`, no override (decision [4]) | `LAKEKEEPER_MANAGEMENT_URL` | No deployment needs it yet, and the token stays on the catalog's origin |
| `USER_MAPPING` is a MiniJinja template, and the adapter trusts its author (decision [5]) | A rule list with a clash check. CEL | Expresses shared principals, lookup tables, and deny lists. The `ALTER` holder is already trusted |
| One error for every kind other than Iceberg REST (decision [6]) | One error per kind | A new kind is rejected by default |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| lakekeeper-permission-check | NEW | `specs/_plans/add-lakekeeper-permission-check/vs-adapter/lakekeeper-permission-check/spec.md` |
| pushdown-catalog-session | CHANGED | `specs/_plans/add-lakekeeper-permission-check/vs-adapter/pushdown-catalog-session/spec.md` |
| pushdown-format-neutral-resolution | CHANGED | `specs/_plans/add-lakekeeper-permission-check/vs-adapter/pushdown-format-neutral-resolution/spec.md` |

## Impact

- With `PERMISSION_CHECK` absent, nothing changes: the same SQL, the same catalog requests, and no management API contact.
- To turn the check on, an operator sets `PERMISSION_CHECK = 'LAKEKEEPER'` and `USER_MAPPING`. The check works only under the Iceberg REST catalog kind. Every other kind gets `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind` at create, refresh, SET, and query time.
- No property names the management API. The adapter builds its URL from the CONNECTION's catalog URI: it drops one trailing `/` and replaces the trailing `/catalog` with `/management`. A catalog URI that does not end in `/catalog` is rejected, so a catalog behind a path-rewriting gateway cannot use the check yet.
- `USER_MAPPING` is a MiniJinja template with one variable, `user`, the querying user's name as Exasol reports it. Its trimmed output is the principal: `oidc~{{ user|lower }}@corp.net` maps `ALICE` to `oidc~alice@corp.net`. Conditions, lookup tables, allow lists, and deny lists use MiniJinja's built-in tags, filters, and tests. A template may span several lines.
- The adapter trusts the template author and checks no principal's uniqueness or owner. Several Exasol users may share one principal. Under `{{ user|lower }}`, the delimited user `"alice"` gets the principal of `ALICE`. An operator who needs the strict rule writes `{% if user == user|upper %}...{% endif %}`.
- CREATE, refresh, and SET reject a template syntax error, but they do not render the template. A query is refused, with an error that names the user, when the request names no current user, the render fails, or the principal is empty or holds whitespace or a control character. A render fails on a missing lookup, an unknown filter, or exhausted fuel, so such a template refuses every query.
- The `.so` gains `minijinja` 2.24.0 and its one new dependency `memo-map` 0.3.4, both Apache-2.0. The PR #457 review measured about 1 MB more in a stripped binary. Exasol's property-length limit bounds the template size.
- The CONNECTION's identity needs a grant that includes `can_read_assignments` on the checked tables, for example `manage_grants` on the warehouse or the namespace. Every Lakekeeper `v0.13.1` privilege that permits the check also permits writing grants. The CONNECTION's client secret therefore becomes a grant-administration credential. #416's trust-model documentation explains this to operators.
- Lakekeeper grants have to name the mapped principal ids, or `LAKEKEEPER__OPENID_SUBJECT_CLAIM` has to name a claim whose value the mapping reproduces.
- With the check on, each query costs one more HTTP round trip, the batch-check. The request has no timeout of its own, the same as the existing `loadTable` GET.
- A query by a user without a grant fails with an error that names the user, the principal, and the table. A catalog that is not Lakekeeper fails every query with an error that names the batch-check URL and the HTTP status. A catalog or management API outage refuses every query of that virtual schema.
- Breaking for one edge case only: a virtual schema that already carries a `PERMISSION_CHECK` value now has it enforced. An unrecognized value, or `LAKEKEEPER` without a `USER_MAPPING` that compiles, fails refresh, SET, and every query until it is fixed or unset.
- The listing still shows every table to every user with `SELECT` on the schema (#416 asserts this).

## Dependencies

- #414 fixtures and stack: `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/`, OpenFGA in `docker-compose.lakekeeper.yml`, and the `lakehouse-reader-b` client.
- `minijinja = { version = "2.24.0", default-features = false, features = ["builtins", "fuel", "serde"] }` in `[workspace.dependencies]` of the root `Cargo.toml`, used by `crates/lakehouse-engine` only. Its one new transitive crate is `memo-map` 0.3.4. Both are Apache-2.0, which `deny.toml` and `about.toml` already accept.
- The batch-check client adds no crate: `reqwest`, `serde`, and `serde_json` are already dependencies of `lakehouse-catalog`.
- The implementing commit references `Closes #415`. #416 builds on decision [1].

## Implementation Tasks

### 1. Lakekeeper batch-check client (`lakehouse-catalog`)

- [ ] 1.1 In `crates/lakehouse-catalog/src/session.rs`, add `pub(crate)` read accessors for the client, the auth, the catalog URI, and the prefix. Keep every field private. Name `lakekeeper.rs` as the second reader in the module doc comment.
- [ ] 1.2 In `crates/lakehouse-catalog/src/iceberg_io.rs`, add a `pub(crate)` authed JSON POST beside `authed_get_json`. Share its bearer, SigV4, and redaction handling. Return the status and the redacted body of a non-2xx answer.
- [ ] 1.3 Add `crates/lakehouse-catalog/src/lakekeeper.rs` with `lakekeeper_management_url`, `TableReadDecision`, and `lakekeeper_batch_check`. Declare the module in `lib.rs`, and re-export the three items at the crate root. Keep the wire types crate-private, and make `identity` a required field. Derive the management URL from the session's own catalog URI: drop one trailing `/`, then replace the trailing `/catalog` with `/management`, so `http://lakekeeper:8181/catalog/` gives `http://lakekeeper:8181/management`, and reject any other URI. Treat a body as a batch-check answer only when its `results` hold exactly one boolean `allowed` for each check id. Deduplicate the tables in first-seen order. Give the first check the id `read-data` and each later check `read-data-<n>`, so a one-table request equals its fixture. Split each identifier with `parse_table_ident`. Send the session prefix unchanged as `warehouse-id`, with `error-on-not-found: false`. Map each answer to decisions or to the errors of the spec delta's denial and failure scenarios.
- [ ] 1.4 Add `crates/lakehouse-catalog/src/lakekeeper_tests.rs`. Compare each built request with its fixture's `request` after placeholder substitution. Check that each fixture's response yields its decision. Send the principal `oidc~A"B\C@corp.net`, and check that the body's `identity` decodes to it exactly. Cover each failure class with credential sentinels echoed in the answer body. Include a 400 `application/json` `BadRequestException` answer and a 422 `text/plain` answer. Check that each error carries the redacted body and the Lakekeeper statement. Extend the recording server in `test_support_tests.rs` to answer a POST and capture its body. Cover `lakekeeper_management_url` with and without a trailing `/`, and with a URI that does not end in `/catalog`. Cover a body whose `results` miss a check id, repeat one, or hold a non-boolean `allowed`.
- [ ] 1.5 In `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, name the three new items, and add `lakekeeper.rs` to `CATALOG_SOURCES`. Assert that the wire types are not `pub` and that `CatalogSession` gains no `pub` method. Assert that `lakekeeper.rs` contains none of `PERMISSION_CHECK`, `USER_MAPPING`, `current_user`, `scope_user`, `UdfContext`, or `lakehouse_engine`.

### 2. Permission check in the adapter

- [ ] 2.1 Pin `minijinja` in the root `Cargo.toml` as § Dependencies states, and add `minijinja = { workspace = true }` to `crates/lakehouse-engine/Cargo.toml`. Add `crates/lakehouse-engine/src/adapter/permission.rs` and its sibling `permission_tests.rs`. Define the constants `PERMISSION_CHECK` and `USER_MAPPING`. Add `PermissionSettings::parse`, which reads through `super::nonempty_str` and compiles `USER_MAPPING`. Configure the environment per decision [5]: strict undefined values, no template loader, an `AutoEscape::None` callback, fuel 100,000, `trim_blocks`, and `lstrip_blocks`. Add `UserMapping::principal_for`. It passes the user name only as the `user` value, trims the output, and refuses an absent or empty user, a render failure, an empty principal, and a principal with a whitespace or control character. Unit-test, beyond the spec delta's examples: each of `oidc~{{ user|lower }}@corp.net`, `oidc~{{ user[4:]|lower }}@corp.net` for `EXA_ALICE`, and the replace template for `ALICE_COOPER`; the one-line `if`/`elif`/`else` template with `ETL_SVC`, `BOB_EXT`, and `ALICE_COOPER`, also with each tag and branch on its own indented line; a `{% set ids = {...} %}` lookup table for a listed and an unlisted user; a deny list refusing `SYS`; `{% if user == user|upper %}...{% endif %}` refusing the delimited user `alice`; a template with an unknown filter and one that runs out of fuel, each refusing every user; `{% include %}`, `{% import %}`, `{% extends %}`, and `{% macro %}`, each failing to compile; a missing lookup failing the render; and `oidc~@corp.net`-style partial output never reaching the principal.
- [ ] 2.2 In `permission.rs`, add `PermissionGate`, which holds the Exasol user and the principal. Make `PermissionGate::authorize` call `lakekeeper_batch_check` and compose the denial refusal from the decisions.
- [ ] 2.3 In `crates/lakehouse-engine/src/adapter/mod.rs`, give `resolve_connection_config` a `&PermissionSettings` parameter. If the check is on, compare the kind with `CatalogKind::IcebergRest` before the CONNECTION read. Use `!=`, not a `match`, and return the one kind error. After the CONNECTION read, call `lakekeeper_management_url` on the catalog URI, and return its error. In `handle_create_virtual_schema`, parse the settings and pass them in. Send no batch-check there. Make the test helper `resolve_config_over` in `adapter_tests.rs` pass `PermissionSettings::Off`.
- [ ] 2.4 In the pushdown arm of `dispatch`, parse the settings first. If the check is on, map `ctx.current_user()` before `resolve_connection_config`. Add the field `pub(crate) permission_gate: Option<PermissionGate>` to `ResolvedConnectionConfig`. Set it to `None` in `resolve_connection_config`, and store the gate in the pushdown arm. Keep the signatures of `handle_pushdown_request`, `handle_pushdown`, and `plan_join` unchanged. Add `permission_gate: None` to every test struct literal of `ResolvedConnectionConfig`.
- [ ] 2.5 In `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs`, add the parameter `gate: Option<&PermissionGate>` to `for_request`. Pass `conn.permission_gate.as_ref()` from `handle_pushdown` and from `plan_join`. Pass `None` from every existing `for_request` test call. In the Iceberg arm, call `gate.authorize` right after `CatalogSession::resolve`. While gated, record the checked identifiers, and make `resolve` refuse any other identifier. Under the other kinds a gated resolver checks nothing, so `resolve` refuses every identifier. [expert]
- [ ] 2.6 In `crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs`, extend `RecordingCatalog` to record the method, the `Authorization` header, and the body. Read each request body through its `Content-Length`. Add a Lakekeeper responder. It serves the token endpoint, `/v1/config` with a prefix, a configurable batch-check answer, and a snapshotless `loadTable`.
- [ ] 2.7 Add the dispatch-level tests that the Scenario Coverage table names to `crates/lakehouse-engine/src/adapter/adapter_tests.rs`. Build each `TestContext` with `with_current_user` and `with_scope_user`. Iterate the kind test over every `CATALOG_KIND` constant of `catalog_kind.rs`, so a new kind joins the test.
- [ ] 2.8 Add the resolver tests in `scan_resolution_tests.rs` and the seam tests in `pushdown_tests.rs` that the Scenario Coverage table names. Run every shape through the existing shape fixtures, and assert byte-identical SQL. Keep every existing assertion.
- [ ] 2.9 In `specs/mission.md` § External Dependencies, add a row for the Lakekeeper management API. Purpose: the permission check of a virtual schema that sets `PERMISSION_CHECK = 'LAKEKEEPER'`. Failure impact: every query of such a virtual schema is refused.
- [ ] 2.10 Check the dependency footprint. Confirm that `git diff Cargo.lock` adds exactly the packages `minijinja` 2.24.0 and `memo-map` 0.3.4. Run `cargo tree -p lakehouse-engine -e normal -i minijinja`, and confirm that only `lakehouse-engine` depends on it. Run `cargo deny check licenses advisories`, and confirm that it passes with no change to `deny.toml` or `about.toml`. Record the size of the `.so` from `make cross-udf-build` before and after the change in the PR.

### 3. Live end-to-end proof (`lakekeeper-e2e`)

The live tests cover the row scan only, because #416 owns the per-shape end-to-end test. Three Exasol users and one non-inspecting CONNECTION prove three claims live. The adapter receives the querying user. A grant on a mapped id authorizes that user. Exasol does not serve one user's virtual-schema result to another user.

- [ ] 3.1 In `crates/lakehouse-engine/tests/common/e2e_harness.rs`, add `VsProps::with_property(name, value)`.
- [ ] 3.2 In `tests/common/lakekeeper_authz.rs`, seed the events table into `lakehouse_authz`, and resolve its table id. Grant `select` on it to `oidc~lk.allowed@lakehouse.test`. Grant `select` on `authz_alpha` to `oidc~lk.denied@lakehouse.test`. Grant nothing to `oidc~lk.unknown@lakehouse.test`. Keep the provisioning idempotent. In `tests/common/lakekeeper.rs`, correct the doc comment of `WarehouseProfile::authz`, which states that no scan reads the warehouse.
- [ ] 3.3 In `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`, drop the test's virtual schemas, users, and CONNECTIONs first, as `e2e_credential_exposure_test.rs` does. Create the users `LK_PERM_ALLOWED`, `LK_PERM_DENIED`, and `LK_PERM_UNKNOWN` with `CREATE SESSION`. Create virtual schema `LK_PERM_LAKEHOUSE` over `lakehouse_authz` through the operator CONNECTION `LK_PERM_CATALOG_CREDS`. Set `PERMISSION_CHECK = 'LAKEKEEPER'` and `USER_MAPPING` to the three-line template of the spec delta's e2e scenario: `{% if user is startingwith("LK_PERM_") %}`, then a tab and `oidc~lk.{{ user[8:]|lower }}@lakehouse.test`, then `{% endif %}`, joined by `\n`. Create `LK_PERM_UNINSPECTING` with the same properties through its own CONNECTION `LK_PERM_UNINSPECTING_CREDS`, which first holds the operator credentials. Then replace that CONNECTION's credentials with the client credentials of `lakehouse-reader-b`. The operator credentials come first because `lakehouse-reader-b` cannot list the namespace. Grant `SELECT` on both schemas to the three users.
- [ ] 3.4 Add `permission_check_grants_decide_each_mapped_users_query`. Assert that `EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` returns the `USER_MAPPING` value byte for byte. Run one `SELECT` as `LK_PERM_ALLOWED` first, and assert the seeded rows. Run the byte-identical statement as each other user, and assert the denial. Fail, not skip, when the stack is unavailable.
- [ ] 3.5 Add `permission_check_refuses_when_the_connection_cannot_inspect_permissions`. Run the `SELECT` as `LK_PERM_ALLOWED` against `LK_PERM_UNINSPECTING`, and assert the `CannotInspectPermissions` refusal. In both tests, assert that no error carries a client secret or a token.
- [ ] 3.6 Run `make test-e2e-lakekeeper` and `make test-e2e`, and record both results in the PR.
- [ ] 3.7 Measure Exasol's property-length limit live on the Docker Exasol of `make test-e2e-lakekeeper`, with `exapump`. Set `USER_MAPPING` on `LK_PERM_LAKEHOUSE` to valid templates of 2,000, 100,000, and 2,000,000 characters, then bisect to the first value that fails. A value passes when `ALTER VIRTUAL SCHEMA ... SET` succeeds and `EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` returns it byte for byte. Record the limit, the failure message, and the Exasol version in the PR. Restore the e2e template afterwards.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Lakekeeper batch-check client | 1.1-1.5 | — | spec delta `vs-adapter/lakekeeper-permission-check` (and the scenarios "One batch-check per query decides every table before any table is read", "A denied table refuses the whole query", "A failed batch-check refuses the query with an error that names the cause", and the JSON-string step of "The adapter trusts the template author and evaluates no user name as template text"); plan.md § Context and decisions [3] and [4] for the warehouse id and the management URL; recorded `vs-adapter/catalog-crate-public-surface-extensions`; `crates/lakehouse-catalog/src/{session.rs,iceberg_io.rs,lakekeeper.rs,lakekeeper_tests.rs,lib.rs,test_support_tests.rs}`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/` |
| B: Permission check in the adapter | 2.1-3.7 | A (consumes `lakekeeper_batch_check`, `lakekeeper_management_url`, `TableReadDecision`) | spec deltas `vs-adapter/lakekeeper-permission-check`, `vs-adapter/pushdown-catalog-session`, `vs-adapter/pushdown-format-neutral-resolution`; recorded `vs-adapter/catalog-kind-selection`, `lakekeeper-e2e/lakekeeper-e2e-harness`; `Cargo.toml`, `Cargo.lock`, `deny.toml`, `about.toml`, `crates/lakehouse-engine/Cargo.toml`, `crates/lakehouse-engine/src/adapter/{permission.rs,permission_tests.rs,mod.rs,adapter_tests.rs}`, `crates/lakehouse-engine/src/adapter/pushdown/{mod.rs,scan_resolution.rs,scan_resolution_tests.rs,pushdown_tests.rs,test_support_tests.rs}`, `crates/lakehouse-engine/src/adapter/pushdown/joins/{mod.rs,joins_tests.rs}`, `crates/lakehouse-engine/tests/{e2e_lakekeeper_test.rs,common/lakekeeper_authz.rs,common/lakekeeper.rs,common/e2e_harness.rs}`, `specs/mission.md` |

Both groups read the one spec delta, but they share no source file. Group B runs after Group A, because it consumes the client. Group B keeps the end-to-end tests with the adapter code that they prove. Task 2.5 carries `[expert]`, because it is the security chokepoint that every pushdown path reaches. Task 2.1 configures a template library and carries no tag.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | The plan adds a feature. No code becomes obsolete. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Absent PERMISSION_CHECK leaves every request unchanged and contacts no management API | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `absent_permission_check_sends_no_management_request_and_changes_no_output` |
| Invalid permission properties are rejected before the catalog is contacted | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `invalid_permission_properties_are_rejected_before_the_connection_is_read` |
| Invalid permission properties are rejected before the catalog is contacted | Unit | `crates/lakehouse-engine/src/adapter/permission_tests.rs` | `user_mapping_rejects_a_template_that_does_not_compile` |
| The check is accepted only for an Iceberg REST catalog URI that ends in /catalog | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `permission_check_rejects_every_other_kind_with_one_error` |
| The check is accepted only for an Iceberg REST catalog URI that ends in /catalog | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `permission_check_rejects_a_catalog_uri_that_does_not_end_in_catalog` |
| The check is accepted only for an Iceberg REST catalog URI that ends in /catalog | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `listing_requests_with_the_check_on_list_unchanged_and_send_no_check` |
| The check is accepted only for an Iceberg REST catalog URI that ends in /catalog | Unit | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `management_url_replaces_the_trailing_catalog_segment` |
| USER_MAPPING maps the querying user to a principal | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `the_principal_is_rendered_from_the_current_user` |
| USER_MAPPING maps the querying user to a principal | Unit | `crates/lakehouse-engine/src/adapter/permission_tests.rs` | `each_example_template_renders_its_principal` |
| A user that USER_MAPPING cannot map is refused before any request | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `an_unmappable_user_is_refused_before_any_request` |
| A user that USER_MAPPING cannot map is refused before any request | Unit | `crates/lakehouse-engine/src/adapter/permission_tests.rs` | `each_render_failure_and_rejected_principal_is_refused` |
| The adapter trusts the template author and evaluates no user name as template text | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `a_shared_principal_is_accepted_and_sent_as_a_json_string` |
| The adapter trusts the template author and evaluates no user name as template text | Unit | `crates/lakehouse-engine/src/adapter/permission_tests.rs` | `a_user_name_is_never_compiled_as_template_text` |
| The adapter trusts the template author and evaluates no user name as template text | Unit | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `batch_check_identity_decodes_to_the_exact_principal` |
| One batch-check per query decides every table before any table is read | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `every_iceberg_shape_is_checked_once_before_any_table_load` |
| One batch-check per query decides every table before any table is read | Integration | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `a_gated_resolver_refuses_an_unchecked_identifier` |
| One batch-check per query decides every table before any table is read | Unit | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `batch_check_request_equals_each_fixture_request` |
| One batch-check per query decides every table before any table is read | Integration | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `batch_check_sends_one_check_per_distinct_table` |
| A denied table refuses the whole query | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_denied_or_missing_table_refuses_with_no_table_load` |
| A denied table refuses the whole query | Unit | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `each_fixture_response_yields_its_decision` |
| A failed batch-check refuses the query with an error that names the cause | Integration | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `every_batch_check_failure_names_the_url_and_status_and_no_credential` |
| A failed batch-check refuses the query with an error that names the cause | Integration | `crates/lakehouse-catalog/src/lakekeeper_tests.rs` | `a_cannot_inspect_answer_names_the_missing_privilege` |
| A failed batch-check refuses the query with an error that names the cause | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_failed_batch_check_refuses_with_no_table_load` |
| A failed batch-check refuses the query with an error that names the cause | E2E | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_refuses_when_the_connection_cannot_inspect_permissions` |
| Grants on the mapped principal decide a live Exasol user's query | E2E | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_grants_decide_each_mapped_users_query` |
| The Lakekeeper permission check reuses the request's one catalog session | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `permission_check_adds_one_request_and_no_grant_on_the_session` |
| Every pushdown request shape resolves through the one format-reader seam | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `every_request_shape_resolves_through_the_format_reader_seam` (existing, unchanged) |
| One catalog session per request serves every table the request resolves | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_two_leg_join_resolves_both_legs_on_one_catalog_session` (existing, unchanged) and `permission_check_adds_one_request_and_no_grant_on_the_session` |

The unit tests cover pure computation only: template compilation and rendering, the principal checks, URL derivation, and JSON serialization and parsing against the fixtures.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| lakekeeper-permission-check | `make test-e2e-lakekeeper` | Every test passes, and none is reported ignored |
| lakekeeper-permission-check | `exapump sql "SELECT COUNT(*) FROM LK_PERM_LAKEHOUSE.EVENTS" -d "exasol://LK_PERM_ALLOWED:<password from e2e_lakekeeper_test.rs>@localhost:28563?validateservercertificate=0"` | `20` |
| lakekeeper-permission-check | the same command with `LK_PERM_DENIED` and its password | An error that names `LK_PERM_DENIED`, `oidc~lk.denied@lakehouse.test`, and `e2e_lakehouse.events` |
| lakekeeper-permission-check | the same command as `LK_PERM_ALLOWED` against `LK_PERM_UNINSPECTING.EVENTS` | An error that names `CannotInspectPermissions` and `manage_grants`, with no secret |
| lakekeeper-permission-check | `exapump sql "ALTER VIRTUAL SCHEMA LK_PERM_LAKEHOUSE SET USER_MAPPING = 'oidc~{{ user\|lower @corp.net'" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | An error that states that the template does not compile and names line 1, and the property keeps its old value |
| lakekeeper-permission-check | `exapump sql "ALTER VIRTUAL SCHEMA LK_PERM_LAKEHOUSE SET USER_MAPPING = 'oidc~{{ user\|nosuch }}@lakehouse.test'" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"`, then the `SELECT` as `LK_PERM_ALLOWED` | The SET succeeds. The `SELECT` fails with an error that names `LK_PERM_ALLOWED` and the unknown filter `nosuch` |
| lakekeeper-permission-check | `exapump sql "ALTER VIRTUAL SCHEMA LK_PERM_LAKEHOUSE SET CATALOG_KIND = 'GLUE'" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind`, and the property stays unset |
| lakekeeper-permission-check | `cargo test -p lakehouse-catalog lakekeeper` | All tests pass |
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
| Licenses | `cargo deny check licenses advisories` | Pass, with no change to `deny.toml` or `about.toml` |
| Plan | `speq plan validate add-lakekeeper-permission-check` | Pass |
