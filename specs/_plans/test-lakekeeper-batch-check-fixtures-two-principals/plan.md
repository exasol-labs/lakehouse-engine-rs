# Plan: test-lakekeeper-batch-check-fixtures-two-principals

## Summary

An OpenFGA overlay on the `lakekeeper-e2e` stack gains three test principals with distinct grants
and an e2e suite that asserts Lakekeeper's `batch-check` contract: the principal id convention, the
privilege a caller needs to check another identity, and the management API's location relative to
the catalog URI. Committed `batch-check` fixtures and their README hand that contract to #415, and
no adapter code changes (#414).

## Design

### Context

Issue #414 is increment 1 of 3 of the *Per-user permission enforcement via Lakekeeper* milestone.
#415 adds the adapter's permission client and #416 extends it to every pushdown shape. Today the
`lakekeeper-e2e` stack runs Lakekeeper's `allowall` backend (`docker-compose.lakekeeper.yml`) with
one principal (`lakehouse`) and no grants, so no permission check is observable on it. The AWS
deployment (`deploy/lakekeeper-stack`) also runs `allowall`. No deployment in this repo holds an
OpenFGA grant, so "the principal id convention the existing grants use" (#414) is established on
the grants this plan seeds.

A planning probe on 2026-09-30 ran an isolated compose project: Lakekeeper `v0.13.1`, OpenFGA
`v1.8.16` (the newest patch of the `v1.8` line upstream's `examples/access-control-simple` uses),
Keycloak `26.0.7` with this repo's realm file, and MinIO as the object store. It confirmed the
facts below. No fact below depends on the object store, and the suite runs on the SeaweedFS stack.
Source references are to the `v0.13.1` tag of `lakekeeper/lakekeeper`.

- `batch-check` is `POST /management/v1/action/batch-check` (`crates/lakekeeper/src/api/endpoints.rs:263`).
  The request and response types are `CatalogActionsBatchCheckRequest` and
  `CatalogActionsBatchCheckResponse` (`crates/lakekeeper/src/api/management/v1/check.rs:226-272`).
- A user id is `<idp-id>~<subject>`. The primary provider's `idp-id` is `oidc`, and the default
  subject claims are `oid`, then `sub` (`crates/lakekeeper/src/service/authn.rs:91`). A Keycloak
  client-credentials token carries no `oid`, so the id is `oidc~<service-account UUID>`.
  `preferred_username` (`service-account-<client-id>`) plays no part.
- A grant to a never-registered id (`oidc~never-logged-in`) returned 204, and a check for that id
  answered `allowed: true`. With `LAKEKEEPER__OPENID_SUBJECT_CLAIM=preferred_username`, the same
  client's id became `oidc~service-account-reader-a`.
- A check that names another identity adds a guard: the caller needs `can_read_assignments` on each
  checked object (`crates/authz-openfga/src/authorizer.rs:581-600`). On a table that relation
  derives from `manage_grants` (`authz/openfga/v4.7/components/lakekeeper_table.fga`). The probe
  answered 403 `CannotInspectPermissions` for the whole batch with no grant, with server `admin`
  only, and with warehouse `select` only. It answered 200 with warehouse `manage_grants` or project
  `security_admin`.
- `error-on-not-found: false` answers a missing table as `allowed: false`. `true` fails the batch
  with 404 `NoSuchTableException`. A malformed identity (`alice`) answers 422 with a plain-text
  body. A check with no `identity` answers for the caller.
- The router nests `/catalog/v1` and `/management/v1` side by side at the server root
  (`crates/lakekeeper/src/api/router.rs:151-152`). `LAKEKEEPER__BASE_URI` and the `x-forwarded-*`
  headers rewrite only the advertised `overrides.uri` of `GET /catalog/v1/config`. The routes stay
  in place. No config value names the management API.

The forces:

- `scripts/keycloak-realm-iceberg.json` also provisions the AWS deployment
  (`deploy/lakekeeper-stack/main.tf:94`, `deploy/lakekeeper-stack/locals.tf:4`), and that
  deployment runs `allowall`. A test principal with a committed secret in that file would reach a
  public-IP Keycloak whose tokens read every table.
- The allowall suites share the `lakekeeper` service. Lakekeeper rebuilds no ownership, grants,
  or role assignments across a backend switch after bootstrap (Lakekeeper
  `docs/docs/authorization-openfga.md`, § Switching to OpenFGA or replacing the store).
- Every e2e suite in this repo fails, never skips, without its stack (CLAUDE.md).
- The contract needs Keycloak, Lakekeeper, OpenFGA, and SeaweedFS (warehouse creation writes to
  storage). It needs no Exasol.
- The plan targets the e2e stack of PR #441, which replaces MinIO with the `seaweedfs` service
  (commit `a173738`). That service creates the `warehouse` bucket itself (`-bucket=warehouse`), so
  the stack has no storage one-shot.
- Iceberg and Delta specification compliance does not apply. This plan changes no scan, pushdown,
  or schema or type handling.

- **Goals**: an OpenFGA-backed stack variant, three runtime-provisioned principals with distinct
  grants, e2e assertions for each of #414's three findings, and committed fixtures with a drift
  test.
- **Non-Goals**: any adapter or catalog-crate production code, a `USER_MAPPING` or
  `PERMISSION_CHECK` property (#415), OPA or Cedar authorizers, a reverse proxy in the stack, and
  changing the AWS deployment.

### Decision

#### Architecture

```
host: e2e_lakekeeper_authz_test (feature lakekeeper-authz-e2e)
  │  common/lakekeeper.rs (bootstrap, tokens, server info)
  │  common/lakekeeper_authz.rs (principals, fixture grants, batch-check, fixture shape)
  ├──▶ Keycloak :28080   Admin REST API (create clients) + client-credentials tokens
  ├──▶ Lakekeeper :28181 /management/v1 (bootstrap, users, grants, batch-check)
  │                      /catalog/v1   (config, namespace, metadata-only tables)
  │        └──▶ OpenFGA (gRPC :8081, PostgreSQL store)   SeaweedFS (warehouse bucket)
  └──▶ crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/*.json (+ README.md)

docker-compose.yml + docker-compose.lakekeeper.yml + docker-compose.lakekeeper.openfga.yml
```

The overlay `docker-compose.lakekeeper.openfga.yml` adds `openfga-db`, `openfga-migrate`, and
`openfga`. It sets `LAKEKEEPER__AUTHZ_BACKEND=openfga` and `LAKEKEEPER__OPENFGA__ENDPOINT` on the
existing `lakekeeper-migrate` and `lakekeeper` services, which is the pattern
`docker-compose.lakekeeper.azure.yml` uses. The allowall overlay keeps its services and gains one
header-comment pointer.

`common/lakekeeper_authz.rs` is test-only. It owns one decision: how the contract fixture is
provisioned and queried. `common/lakekeeper.rs` keeps the Lakekeeper and Keycloak basics that both
suites share.

The findings #415 inherits are:

| Finding | Asserted by | Consequence for #415 |
|---------|-------------|----------------------|
| Direct-login id is `oidc~<sub>` (opaque on Keycloak). A grant to a template-shaped id takes effect before first login. | `authz_principal_id_is_idp_prefix_and_token_subject` | A `USER_MAPPING` template needs no per-user setup only when grants name template-derived ids or `LAKEKEEPER__OPENID_SUBJECT_CLAIM` names a claim the template reproduces |
| The least standing privilege is `manage_grants` on the warehouse. Server `admin` and `select` do not suffice. | `authz_check_for_another_identity_is_forbidden_without_grant_management`, `authz_warehouse_manage_grants_allows_checking_another_identity` | `manage_grants` also permits grant writes, and model `v4.7` has no read-only inspect relation. A 403 fails the whole batch. |
| The management API sits beside `/catalog`, and the catalog never advertises it | `authz_management_api_is_mounted_beside_catalog_path`, `authz_catalog_never_advertises_management_base` | Derivation from the catalog URI is defined only for a URI that ends in `/catalog`, so #415 needs an explicit property |
| A missing table answers `allowed: false`, the same as a denied one | `authz_batch_check_fixtures_match_live_contract` | Fail-closed holds with `error-on-not-found: false` |

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Overlay that edits one service in place | `docker-compose.lakekeeper.openfga.yml` | Interview answer 1, `docker-compose.lakekeeper.azure.yml` precedent |
| Idempotent in-process provisioning | `common/lakekeeper_authz.rs` | The local stack persists across runs, the same rule `lakekeeper_bootstrap` follows |
| Golden files with an explicit capture mode | fixtures + `LH_LAKEKEEPER_FIXTURE_CAPTURE=1` | Fixtures come from the live stack, and a normal run only compares |
| Shape comparison over volatile fields | `authz_batch_check_fixtures_match_live_contract` | Message text and `Error ID` values change per run |

#### Quick diagnostic (`common/lakekeeper_authz.rs`)

| Question | Answer |
|----------|--------|
| One-sentence responsibility? | It provisions and queries the OpenFGA contract fixture. |
| Easier to call than to rebuild? | Yes. Tests call `setup()`, `batch_check`, and the grant helpers and never build Keycloak or Lakekeeper requests. |
| Internal change forces an outside edit? | No. Only the test binary calls it. |
| Doc comment explains the reasoning? | The module doc states why principals are created at runtime (decision [2]). |
| One owner per decision? | Yes. Fixture layout and placeholders live here, and `common/lakekeeper.rs` keeps shared Lakekeeper basics. |
| Boundary visible? | Yes: shared basics versus OpenFGA-only fixture code. |
| Tactical shortcut with follow-up? | None. |
| Business logic depends inward? | Not applicable. The module is test infrastructure. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Overlay switches `lakekeeper` in place (decision [1]) | A second Lakekeeper service. OpenFGA in the base overlay. | Interview answer 1. One Lakekeeper per stack. A persisted stack needs `down -v` to switch. |
| Principals via Keycloak Admin REST API (decision [2]) | The shared realm file. A second realm file. | The shared file reaches AWS. A second file duplicates the realm and a second realm changes the issuer. |
| Third principal `lakehouse-checker` (decision [3]) | A reader as checker. The operator as the only positive case. | Per-test grants on one principal prove the minimum without disturbing the readers. |
| New feature `lakekeeper-authz-e2e`, no Exasol (decision [4]) | Reuse `lakekeeper-e2e`. | `--features lakekeeper-e2e` keeps its current meaning. The contract needs no Exasol. |
| Fixtures in `crates/lakehouse-catalog`, compared by shape (decision [5]) | `crates/lakehouse-engine/tests/assets/`. Byte equality. | #415's client lives beside `CatalogSession`. Volatile fields change per run. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| lakekeeper-authz-contract | NEW | `specs/_plans/test-lakekeeper-batch-check-fixtures-two-principals/lakekeeper-e2e/lakekeeper-authz-contract/spec.md` |
| lakekeeper-e2e-harness | CHANGED | `specs/_plans/test-lakekeeper-batch-check-fixtures-two-principals/lakekeeper-e2e/lakekeeper-e2e-harness/spec.md` |

## Impact

None for users or operators: the adapter, the UDFs, and the `.so` do not change. Developers get
`make test-e2e-lakekeeper-authz`, the `lakekeeper-authz-e2e` feature, and an `E2E (Lakekeeper
authz)` CI job. `make test-e2e-lakekeeper` gains one test that fails if the allowall stack reports
another backend. A local stack that switches between the allowall and OpenFGA overlays needs
`docker compose ... down -v` first. The AWS deployment is unaffected because the shared realm file
does not change.

## Dependencies

- PR #441 (`feat/add-aws-assume-role-credentials`) is a merge-order prerequisite. Merge it before
  this plan's implementing PR. It moves the e2e stack from MinIO to `seaweedfs`, and every compose
  command, CI step, and harness reference in this plan names that stack.
- Image `openfga/openfga:v1.8.16`. No new crate: `reqwest`, `serde_json`, and `base64` are already
  available to the engine's tests.
- The implementing commit references `Closes #414`. #415 consumes the fixtures and the README.

## Implementation Tasks

### 1. OpenFGA stack and contract fixture

- [ ] 1.1 Add `docker-compose.lakekeeper.openfga.yml`. It declares `openfga-db` (`postgres:17`, `pg_isready` healthcheck), `openfga-migrate` (`openfga/openfga:v1.8.16 migrate`, one-shot, PostgreSQL datastore via `OPENFGA_DATASTORE_ENGINE` and `OPENFGA_DATASTORE_URI` as upstream's `examples/access-control-simple/docker-compose.yaml` sets them), and `openfga` (`run`, PostgreSQL datastore, `OPENFGA_AUTHN_METHOD=none`, `OPENFGA_PLAYGROUND_ENABLED=false`, healthcheck `/usr/local/bin/grpc_health_probe -addr=openfga:8081`, gated on `openfga-migrate` completing). It sets `LAKEKEEPER__AUTHZ_BACKEND=openfga` and `LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081` on `lakekeeper-migrate` and `lakekeeper`, and gates both on `openfga` healthy. The header comment names the layering command, the one-shot order (`openfga-migrate`, then `lakekeeper-migrate`), the absence of Exasol, and the `down -v` rule. Replace the "OpenFGA is omitted" header lines of `docker-compose.lakekeeper.yml` with a pointer to the new overlay.
- [ ] 1.2 Add the `lakekeeper-authz-e2e` feature to `crates/lakehouse-engine/Cargo.toml` with the FAIL-contract comment the other e2e features carry. Add it to the top-level gate and the `lakekeeper` module gate in `tests/common/mod.rs` and to the inner `cfg` of `tests/common/lakekeeper.rs`. Declare `#[cfg(feature = "lakekeeper-authz-e2e")] pub mod lakekeeper_authz;`.
- [ ] 1.3 In `tests/common/lakekeeper.rs`, extract `keycloak_client_credentials_token_for(client_id, client_secret)` and keep `keycloak_client_credentials_token()` as its `lakehouse` call. Make `management_base` and `http_client` public. Add `lakekeeper_server_info()` (`GET /management/v1/info` with the `lakehouse` token) and `assert_authz_backend(info, expected)`, which panics with the reported backend and the overlays to start. Add `WAREHOUSE_AUTHZ = "lakehouse_authz"` and `WarehouseProfile::authz()` (static credentials). Correct `lakekeeper_bootstrap`'s doc comment: `is-operator` also makes the client the OpenFGA server `operator`.
- [ ] 1.4 Add `tests/common/lakekeeper_authz.rs` with the principal constants and `ensure_client(client_id, secret)`. It takes a master-realm `admin-cli` token with the compose file's bootstrap admin, looks the client up by `clientId`, and creates it only when absent: confidential, service accounts on, all other flows off, and an `oidc-audience-mapper` emitting `lakekeeper`. The module doc states why the realm file is not used (decision [2]).
- [ ] 1.5 In `lakekeeper_authz.rs`, add the idempotent catalog-object provisioning. It creates the `lakehouse_authz` warehouse with `WarehouseProfile::authz()`, reads `defaults.prefix` from `GET /catalog/v1/config?warehouse=lakehouse_authz`, and creates namespace `authz` and the metadata-only tables `authz_alpha` and `authz_beta` (one `long` column `id`) through the Iceberg REST API. An existing namespace or table is tolerated. It returns the warehouse id and each table's `metadata.table-uuid` from `loadTable`. A 403 panics with a hint to recreate the stack with `down -v`.
- [ ] 1.6 In `lakekeeper_authz.rs`, add the idempotent principal provisioning. Each of the three principals self-registers (`POST /management/v1/user` with body `{}`, an existing user tolerated), and its id is read from `GET /management/v1/whoami`. The fixture grants go through `/management/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/assignments`, and the helper reads the table's current assignments and writes only the absent ones. No token or secret appears in any panic message.
- [ ] 1.7 In `lakekeeper_authz.rs`, add `batch_check(caller_token, checks, error_on_not_found)` returning the status and the raw JSON body. Add `set_checker_assignments(state)`, which reads the checker's server, project, and warehouse assignments and writes or deletes until it holds exactly the requested state. Add `jwt_claims(token)` (base64url payload decode).

### 2. Contract scenarios

- [ ] 2.1 Add `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` (`#![cfg(feature = "lakekeeper-authz-e2e")]`) with a `OnceLock` `setup()`: wait for Keycloak and Lakekeeper, bootstrap, `assert_authz_backend(.., "openfga")`, ensure the three clients, and provision the fixture. Add `authz_stack_runs_openfga_backend` and `authz_suite_fails_when_stack_unavailable`. The second test asserts through `catch_unwind` that a readiness wait on `http://127.0.0.1:1/health` panics and that `assert_authz_backend` panics on `{"authz-backend": "allow-all"}`. The module doc states that the tests share the checker's state and therefore run with `--test-threads=1`.
- [ ] 2.2 Add `authz_principals_stay_out_of_the_shared_realm_file`. It parses `scripts/keycloak-realm-iceberg.json` and asserts that no `clients` entry carries one of the three principal client ids. It needs no stack.
- [ ] 2.3 Add `authz_two_principals_hold_different_table_grants`: one operator batch of four checks with ids, asserted per id.
- [ ] 2.4 Add `authz_principal_id_is_idp_prefix_and_token_subject`: `whoami` id equals `oidc~` plus `sub`, `sub` has the 8-4-4-4-12 hex shape and differs from `preferred_username`, and `oidc~template-user@corp` is allowed on `authz_alpha`.
- [ ] 2.5 Add `authz_check_for_another_identity_is_forbidden_without_grant_management` (three `set_checker_assignments` states, each a 403 `CannotInspectPermissions` with no `results`) and `authz_warehouse_manage_grants_allows_checking_another_identity` (warehouse `manage_grants` only, answers equal to the operator's). Each test sets its checker state first.
- [ ] 2.6 Add `authz_management_api_is_mounted_beside_catalog_path` and `authz_catalog_never_advertises_management_base`. The management base comes from the catalog URI `http://localhost:<LH_LAKEKEEPER_PORT>/catalog`, never from `management_base()`, so the tests exercise the derivation under test.

### 3. Fixtures and the #415 hand-off

- [ ] 3.1 In `lakekeeper_authz.rs`, add the fixture codec: placeholder substitution into a request (`<warehouse-id>`, `<principal:lakehouse-reader-a>`), normalization of a live exchange back to placeholders (ids, table UUIDs inside messages, `Error ID: <error-id>`), and `shape_matches(live, fixture)` per the Background definition. Add stack-free tests of the codec to the test binary: a changed message still matches, and a changed `allowed`, a changed `error.type`, or an extra key does not.
- [ ] 3.2 Add `authz_batch_check_fixtures_match_live_contract` for the cases `allowed` (operator, reader-a, `authz_alpha`), `denied` (operator, reader-a, `authz_beta`), `missing` (operator, reader-a, `authz_missing`), and `cannot-inspect` (caller reader-b, reader-a, `authz_alpha`), all with `error-on-not-found: false`. The test compares the shapes, asserts that `missing` and `denied` carry the same answer, and scans each fixture's text for the three client secrets, a `eyJ` token prefix, and the live warehouse, table, and principal ids. With `LH_LAKEKEEPER_FIXTURE_CAPTURE=1` it writes the normalized exchanges to `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/<case>.json` instead of comparing. A missing fixture file fails the test with the capture command in the message.
- [ ] 3.3 Capture the four fixtures from the live stack and commit them. Run the suite again without the capture variable and confirm that `git status` shows no fixture change.
- [ ] 3.4 Write `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/README.md`. It covers the file format, the placeholders, the capture command, and the pinned versions. It then lists every finding in the Context section. Each finding carries "asserted by `<test>`" or "planning probe 2026-09-30, not suite-asserted". The probe-only entries are the subject-claim lever, `LAKEKEEPER__BASE_URI`, project `security_admin`, the `error-on-not-found: true` 404, the 422 plain-text body, and the identity-less check. State the #415 consequences from the findings table as findings, not decisions.
- [ ] 3.5 Add `test-e2e-lakekeeper-authz` to `Makefile` (`cargo test --features lakekeeper-authz-e2e --test e2e_lakekeeper_authz_test -- --test-threads=1`, no `cross-udf-build` prerequisite) with a `Requires:` comment naming the compose commands. Add it to `.PHONY`.
- [ ] 3.6 Add the `e2e-lakekeeper-authz` job to `.github/workflows/ci.yml`. It declares `needs: [build-so]`, because `./.github/actions/e2e-setup` downloads the `.so`. It pulls with the three compose files, runs `docker wait` on `openfga-migrate` and `lakekeeper-migrate`, then brings up `seaweedfs keycloak lakekeeper-db openfga-db openfga lakekeeper` with `--wait`. It runs `make test-e2e-lakekeeper-authz`, dumps the Keycloak, OpenFGA, and Lakekeeper logs on failure, and always runs `down -v`. Add the comment `# Not yet a required check.` A human decides branch protection.

### 4. Allowall guard

- [ ] 4.1 Add `lakekeeper_stack_runs_allowall_backend` to `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`. It calls `setup()`, then `assert_authz_backend(&lakekeeper_server_info(), "allow-all")`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Lakekeeper authorization contract | 1.1-4.1 | — | spec deltas `lakekeeper-e2e/lakekeeper-authz-contract` and `lakekeeper-e2e/lakekeeper-e2e-harness`; recorded `lakekeeper-e2e/lakekeeper-e2e-harness` (Background); `docker-compose.lakekeeper.openfga.yml`, `docker-compose.lakekeeper.yml`, `scripts/keycloak-realm-iceberg.json` (read only), `crates/lakehouse-engine/Cargo.toml`, `crates/lakehouse-engine/tests/common/mod.rs`, `common/lakekeeper.rs`, `common/lakekeeper_authz.rs`, `tests/e2e_lakekeeper_authz_test.rs`, `tests/e2e_lakekeeper_test.rs`, `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/`, `Makefile`, `.github/workflows/ci.yml`; this plan's Context section (probe facts and upstream source lines) |

One group: task 4.1 calls the `common/lakekeeper.rs` helpers of task 1.3, and a second group would
share that file. No task carries `[expert]`. The work is compose configuration, HTTP provisioning, and pure JSON
helpers. None of it involves concurrency, a novel algorithm, or a production security path.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | The plan only adds test infrastructure. The replaced header comment in `docker-compose.lakekeeper.yml` is edited, not removed code. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The OpenFGA overlay runs Lakekeeper on the OpenFGA authorizer | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_stack_runs_openfga_backend` |
| The authorization suite fails when its stack is unavailable | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_suite_fails_when_stack_unavailable` |
| The shared realm file carries no authorization-suite principal | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_principals_stay_out_of_the_shared_realm_file` |
| Two principals hold different table grants | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_two_principals_hold_different_table_grants` |
| A principal's Lakekeeper id is its IdP prefix and its token subject | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_principal_id_is_idp_prefix_and_token_subject` |
| A caller without a grant-management privilege cannot check another identity | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_check_for_another_identity_is_forbidden_without_grant_management` |
| Warehouse manage_grants lets a caller check another identity | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_warehouse_manage_grants_allows_checking_another_identity` |
| The management API is mounted beside the catalog path, not under it | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_management_api_is_mounted_beside_catalog_path` |
| The catalog never advertises the management base, so a client cannot derive it safely | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_catalog_never_advertises_management_base` |
| Committed batch-check fixtures match the live contract | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` | `authz_batch_check_fixtures_match_live_contract` |
| The allowall suite runs on the allowall authorizer | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_stack_runs_allowall_backend` |

The fixture codec tests of task 3.1 guard the shape rule. They implement no scenario.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| lakekeeper-authz-contract | `C="docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml -f docker-compose.lakekeeper.openfga.yml"; $C up openfga-migrate lakekeeper-migrate && $C up -d --wait seaweedfs keycloak lakekeeper-db openfga-db openfga lakekeeper` | The one-shots exit 0. Every listed service reports healthy, and no `exasol` container starts. |
| lakekeeper-authz-contract | `make test-e2e-lakekeeper-authz` | Every test passes and none is reported ignored |
| lakekeeper-authz-contract | `LH_LAKEKEEPER_FIXTURE_CAPTURE=1 make test-e2e-lakekeeper-authz && git status --short crates/lakehouse-catalog/tests/fixtures` | No output from `git status` after the committed capture |
| lakekeeper-authz-contract | `T=$(curl -s -d grant_type=client_credentials -d client_id=lakehouse -d client_secret=lakehouse-engine-secret http://localhost:28080/realms/iceberg/protocol/openid-connect/token \| jq -r .access_token); curl -s -H "Authorization: Bearer $T" http://localhost:28181/management/v1/info \| jq -r '."authz-backend"'` | `openfga` |
| lakekeeper-authz-contract | `$C down -v && cargo test -p lakehouse-engine --features lakekeeper-authz-e2e --test e2e_lakekeeper_authz_test -- --test-threads=1` | The run fails, and no test is reported ignored |
| lakekeeper-e2e-harness | `docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d --wait seaweedfs exasol keycloak lakekeeper-db lakekeeper-migrate lakekeeper && make test-e2e-lakekeeper` (one-shots first, as in CI) | Every test passes, including `lakekeeper_stack_runs_allowall_backend` |
| lakekeeper-e2e-harness | `cargo test -p lakehouse-engine --features lakekeeper-e2e --test e2e_lakekeeper_test lakekeeper_stack_runs_allowall_backend` against the OpenFGA stack | Fails with a message naming the reported backend `openfga` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 (`make test-e2e-lakekeeper` needs it, the authz target does not) |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e-lakekeeper-authz` and `make test-e2e-lakekeeper` on their stacks | 0 failures |
| Lint | `cargo clippy --all-targets -- -D warnings` and `cargo clippy -p lakehouse-engine --all-targets --features lakekeeper-e2e,lakekeeper-authz-e2e -- -D warnings` | 0 errors or warnings |
| Format | `cargo fmt --check` | No changes |
| Plan | `speq plan validate test-lakekeeper-batch-check-fixtures-two-principals` | Pass |
