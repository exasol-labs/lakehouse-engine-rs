# Plan: test-lakekeeper-batch-check-fixtures-two-principals

## Summary

The `lakekeeper-e2e` stack gains OpenFGA, so Lakekeeper enforces permissions, and three Keycloak
test clients with distinct table grants. New tests in `e2e_lakekeeper_test.rs` and committed
`batch-check` fixtures record what Lakekeeper answers for an allowed, a denied, and a missing table.
The findings go to the decision log and the fixtures README as input for #415. No adapter code
changes (#414).

## Design

### Context

Issue #414 is increment 1 of 3 of the *Per-user permission enforcement via Lakekeeper* milestone.
#415 adds the adapter's permission client and #416 extends it to every pushdown shape. Today the
`lakekeeper-e2e` stack runs Lakekeeper's `allowall` backend (`docker-compose.lakekeeper.yml`) with
one principal (`lakehouse`) and no grants, so no permission check is observable on it. The AWS
deployment (`deploy/lakekeeper-stack`) also runs `allowall` and stays that way. No deployment in
this repo holds an OpenFGA grant, so "the principal id convention the existing grants use" (#414)
is established on the grants this plan seeds.

A planning probe on 2026-09-30 ran an isolated compose project: Lakekeeper `v0.13.1`, OpenFGA
`v1.8.16`, Keycloak `26.0.7` with this repo's realm file, and MinIO as the object store. No fact
below depends on the object store. The review of this plan found that Lakekeeper documents OpenFGA
v1.11 or later as required and tests against v1.14, so the stack pins `v1.14.2` and task 1.7
re-runs the probe facts against it. Source references are to the `v0.13.1` tag of
`lakekeeper/lakekeeper`.

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
  checked object (`crates/authz-openfga/src/authorizer.rs:581-600`). The probe answered 403
  `CannotInspectPermissions` for the whole batch with no grant, with server `admin` only, and with
  warehouse `select` only. It answered 200 with warehouse `manage_grants` or project
  `security_admin`. Warehouse `manage_grants` is enough across a warehouse but is not the minimum:
  namespace `manage_grants` also works, because `manage_grants` is inherited from the parent, and
  `can_read_assignments` includes `can_grant_select`
  (`authz/openfga/v4.7/components/lakekeeper_table.fga`). Task 1.7 checks the narrower options live.
- `error-on-not-found: false` answers a missing table as `allowed: false`. `true` fails the batch
  with 404 `NoSuchTableException`. A malformed identity (`alice`) answers 422 with a plain-text
  body. A check with no `identity` answers for the caller.
- The router nests `/catalog/v1` and `/management/v1` side by side at the server root
  (`crates/lakekeeper/src/api/router.rs:151-152`). Lakekeeper builds both bases from the same base
  URL (`{base}/catalog` and `{base}/management`), and `x-forwarded-prefix` moves both. The routes
  themselves stay at the root, so a gateway that rewrites paths is the case that breaks deriving
  the management base from the catalog URI. No config value names the management API.

The forces:

- `scripts/keycloak-realm-iceberg.json` already holds the `lakehouse` client and its secret, and
  `deploy/lakekeeper-stack` is a short-lived benchmark box that accepts traffic only from the
  deployer's IP (`deploy/lakekeeper-stack/main.tf:94`, `locals.tf:4`). Three more clients with
  committed secrets expose nothing new, and the realm file is the simplest way to create them.
- Lakekeeper rebuilds no ownership, grants, or role assignments across a backend switch after
  bootstrap (Lakekeeper `docs/docs/authorization-openfga.md`, § Switching to OpenFGA or replacing
  the store). A persisted local stack therefore needs `down -v` once.
- Every e2e suite in this repo fails, never skips, without its stack (CLAUDE.md).
- `docker-compose.lakekeeper.azure.yml` replaces `lakekeeper`'s `depends_on` with `!override`, so
  it must name `openfga` again. The `e2e-lakekeeper` and `e2e-azure` CI jobs list the services they
  start and gain the OpenFGA ones.
- Iceberg and Delta specification compliance does not apply. This plan changes no scan, pushdown,
  or schema or type handling.

- **Goals**: OpenFGA in the existing Lakekeeper stack, three test principals with distinct grants,
  tests that pin #414's three findings, and committed fixtures with a drift test.
- **Non-Goals**: any adapter or catalog-crate production code, a `USER_MAPPING` or
  `PERMISSION_CHECK` property (#415), OPA or Cedar authorizers, a reverse proxy in the stack, a
  second Lakekeeper suite, and changing the AWS deployment.

### Decision

#### Architecture

```
host: e2e_lakekeeper_test (feature lakekeeper-e2e)
  │  common/lakekeeper.rs (bootstrap, tokens, server info, principals, grants, batch-check)
  ├──▶ Keycloak :28080   client-credentials tokens (realm file: lakehouse + 3 test clients)
  ├──▶ Lakekeeper :28181 /management/v1 (bootstrap, users, grants, batch-check)
  │                      /catalog/v1   (config, namespace, metadata-only tables)
  │        └──▶ OpenFGA (gRPC :8081, PostgreSQL store)   SeaweedFS (warehouse bucket)
  └──▶ crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/*.json (+ README.md)

docker-compose.yml + docker-compose.lakekeeper.yml (+ docker-compose.lakekeeper.azure.yml)
```

`docker-compose.lakekeeper.yml` gains `openfga-db`, `openfga-migrate`, and `openfga`, all on the
`lakehouse` network. It sets `LAKEKEEPER__AUTHZ_BACKEND=openfga` and
`LAKEKEEPER__OPENFGA__ENDPOINT` on `lakekeeper-migrate` and `lakekeeper`, and gates both on
OpenFGA. Some tests use permissions and some do not; the shared `lakehouse` operator keeps the
existing scenarios working.

The provisioning and fixture helpers live beside the existing Lakekeeper helpers in
`common/lakekeeper.rs`, split into a sibling module under the same `lakekeeper-e2e` gate only if
the file grows unwieldy. The tests need no Exasol, but they share the suite's feature, binary,
Makefile target, and CI job.

The findings #415 inherits go to the decision log (decision [5]) and the fixtures README. #415
turns them into specs written as the adapter's behaviour: user mapping, the endpoint called, and
what happens for a denied or missing table.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Idempotent in-process provisioning | `common/lakekeeper.rs` | The local stack persists across runs, the same rule `lakekeeper_bootstrap` follows |
| Golden files with an explicit capture mode | fixtures + `LH_LAKEKEEPER_FIXTURE_CAPTURE=1` | Fixtures come from the live stack, and a normal run only compares |
| Shape comparison over volatile fields | `authz_batch_check_fixtures_match_live_contract` | Message text and `Error ID` values change per run |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| OpenFGA added to `docker-compose.lakekeeper.yml` (decision [1]) | A separate overlay and a second suite | One stack, one suite. The existing scenarios run on the enforcing stack, and a persisted stack needs `down -v` once. |
| Test clients in the realm file (decision [2]) | Keycloak Admin REST API at runtime | The file already holds `lakehouse` and its secret, and the deployment is short-lived and IP-restricted. |
| Third principal `lakehouse-checker` (decision [3]) | A reader as checker | Per-test grants on one principal prove the privilege finding without disturbing the readers. |
| Fixtures in `crates/lakehouse-catalog`, compared by shape (decision [4]) | `crates/lakehouse-engine/tests/assets/`. Byte equality. | #415's client lives beside `CatalogSession`. Volatile fields change per run. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| lakekeeper-e2e-harness | CHANGED | `specs/_plans/test-lakekeeper-batch-check-fixtures-two-principals/lakekeeper-e2e/lakekeeper-e2e-harness/spec.md` |

## Impact

None for users or operators: the adapter, the UDFs, and the `.so` do not change. Developers with a
persisted local Lakekeeper stack run `docker compose ... down -v` once, because the backend
switches to OpenFGA. The AWS benchmark box stays on `allowall`.

## Dependencies

- Image `openfga/openfga:v1.14.2`. No new crate: `reqwest`, `serde_json`, and `base64` are already
  available to the engine's tests.
- The implementing commit references `Closes #414`. #415 consumes the fixtures and the README.

## Implementation Tasks

### 1. OpenFGA in the stack, principals, and fixture

- [ ] 1.1 In `docker-compose.lakekeeper.yml`, add `openfga-db` (`postgres:17`, `pg_isready` healthcheck), `openfga-migrate` (`openfga/openfga:v1.14.2 migrate`, one-shot, PostgreSQL datastore via `OPENFGA_DATASTORE_ENGINE` and `OPENFGA_DATASTORE_URI`), and `openfga` (`run`, PostgreSQL datastore, `OPENFGA_AUTHN_METHOD=none`, `OPENFGA_PLAYGROUND_ENABLED=false`, gRPC healthcheck, gated on `openfga-migrate` completing), all on the `lakehouse` network. Set `LAKEKEEPER__AUTHZ_BACKEND=openfga` and `LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081` on `lakekeeper-migrate` and `lakekeeper`, and gate both on `openfga` healthy. Replace the "OpenFGA is omitted" header lines with the new service list, the one-shot order, and the rule that an existing local stack needs `down -v` once.
- [ ] 1.2 In `docker-compose.lakekeeper.azure.yml`, add `openfga: service_healthy` to the `!override` `depends_on` of `lakekeeper`, and extend its header command with the OpenFGA services.
- [ ] 1.3 In `.github/workflows/ci.yml`, add the OpenFGA services to the `e2e-lakekeeper` and `e2e-azure` jobs: `docker wait` on `openfga-migrate` before `lakekeeper-migrate`, and `openfga-db openfga` in each `up -d --wait` list and in the Azure `pull` list. Dump OpenFGA logs on failure. Update the `Makefile` `Requires:` comments of `test-e2e-lakekeeper` and `test-e2e-azure` to match, and `test-lakekeeper-local` if its stack changes.
- [ ] 1.4 In `deploy/README.md`, change the line that says the bench box runs the same services as `docker-compose.lakekeeper.yml`: the box keeps PostgreSQL, Keycloak, and Lakekeeper on `allowall`, without OpenFGA.
- [ ] 1.5 In `scripts/keycloak-realm-iceberg.json`, add the confidential service-account clients `lakehouse-reader-a`, `lakehouse-reader-b`, and `lakehouse-checker`, each with a fixed test secret and the `oidc-audience-mapper` that emits `lakekeeper`, copied from the `lakehouse` client's shape. Correct `lakekeeper_bootstrap`'s doc comment in `tests/common/lakekeeper.rs`: `is-operator` also makes the client the OpenFGA server `operator`.
- [ ] 1.6 In `tests/common/lakekeeper.rs`, extract `keycloak_client_credentials_token_for(client_id, client_secret)` and keep `keycloak_client_credentials_token()` as its `lakehouse` call. Add `lakekeeper_server_info()` (`GET /management/v1/info`), `ensure_authz_fixture()` and `batch_check(caller_token, checks, error_on_not_found)`. The fixture is idempotent and creates warehouse `lakehouse_authz` (static credentials), namespace `authz`, and the metadata-only tables `authz_alpha` and `authz_beta` through the Iceberg REST API. Each principal self-registers (`POST /management/v1/user`, an existing user tolerated) and its id is read from `GET /management/v1/whoami`. Grants go through `/management/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/assignments`, reading the current assignments first and writing only the absent ones. A 403 panics with a hint to recreate the stack with `down -v`. No token or secret appears in a panic message.
- [ ] 1.7 Run the whole existing `e2e_lakekeeper_test` suite on the OpenFGA stack and record the result in the PR. If a scenario fails for a missing grant, grant it to the `lakehouse` operator in the harness. Re-run the probe facts against OpenFGA `v1.14.2`, including the narrower privilege options: namespace `manage_grants` and the `can_grant_select` path. Update the README (task 3.4) and decision [5] with anything that differs from the `v1.8.16` probe.

### 2. Tests

- [ ] 2.1 In `e2e_lakekeeper_test.rs`, add `lakekeeper_stack_enforces_permissions` (`authz-backend` is `openfga`; the helper panics naming the reported backend otherwise). It uses the suite's existing `setup()` and fail-not-skip discipline, so no second stack-unavailable test is added.
- [ ] 2.2 Add `lakekeeper_two_principals_hold_different_table_grants`: one operator batch of four checks with ids, asserted per id.
- [ ] 2.3 Add `authz_principal_id_is_idp_prefix_and_token_subject`: `whoami` id equals `oidc~` plus `sub`, `sub` has the 8-4-4-4-12 hex shape and differs from `preferred_username`, and `oidc~template-user@corp` is allowed on `authz_alpha`. It needs `jwt_claims(token)` (base64url payload decode) in `common/lakekeeper.rs`.
- [ ] 2.4 Add `authz_check_for_another_identity_is_forbidden_without_grant_management` (checker holds no assignment, only server `admin`, and only warehouse `select`, each a 403 `CannotInspectPermissions` with no `results`) and `authz_warehouse_manage_grants_allows_checking_another_identity` (warehouse `manage_grants` only, answers equal to the operator's). Each test sets the checker's server, project, and warehouse assignments first, through a `set_checker_assignments(state)` helper that writes or deletes until the checker holds exactly the requested state. Tests that change the checker's state run serially, as the Makefile target already does.
- [ ] 2.5 Add `authz_management_api_is_mounted_beside_catalog_path`. The management base comes from the catalog URI `http://localhost:<LH_LAKEKEEPER_PORT>/catalog`, never from `management_base()`, so the test exercises the derivation it documents.

### 3. Fixtures and the #415 hand-off

- [ ] 3.1 Add the fixture codec to `common/lakekeeper.rs`: placeholder substitution into a request (`<warehouse-id>`, `<principal:lakehouse-reader-a>`), normalization of a live exchange back to placeholders (ids, table UUIDs inside messages, `Error ID: <error-id>`), and `shape_matches(live, fixture)`: equal status, equal keys and JSON value types at every depth, and equal `allowed`, `error.type`, and `error.code`. Add stack-free tests of the codec: a changed message still matches, and a changed `allowed`, a changed `error.type`, or an extra key does not.
- [ ] 3.2 Add `authz_batch_check_fixtures_match_live_contract` for the cases `allowed` (operator, reader-a, `authz_alpha`), `denied` (operator, reader-a, `authz_beta`), `missing` (operator, reader-a, `authz_missing`), and `cannot-inspect` (caller reader-b, reader-a, `authz_alpha`), all with `error-on-not-found: false`. The test compares the shapes, asserts that `missing` and `denied` carry the same answer, and scans each fixture's text for the three client secrets, a `eyJ` token prefix, and the live warehouse, table, and principal ids. With `LH_LAKEKEEPER_FIXTURE_CAPTURE=1` it writes the normalized exchanges to `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/<case>.json` instead of comparing. A missing fixture file fails the test with the capture command in the message. It implements no scenario.
- [ ] 3.3 Capture the four fixtures from the live stack and commit them. Run the suite again without the capture variable and confirm that `git status` shows no fixture change.
- [ ] 3.4 Write `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/README.md`: the file format, the placeholders, the capture command, and the pinned Lakekeeper and OpenFGA versions (a version change needs a recapture). It then lists every finding in the Context section as input for #415, each marked "asserted by `<test>`" or "planning probe 2026-09-30, not suite-asserted". The probe-only entries are the subject-claim lever, `x-forwarded-prefix`, project `security_admin`, the `error-on-not-found: true` 404, the 422 plain-text body, and the identity-less check. Describe the privilege as "enough across a warehouse" and list the narrower options with the result of task 1.7. The README states findings, not decisions.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Lakekeeper permission-enforcing stack | 1.1-3.4 | — | spec delta `lakekeeper-e2e/lakekeeper-e2e-harness`; recorded `lakekeeper-e2e/lakekeeper-e2e-harness`; `docker-compose.lakekeeper.yml`, `docker-compose.lakekeeper.azure.yml`, `scripts/keycloak-realm-iceberg.json`, `crates/lakehouse-engine/tests/common/lakekeeper.rs`, `tests/e2e_lakekeeper_test.rs`, `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/`, `Makefile`, `.github/workflows/ci.yml`, `deploy/README.md`; this plan's Context section |

One group: every task edits the stack or `common/lakekeeper.rs`, and a second group would share that
file. No task carries `[expert]`. The work is compose configuration, HTTP provisioning, and pure
JSON helpers. None of it involves concurrency, a novel algorithm, or a production security path.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | The plan only adds test infrastructure. The replaced header comment in `docker-compose.lakekeeper.yml` is edited, not removed code. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The Lakekeeper stack enforces permissions | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_stack_enforces_permissions` |
| Two principals hold different table grants | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_two_principals_hold_different_table_grants` |
| The existing Lakekeeper scenarios pass with permissions enforced | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | the existing scenario tests, run on the OpenFGA stack (task 1.7) |

Tests without a scenario: `authz_principal_id_is_idp_prefix_and_token_subject`, the two
`authz_*privilege*` tests, `authz_management_api_is_mounted_beside_catalog_path`,
`authz_batch_check_fixtures_match_live_contract`, and the codec tests. They pin Lakekeeper's
behaviour for #415 and implement no scenario of ours.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| lakekeeper-e2e-harness | `C="docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml"; $C down -v; $C up openfga-migrate lakekeeper-migrate && $C up -d --wait seaweedfs exasol keycloak lakekeeper-db openfga-db openfga lakekeeper` | The one-shots exit 0 and every listed service reports healthy |
| lakekeeper-e2e-harness | `make test-e2e-lakekeeper` | Every test passes and none is reported ignored |
| lakekeeper-e2e-harness | `T=$(curl -s -d grant_type=client_credentials -d client_id=lakehouse -d client_secret=lakehouse-engine-secret http://localhost:28080/realms/iceberg/protocol/openid-connect/token \| jq -r .access_token); curl -s -H "Authorization: Bearer $T" http://localhost:28181/management/v1/info \| jq -r '."authz-backend"'` | `openfga` |
| lakekeeper-e2e-harness | `LH_LAKEKEEPER_FIXTURE_CAPTURE=1 make test-e2e-lakekeeper && git status --short crates/lakehouse-catalog/tests/fixtures` | No output from `git status` after the committed capture |
| lakekeeper-e2e-harness | `$C down -v && cargo test -p lakehouse-engine --features lakekeeper-e2e --test e2e_lakekeeper_test -- --test-threads=1` | The run fails, and no test is reported ignored |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e-lakekeeper` and the Azure suite on its stack | 0 failures |
| Lint | `cargo clippy --all-targets -- -D warnings` and `cargo clippy -p lakehouse-engine --all-targets --features lakekeeper-e2e -- -D warnings` | 0 errors or warnings |
| Format | `cargo fmt --check` | No changes |
| Plan | `speq plan validate test-lakekeeper-batch-check-fixtures-two-principals` | Pass |
