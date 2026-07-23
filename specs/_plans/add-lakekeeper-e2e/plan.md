# Plan: add-lakekeeper-e2e

## Summary

Add a CI-gated E2E suite that proves the lakehouse engine interoperates with a Lakekeeper
Iceberg REST catalog, OpenID-secured via Keycloak and backed by MinIO. The suite
authenticates with the engine's existing OAuth2 client-credentials CONNECTION fields and
reads data files under both static and vended (STS) S3 credentials, each a hard pass/fail
requirement.

## Design

### Context

Lakekeeper is a widely-used open-source Iceberg REST catalog with three traits the current
E2E baseline (an unauthenticated `apache/iceberg-rest-fixture`) never exercises: it is
OpenID-secured (no unauthenticated mode is documented), it is multi-warehouse (tables are
addressed under a per-warehouse `overrides.prefix`), and it serves the REST API under a
base-path prefix (`/catalog`). The engine already implements every mechanism Lakekeeper
needs — the OAuth2 client-credentials grant (`vs-adapter/rest-catalog-oauth-auth`), the
`GET /v1/config?warehouse=` prefix negotiation (`resolve_load_table_prefix`), the
`X-Iceberg-Access-Delegation: vended-credentials` header and STS extraction
(`vs-adapter/pushdown-planning-cloud-credentials`) — but only against AWS Glue and the
unauthenticated fixture, never against an OIDC-secured multi-warehouse catalog. This plan
proves the interoperation end-to-end and captures the base-path/per-warehouse-prefix
contract in the adapter spec.

- **Goals** — Verify Lakehouse-over-Lakekeeper end-to-end: OIDC catalog auth, MinIO object
  storage, correct projection/filter/LIMIT results, under both static and vended S3
  credentials. Reuse the existing CONNECTION field shape without inventing new fields.
  Verify continuously in CI.
- **Non-Goals** — No new adapter capability, no new CONNECTION field, no change to the fast
  unauthenticated `exasol-e2e` baseline, no Lakekeeper-specific catalog client code path
  (Lakekeeper is reached through the same `iceberg-catalog-rest` client as every REST
  catalog), no S3 remote-signing support (the engine reads with static or vended credentials
  only).

### Decision

The Iceberg REST OpenAPI spec defines `GET /v1/config` returning `overrides` (with an
optional `prefix`) and the `X-Iceberg-Access-Delegation: vended-credentials` header carrying
credentials back in the `LoadTableResponse.config` field. Lakekeeper's `/catalog` base path
and per-warehouse `prefix` are spec-compliant uses of these mechanisms; the engine already
consumes them. This plan therefore adds no adapter code on the expected (green) path — its
weight is the harness, the Docker stack, and CI.

Two distinct OAuth2 client-credentials implementations reach Keycloak on the green path, and
both MUST be verified independently: `iceberg-catalog-rest` 0.9.1's built-in OAuth2 client
(`credentials.rs:106-119`, used by the createVirtualSchema enumeration path) and the adapter's
own `oauth2_client_credentials_grant` (`credentials.rs:247`, used by the scan/file-resolution
path). The enumeration test (task 5.2) exercises the built-in client; the scan tests (tasks 5.3,
5.4) exercise the self-issued grant. If task 6's contingent interop-fix is needed,
`iceberg-catalog-rest`'s OAuth2-vs-external-IdP behavior is the primary candidate.

If implementation surfaces a genuine interop gap (for example a base-path URL malformation), the
fix lands in this plan as a `CHANGED` delta on the affected `vs-adapter/*` feature; a deliberate
trade-off instead becomes a tracked GitHub issue cited inline in the spec, never a silent gap.
MinIO STS enablement is not such a gap: it is a deterministic stack-configuration step (task 1.3),
so the vended-credential path is a hard requirement, not best-effort. See Iceberg-spec compliance
below.

#### Architecture

```
┌──────────────┐   OAuth2 client-creds   ┌───────────┐
│   Keycloak   │◀───────(token)──────────│  Exasol   │
│ (realm seed) │                         │  VS + UDF │
└──────────────┘                         └─────┬─────┘
       ▲ JWKS/aud validate                     │ REST /catalog (bearer)
       │                                        ▼
┌──────┴───────┐   /v1/config?warehouse   ┌───────────┐   loadTable   ┌────────┐
│  Lakekeeper  │◀────────prefix──────────▶│ iceberg-  │──────────────▶│  MinIO │
│  + Postgres  │   mgmt API: bootstrap,   │ rest cli  │  static/STS   │  (S3)  │
└──────────────┘   create-warehouse       └───────────┘   data reads  └────────┘
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Compose overlay file | `docker-compose.lakekeeper.yml` layered on the base file | Adds Lakekeeper/Postgres/Keycloak, reuses base `minio`+`exasol`, leaves the baseline file untouched |
| In-process provisioning | `tests/common` harness bootstraps Lakekeeper + creates warehouses | Mirrors the existing SLC/VS in-process setup; keeps compose lean |
| Existing-field reuse | OAuth2 `client_id`/`client_secret`/`oauth2_server_uri`/`scope` | Proves interop through the shipped CONNECTION shape; no schema change |
| Two warehouses | static (delegation off) + vended (`sts-enabled`) | Covers both shipped S3 credential modes; each warehouse is one management-API POST |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Additive `lakekeeper-e2e` feature + overlay compose | Replace the baseline REST fixture; fold Lakekeeper into the baseline stack | Keeps the fast unauthenticated baseline unchanged and isolates the heavier OIDC stack |
| Dedicated CI job `e2e-lakekeeper` | Opt-in-only (never in CI, like `cloud-e2e`); fold into the existing `e2e` job | The stack is all local containers (unlike `cloud-e2e`'s AWS need), so continuous CI verification is achievable; a separate job protects the baseline job's stability |
| Keycloak as the IdP | Lakekeeper built-in auth (none documented); a mock token endpoint | Keycloak is Lakekeeper's documented reference IdP and exercises a real OAuth2 client-credentials grant against the engine's shipped code |
| Test both static and vended S3 credentials as hard requirements | Vended-only (Lakekeeper default); static-only (closest to baseline); vended as best-effort with a skip-if-flaky off-ramp | Both are shipped engine credential modes (mission Capability 8); vended proves the STS path on a non-Glue catalog, static the direct-credential path. MinIO's STS AssumeRole endpoint is configured deterministically in the stack (task 1.3), so vended is a hard pass/fail requirement, not best-effort |

## Iceberg Specification Compliance

Per `CLAUDE.md`, checked against the Apache Iceberg REST Catalog OpenAPI specification
(`apache/iceberg/open-api/rest-catalog-open-api.yaml`), the normative surface this plan
depends on:

- **`GET /v1/config`** returns a `CatalogConfig` with `defaults` and `overrides`; clients merge
  `client-defaults < server-defaults < server-overrides`. The `warehouse` query parameter selects
  the warehouse; `overrides.prefix` (when present) prepends to all subsequent routes. Lakekeeper's
  per-warehouse `prefix` and `/catalog` base path are spec-conformant uses of this endpoint. The
  engine already consumes `overrides.prefix` in `resolve_load_table_prefix`.
- **Access delegation**: the `X-Iceberg-Access-Delegation: vended-credentials` request header is in
  the spec; delegated credentials are returned in `LoadTableResponse.config`. The engine already
  sends this header and extracts credentials in `pushdown/credentials.rs`.

This plan touches catalog-protocol behavior, not the Iceberg table format (data files, manifests,
schema evolution, row-level deletes); the table read semantics are unchanged, so the table-spec
compliance surface is not affected. No deviation is introduced; no tracked exception is required.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| packaging/lakekeeper-e2e-harness | NEW | `packaging/lakekeeper-e2e-harness/spec.md` |
| vs-adapter/rest-catalog-oauth-auth | CHANGED | `vs-adapter/rest-catalog-oauth-auth/spec.md` |

## Dependencies

- Lakekeeper container image (`quay.io/lakekeeper/catalog`), PostgreSQL 17, Keycloak, MinIO
  (already in the stack), Exasol (already in the stack).
- A pre-seeded Keycloak realm-export JSON: realm, a confidential client with the
  client-credentials grant enabled, and an audience mapper emitting `aud` = Lakekeeper's
  `LAKEKEEPER__OPENID_AUDIENCE`.
- No new Rust production dependency. Test-only: reuse `reqwest` (already a dev-dependency) for the
  Keycloak token request and Lakekeeper management API calls.

## Implementation Tasks

1. Stack
   1. Add `docker-compose.lakekeeper.yml` overlay: `lakekeeper` (`serve`), `lakekeeper-migrate`
      (`migrate`, run-once), `lakekeeper-db` (PostgreSQL), and `keycloak` (with realm import),
      on the shared `lakehouse` network with static IPs and health checks; reuse the base
      `minio` and `exasol` services. [expert]
   2. Add the Keycloak realm-export JSON (realm, confidential client, client-credentials grant,
      audience mapper matching `LAKEKEEPER__OPENID_AUDIENCE`) under `scripts/`. [expert]
   3. Configure MinIO for STS AssumeRole vending: enable MinIO's STS/AssumeRole endpoint and
      create the IAM policy/role Lakekeeper assumes to vend short-lived credentials scoped to the
      warehouse bucket; expose the role/policy so `lakekeeper_create_warehouse` references it for
      the `sts-enabled` warehouse. Without this step the vended-credential path cannot pass. [expert]
2. Feature flag & build wiring
   1. Add the `lakekeeper-e2e` cargo feature to `crates/lakehouse-engine/Cargo.toml`.
   2. Gate the new stack helpers and test binary on `feature = "lakekeeper-e2e"`; add a
      `make test-e2e-lakekeeper` target that brings up the overlay stack and runs the binary
      single-threaded.
3. Harness helpers (`tests/common`)
   1. Extend `CatalogConnectionPassword` (in `stack.rs`) to serialize the optional catalog-auth
      fields `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, keeping every
      new field optional and defaulted-absent so existing callers are unchanged.
   2. Add a `lakekeeper` module: readiness waits for Keycloak and Lakekeeper; a
      `keycloak_client_credentials_token()` helper; `lakekeeper_bootstrap()` and
      `lakekeeper_create_warehouse(profile)` calling the management API with the bearer token;
      and a `lakekeeper_connection_password(warehouse_name, vended)` builder. [expert]
   3. Parameterize VS creation so the Lakekeeper CONNECTION password, warehouse-name, and
      namespace are passed in without re-declaring script/SLC provisioning (extend `VsProps`
      or add `create_virtual_schema_with_password`), preserving the shared-harness invariant.
4. Seeding
   1. Add an authenticated seed-catalog variant: parameterize `build_seed_catalog`
      (`tests/common/seed.rs:133`, which currently hardcodes static `minioadmin` S3 creds and
      injects no catalog auth) with optional OAuth2 client-credentials and storage credentials, so
      seeding can target the OAuth2-secured Lakekeeper warehouse. This is a genuine extension, not
      a plain reuse. [expert]
   2. Seed an Iceberg table into each Lakekeeper warehouse through that authenticated catalog:
      create the namespace and append data files via `iceberg-catalog-rest` against Lakekeeper.
5. Test binary (`tests/e2e_lakekeeper_test.rs`, `#[cfg(feature = "lakekeeper-e2e")]`)
   1. `OnceLock`-guarded setup: bring-up waits, bootstrap, warehouse creation, seeding, SLC/`.so`/
      script provisioning, and VS creation for both warehouses.
   2. createVirtualSchema-over-OIDC enumeration test; confirms `iceberg-catalog-rest`'s built-in
      OAuth2 client authenticates against Keycloak.
   3. Static-credential projection/filter/LIMIT correctness test; confirms the adapter's own
      `oauth2_client_credentials_grant` (scan/file-resolution path) authenticates against Keycloak.
   4. Vended-credential (STS) projection/filter correctness test; assert results equal the
      static-warehouse query and that the access-delegation path is exercised. [expert]
   5. Fail-not-skip assertion when the stack is down.
   6. Assert no credential value appears in captured output.
6. Interop-gap handling (contingent) [expert]
   1. If a gap surfaces (base-path URL malformation, STS-against-MinIO failure, `aud`/prefix
      mismatch), fix it as a `CHANGED` delta on the affected `vs-adapter/*` feature within this
      plan, or record a deliberate trade-off as a tracked GitHub issue cited inline in the spec.
7. CI
   1. Add an `e2e-lakekeeper` job mirroring the `e2e` job's setup (disk cleanup, `.so` download,
      userns sysctl) that brings up the overlay stack and runs `make test-e2e-lakekeeper`, with
      log dumping on failure and `docker compose ... down -v` cleanup.
   2. Set per-service health-gate timeouts (Keycloak, Lakekeeper-db, Lakekeeper-migrate,
      Lakekeeper, MinIO, Exasol) and a wall-clock CI budget for the job; fail fast when a service
      does not become healthy within its timeout, so a stuck stack surfaces as a clear error rather
      than a silent long hang.
8. Docs
   1. Add `docs/catalogs.md` (or extend the existing catalog docs) listing Lakekeeper as a
      tested catalog: OIDC client-credentials auth, MinIO backing, static and vended S3 modes.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3, 2.1, 3.1 |
| Group B | 2.2, 3.2, 3.3, 4.1, 4.2 |
| Group C | 5.1–5.6 |
| Group D | 7.1, 7.2, 8.1 |

Sequential dependencies:
- Group A → Group B (helpers depend on the feature flag and the extended password struct)
- Within Group B, task 4.2 (seed tables) depends on task 4.1 (authenticated seed-catalog variant)
- Group B → Group C (the test binary depends on the harness helpers and seeding)
- Group C → Group D (CI and docs finalize once the suite is green)
- Task 6 is contingent and interleaves with Group C only if a gap surfaces.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| — | — | None. This plan is additive; it removes no existing code or test. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Harness bootstraps Lakekeeper and creates the MinIO-backed warehouses | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_bootstrap_and_warehouses_provision` |
| createVirtualSchema enumerates Lakekeeper tables over OAuth2 client-credentials auth | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_create_virtual_schema_lists_tables_over_oidc` |
| End-to-end scan over a static-credential Lakekeeper warehouse returns correct rows | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_static_creds_projection_filter_limit` |
| End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_creds_projection_filter` |
| Lakekeeper suite fails when the stack is unavailable | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_suite_fails_when_stack_unavailable` |
| Lakekeeper binary provisions the scan path from the shared harness definition | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_binary_uses_shared_harness_provisioning` |
| OAuth2 client-credentials path resolves tables from a multi-warehouse catalog served under a base path | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_oauth_prefix_under_base_path_resolves` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| packaging/lakekeeper-e2e-harness | `docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d --wait minio exasol keycloak lakekeeper-db lakekeeper-migrate lakekeeper && make test-e2e-lakekeeper` | The `e2e_lakekeeper_test` binary passes; no credential value appears in output |
| vs-adapter/rest-catalog-oauth-auth | `cargo test --features lakekeeper-e2e --test e2e_lakekeeper_test lakekeeper_oauth_prefix_under_base_path_resolves -- --nocapture` | Table resolves via `/catalog/v1/{prefix}/…`; secret not printed |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E (Lakekeeper) | `make test-e2e-lakekeeper` | 0 failures; fails (not skips) if stack down |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
