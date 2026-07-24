# Decisions: add-lakekeeper-e2e

## ADR: Additive `lakekeeper-e2e` feature, not a baseline replacement

**ID:** additive-lakekeeper-e2e-feature-not-baseline-replacement
**Plan:** `add-lakekeeper-e2e`
**Status:** Accepted

### Context

The existing `exasol-e2e` baseline runs against an unauthenticated `apache/iceberg-rest-fixture`
and is fast by design. Lakekeeper is OpenID-secured (Keycloak), multi-warehouse, and served under
a base-path prefix — proving interop with it requires Postgres and Keycloak alongside the existing
MinIO and Exasol services, weight the baseline suite should not carry.

### Decision

Add a dedicated `lakekeeper-e2e` cargo feature and an overlay compose file
(`docker-compose.lakekeeper.yml`) layered on the base compose file. Leave the unauthenticated
`exasol-e2e` baseline and its stack untouched.

### Options Considered

| Option | Verdict |
|--------|---------|
| Additive `lakekeeper-e2e` feature + overlay compose | ✓ Chosen — keeps the fast unauthenticated baseline unchanged and isolates the heavier OIDC stack |
| Replace the `apache/iceberg-rest-fixture` baseline with Lakekeeper | ✗ Rejected — loses the fast, unauthenticated smoke-test path |
| Fold Lakekeeper services into the baseline `docker-compose.yml` | ✗ Rejected — couples every baseline run to Postgres + Keycloak bring-up |

### Consequences

The baseline suite's speed and failure signal stay clean. Lakekeeper interop is proven end-to-end
in its own suite, gated behind its own cargo feature, with no risk to the existing baseline.

## ADR: Verify Lakekeeper continuously in a dedicated CI job

**ID:** verify-lakekeeper-continuously-dedicated-ci-job
**Plan:** `add-lakekeeper-e2e`
**Status:** Accepted

### Context

Unlike `cloud-e2e` (which needs real AWS credentials and is opt-in-only), the Lakekeeper stack —
Lakekeeper, Postgres, Keycloak, MinIO, Exasol — runs entirely in local Docker containers, so
continuous CI verification is achievable without external dependencies.

### Decision

Add an `e2e-lakekeeper` CI job that mirrors the `e2e` job's setup (disk cleanup, `.so` download,
userns sysctl), brings up the overlay stack, and runs `make test-e2e-lakekeeper`, with per-service
health-gate timeouts, a wall-clock CI budget, log dumping on failure, and `docker compose ... down
-v` cleanup — as its own job, separate from the baseline `e2e` job.

### Options Considered

| Option | Verdict |
|--------|---------|
| Dedicated always-on CI job `e2e-lakekeeper` | ✓ Chosen — the stack is all local containers, so continuous verification catches regressions; a separate job protects the baseline job's stability |
| Opt-in-only (never in CI, like `cloud-e2e`) | ✗ Rejected — the user's intent is to verify Lakekeeper actually works, not just at plan time |
| Fold into the existing `e2e` job | ✗ Rejected — couples the baseline job's runtime and stability to the heavier OIDC stack |

### Consequences

Lakekeeper interop is regression-tested on every CI run, at the cost of one additional CI job with
its own health-gate timeouts and wall-clock budget to fail fast rather than hang on a stuck stack.

## ADR: Reuse existing OAuth2 CONNECTION fields for Lakekeeper; no schema change

**ID:** reuse-oauth2-connection-fields-no-lakekeeper-schema-change
**Plan:** `add-lakekeeper-e2e`
**Status:** Accepted

### Context

Research confirmed Lakekeeper uses the standard OAuth2 client-credentials grant and the standard
`/v1/config?warehouse=` prefix mechanism, both already implemented in the adapter
(`vs-adapter/rest-catalog-oauth-auth`). The engine needs no new CONNECTION field to reach
Lakekeeper.

### Decision

Use the existing `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` CONNECTION fields
for catalog auth, and carry the Lakekeeper warehouse-name in the existing `warehouse` field. Add no
Lakekeeper-specific CONNECTION field.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse existing OAuth2 CONNECTION fields and `warehouse` | ✓ Chosen — proves interop through the shipped CONNECTION shape; no schema change |
| Introduce Lakekeeper-specific auth or warehouse fields | ✗ Rejected — unnecessary given the existing fields already express Lakekeeper's auth and warehouse-naming needs |

### Consequences

Lakekeeper interop is proven through the exact CONNECTION shape shipped for every other REST
catalog. Two genuine adapter interop gaps (prefix location, vended S3 endpoint/path-style) were
found and fixed during implementation — tracked as `CHANGED` deltas on `vs-adapter/rest-catalog-oauth-auth`
and `vs-adapter/pushdown-planning-cloud-credentials`, not as new CONNECTION fields.
