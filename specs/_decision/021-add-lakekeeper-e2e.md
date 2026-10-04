# Decisions: add-lakekeeper-e2e

## ADR: Additive `lakekeeper-e2e` feature, not a baseline replacement

**ID:** additive-lakekeeper-e2e-feature-not-baseline-replacement
**Plan:** `add-lakekeeper-e2e`
**Status:** Accepted

### Context

The `exasol-e2e` baseline runs against an unauthenticated REST fixture and is fast by design. Lakekeeper needs Postgres and Keycloak on top of MinIO and Exasol, which the baseline should not carry.

### Decision

Lakekeeper E2E runs under its own cargo feature with an overlay compose file. The baseline suite and stack stay unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Replace the baseline fixture with Lakekeeper | Rejected: loses the fast unauthenticated smoke-test path |
| Fold Lakekeeper services into the baseline compose file | Rejected: couples every baseline run to Postgres and Keycloak bring-up |

### Consequences

The baseline keeps its speed and a clean failure signal.

## ADR: Verify Lakekeeper continuously in a dedicated CI job

**ID:** verify-lakekeeper-continuously-dedicated-ci-job
**Plan:** `add-lakekeeper-e2e`
**Status:** Accepted

### Context

Unlike `cloud-e2e`, which needs real AWS credentials, the Lakekeeper stack runs entirely in local Docker containers, so CI can run it continuously.

### Decision

A dedicated `e2e-lakekeeper` CI job runs the Lakekeeper suite on every CI run, with per-service health-gate timeouts, a wall-clock budget, log dumping on failure, and teardown.

### Options Considered

| Option | Verdict |
|--------|---------|
| Opt-in only, like `cloud-e2e` | Rejected: the goal is to verify Lakekeeper works continuously |
| Fold into the existing `e2e` job | Rejected: couples the baseline job's runtime and stability to the heavier stack |

### Consequences

CI gains one job, and the timeouts make a stuck stack fail fast.

## ADR: Reuse existing OAuth2 CONNECTION fields for Lakekeeper; no schema change

**ID:** reuse-oauth2-connection-fields-no-lakekeeper-schema-change
**Plan:** `add-lakekeeper-e2e`
**Status:** Accepted

### Context

Lakekeeper uses the standard OAuth2 client-credentials grant and the standard `/v1/config?warehouse=` prefix mechanism, both already implemented in the adapter.

### Decision

Lakekeeper uses the existing OAuth2 CONNECTION fields, and the warehouse name goes in the existing `warehouse` field. No Lakekeeper-specific field is added.

### Options Considered

| Option | Verdict |
|--------|---------|
| Lakekeeper-specific auth or warehouse fields | Rejected: existing fields already express its needs |

### Consequences

Two adapter interop gaps found during implementation (prefix location, vended S3 endpoint and path-style) were fixed as `CHANGED` deltas on `vs-adapter/rest-catalog-oauth-auth` and `vs-adapter/pushdown-planning-cloud-credentials`, not as new CONNECTION fields.
