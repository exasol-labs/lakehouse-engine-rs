# Decisions: add-lakekeeper-e2e

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
