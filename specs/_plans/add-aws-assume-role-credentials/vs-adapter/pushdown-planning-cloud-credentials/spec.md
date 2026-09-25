<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer for a CONNECTION that names no `aws_assume_role_arn`: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec. A CONNECTION that names a role resolves its catalog-signing and storage credentials under `vs-adapter/connection-credentials-assume-role` instead, whatever its `use_vended_credentials` states, so every scenario below, except § "SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request", describes a CONNECTION that names no role.
<!-- /DELTA:CHANGED -->

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md`.

## Scenarios

### Scenario: SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and set `warehouse` to a bare AWS account id (e.g. `123456789012`)
* *WHEN* the adapter issues a self-issued catalog HTTP request under SigV4 — the `loadTable` GET that resolves the file list during `pushdown`, or the namespace/table list GETs that enumerate tables during `createVirtualSchema`
* *THEN* the adapter SHALL address the catalog under the REST prefix `catalogs/{warehouse}`, derived by unconditionally prepending `catalogs/` to the configured `warehouse`, so account id `123456789012` yields the path segment `catalogs/123456789012`
* *AND* the adapter SHALL apply this identical derived prefix on both the `loadTable` path and the namespace/table enumeration path, from one shared derivation
* *AND* the adapter MUST NOT contact the `/v1/config` endpoint to resolve the prefix on the SigV4/Glue path
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message
