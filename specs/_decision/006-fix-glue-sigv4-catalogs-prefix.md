# Decisions: fix-glue-sigv4-catalogs-prefix

## ADR: Derive the Glue REST Prefix in Code; Bare Account ID Stays the User-Facing Warehouse

**ID:** derive-glue-rest-prefix-bare-account-id-warehouse
**Plan:** fix-glue-sigv4-catalogs-prefix
**Status:** Accepted

### Context

AWS Glue's Iceberg REST catalog requires the REST prefix in the `catalogs/{catalogId}`
form. The adapter's SigV4 path passed the configured `warehouse` straight through,
so a bare account id reached Glue unprefixed and Glue returned
`400 "Prefix must follow the 'catalogs/{catalogId}' format."` (#123).

### Decision

Under SigV4/Glue, derive `catalogs/{warehouse}` from the configured bare account id
inside the adapter. The user continues to supply the bare account id as `warehouse`
everywhere — docs, bench config, and deploy config.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive the prefix in code from the bare account id | ✓ Chosen — matches standard Iceberg-client behavior; keeps one documented input value correct across code, docs, bench, and deploy |
| Require users to enter `catalogs/{account-id}` themselves | ✗ Rejected — diverges from every other Iceberg client and this project's own docs; pushes a Glue-proprietary path convention onto the user |

### Consequences

A bare AWS account id is the sole correct `warehouse` value across every surface.
The adapter owns the Glue-proprietary `catalogs/` convention, so docs, bench
README, and Terraform config no longer need a manual workaround note.

## ADR: Glue-Only Derivation — Do Not Generalize to All SigV4 Catalogs

**ID:** glue-only-catalogs-prefix-derivation-no-sigv4-generalization
**Plan:** fix-glue-sigv4-catalogs-prefix
**Status:** Accepted

### Context

`CatalogAuth::Sigv4` is today exclusively the Glue path. Issue #123 flags the
prefix convention for other SigV4-style catalogs, such as S3 Tables, as unverified.

### Decision

Apply the `catalogs/{account-id}` derivation only to the Glue path. Do not
generalize the derivation to every `CatalogAuth::Sigv4` caller.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive the prefix only for the documented Glue path | ✓ Chosen — avoids assuming all SigV4 auth implies Glue's prefix format without verification |
| Derive the prefix for every `CatalogAuth::Sigv4` caller | ✗ Rejected — issue #123 flags the S3 Tables prefix convention as unverified; generalizing would risk a silent wrong-prefix bug for a catalog this codebase has not tested |

### Consequences

The derivation stays a scope-clarity boundary in prose, not a code branch, since
`CatalogAuth::Sigv4` is exclusively Glue today. A follow-up issue is needed before
extending SigV4 support to S3 Tables or another non-Glue SigV4 catalog.
