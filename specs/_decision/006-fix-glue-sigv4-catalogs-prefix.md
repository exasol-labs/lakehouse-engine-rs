# Decisions: fix-glue-sigv4-catalogs-prefix

## ADR: Derive the Glue REST Prefix in Code; Bare Account ID Stays the User-Facing Warehouse

**ID:** derive-glue-rest-prefix-bare-account-id-warehouse
**Plan:** fix-glue-sigv4-catalogs-prefix
**Status:** Accepted

### Context

AWS Glue's Iceberg REST catalog requires the prefix `catalogs/{catalogId}`. The adapter passed the configured `warehouse` through unchanged, so a bare account id made Glue return a 400 error (#123).

### Decision

Under SigV4/Glue, the adapter derives `catalogs/{warehouse}` from the bare account id. Users supply the bare account id as `warehouse` everywhere.

### Options Considered

| Option | Verdict |
|--------|---------|
| Require users to enter `catalogs/{account-id}` | Rejected: diverges from other Iceberg clients and project docs, and exposes a Glue-specific convention |

### Consequences

The bare account id is the only correct `warehouse` value on every surface. Docs and deploy config need no workaround note.
