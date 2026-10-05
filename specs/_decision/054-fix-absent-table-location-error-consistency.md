# Decisions: fix-absent-table-location-error-consistency

## ADR: An absent table location is a hard error on every path, and the REST `warehouse` is never a storage anchor

**ID:** absent-table-location-hard-error-every-path
**Plan:** fix-absent-table-location-error-consistency
**Status:** Accepted

### Context

The absent-location check sat inside the vended-credentials branch, so the non-vended path silently resolved an empty table root. The Iceberg table spec marks `location` required in v1, v2, and v3, so an absent location is a malformed catalog response regardless of credentials. The REST `warehouse` only builds the `loadTable` URL prefix and denotes no object store.

### Decision

The adapter rejects a `loadTable` response with an empty `location` through one check before the vended/static split, returning a `UdfError::User`. No CONNECTION-derived value, including `warehouse` and `endpoint`, may substitute for it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the check vended-only | Rejected: the non-vended path would still resolve an empty table root and change the wire encoding of every file path |
| Warn and continue | Rejected: an empty root silently changes the encoding of every file path |

### Consequences

Spec-conformant catalogs are unaffected. A malformed response now fails at plan time on both paths, and the error text is path-independent, so it does not point operators at a credentials fault.
