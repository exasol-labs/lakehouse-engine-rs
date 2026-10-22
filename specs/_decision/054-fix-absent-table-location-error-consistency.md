# Decisions: fix-absent-table-location-error-consistency

## ADR: An absent table location is a hard error on every path, and the REST `warehouse` is never a storage anchor

**ID:** absent-table-location-hard-error-every-path
**Plan:** fix-absent-table-location-error-consistency
**Status:** Accepted

### Context

The Iceberg table spec marks `location` required, so an absent location is a malformed catalog response whatever the credential mode. The REST `warehouse` only builds the `loadTable` URL prefix and denotes no object store.

### Decision

The adapter rejects a `loadTable` response with an empty `location` through one check before the vended and static paths split, as a user error. No CONNECTION value, including `warehouse` and `endpoint`, substitutes for it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the check vended-only | Rejected: the non-vended path would still resolve an empty table root |
| Warn and continue | Rejected: an empty root silently changes the encoding of every file path |

### Consequences

- Spec-conformant catalogs are unaffected.
- A malformed response fails at plan time on both paths, and the error text does not point operators at a credentials fault.
