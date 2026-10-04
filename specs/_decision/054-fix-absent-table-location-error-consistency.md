# Decisions: fix-absent-table-location-error-consistency

## ADR: An absent table location is a hard error on every path, and the REST `warehouse` is never a storage anchor

**ID:** absent-table-location-hard-error-every-path
**Plan:** fix-absent-table-location-error-consistency
**Status:** Accepted

### Context

Commit `6d08c8a` sited the absent-table-location check inside the `if creds.use_vended_credentials`
arm of `resolve_file_list`, so the non-vended path still tolerated an absent `location` silently
and resolved an empty `table_root`. The Apache Iceberg table spec marks `location` `_required_` in
the v1, v2, and v3 columns of `format/spec.md`'s Table Metadata field table, so an absent location
is a malformed catalog response independently of how credentials are obtained. The REST `warehouse`
builds only the `loadTable` URL prefix — a bare AWS account id under Glue, a warehouse name or
per-warehouse UUID under Lakekeeper — and denotes no object store, so it cannot substitute for a
missing location.

### Decision

Reject a `loadTable` response carrying an empty table metadata `location` with a `UdfError::User`,
from one check that runs before the vended/static storage split. No CONNECTION-derived value —
`warehouse`, `endpoint`, or any other — may be substituted for it, with or without vended
credentials.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hoist the guard above the vended/static split, unconditional on every path | ✓ Chosen — matches the Iceberg spec's required-`location` guarantee and closes the non-vended gap the shipped state left open |
| Keep the check vended-only (the shipped state after commit `6d08c8a`) | ✗ Rejected — leaves the non-vended path to resolve an empty `table_root` silently, changing the wire encoding of every file path |
| Warning-and-continue on an absent location | ✗ Rejected — not considered viable; an empty root silently changes the wire encoding of every file path |

### Consequences

A spec-conformant catalog is unaffected: every Iceberg v1/v2/v3 `loadTable` response carries a
`location`, so no existing query reaches the new error. A malformed response now fails at plan
time on the non-vended path too, instead of resolving an empty table root and emitting every file
path as an absolute URI. The error text becomes path-independent, so an operator is never
misdirected toward a credentials fault when the actual cause is a malformed catalog response.

