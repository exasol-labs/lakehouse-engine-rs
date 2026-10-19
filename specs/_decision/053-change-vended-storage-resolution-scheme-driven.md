# Decisions: change-vended-storage-resolution-scheme-driven

## ADR: Two selectors on disjoint inputs, not one selection site

**ID:** two-vended-static-selectors-on-disjoint-inputs
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

`vs-adapter/storage-backend-enum` named `storage_block` as the only place a backend is selected from input. The vended path now selects its variant from the table location's URI scheme, so that clause is misleading. A single selector would need CONNECTION parsing deferred past `loadTable` for every request, and still could not serve a request that resolves no table.

### Decision

`storage_block` stays the static selector, reads the CONNECTION credential shape, and runs only when `use_vended_credentials` is false. `resolve_vended_storage` is the vended selector, reads the table location scheme from the `loadTable` response, and runs only when vending is true. One decision point, the `use_vended_credentials` branch in `resolve_file_list`, chooses between them.

### Options Considered

| Option | Verdict |
|--------|---------|
| One selector that receives the URI scheme | Rejected: the scheme is known only after `loadTable`, which runs later, once per table, and never on `createVirtualSchema` |
| The vended arm mutates the payload `storage_block` returned | Rejected: the feature already forbids reaching into the payload to finish construction |
| Call the scheme switch "not a selection" and keep the one-site clause | Rejected: a false clause on a credentials path is worse than superseding it |

### Consequences

The spec clause becomes "exactly two selectors on disjoint inputs, one decision point". No path runs both selectors, and neither overrides the other.

## ADR: `ALLOW_HTTP` stays the operator's consent gate for plaintext transport

**ID:** allow-http-threaded-as-vended-selector-parameter
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

Deriving `allow_http` from the vended endpoint's scheme would permit plaintext to any endpoint a catalog names as `http://`. The shipped rule defaults `allow_http` to false when `ALLOW_HTTP` is absent, so it permits plaintext to no endpoint. A misconfigured or compromised catalog could then put STS credentials in cleartext with no operator control.

### Decision

The resolved `ALLOW_HTTP` virtual-schema property is passed into `resolve_vended_storage` as its own boolean. A vended `http://` `s3.endpoint` or an `abfs://` anchor is honoured only when it is true. Otherwise the function returns a `UdfError::User` naming the plaintext scheme and the `ALLOW_HTTP` property.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive `allow_http` from the vended endpoint's scheme | Rejected: a security regression in the default configuration |
| Read it from the base backend or an accessor | Rejected: reopens the CONNECTION-derived read that the dropped base-backend parameter closes |

### Consequences

Passing the value adds a 4-tuple to `resolve_connection_config` (2 call sites) and one boolean to four functions, following the convention for `s3_max_connections` and the DataFusion tuning knobs. Removing `ALLOW_HTTP` from the vended path was a planner decision, not an interview outcome.

