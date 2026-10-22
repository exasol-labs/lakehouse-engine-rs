# Decisions: change-vended-storage-resolution-scheme-driven

## ADR: Two selectors on disjoint inputs, not one selection site

**ID:** two-vended-static-selectors-on-disjoint-inputs
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

The vended path selects its storage backend from the table location's URI scheme, which is known only after the catalog loads the table. The static path selects from the CONNECTION credential shape. One selector would need CONNECTION parsing deferred for every request, and could not serve a request that resolves no table.

### Decision

There are two selectors on disjoint inputs. The static selector reads the CONNECTION credential shape and runs only when vended credentials are off. The vended selector reads the table location scheme and runs only when they are on. One branch on that setting chooses between them.

### Options Considered

| Option | Verdict |
|--------|---------|
| One selector that receives the URI scheme | Rejected: the scheme is known only after the table load, which runs later, once per table, and never at virtual schema creation |
| The vended arm mutates the payload `storage_block` returned | Rejected: reaching into a built payload to finish construction is forbidden |
| Call the scheme switch "not a selection" and keep the one-site clause | Rejected: a false one-site claim on a credentials path misleads |

### Consequences

- No path runs both selectors, and neither overrides the other.

## ADR: `ALLOW_HTTP` stays the operator's consent gate for plaintext transport

**ID:** allow-http-threaded-as-vended-selector-parameter
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

Deriving plaintext permission from the vended endpoint's scheme would allow plaintext to any endpoint a catalog names as `http://`. A misconfigured or compromised catalog could then send STS credentials in cleartext with no operator control.

### Decision

The `ALLOW_HTTP` virtual-schema property stays the operator's consent gate and defaults to false. A vended `http://` endpoint or `abfs://` anchor is honoured only when it is true. Otherwise the adapter returns a user error naming the plaintext scheme and the property.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive `allow_http` from the vended endpoint's scheme | Rejected: permits plaintext by default |
| Read it from the base backend or an accessor | Rejected: reopens a CONNECTION-derived read |

### Consequences

- The value is threaded as an explicit parameter, like the other virtual-schema tuning properties.

