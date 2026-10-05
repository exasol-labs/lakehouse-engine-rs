# Decisions: fix-connection-credential-exposure

## ADR: Reference the CONNECTION by name; the scan UDF resolves it

**ID:** reference-connection-by-name-scan-udf-resolves-it
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context

`EXPLAIN VIRTUAL` returns the adapter's pushdown SQL with no redaction, and the adapter serialized resolved credentials into that SQL.

### Decision

The pushdown SQL carries the `CATALOG_CONNECTION` name and `ALLOW_HTTP` instead of credentials. The scan UDF resolves the credentials with `ctx.connection()`.

### Consequences

This matches the fix in `exasol-virtual-schema` 4.0.0. `ctx.connection()` is one engine-local metadata request.

## ADR: No fallback when the script-scoped connection grant is absent

**ID:** no-fallback-when-script-scoped-connection-grant-absent
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context

A deployment upgrading to the reference-based design needs a new `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` before the scan UDF can resolve its credential.

### Decision

A missing grant fails at scan time with a named error. There is no inline-credential fallback.

### Consequences

This is a breaking deployment change. The installer template prints both grants.

## ADR: Seal the vended storage block under a key derived from the CONNECTION

**ID:** seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context

A vended credential has no CONNECTION name that a scan UDF can reference, so it cannot use the connection-reference path.

### Decision

The adapter seals the vended storage block with AES-256-GCM under a key derived by HKDF-SHA256 from the CONNECTION password, with a fresh 96-bit nonce. It refuses to seal unless at least one secret field is non-empty.

### Consequences

Sealing defeats plaintext reads but not offline cryptanalysis. This is acceptable because vended values are short-lived and the key material is what `ACCESS ON CONNECTION` already reveals.
