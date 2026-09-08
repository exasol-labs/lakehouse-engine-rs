# Decisions: fix-connection-credential-exposure

## ADR: Reference the CONNECTION by name; the scan UDF resolves it

**ID:** reference-connection-by-name-scan-udf-resolves-it
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
`EXPLAIN VIRTUAL` returns the adapter's pushdown SQL with no redaction, and the adapter serialized the resolved `StorageBackend` — including `access_key`/`secret_key` — straight into that SQL. The Exasol Virtual Schema contract permits exactly one pushdown response field (`{"type":"pushdown","sql":<string>}`), so a redacted placeholder, a schema property, an adapter note, and a bind parameter are all unavailable.

### Decision
Carry the `CATALOG_CONNECTION` property value and the resolved `ALLOW_HTTP` flag in the scan spec's storage block instead of the credential. The scan UDF calls `ctx.connection(name)` and applies the same derivation the adapter applies.

### Consequences
`ctx.connection()` is one engine-local metadata request, not a catalog round-trip. This matches the fix `exasol-virtual-schema` 4.0.0 used for the same class of defect (issue #24).

## ADR: No fallback when the script-scoped connection grant is absent

**ID:** no-fallback-when-script-scoped-connection-grant-absent
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
A deployment upgrading to the reference-based design needs a new `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` before the scan UDF can resolve its credential.

### Decision
A deployment missing the grant fails at scan time with an error naming the connection and the missing access. No code path reads an inline credential when the reference cannot be resolved.

### Consequences
A hard failure is the only outcome an operator cannot silently ignore. Matches `exasol-virtual-schema` 4.0.0's position.

## ADR: Resolve scan storage once per invocation; the redaction secret set follows the resolved value

**ID:** resolve-scan-storage-once-per-invocation-secret-set-follows-resolved-value
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
Once the spec carries a connection reference instead of a credential, `CommonScanSpec::all_secret_values` has nothing to yield — redaction would quietly go empty with every existing test still green.

### Decision
One `resolve_scan_storage` call at the top of `run_scan` resolves both join sides into a `ResolvedScanStorage`, which owns `all_secret_values()`. The wire wrapper exposes no secret accessor, so a site left reading the unresolved wire value fails to compile.

### Consequences
The secret set follows the credential rather than the spec, closing the path where the fix could silently weaken error-path redaction.

## ADR: The scan UDF reads a storage-only credential projection; derivation stays in the adapter module

**ID:** scan-udf-storage-only-projection-derivation-stays-in-adapter
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
Running `parse_creds` inside the UDF would materialize catalog-auth fields (`token`, `client_secret`) on every shard invocation, outside the storage-only redaction set. Moving `parse_creds` and `storage_block` into `lakehouse-catalog` would reverse `catalog-crate-structure`'s recorded prohibition.

### Decision
All six CONNECTION-interpretation functions stay in `lakehouse_engine::adapter::connection`. `lakehouse-catalog` gains a `StorageCreds` projection (nine storage fields only), `StorageCreds::from_json`, `StorageCreds::backend`, and one `From<&ConnectionCreds>` conversion. The scan UDF calls `from_json` then `backend` and never constructs a `ConnectionCreds`.

### Consequences
The exclusion of catalog-auth fields from the UDF is structural (enforced by the type's field list). What crosses the crate boundary is a credential TYPE, not a delivery-mechanism-aware function.

## ADR: Seal the vended storage block under a key derived from the CONNECTION

**ID:** seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
A vended credential comes from the `loadTable`/Unity response and has no CONNECTION name to reference, so the reference design alone cannot close issue #378.

### Decision
Under `use_vended_credentials`, the resolved `StorageBackend` is sealed with AES-256-GCM under a 32-byte key derived via HKDF-SHA256 from the CONNECTION password bytes, with a fresh random 96-bit nonce per encryption. The gate tests non-emptiness of a secret field on the CONNECTION password (`token`, `client_secret`, `secret_key`, `session_token`, `account_key`, or `sas_token`). When no field is non-empty and vending is enabled, planning is refused with a named error.

### Consequences
The guarantee defeats a plaintext read of `EXPLAIN VIRTUAL` or error text, not offline cryptanalysis. Acceptable because vended values are short-lived and prefix-scoped, and the key material is what `ACCESS ON CONNECTION` already reveals.

## ADR: The installer grants scan-script connection access to a deployment-scoped role

**ID:** installer-grants-connection-access-to-deployment-scoped-role-not-public
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
The scan UDF's new script-scoped connection grant needs a repeatable installer story. `GRANT ACCESS ANY CONNECTION` lets a grantee's own script resolve ANY connection; `... TO PUBLIC` cannot be scoped down later.

### Decision
The installer template creates one schema-qualified role, grants `ACCESS ON CONNECTION ... FOR SCRIPT` to that role once per script, and grants the role to the installing user.

### Consequences
Onboarding a further user is one `GRANT <role> TO <user>`. The cost: `CREATE ROLE` has no `IF NOT EXISTS` form, so the template must tell the operator to check `EXA_ALL_ROLES` before re-running.
