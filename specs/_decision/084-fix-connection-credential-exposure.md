# Decisions: fix-connection-credential-exposure

## ADR: Reference the CONNECTION by name; the scan UDF resolves it

**ID:** reference-connection-by-name-scan-udf-resolves-it | **Status:** Accepted

The adapter serialized resolved credentials into pushdown SQL, visible via `EXPLAIN VIRTUAL`. The VS contract has no bind parameter or second field. Carry the `CATALOG_CONNECTION` name and `ALLOW_HTTP` instead; the scan UDF calls `ctx.connection()` (engine-local, not a catalog round-trip). Matches `exasol-virtual-schema` 4.0.0's fix for the same class.

## ADR: No fallback when the script-scoped connection grant is absent

**ID:** no-fallback-when-script-scoped-connection-grant-absent | **Status:** Accepted

A missing `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` fails at scan time with a named error. No inline-credential fallback. A hard failure is the only outcome an operator cannot silently ignore.

## ADR: Resolve scan storage once; the redaction secret set follows the resolved value

**ID:** resolve-scan-storage-once-per-invocation-secret-set-follows-resolved-value | **Status:** Accepted

One `resolve_scan_storage` at the top of `run_scan` resolves both join sides into `ResolvedScanStorage`, which owns `all_secret_values()`. The wire wrapper exposes no secret accessor — a site left reading the unresolved value fails to compile.

## ADR: The scan UDF reads a storage-only projection; derivation stays in the adapter module

**ID:** scan-udf-storage-only-projection-derivation-stays-in-adapter | **Status:** Accepted

All CONNECTION-interpretation functions stay in `adapter::connection`. `lakehouse-catalog` gains `StorageCreds` (nine storage fields only) with `from_json` and `backend`. The UDF calls those — never `ConnectionCreds` — so catalog-auth fields are structurally excluded.

## ADR: Seal the vended storage block under a key derived from the CONNECTION

**ID:** seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material | **Status:** Accepted

A vended credential has no CONNECTION name to reference. Seal with AES-256-GCM under HKDF-SHA256 from the CONNECTION password, fresh 96-bit nonce per seal. Gate: at least one secret field non-empty. Defeats plaintext reads, not offline cryptanalysis — acceptable because vended values are short-lived and the key material is what `ACCESS ON CONNECTION` already reveals.

