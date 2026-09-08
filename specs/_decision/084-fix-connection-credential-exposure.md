# Decisions: fix-connection-credential-exposure

## ADR: Reference the CONNECTION by name; the scan UDF resolves it

**ID:** reference-connection-by-name-scan-udf-resolves-it | **Status:** Accepted

Carry the `CATALOG_CONNECTION` name and `ALLOW_HTTP` instead of resolved credentials; the scan UDF calls `ctx.connection()` (engine-local). Matches `exasol-virtual-schema` 4.0.0's fix.

## ADR: No fallback when the script-scoped connection grant is absent

**ID:** no-fallback-when-script-scoped-connection-grant-absent | **Status:** Accepted

A missing `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` fails at scan time with a named error. No inline-credential fallback.

## ADR: Resolve scan storage once; the redaction secret set follows the resolved value

**ID:** resolve-scan-storage-once-per-invocation-secret-set-follows-resolved-value | **Status:** Accepted

One `resolve_scan_storage` at the top of `run_scan` resolves both join sides into `ResolvedScanStorage`, which owns `all_secret_values()`. The wire wrapper exposes no secret accessor — compile failure, not an empty redaction set.

## ADR: Seal the vended storage block under a key derived from the CONNECTION

**ID:** seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material | **Status:** Accepted

AES-256-GCM under HKDF-SHA256 from the CONNECTION password, fresh 96-bit nonce. Gate: at least one secret field non-empty. Defeats plaintext reads, not offline cryptanalysis — acceptable because vended values are short-lived and the key material is what `ACCESS ON CONNECTION` already reveals.
