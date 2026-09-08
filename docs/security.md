[lakehouse-engine](../README.md) › [Docs](index.md) › Security

---

# Security

The catalog CONNECTION carries credentials that reach object storage and the catalog.

## Privilege model

Both scripts resolve the CONNECTION by name (`ctx.connection()`). Each needs a script-scoped grant:

```sql
GRANT ACCESS ON CONNECTION <conn> FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER TO <vs-owner>;
GRANT ACCESS ON CONNECTION <conn> FOR SCRIPT <schema>.LAKEHOUSE_SCAN   TO <vs-owner>;
```

The grantee is the **virtual schema's OWNER**, not the querying user — Exasol evaluates the check
against the owner when the script is reached through VS pushdown SQL. Run these **before**
`CREATE VIRTUAL SCHEMA`. A DBA owner (`SYS`) needs none of this.

`CREATE OR REPLACE CONNECTION` and `CREATE OR REPLACE SCRIPT` both drop the grant — re-issue after
re-running the installer. `ALTER CONNECTION` preserves grants. See
[install.md](install.md#point-the-vs-at-your-data) for the recommended role-based pattern.

`EXPLAIN VIRTUAL` and pushdown-path errors carry the pushdown SQL verbatim — credential VALUES must
never appear in it.

## Sealed vended-credential envelope (#378)

A vended credential has no CONNECTION name to reference. It travels as AES-256-GCM ciphertext
(HKDF-SHA256 key from the CONNECTION password, fresh 96-bit nonce). Vending without key material is
refused at plan time.

## Rotation

Every query re-resolves the CONNECTION — no cache, no restart. Use `ALTER CONNECTION` (preserves
grants). Sealed envelopes of in-flight vended queries fail the AEAD open — rotate when no vended
query is in flight.
