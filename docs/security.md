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

The grantee is the **virtual schema's OWNER**, not the querying user. Run these **before** `CREATE VIRTUAL SCHEMA`. A DBA owner (`SYS`) needs none of this. `CREATE OR REPLACE CONNECTION` and `CREATE OR REPLACE SCRIPT` both drop the grant — re-issue after re-running the installer. `ALTER CONNECTION` preserves grants.

`EXPLAIN VIRTUAL` and pushdown-path errors carry the pushdown SQL verbatim — credential VALUES must never appear in it. See [Plan visibility versus plan execution](#plan-visibility-versus-plan-execution) for what else that plan text exposes and why reading it is safe while running it is not.

## Plan visibility versus plan execution

The pushdown plan `EXPLAIN VIRTUAL` returns is readable by any user who can query the virtual schema — it is not a secret. Safety comes from `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` being separable from `SELECT` on the virtual schema: running a script against real data needs BOTH that grant AND the script-scoped `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` grant from `## Privilege model` above — two independent gates, neither a substitute for the other.

Granting `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN`, or `EXECUTE ANY SCRIPT`, to a querying user makes every adapter-side predicate advisory for that user once they also hold connection access: they can submit a hand-edited plan directly instead of the one the adapter generated. `LAKEHOUSE_DISTRIBUTE_FILES` is not an independent path to that outcome — every generated plan calls it only from inside the `LAKEHOUSE_SCAN` invocation's `FROM` clause, so a user without `LAKEHOUSE_SCAN` execute (or `EXECUTE ANY SCRIPT`) is denied before the distributor script is ever reached, even when granted execute on it directly (verified live: see `specs/_plans/add-pushdown-plan-execution-boundary/notes/A.md` §6).

A reader with only `SELECT` on the virtual schema still sees, in the plan text: the table root, the bucket layout, file names and byte sizes, and the catalog CONNECTION name. None of that is a credential. A sealed vended-credential envelope, when present, travels in the plan as ciphertext — readable but not openable without the CONNECTION password (see [Sealed vended-credential envelope](#sealed-vended-credential-envelope-378) below).

## Sealed vended-credential envelope (#378)

A vended credential has no CONNECTION name to reference. It travels as AES-256-GCM ciphertext (HKDF-SHA256 key from the CONNECTION password, fresh 96-bit nonce). Vending without key material is refused at plan time.

## Rotation

Every query re-resolves the CONNECTION — no cache, no restart. Use `ALTER CONNECTION` (preserves grants). Sealed envelopes of in-flight vended queries fail the AEAD open — rotate when no vended query is in flight.
