# Group A — live verification notes (task 2.2)

Verified against the local docker-compose stack (`lakehouse-engine-rs-2-exasol-1`,
`exapump --profile docker`, CREDEXP_VS/CREDEXP_OWNER/CREDEXP_READER provisioned by
`e2e_credential_exposure_test`).

## 1. `EXPLAIN VIRTUAL` cell layout (single-table projection+filter query)

Query: `SELECT ID, NAME, SCORE FROM CREDEXP_VS.EVENTS WHERE SCORE > 15.0`

`EXPLAIN VIRTUAL <query>` returns exactly 1 row, 4 columns:
`PUSHDOWN_ID, PUSHDOWN_SQL, PUSHDOWN_JSON, PUSHDOWN_INVOLVED_TABLES`.

- `PUSHDOWN_SQL` (2nd cell, 0-based index 1) is already a complete, directly
  submittable SQL statement — the adapter-generated
  `SELECT "LHVS".LAKEHOUSE_SCAN('{...}', files) EMITS (...) FROM (SELECT
  "LHVS".LAKEHOUSE_DISTRIBUTE_FILES(files) FROM (VALUES ...) AS shards(shard_key,
  files) GROUP BY shard_key)` text, with no other content mixed in.
- `PUSHDOWN_JSON` (3rd cell) is the echoed adapter exchange: a JSON array of
  `getCapabilities` request/response, the `pushdown` request, and the adapter's
  final response object (which itself repeats the same SQL under a `"sql"` key,
  JSON-escaped).
- `PUSHDOWN_INVOLVED_TABLES` (4th cell) is `"EVENTS"`.
- `explain_virtual_sql` flattens all 4 cells (dropping the non-string
  `PUSHDOWN_ID`) and space-joins the rest, so its output is
  `<PUSHDOWN_SQL text> <PUSHDOWN_JSON text> EVENTS` — the generated SQL text
  immediately followed by the JSON debug blob. Not submittable as-is.
- **Isolation rule for the new helper: select cell index 1 (`PUSHDOWN_SQL`) of
  row 0 directly** — do not flatten/join.

## 2. Reader denial error text (submitting the isolated PUSHDOWN_SQL statement directly)

Submitted the isolated `PUSHDOWN_SQL` text as `CREDEXP_READER` (no `EXECUTE ON
SCRIPT` grants held), both over native and websocket transport:

```
insufficient privileges for calling script (Session: <id>) (SQL state: 42500)
```

Stable substring to assert: `"insufficient privileges for calling script"`.
SQL state: `42500`. (Session id is non-deterministic — never assert on it.)

Note: revoking `LAKEHOUSE_ADAPTER`'s grant produces a *different* wording —
`"insufficient privileges for calling adapter script"` — because that failure
happens at adapter-invocation time, not scan/distributor invocation time. The
new test only revokes nothing (uses the existing owner grants as-is) and
submits the reader-captured plan directly, so the wording it hits is the
scan-script one (no "adapter") above.

## 3. DBA views for object/system privilege and role membership

- `EXA_DBA_OBJ_PRIVS` — columns `OBJECT_SCHEMA, OBJECT_NAME, OBJECT_TYPE,
  PRIVILEGE, GRANTEE, GRANTOR, OWNER`. Filter `GRANTEE = 'CREDEXP_READER' AND
  OBJECT_NAME = '<script>' AND OBJECT_TYPE = 'SCRIPT' AND PRIVILEGE =
  'EXECUTE'` → 0 rows today (reader only has a `SCHEMA`-level `SELECT` row).
- `EXA_DBA_SYS_PRIVS` — columns `GRANTEE, PRIVILEGE, ADMIN_OPTION`. Filter
  `GRANTEE = 'CREDEXP_READER' AND PRIVILEGE = 'EXECUTE ANY SCRIPT'` → 0 rows
  (reader's only system privilege is `CREATE SESSION`).
- `EXA_DBA_ROLE_PRIVS` — columns `GRANTEE, GRANTED_ROLE, ADMIN_OPTION`. Filter
  `GRANTEE = 'CREDEXP_READER'` → 0 rows (no roles at all).

## 4. Which `EXECUTE ON SCRIPT` grants on `CREDEXP_OWNER` are necessary

All three are necessary — each was individually revoked from `CREDEXP_OWNER`,
then `DROP VIRTUAL SCHEMA CREDEXP_VS CASCADE` + re-create (same `VsProps`) +
re-`GRANT SELECT ON SCHEMA ... TO CREDEXP_READER`, then the reader's
projection-and-filter query, then restored + re-verified 17 rows
(`SEED_ROWS_SCORE_GT_15`) before moving to the next:

- `LAKEHOUSE_ADAPTER`: revoking it made **`DROP VIRTUAL SCHEMA ... CASCADE`
  itself fail** (`insufficient privileges for calling adapter script`) — the
  drop never completed, so the schema was never absent. Separately confirmed
  it is also needed for the ordinary reader `SELECT` (revoke it alone, no
  drop/recreate → same denial on the reader's query; adapter is invoked for
  pushdown planning on every query, not only at `CREATE`/`DROP`).
- `LAKEHOUSE_SCAN`: drop/recreate/grant succeeded (adapter grant intact), but
  the reader's query then failed with `insufficient privileges for calling
  script`.
- `LAKEHOUSE_DISTRIBUTE_FILES`: same as `LAKEHOUSE_SCAN` — drop/recreate/grant
  succeeded, reader query denied with the same message.

All three restored; reader query returns 17 rows after each restore.

## 5. Does `CREATE OR REPLACE SCRIPT` drop an `EXECUTE ON SCRIPT` grant?

Yes. Re-issued the exact statement form both
`crates/lakehouse-engine/tests/common/e2e_harness.rs:174` and
`deploy/scripts/install.sh:1354` use:

```sql
CREATE OR REPLACE RUST SCALAR SCRIPT LHVS.LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/
```

`EXA_DBA_OBJ_PRIVS` for `CREDEXP_OWNER`/`LAKEHOUSE_SCAN` went from 1 row to 0
rows across the replace. The already-documented `GRANT ACCESS ON CONNECTION
... FOR SCRIPT` grant is ALSO dropped by the same statement (confirmed
separately: reader query failed on connection access after only restoring
`EXECUTE ON SCRIPT`, needed both restored). Both grants restored; reader query
back to 17 rows.

**Conclusion for docs/install.md (R2):** neither grant survives
`CREATE OR REPLACE SCRIPT` — an operator re-running the installer must
re-issue both the `EXECUTE ON SCRIPT` grants (all three scripts) and the
`GRANT ACCESS ON CONNECTION ... FOR SCRIPT` grants afterward.

## 6. Is `LAKEHOUSE_DISTRIBUTE_FILES`-alone execute access sufficient to defeat an adapter-injected predicate?

No. As `SYS`: `GRANT EXECUTE ON SCRIPT LHVS.LAKEHOUSE_DISTRIBUTE_FILES TO
CREDEXP_READER` (no `LAKEHOUSE_SCAN` grant). `CREDEXP_READER` then submitted
the isolated `PUSHDOWN_SQL` statement captured per §1 directly — same
denial as the ungranted case:

```
insufficient privileges for calling script (Session: 1876577994990092288) (SQL state: 42500)
```

The statement's outermost `SELECT` invokes `LHVS.LAKEHOUSE_SCAN` first;
`LAKEHOUSE_DISTRIBUTE_FILES` is nested inside `LAKEHOUSE_SCAN`'s `FROM`
clause and is never reached because the scan-script check fails first.
Holding execute on the distributor alone cannot defeat the predicate —
only `LAKEHOUSE_SCAN` execute (or `EXECUTE ANY SCRIPT`) can, per §4.

Revoked the grant afterward; reader's ordinary VS query re-confirmed at
17 rows (`SEED_ROWS_SCORE_GT_15`).

**Conclusion for docs/security.md:** the "Plan visibility versus plan
execution" section must name `LAKEHOUSE_SCAN` (and `EXECUTE ANY SCRIPT`) as
the grants that make an adapter-side predicate advisory, and describe
`LAKEHOUSE_DISTRIBUTE_FILES` by its actual role (the file-list re-emitter
nested inside the scan call, gated behind the same scan-script check) —
not as an independently sufficient grant.
