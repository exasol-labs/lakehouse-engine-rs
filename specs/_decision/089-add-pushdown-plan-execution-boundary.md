# Decisions: add-pushdown-plan-execution-boundary

## ADR: Plan visibility and plan execution are separate privileges, guarded by `EXECUTE ON SCRIPT` and the script-scoped connection grant

**ID:** plan-visibility-execution-privilege-split
**Plan:** add-pushdown-plan-execution-boundary
**Status:** Accepted

### Context

`EXPLAIN VIRTUAL` returns the pushdown SQL in full, including projection, filter, limit, and file list, to any user with `SELECT` on the virtual schema. What stops a reader from resubmitting or editing the plan is `EXECUTE ON SCRIPT` on the scan and distributor scripts, which `SELECT` does not confer. That boundary existed only as an unasserted test-fixture arrangement, and issue #402 named the risk that a role-wide `GRANT EXECUTE` would make every pushed predicate advisory. It is also the precondition for future OPA row-level security and per-user catalog identity.

### Decision

`EXECUTE ON SCRIPT` on the scan and distributor scripts, held by the virtual schema owner alone, is the stated and guarded boundary. The script-scoped `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` is a second independent gate, and running a script against real data needs both. Plan text is not a secret, and plan execution is the guarded act. Future per-user authorization work must preserve both gates.

### Options Considered

| Option | Verdict |
|--------|---------|
| Obfuscate or encrypt the plan text | Rejected: `EXPLAIN VIRTUAL` is the supported debugging surface, and secrecy is not what enforces a predicate |
| Grant `EXECUTE ON SCRIPT` to querying users | Rejected: for a grantee with script-scoped connection access, every adapter-side predicate becomes advisory |
| Leave the boundary implicit in the test fixture | Rejected: no test reads the fact, so adding a reader to the grant loop breaks nothing |

### Consequences

Granting `EXECUTE ON SCRIPT` or `EXECUTE ANY SCRIPT` to a querying user is a documented privilege-model change. The plan still exposes the table root, bucket layout, file names, byte sizes, and the catalog CONNECTION name to any reader. A sealed vended-credential envelope travels as ciphertext that needs the CONNECTION password to open. `docs/security.md` states this exposure.
