# Decisions: add-pushdown-plan-execution-boundary

## ADR: Plan visibility and plan execution are separate privileges, guarded by `EXECUTE ON SCRIPT` and the script-scoped connection grant

**ID:** plan-visibility-execution-privilege-split
**Plan:** add-pushdown-plan-execution-boundary
**Status:** Accepted

### Context

`EXPLAIN VIRTUAL` shows the full pushdown SQL, including filter and file list, to any user with `SELECT` on the virtual schema. What stops a reader from resubmitting or editing the plan is `EXECUTE ON SCRIPT` on the scan and distributor scripts, which `SELECT` does not confer. A role-wide `GRANT EXECUTE` would make every pushed predicate advisory.

### Decision

`EXECUTE ON SCRIPT` on the scan and distributor scripts, held by the virtual schema owner alone, is the guarded boundary. The script-scoped connection grant is a second independent gate, and running a script against real data needs both. Plan text is not a secret, and plan execution is the guarded act.

### Options Considered

| Option | Verdict |
|--------|---------|
| Obfuscate or encrypt the plan text | Rejected: `EXPLAIN VIRTUAL` is the supported debugging surface, and secrecy does not enforce a predicate |
| Grant `EXECUTE ON SCRIPT` to querying users | Rejected: for a grantee with script-scoped connection access, every adapter-side predicate becomes advisory |
| Leave the boundary implicit in the test fixture | Rejected: nothing asserts it, so a grant change would break the boundary unnoticed |

### Consequences

- Granting `EXECUTE ON SCRIPT` or `EXECUTE ANY SCRIPT` to a querying user changes the privilege model.
- Any reader sees the table root, bucket layout, file names, byte sizes, and catalog CONNECTION name. A sealed credential envelope is ciphertext that needs the CONNECTION password.
- Future per-user authorization must preserve both gates.
