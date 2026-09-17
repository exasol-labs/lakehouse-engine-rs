# Decisions: add-pushdown-plan-execution-boundary

## ADR: Plan visibility and plan execution are separate privileges, guarded by `EXECUTE ON SCRIPT` and the script-scoped connection grant

**ID:** plan-visibility-execution-privilege-split
**Plan:** add-pushdown-plan-execution-boundary
**Status:** Accepted

### Context

Every adapter-side pushdown decision reaches the engine as SQL text. `EXPLAIN VIRTUAL` returns that
text in full to any user holding `SELECT` on the virtual schema, including the projection, the
filter, the limit, and the per-shard file list as plaintext literals. A reader can read the exact
predicate the adapter injected on its behalf. What stops that reader from resubmitting or
hand-editing the plan is a separate privilege: `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN` and
`LAKEHOUSE_DISTRIBUTE_FILES`, which `GRANT SELECT ON SCHEMA <vs>` does not confer. Before this
plan, that boundary existed only as a fixture arrangement in
`crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs`, granted to the VS owner and not
asserted by any test. Issue #402 named the regression risk: an operator closing the undocumented
non-DBA-owner grant gap with a role-wide `GRANT EXECUTE` would silently make every pushed predicate
advisory. This boundary is also the precondition for future OPA-based row-level security and
per-user catalog identity work, so it is a load-bearing architectural constraint rather than an
implementation detail.

### Decision

`EXECUTE ON SCRIPT` on the scan and distributor scripts is the boundary this plan states and
guards, held by the virtual schema owner alone. A second, independent gate exists: the
script-scoped `GRANT ACCESS ON CONNECTION ... FOR SCRIPT`, already documented in
`docs/security.md` § Privilege model and already guarded by
`revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential`. Running a script
against real data needs both grants. Plan text is not a secret. Plan execution is the guarded act.
Any future per-user authorization work MUST preserve both gates rather than assume plan secrecy.

### Options Considered

| Option | Verdict |
|--------|---------|
| State and assert the `EXECUTE ON SCRIPT` / connection-grant split as the enforced boundary | ✓ Chosen — matches how the engine already enforces safety, and issue #402 named the exact way an undocumented boundary gets lost |
| Make the plan text unreadable, by obfuscation or encryption | ✗ Rejected — `EXPLAIN VIRTUAL` is the supported pushdown-debugging surface, and plan secrecy is not what makes a predicate enforced |
| Grant `EXECUTE ON SCRIPT` to querying users for convenience | ✗ Rejected — for a grantee that also holds script-scoped connection access, this makes every adapter-side predicate advisory |
| Leave the boundary implicit in the test fixture | ✗ Rejected — the fixture withholds the grant from the reader, but no test reads that fact, so adding a reader to the grant loop breaks nothing |

### Consequences

Granting `EXECUTE ON SCRIPT`, or `EXECUTE ANY SCRIPT`, to a querying user becomes a documented
privilege-model change rather than an unnoticed convenience. The plan still exposes the table
root, the bucket layout, file names, byte sizes, and the catalog CONNECTION name to any reader; a
sealed vended-credential envelope travels in the plan as ciphertext, readable but not openable
without the CONNECTION password. That exposure is stated in `docs/security.md` rather than left
for a reader to discover. Future per-user authorization or row-level-security work has a stated,
tested boundary to build on instead of an implicit fixture arrangement.
