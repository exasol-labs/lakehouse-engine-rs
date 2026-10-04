# Decisions: refactor-e2e-harness

## ADR: Fold `CloudExaConn` into `ExaConn` via an opt-in `redact_sql` flag

**ID:** fold-cloudexaconn-into-exaconn-redact-flag
**Plan:** refactor-e2e-harness
**Status:** Accepted

### Context

`cloud_e2e_test.rs` re-implemented a whole WebSocket client (`CloudExaConn`) and
`encrypt_password`, duplicating `common/exasol_ws.rs`. The only load-bearing difference from the
shared `ExaConn` is that the cloud suite must redact SQL text and response bodies from failure
output so credential-bearing DDL never leaks SigV4 or vended keys, while the local Docker suite
keeps SQL-in-failure output for debuggability.

### Decision

Delete `CloudExaConn` and cloud's `encrypt_password`. Add a `redact_sql` bool to
`common/exasol_ws::ExaConn`: `connect(...)` keeps `redact_sql=false` for the seven local binaries;
a redacting constructor sets it `true`, omitting the SQL statement and Exasol response body from
the `execute()` DDL-failure panic. Widen the `exasol_ws` gate in `common/mod.rs` to
`any(exasol-e2e, cloud-e2e)`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Opt-in `redact_sql` flag on `ExaConn` | ✓ Chosen — minimal fold that preserves both suites' behaviour with one flag |
| Keep a separate cloud client | ✗ Rejected — ~150 duplicated lines, the duplication the issue exists to remove |
| Always redact | ✗ Rejected — the local suite's SQL-in-failure output is a debugging aid and the Docker stack carries no secrets |

### Consequences

Removes the duplicated cloud WebSocket client and centralizes credential-leak protection in one
place. The redaction covers only the `execute()` DDL-failure path for this fold;
`query_scalar_i64`, `query_row_count`, and the `connect()` auth-failure assertion remain
unredacted, tracked as a standing advisory rather than fixed here.
