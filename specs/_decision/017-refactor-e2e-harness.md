# Decisions: refactor-e2e-harness

## ADR: Shared harness module boundary: `common/e2e_harness` + file-local orchestration

**ID:** e2e-harness-shared-module-boundary
**Plan:** refactor-e2e-harness
**Status:** Accepted

### Context

The seven `exasol-e2e` test binaries each re-declared the same provisioning boilerplate — 10
constants, `install_slc`, `exa_conn`, `create_schema_and_scripts`, and five divergent
`create_virtual_schema` signatures — duplicating roughly 1,000 lines. `install_slc`, `exa_conn`,
and `create_schema_and_scripts` are byte-identical across binaries; `create_virtual_schema`
diverges in VS-property sets, Iceberg namespace, and catalog CONNECTION name; setup orchestration
(waits, seeding, two-tier setup) genuinely differs per binary.

### Decision

Create `common/e2e_harness.rs` (gated `exasol-e2e`) holding the byte-identical constants and
helpers plus a parameterized `create_virtual_schema(conn, &VsProps)`. Each binary keeps its own
`OnceLock`, thin `setup_e2e()`/`setup_full_stack()`, VS-name constants, file-specific seeding, and
assertions.

### Options Considered

| Option | Verdict |
|--------|---------|
| Shared `common/e2e_harness` module + file-local orchestration | ✓ Chosen — merges the byte-identical helpers while keeping genuinely divergent setup logic local |
| Move all setup, including per-binary orchestration, into one shared function | ✗ Rejected — setup orchestration differs per binary (int96's two-tier wait, refresh's no-VS-in-setup, differing seed calls) |
| A `setup.rs` per test type | ✗ Rejected — more files, no gain over one shared module |

### Consequences

Eliminates ~1,000 duplicated lines across the seven binaries while preserving every per-binary
difference. Future E2E binaries gain shared provisioning by importing `e2e_harness` instead of
re-declaring it, at the cost of one added indirection (`VsProps`) for callers that need
non-default VS properties.

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
