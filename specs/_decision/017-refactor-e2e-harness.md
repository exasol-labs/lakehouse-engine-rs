# Decisions: refactor-e2e-harness

## ADR: Fold `CloudExaConn` into `ExaConn` via an opt-in `redact_sql` flag

**ID:** fold-cloudexaconn-into-exaconn-redact-flag
**Plan:** refactor-e2e-harness
**Status:** Accepted

### Context

The cloud E2E suite duplicated the shared Exasol WebSocket client. It differs in one way: it must redact SQL text and response bodies from failure output so credential-bearing DDL does not leak, while the local suite keeps them for debugging.

### Decision

The shared client gains an opt-in redaction flag, and the cloud suite uses it instead of its own client. The local suite keeps redaction off.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep a separate cloud client | Rejected: keeps about 150 duplicated lines |
| Always redact | Rejected: local SQL output aids debugging and the Docker stack holds no secrets |

### Consequences

Redaction covers only the DDL-failure path. Scalar and row-count queries and the connect auth-failure assertion stay unredacted, tracked as a standing advisory.
