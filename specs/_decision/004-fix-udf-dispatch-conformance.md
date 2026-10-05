# Decisions: fix-udf-dispatch-conformance

## ADR: Build the Scan Tokio Runtime Per run() Call, Never Cache It at Process Scope

**ID:** build-scan-runtime-per-call-no-process-cache
**Plan:** `fix-udf-dispatch-conformance`
**Status:** Accepted

### Context

The UDF SDK invokes scalar `run()` once per row and offers no per-process init or cleanup hook. Runtime sizing depends on `df_threads_per_udf`, a per-call input. Exasol reuses a UDF VM process across queries with different values.

### Decision

Each scan call builds a Tokio runtime sized from its own `df_threads_per_udf` and tears it down before returning. A UDF resource may be cached at process scope only if its construction does not depend on a per-call input.

### Options Considered

| Option | Verdict |
|--------|---------|
| Process-global runtime reused across calls | Rejected: breaks the "no cross-call state" UDF invariant and applies stale sizing in a pooled VM process |

### Consequences

Every call pays runtime construction cost. No scan applies another query's thread count.
