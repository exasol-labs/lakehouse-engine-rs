# Decisions: fix-udf-dispatch-conformance

## ADR: Build the Scan Tokio Runtime Per run() Call, Never Cache It at Process Scope

**ID:** build-scan-runtime-per-call-no-process-cache
**Plan:** `fix-udf-dispatch-conformance`
**Status:** Accepted

### Context

language-container-rs v0.21.0 invokes scalar `run()` once per row and exposes no
per-process init/cleanup hook. The prior code built one Tokio runtime per VM's whole batch
of shard rows and reused it. The runtime's sizing depends on `df_threads_per_udf`, which
arrives as a per-call `UdfContext` input parameter carried in the row's `ScanSpec`. Exasol
pools and reuses a UDF VM process across invocations that may belong to different queries
carrying different `df_threads_per_udf` values.

### Decision

`run_scan` constructs a fresh Tokio runtime sized from the current call's
`df_threads_per_udf` and tears it down before returning. It MUST NOT cache a runtime at
`static`/process scope across `run()` calls. General principle: a UDF resource may be
cached at process/static scope only if its construction does not depend on a per-call
input parameter; if it does, it must be rebuilt every call.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build a fresh runtime per `run()` call | ✓ Chosen — the only option consistent with `mission.md`'s "stateless and disposable — no cross-call state" UDF invariant and immune to stale sizing across pooled-process queries |
| Process-global `OnceLock<Runtime>` reused across invocations | ✗ Rejected — cross-call state; a pooled VM process later serving a query with a different `df_threads_per_udf` would apply stale sizing |

### Consequences

Every `run()` call pays runtime-construction cost, but no scan instance can apply another
query's thread-count sizing. The scan UDF stays conformant with SDK 0.21.0's per-row
dispatch and the project's no-cross-call-state invariant.
