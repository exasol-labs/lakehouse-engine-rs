# Decisions: fix-greatest-least-null-semantics

## ADR: An advertised capability's NULL contract is the adapter's to reproduce

**ID:** advertised-capability-null-contract-is-adapters-to-reproduce
**Plan:** fix-greatest-least-null-semantics
**Status:** Accepted

### Context

`GREATEST`/`LEAST` shared Exasol's name and arity with a DataFusion function of the same name, but
not its NULL contract: Exasol returns NULL if ANY argument is NULL (captured live on the pinned
Exasol 2025.2.1 container), while DataFusion's `greatest`/`least` return NULL only if ALL arguments
are NULL (`datafusion-functions-54.1.0/src/core/greatest.rs:40`, `.../least.rs:40`). The 1:1 name
mapping the translator used diverged on every pushed-down call over a nullable argument, causing
issue #202's silent wrong results end-to-end.

### Decision

When a DataFusion function shares an Exasol function's name and arity but not its NULL contract,
the DataFusion-dialect rendering wraps the call in whatever SQL reproduces Exasol's contract.
Withdrawing the capability is not the remedy.

### Options Considered

| Option | Verdict |
|--------|---------|
| NULL-guard the DataFusion rendering | ✓ Chosen — fixes the semantics at the source, keeps the pushdown and the projection/filter narrowing it enables, and generalizes the precedent already set for `CONCAT` (issue #200) |
| Withdraw `FN_GREATEST`/`FN_LEAST` from `capabilities.rs` | ✗ Rejected — forfeits the pushdown and treats a fixable rendering defect as an unfixable capability gap |
| Register a custom DataFusion UDF with Exasol semantics | ✗ Rejected — adds a scan-side registration and puts the contract in a second place |

### Consequences

Exasol delegates an advertised predicate or function shape fully and never independently
re-checks it, so there is no engine-side safety net once a capability is advertised — an adapter
that renders different semantics returns wrong rows, not a deferred check. This decision commits
the adapter to reproducing the target engine's NULL contract for every advertised capability, not
just `GREATEST`/`LEAST`, making future NULL-semantics divergences (like `CONCAT`'s) a rendering fix
rather than a capability withdrawal.

