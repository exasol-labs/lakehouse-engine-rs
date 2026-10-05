# Decisions: fix-greatest-least-null-semantics

## ADR: An advertised capability's NULL contract is the adapter's to reproduce

**ID:** advertised-capability-null-contract-is-adapters-to-reproduce
**Plan:** fix-greatest-least-null-semantics
**Status:** Accepted

### Context

Exasol `GREATEST` and `LEAST` return NULL if any argument is NULL, and DataFusion's functions of the same name return NULL only if all arguments are NULL. The 1:1 name mapping gave silent wrong results over nullable arguments (issue #202). Exasol never re-checks a delegated capability, so a differing rendering returns wrong rows.

### Decision

When a DataFusion function shares an Exasol function's name and arity but not its NULL contract, the DataFusion-dialect rendering wraps the call in SQL that reproduces Exasol's contract. The adapter does not withdraw the capability.

### Options Considered

| Option | Verdict |
|--------|---------|
| Withdraw `FN_GREATEST` and `FN_LEAST` from `capabilities.rs` | Rejected: forfeits the pushdown for a fixable rendering defect |
| Register a custom DataFusion UDF with Exasol semantics | Rejected: adds a scan-side registration and a second home for the contract |

### Consequences

The rule covers every advertised capability, so a future NULL divergence, like `CONCAT`'s, is a rendering fix and not a capability withdrawal.
