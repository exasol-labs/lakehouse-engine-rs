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

## ADR: A recorded normative claim about Exasol's GREATEST was false and is corrected

**ID:** correct-false-exasol-greatest-null-claim
**Plan:** fix-greatest-least-null-semantics
**Status:** Accepted

### Context

`vs-adapter/pushdown-agg-sql-consolidation`'s scenario, and `stddev_of`'s and `merge_select_items`'
doc comments in `adapter/pushdown/scalar_over_agg.rs`, and two NULL-passthrough tests' doc comments
in `adapter/pushdown/grouped_agg_tests.rs`, all recorded that "Exasol's `GREATEST(0.0, NULL)`
returns `0.0`, not `NULL` (returns the max of non-NULL inputs; only returns NULL if ALL inputs are
NULL)". That is DataFusion's contract, not Exasol's, and it directly contradicted this plan's
premise. `SELECT GREATEST(0.0, NULL), SQRT(GREATEST(0.0, NULL)), GREATEST(1, 2, NULL),
GREATEST(CAST(NULL AS DOUBLE)), GREATEST(5) FROM dual` on the pinned Exasol 2025.2.1 image returned
NULL, NULL, NULL, NULL, `5`: issue #202's premise is correct, and the recorded claim was false.

### Decision

Correct the claim in every place it is recorded — the `vs-adapter/pushdown-agg-sql-consolidation`
scenario clause and the four doc comments above. Retain the `CASE WHEN … IS NULL` guard those
comments describe, and change no generated SQL.

### Options Considered

| Option | Verdict |
|--------|---------|
| Correct the claim in all recorded locations, keep the guard, change no SQL | ✓ Chosen — settles the contradiction the way this repository requires: against the running Exasol container |
| Leave the false claim standing and keep this plan to one feature | ✗ Rejected — the library would hold two normative scenarios asserting opposite `GREATEST` contracts for one engine, and the false comment sits in a future reader's path to re-litigating #202 |
| Delete the now-redundant `CASE WHEN … IS NULL` guard | ✗ Rejected — changes SQL that golden fixtures pin byte-for-byte, for no correctness gain |

### Consequences

The library now states one Exasol `GREATEST` NULL contract instead of two contradictory ones. The
retained guard is re-justified as an explicit statement of the NULL path rather than as the thing
that produces it, so a reader no longer needs to derive Exasol's real contract from stale prose. No
generated SQL or golden fixture changed as a result of this correction.
