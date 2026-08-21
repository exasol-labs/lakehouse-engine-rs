# Decision Log: fix-greatest-least-null-semantics

Fixes GitHub issue [#202](https://github.com/exasol-labs/lakehouse-engine-rs/issues/202).

## Interview

**Q:** How should the fix restore Exasol's NULL-propagates semantics for pushed-down
`GREATEST`/`LEAST`?
**A:** NULL-guard the SQL. Render as `CASE WHEN <arg> IS NULL OR <arg> IS NULL … THEN NULL ELSE
greatest/least(…) END` in the DataFusion-dialect rendering only. Keeps the pushdown and its
performance, fixes the semantics at the source, and follows this project's rule that an adapter owns
generating correct-equivalent SQL for anything it advertises as pushdown-capable: Exasol never
re-checks or re-applies an advertised capability, so once `FN_GREATEST`/`FN_LEAST` are advertised the
adapter alone is responsible for correctness. The "withdraw the capability" alternative — stop
advertising `FN_GREATEST`/`FN_LEAST` in `capabilities.rs` — was explicitly rejected. `capabilities.rs`
stays unchanged and both functions remain advertised and pushed down.

**Q:** Should this plan also audit the other `TRANSLATED_SCALAR_FNS` entries for the same class of
NULL-propagation mismatch, or stay scoped to `GREATEST`/`LEAST` as filed?
**A:** Broader audit. A dedicated research agent audited every arm of the `function_scalar` match
against DataFusion's documented NULL semantics and found no other divergence. Implementation scope
stays exactly at `GREATEST`/`LEAST`, but the plan and this log record that the audit ran and found
nothing else, so a future reader knows the scope boundary was deliberate rather than an oversight.

**Q:** What test depth should the plan require for verifying the fix?
**A:** Unit tests plus a live E2E. Unit tests in `crates/vs-expression/src/lib_tests.rs` asserting
the exact rendered SQL for the DataFusion dialect — one argument, all-non-NULL arguments, a
NULL argument — and confirming the Exasol-dialect verbatim rendering is untouched. Plus a new E2E
test against the Docker Exasol container reproducing the issue's repro shape (a `LEAST`/`GREATEST`
call with a NULL-producing argument), asserting Exasol-matching NULL propagation, so the fix is
verified against a live Exasol engine per this repository's verification-discipline rule rather than
only at the translator-unit level.

## Design Decisions

### [1] An advertised capability's NULL contract is the adapter's to reproduce

- **Decision:** When a DataFusion function shares an Exasol function's name and arity but not its
  NULL contract, the DataFusion-dialect rendering wraps the call in whatever SQL reproduces Exasol's
  contract. Withdrawing the capability is not the remedy.
- **Alternatives:** Withdraw `FN_GREATEST`/`FN_LEAST` from `capabilities.rs` and let Exasol evaluate
  the call itself. Rejected in the interview: it forfeits the pushdown and the projection and filter
  narrowing it enables, and it treats a fixable rendering defect as an unfixable capability gap.
  Registering a custom DataFusion UDF with Exasol semantics was also considered and rejected — it
  adds a scan-side registration and puts the contract in a second place.
- **Rationale:** Exasol delegates an advertised predicate or function shape fully and never
  independently re-checks it, so there is no engine-side safety net. An advertised capability the
  adapter renders with different semantics returns wrong rows, not a deferred check. `CONCAT` was
  already fixed this way for the same class of divergence (chained `||` instead of `concat()`, issue
  #200), so this decision generalizes an existing precedent rather than inventing a rule.
- **Promotes to ADR:** yes

### [2] The guard is confined to the DataFusion-dialect arm; the Exasol dialect is untouched

- **Decision:** Change only the `"GREATEST" | "LEAST"` arm of `render_expression_inner`
  (`crates/vs-expression/src/lib.rs:1289-1300`). Both names keep their `ExasolForm::VerbatimCall`
  declaration, so `render_expression_exasol` output stays byte-identical.
- **Alternatives:** Apply the guard in both dialects for symmetry. Rejected: Exasol's own
  `GREATEST`/`LEAST` already propagate NULL, so an Exasol-side guard would be dead SQL, and it would
  break the verbatim rule that a declared name re-emits exactly the call Exasol's compiler sent.
- **Rationale:** The `VerbatimCall` gate at `lib.rs:986` returns for the Exasol dialect ahead of the
  whole per-name `match`, so the arm is structurally unreachable on the Exasol path — the separation
  is enforced by the dispatch, not by a convention. The declaration-driven sweep test
  `exasol_dialect_renders_declared_verbatim_surface` derives its Exasol expectation from the node's
  own uppercased name, so it stays green with no edit and is itself the proof that the guard did not
  leak dialects.
- **Promotes to ADR:** no

### [3] The `ELSE` branch keeps the call so the CASE carries a result type

- **Decision:** Render `ELSE greatest(<args>)`, never a form whose every branch is NULL.
- **Alternatives:** Emit only the NULL-producing shape when an argument is a NULL literal, since the
  result is then always NULL.
- **Rationale:** With all branches NULL-typed, `LEAST(<col>, NULL)` would plan as a Null-typed
  column instead of one carrying the arguments' common type, and Arrow `Null` is not in this
  project's Arrow-to-Exasol type mapping. Keeping the call in `ELSE` lets DataFusion's CASE type
  coercion pin the result to the arguments' common type while the guard still forces NULL for every
  row.
- **Promotes to ADR:** no

### [4] The guard duplicates an argument's rendered text, which is safe only because the translated surface is deterministic

- **Decision:** Call `render_args` once and reference each resulting SQL string twice — in its
  `IS NULL` clause and inside the call — rather than rendering each argument twice or binding it.
- **Alternatives:** Render each argument twice through `render_expression_inner` (rejected: two
  renders of one node could diverge and the walk cost doubles); introduce a binding form so the
  argument appears once (rejected: restructures the whole expression for no correctness gain).
- **Rationale:** Duplicating an argument's text means DataFusion may evaluate the sub-expression
  twice, which would be a correctness bug for a non-deterministic argument. No translated
  `function_scalar` name is non-deterministic: `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and
  `SYSTIMESTAMP` are deliberately absent from `TRANSLATED_SCALAR_FNS` and their capabilities are
  withdrawn, precisely because their value depends on context the scan never receives. The
  safety therefore rests on an existing, documented invariant rather than on an assumption, and the
  spec records the dependency so a future widening of the translated surface has to revisit it.
- **Promotes to ADR:** no

### [5] The guard is emitted unconditionally, with no nullability inference

- **Decision:** Emit the guard for every argument list, including one whose arguments are all
  non-nullable in the source table.
- **Alternatives:** Skip the guard when every argument is a non-nullable column or a non-NULL
  literal.
- **Rationale:** The translator sees only a request-JSON node. A `column` node carries no nullability
  metadata, so any skip would be a guess whose failure mode is silent wrong results — the exact
  failure this plan removes. The cost of an unnecessary guard is a CASE that always takes its `ELSE`
  branch, which DataFusion's simplifier handles.
- **Promotes to ADR:** no

### [6] The broader NULL-semantics audit found no other divergence, and the scope boundary is recorded

- **Decision:** Implementation scope stays at `GREATEST`/`LEAST`. No other translated scalar function
  gains a guard.
- **Alternatives:** Leave the audit result out of the artifacts and keep the plan minimal.
- **Rationale:** Every arm of the `function_scalar` match was audited against DataFusion's documented
  NULL semantics. DataFusion documents NULL-skipping for exactly five functions — `coalesce`,
  `concat`, `concat_ws`, `greatest`, `least` — and of the names this translator maps only `CONCAT`
  (already fixed as chained `||`, issue #200) and `GREATEST`/`LEAST` reach that set. `NULLIF` matches
  Exasol's own documented `CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END` definition under
  identical three-valued logic (confirmed against `datafusion/functions/src/core/nullif.rs`).
  `NULLIFZERO` → `nullif(arg, 0)` and `ZEROIFNULL` → `coalesce(arg, 0)` pass a single argument plus a
  literal, so `coalesce`'s NULL-skipping is the intended behavior there. `MOD` renders the `%`
  operator, which propagates NULL in both engines. `LPAD`/`RPAD`, `TRANSLATE`, `REPLACE`,
  `INSTR`/`LOCATE`, `POWER`/`ATAN2`, two-argument `ROUND`/`TRUNC`/`LOG`, `TO_DATE`/`TO_TIMESTAMP`,
  and the `*_BETWEEN` family carry no ignore-NULL behavior on either side. Recording the negative
  result in the spec Background is what keeps a future reader from re-litigating the question or
  mistaking the narrow scope for an oversight.
- **Promotes to ADR:** no

### [7] A recorded normative claim about Exasol's `GREATEST` was FALSE and is corrected in this plan

- **Decision:** Correct the claim in both places it is recorded — the
  `vs-adapter/pushdown-agg-sql-consolidation` scenario clause, and the four doc comments in
  `adapter/pushdown/scalar_over_agg.rs` and `adapter/pushdown/grouped_agg_tests.rs`. Retain the
  `CASE WHEN … IS NULL` guard and change no generated SQL.
- **Alternatives:** Leave the false claim standing and keep this plan to one feature (rejected: the
  library would hold two normative scenarios asserting opposite contracts for one engine, and the
  false comment sits directly in a future reader's path to re-litigating #202). Delete the now
  redundant `CASE` guard (rejected: it changes SQL that golden fixtures pin byte-for-byte, for no
  correctness gain).
- **Rationale:** `pushdown-agg-sql-consolidation`'s scenario recorded, and `scalar_over_agg.rs`'s doc
  comment repeated, that "Exasol's `GREATEST(0.0, NULL)` returns `0.0`, not `NULL` (returns the max
  of non-NULL inputs; only returns NULL if ALL inputs are NULL)". That is DataFusion's contract, not
  Exasol's, and it directly contradicts this plan's premise. Both claims were said to come from live
  investigations, so the contradiction was settled the way this repository requires — against the
  running container. `SELECT GREATEST(0.0, NULL), SQRT(GREATEST(0.0, NULL)), GREATEST(1, 2, NULL),
  GREATEST(CAST(NULL AS DOUBLE)), GREATEST(5) FROM dual` on the pinned Exasol 2025.2.1 image returned
  NULL, NULL, NULL, NULL, `5`. Issue #202's premise is correct; the recorded claim is false. The
  guard becomes redundant under the true contract but is retained and re-justified as an explicit
  statement of the NULL path, while the `GREATEST(0.0, …)` clamp keeps its own untouched purpose of
  stopping a tiny negative rounding artifact from reaching `SQRT`.
- **Supersedes:** the `GREATEST`-NULL-contract rationale recorded in
  `vs-adapter/pushdown-agg-sql-consolidation`'s "The sufficient-statistics fragments have one owner
  per denominator" scenario.
- **Promotes to ADR:** yes

### [8] Both divergent contracts were captured, not recalled

- **Decision:** Cite primary evidence for both engines in the spec Background and the plan Design,
  and treat neither side's semantics as known.
- **Alternatives:** Accept issue #202's repro table as sufficient.
- **Rationale:** This repository's verification discipline requires a claimed SQL capability or
  limitation to be checked against a live Exasol system, and the contradicting in-repo claim proved
  why: a plausible, confidently-worded, allegedly-live-verified statement about `GREATEST` was wrong
  and had been shipped into a normative spec. The Exasol half is a live capture on the pinned
  container; the DataFusion half is the pinned crate source
  (`datafusion-functions-54.1.0/src/core/greatest.rs:40` and `.../least.rs:40`), not the
  documentation website, so both halves are pinned to the versions this project actually builds
  against.
- **Promotes to ADR:** no

### [9] The E2E fixture must discriminate, and must prove delegation

- **Decision:** The E2E test derives a NULL for SOME rows only, via `NULLIF(MOD(id, 5), 0)` over the
  seeded 20-row `id` 1..20 fixture, and additionally asserts through `EXPLAIN VIRTUAL` that the
  guarded form reached the scan spec.
- **Alternatives:** Use a literal `NULL` argument so every row is NULL (rejected: a fixture in which
  every row is NULL, or none is, cannot distinguish correct from buggy behavior); assert results
  only (rejected below).
- **Rationale:** A result-only assertion is not sufficient evidence here. If the expression were left
  for Exasol to evaluate rather than pushed into the DataFusion scan, the result would be correct for
  a reason that says nothing about the translator, and the test would pass against the unfixed code.
  The `EXPLAIN VIRTUAL` probe is what makes the test discriminating — the scan spec's rendered SQL
  appears in plaintext in that output, as the existing `arg_expr` and `aggregates` assertions in the
  same file already rely on. The seed fixture has no nullable column, so `NULLIF` is the established
  way to produce one (see `test_group_by_null_key_grouping`).
- **Promotes to ADR:** no

## Review Findings
