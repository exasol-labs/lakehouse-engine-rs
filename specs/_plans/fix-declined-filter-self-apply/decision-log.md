# Decision Log: fix-declined-filter-self-apply

## Interview

**Q:** How much of the three declining surfaces should this plan fix?
**A:** All three surfaces — single-table WHERE in `handle_pushdown`/`build_row_scan_sql`,
broadcast-join filter in `render_broadcast_join`/`build_broadcast_join_sql`, and N-scan per-leg
filter in `build_side_fan_out_sql` — get the same correctness fix in one plan.

**Q:** What should the fix mechanism be when a predicate is declined?
**A:** Self-apply via outer WHERE. Generalize the existing
`cross_side_residual_filter` / `render_df_filter_qualified` pattern already used by the N-scan
join's outer wrapper for genuinely cross-table residual conjuncts: render the declined predicate in
Exasol dialect and have the adapter's own returned SQL include an outer WHERE that actually
evaluates it, rather than omitting it and hoping Exasol re-checks. Rejected: failing the whole query
on decline (correctness-safe but availability-losing, not requested), and blocking on researching a
native Exasol partial-pushdown acknowledgment mechanism first — investigate, but do not gate the fix
on it.

**Q:** `build_row_scan_sql`'s single-table path deliberately has NO outer SELECT wrapper today (a
prior decision to avoid a `SELECT * FROM (...)` materialization boundary for performance).
Self-applying a declined predicate there means adding a wrapper back. How to scope that tradeoff?
**A:** Wrapper only on the decline path. The fast no-wrapper scan stays exactly as-is for the common
case (no decline); an outer WHERE wrapper appears ONLY when a predicate actually declines — a rare,
already-slower correctness path — so there is no regression to the normal query path.

**Q:** Issue #228 (`INSTR`/`LOCATE` with more than two arguments, via
`string_function_arg_type_guard`) independently asserts the same now-disproven claim and likely
shares the exact same fix seam. Should this plan fold it in?
**A:** Keep separate. This plan stays scoped to #279. If the general fix mechanism also covers
#228's surfaces as a structural side effect, note that in the plan and decision log, but #228 gets
independently re-verified and closed later as its own tracked work. Do NOT expand scope to verify or
close #228.

## Design Decisions

### [1] There is no Exasol-side fallback for a predicate whose capability the adapter advertised

- **Decision:** Record as a proven protocol fact that once the capabilities response advertises a
  predicate or function shape, Exasol delegates it fully and never independently re-checks or
  re-applies it. The adapter owns generating the equivalent SQL for anything it cannot faithfully
  push to DataFusion. Recorded in `CLAUDE.md` as a general fact with no discovery narrative and no
  issue number, per the issue's explicit instruction.
- **Alternatives:** Treat the omission as a merely-unverified assumption and keep it — rejected, it
  was disproven live three separate ways against the Docker Exasol stack, each returning all 12 rows
  of `TYPED_DISTINCT_PROBE` where 0, 3, and 7 were correct, with `EXPLAIN VIRTUAL` confirming the
  emitted SQL carries neither a scan-spec `"filter"` nor any `WHERE`.
- **Rationale:** The documented pushdown response carries exactly two fields, `type` and `sql`.
  `PushDownResponse` holds one member. Exasol's documented query process splits the query from the
  capabilities response alone, BEFORE the pushdown request exists, and post-processes only what the
  adapter did not advertise. The only documented escape hatch, `EXCLUDED_CAPABILITIES`, is
  whole-capability and whole-schema at DDL time. So an advertised capability is a commitment and
  there is nothing per-query to hand back.
- **Promotes to ADR:** yes

### [2] The recorded LIKE-guard consequence is corrected, not merely superseded

- **Decision:** `specs/_decision/026-fix-207-like-non-string-column.md`'s Consequences section
  asserts that a non-string LIKE "declines pushdown of the whole top-level filter, so Exasol
  evaluates the predicate natively instead", "mirroring the existing all-or-nothing
  untranslatable-predicate backstop". That named backstop does not exist and the stated consequence
  was wrong. This entry corrects it. The DECLINE SCOPE decision 026 made — all-or-nothing, never
  partial-filter rewriting — remains correct and is retained; only its stated consequence changes:
  the declined filter is applied by the adapter's own outer WHERE, not by Exasol.
- **Alternatives:** Supersede the record without naming it as an error — rejected. The project's
  precedent is explicit correction: `031-fix-having-unmatched-aggregate-fallback.md` ("The recorded
  HAVING-backstop clause was wrong and is corrected, not merely superseded") and
  `035-fix-191-order-by-offset.md` ("Every pinned unreachability claim needs a live end-to-end
  backstop, not only a `debug_assert!`"). Leaving the contradiction unaddressed would reintroduce,
  in the permanent library, the exact defect class this plan exists to fix.
- **Rationale:** Decisions 031 and 035 each corrected one clause of the same false family (HAVING,
  then ORDER BY/OFFSET) without revisiting the WHERE-filter clause that #207, #219, and #215 all
  inherited unquestioned. Correcting the WHERE clause by name is what stops the third recurrence.
- **Supersedes:** ADR `like-guard-in-adapter-not-vs-expression`'s stated Consequences (plan
  `fix-207-like-non-string-column`). The ADR's Decision — that the type-aware LIKE decision belongs
  in the adapter, not in `vs-expression` — is unchanged and independently correct.
- **Promotes to ADR:** yes

### [3] The single-table decline routes to the existing qualified single-table wrapper

- **Decision:** On a filter decline, `build_dispatch_sql` routes the request to
  `qualified_single_table_fallback_pushdown`, which renders the ORIGINAL (un-type-rewritten)
  predicate as the wrapper's own `WHERE` between the raw fan-out and every other clause, with the
  fan-out spec's `filter` set to `None`.
- **Alternatives:** Wrap the emitted SQL in `SELECT * FROM (<emitted>) WHERE <predicate>` — rejected.
  The single-table path serves five request shapes and four of them (top-N, single-group aggregate,
  grouped aggregate, `COUNT(DISTINCT)`) would then filter AFTER aggregation or AFTER truncation,
  which is wrong. Add a bespoke row-scan-only wrapper and hard-error on the other four shapes —
  rejected, strictly worse than a path that already handles all five.
- **Rationale:** The qualified wrapper is the one shape that positions the `WHERE` correctly for
  every shape, because its fan-out is aggregate-free, sort-free, and LIMIT-free by construction. It
  already exists, already builds the `LHS_T0` alias map, already renders the select list, GROUP BY,
  HAVING, ORDER BY, and LIMIT in Exasol dialect, and is already the destination of three other
  decline guards in the same dispatcher. This is a fourth route to an existing shape, not a new
  shape. It also satisfies the interview's wrapper-only-on-decline constraint exactly: the
  wrapper-free fast path is untouched for every request whose filter renders.
- **Promotes to ADR:** yes

### [4] The broadcast-join decline is a bug against the recorded spec, not an open design fork

- **Decision:** On a filter decline, `render_broadcast_join` returns `Ok(None)` and the caller falls
  through to the N-scan fallback, which applies the predicate. Broadcast gets no wrapper of its own.
- **Alternatives:** Give `build_broadcast_join_sql` its own outer `WHERE` wrapper so it keeps
  broadcast acceleration under a decline — rejected on three independent grounds. First,
  `vs-adapter/pushdown-planning-join`'s recorded broadcast contract already reads "a
  condition/filter/projection the `crates/vs-expression` translator can render; any deviation is
  served by the unified unaccelerated fallback", so the fall-through is the recorded behavior and the
  code simply never enforced the filter half. Second, `build_broadcast_join_sql` has no outer wrapper
  at all today, so the alternative means building one from scratch. Third, and decisively, the
  broadcast projection is narrowed to the select-list items, so a filter-only column is not in scope
  for an outer `WHERE`; putting it in scope means widening the projection, which trips the existing
  widened-projection decline and lands back on the N-scan fallback anyway.
- **Rationale:** The fork the issue posed dissolves once the recorded contract is read: one arm is
  the documented behavior and the other collapses into it. Cost is a lost optimization on a rare
  query, and `Ok(None)` already carries three other decline reasons on that same path, so no new
  contract is introduced.
- **Promotes to ADR:** no

### [5] A declined side-local conjunct is reclassified as residual, screened at the render sites

> Revised after plan-review round 1 — see § Review Findings, `[plan-review] Task 4 screened the
> Iceberg pruning input`. The original decision put the condition inside `side_local_filter` and
> `cross_side_residual_filter`.

- **Decision:** Keep both partition functions purely structural and apply ONE renderability screen —
  `renderable_only` / `declined_only`, exact complements — at the two render call sites inside
  `build_n_scan_join_sql`. A conjunct is pushed into a leg if and only if it is side-local to exactly
  one table AND the DataFusion dialect can render it; every other conjunct is residual. Each side's
  Iceberg manifest-pruning predicate keeps every side-local conjunct, screened or not. The outer
  wrapper's `WHERE` renderer and its rendering of existing residuals are unchanged.
- **Alternatives:** Add the condition inside `side_local_filter` / `cross_side_residual_filter` —
  rejected: `side_local_filter` also feeds `resolve_one_join_side`'s Iceberg manifest pruning
  (`joins/mod.rs:122`), so screening it there would strip declined conjuncts from pruning and open
  more files silently, with correct rows and no failing test. Render the FULL filter into the outer
  `WHERE` unconditionally, accepting that
  side-local conjuncts are applied twice — rejected. It is idempotent and correct for an inner join,
  but it rewrites the emitted SQL for EVERY join query, churns the pinned golden-SQL baseline, and
  fails outright if any conjunct is Exasol-unrenderable, which puts the whole clause back at square
  one. Have `build_side_fan_out_sql` report back which conjuncts it did not push — rejected as
  plumbing for a decision the partition can make directly. Pre-render each leg's filter at the
  partition site and change `build_side_fan_out_sql` to take a `String` — rejected as a signature
  change that buys only the removal of a provably-unreachable second decision point.
- **Rationale:** Renderability matters only where rendering happens. Keeping it out of the partition
  leaves each function with one job — attribution by `tableName` — and leaves the pruning consumer
  untouched, while the screened halves stay exact complements because one is the negation of the
  other over the same conjunct set. Only a conjunct that today vanishes changes position, so nothing
  that works today moves.
- **Promotes to ADR:** no

### [6] No new `vs-expression` entry point; decline detection reuses `render_expression_safe`

- **Decision:** One `pub(super) fn datafusion_renderable(expr: &Json) -> bool` in
  `pushdown/support.rs`, implemented over the existing `render_expression_safe`, shared by all three
  sites. `crates/vs-expression` code is unchanged; only its doc comments are corrected.
- **Alternatives:** Add a three-way `FilterRender { Pushed, TriviallyTrue, Declined }` outcome plus a
  new public classifier, with `render_df_filter_safe` redefined over it — rejected as unnecessary
  once it was established that `render_expression_safe` already returns `None` for exactly the
  declined case, because it does not suppress a trivially-true result. Test the rendered fragment
  against the literal strings `TRUE`/`NULL` at each call site — rejected: that leaks the
  trivially-true rule out of `vs-expression` into three adapter sites, which is precisely the
  back-door duplication that produces the next divergence.
- **Rationale:** Fewer public entry points, one owner for the trivially-true rule, and a fix that is
  entirely adapter-local. The double render of one plan-time tree is not a cost worth an API for.
- **Promotes to ADR:** no

### [7] A predicate unrenderable under both dialects returns a clean error

- **Decision:** When a declined predicate also fails the qualified Exasol render, both the
  single-table wrapper and the N-scan wrapper return their existing client-facing error. No rows are
  returned.
- **Alternatives:** Omit it and return rows — rejected, that is the defect. Return rows with a
  warning — rejected, the protocol has no warning channel and a wrong result is not a warning.
- **Rationale:** A predicate applicable nowhere must not produce a result. This is a new route to an
  existing outcome, not a new failure mode: `vs-adapter/pushdown-planning-selectlist-expressions`
  already records the same terminal for a select-list node untranslatable under both dialects,
  reaching the same wrapper refusal site, and describes it as "correctness-safe — Exasol receives an
  error, never wrong data". Exasol does not re-plan on an adapter error, so the failure is final and
  visible.
- **Promotes to ADR:** no

### [8] The aggregate, top-N, and `COUNT(DISTINCT)` paths are not a fourth exposure

- **Decision:** Resolved during planning, as the brief required. `topn.rs`, `single_group_agg.rs`,
  and `grouped_agg.rs` are NOT a fourth call-site exposure: they never render the filter. Every
  occurrence of `render_df_filter_safe` in those files is inside a `#[cfg(test)]` module. All three
  consume the single already-rendered `Option<String>` that `handle_pushdown` computed, so the
  single-table fix covers them by construction and the issue's count of three sites is correct.
- **Alternatives:** Add a per-path decline guard in each of the three modules — rejected, there is
  nothing to guard; the decline happens once, upstream.
- **Rationale:** The finding is nonetheless load-bearing for the FIX SHAPE, which is why it is
  recorded rather than dismissed. Those paths are a semantic constraint, not a second call site: an
  outer `WHERE` wrapped around their emitted SQL would filter after aggregation or after truncation.
  That constraint is what selects the qualified wrapper in decision [3] over a naive outer wrap.
- **Promotes to ADR:** no

### [9] Issue #228's surface is reached structurally but #228 is not adjudicated here

- **Decision:** `string_function_arg_type_guard`'s `INSTR`/`LOCATE`-with-more-than-two-arguments
  decline is one route into the corrected single-table handling, so the fix reaches it as a side
  effect. `specs/vs-adapter/pushdown-planning-string-fn-type-coercion`'s false backstop claim is
  corrected for library consistency. Issue #228 itself is NOT verified, adjudicated, or closed by
  this plan.
- **Alternatives:** Fold #228 in and close it — rejected, the interview ruled it out of scope.
  Leave the string-fn spec's false claim standing — rejected, it would leave the shipped library
  asserting a fact this plan proves false, which is the recurrence pattern decision [2] exists to
  stop; correcting recorded prose is not the same as adjudicating the issue.
- **Rationale:** The live repro (`WHERE INSTR(C_VARCHAR,'a',2) = 0` returning 12 rows where 7 are
  correct) confirms the shared seam, so the correction is factual rather than speculative. #228's own
  question — whether the 3-argument `INSTR` is additionally SILENTLY TRUNCATED to a 2-argument
  `strpos` on the join paths, which do not run the guard — is a distinct defect this plan does not
  touch.
- **Promotes to ADR:** no

### [10] A native partial-pushdown acknowledgment mechanism is ruled out, not assumed absent

- **Decision:** The interview's open item is closed as a negative finding rather than left open. No
  residual-acknowledgment mechanism exists in Exasol's Virtual Schema protocol.
- **Alternatives:** Proceed without checking — rejected; the interview asked for a quick check, and
  an unverified negative is what produced this issue in the first place.
- **Rationale:** The documented pushdown response has exactly two fields, `type` and `sql`, with one
  documented note ("`sql`: The pushdown SQL statement. It must be either an `SELECT` or `IMPORT`
  statement"). `PushDownResponse.java` holds a single member and `ResponseJsonConverter` serializes
  those two keys only. The word "residual" and any partial-pushdown equivalent appear nowhere in the
  adapter API reference or the Exasol Virtual Schema documentation. The only incomplete-pushdown
  concept in the protocol runs the other direction, Exasol to adapter, as an empty `selectList`.
  Self-application is therefore the only available mechanism, not merely the chosen one.
- **Promotes to ADR:** yes

## Review Findings

### [plan-review] Task 4 screened the Iceberg pruning input

- **Finding:** `plan-reviewer` round 1, BLOCKER `[REQUIREMENT_CONFLICT]`. Task 4 added the
  DataFusion-renderability condition inside `side_local_filter`, which has a second production
  consumer the plan never named: `plan_join` (`joins/mod.rs:122`) passes its result to
  `resolve_one_join_side` as that side's Iceberg manifest-pruning predicate. The condition would have
  stripped declined conjuncts from pruning too — more files opened, correct rows, no failing test —
  contradicting plan.md § Non-Goals, `pushdown-file-pruning`'s "the raw filter tree forwarded to
  pruning is unchanged", and the new feature's Iceberg-spec-check bullet.
- **Direction change:** Both partition functions stay unchanged and purely structural. The screen
  moved to the two render call sites in `build_n_scan_join_sql` as a new `renderable_only` /
  `declined_only` complement pair over the existing `partition_conjuncts`. Decision [5] is revised in
  place. `pushdown-planning-join-fallback`'s § Background gains a bullet stating that each side's
  pruning predicate keeps every side-local conjunct, and its CHANGED scenario a matching clause; the
  self-apply delta names both pruning inputs. Task 4 gains the unit test
  `join_side_pruning_input_unchanged_when_df_render_declines` with its § Scenario Coverage row.
  Generalizes as "screen at the consumer, not in the shared classifier": a predicate-shaping
  condition added to a function serving both a pruning and a rendering consumer degrades pruning
  invisibly.
- **Promotes to ADR:** yes

### [plan-review] Task 4's error condition re-created the root-cause conflation in the Exasol dialect

- **Finding:** `plan-reviewer` round 1, BLOCKER `[REQUIREMENT_CONFLICT]`. Task 4 errored whenever a
  non-empty residual set rendered to `None` through `render_df_filter_qualified`. That renderer
  delegates to `render_df_filter_exasol_safe`, which suppresses a trivially-true result to `None`
  exactly as its DataFusion twin does. A column-free conjunct is residual by construction, so a join
  carrying one trivially-true top-level conjunct would have turned today's correct "no outer `WHERE`"
  into a hard client-facing error — the same three-way `None` conflation this plan exists to delete,
  one dialect over, and a contradiction of the recorded
  `sql-comprehension/vs-expression-translator-scalar-ops` trivially-true-suppression scenario.
- **Direction change:** The error is gated on the NON-suppressing `render_expression_qualified`
  returning `None` for the combined residual tree. Task 4 and task 5 both state three outcomes —
  absent, trivially true, unrenderable — and error only on the third.
  `pushdown-planning-join-fallback`'s § Background bullet and CHANGED scenario now distinguish the
  three explicitly, and the self-apply delta carries the same rule. Task 4 gains the unit test
  `trivially_true_residual_emits_no_outer_where_and_does_not_error` with its § Scenario Coverage row.
  Generalizes as a rule: a renderer that suppresses a no-op result MUST NOT decide unrenderability;
  that decision needs the non-suppressing entry point.
- **Promotes to ADR:** yes

### [plan-review] Advisory findings taken in the same pass

- **Finding:** `plan-reviewer` round 1 raised eight ADVISORY findings. All eight were adopted while
  the same files were open: the task-5 call-site census (4 production + 7 test sites) and the seven
  hand-written pipeline mirrors; the decline path's temp-DB RAM consequence in § Impact plus a
  `TEMP_DB_RAM_PEAK` manual-test row; the two missing backstop doc comments in task 11's census; a
  `DELTA:CHANGED` copy of the primary recorded scenario "Filter predicate is pushed into the scan
  spec"; tests moved from task 6 into the tasks that create the behavior; the like-type-coercion
  coverage row split three ways with two added unit tests; one owner for the decline classification
  (`handle_pushdown`, threaded into `build_dispatch_sql`); and the over-length Feature description cut
  to one sentence.
- **Direction change:** No design direction changed. The single-owner advisory is the only one with
  design weight and it removes a second classification site rather than adding a mechanism.
- **Promotes to ADR:** no

### [plan-review] The decline route sent a `SELECT *` request to a filter-narrowed projection

- **Finding:** `plan-reviewer` round 2, BLOCKER `[UNSTATED_ASSUMPTION]`. Task 5 routed a declined
  filter to `qualified_single_table_fallback_pushdown` for every dispatch shape, but that function
  derives its inner projection from `referenced_column_projection`, which collects only the columns
  the rendered clauses NAME. For an absent, JSON-null, or empty-array `selectList` — a genuine
  `SELECT *`, a shape ONLY the new route can reach — the projection would have been the filter's
  columns alone, and `n_scan_join_select_items`' absent-`selectList` arm enumerates that narrowed set
  through `n_full_row_qualified_items`. Exasol validates the pushdown result positionally, so the
  plan would have turned "wrong rows" into a hard `04000` error (absent/null) or a silently truncated
  single-column result (empty array) — a regression outside the deliberate both-dialects-unrenderable
  trade-off, since the predicate renders fine in the Exasol dialect.
- **Direction change:** `qualified_single_table_fallback_pushdown` gains a SECOND new parameter, a
  pre-computed projection override; the decline route passes the full base row (every `col_types`
  entry, in order, with its Exasol type) when `selectList` is absent, JSON-null, or an empty array,
  and `None` everywhere else. The guard lives at the decline route, NOT inside
  `referenced_column_projection`: that function is shared with the join wrapper's narrowing, and the
  recorded `vs-adapter/pushdown-joins-module-structure` scenario "One clause walk feeds both wrapper
  column-narrowing routines" owns that single walk. `vs-adapter/pushdown-planning`'s NEW scenario
  gains a column-count/order/type clause naming the absent-or-empty case. Task 5 gains the unit test
  `declined_filter_with_absent_select_list_projects_full_row`, task 7 the e2e case
  `e2e_declined_filter_select_star_returns_full_row_shape`, plus § Scenario Coverage rows and a
  § Manual Testing row — the empty-array arity is the one point not settled from code (today's scan
  path emits arity 1 while the N-scan wrapper emits the whole side column set), so it is confirmed
  against the live Docker Exasol container rather than assumed. Generalizes as "widening the set of
  request shapes a wrapper serves widens its column-shape contract": a route added ahead of a
  classifier inherits every shape the classifier used to divert.
- **Promotes to ADR:** yes

### [plan-review] Round-2 advisory findings taken in the same pass

- **Finding:** `plan-reviewer` round 2 raised three ADVISORY findings, all in files the blocker
  already reopened. (1) `[TRACEABILITY_GAP]` — task 5's census advertised itself as exact but omitted
  `joins/sql_builders.rs:2478`, a test caller of `qualified_single_table_fallback_pushdown`, which
  would not have compiled. (2) `[INFORMATION_LEAKAGE]` — the render-site screen falsifies
  `side_local_filter`'s and `cross_side_residual_filter`'s doc comments, and neither task 4 nor task
  11 corrected them, leaving the partition invariant documented in one place and enforced in another.
  (3) `[PROSE_UNCLEAR]` — § Impact said "Three behavior changes" over four bullets, and the bullet
  added in round 1 opened with a 29-word sentence.
- **Direction change:** No design direction changed. The census gains a twelfth row and an intro
  naming both symbols and both caller kinds; task 4 gains a clause requiring both partition doc
  comments rewritten in the same unit of work (bodies still unchanged) and task 11 lists both symbols
  for the docs-last pass; § Impact reads "Four" and the bullet's first sentence is split at the comma.
- **Promotes to ADR:** no
