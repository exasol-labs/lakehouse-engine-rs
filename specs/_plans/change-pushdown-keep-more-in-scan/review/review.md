# Plan Review Findings: change-pushdown-keep-more-in-scan

## Summary
- Axes checked: 6/6
- Total findings: 15 (Blockers: 2, Findings: 13)
- Intent Fidelity blockers: 0

## Failure Modes

1. Exasol's implicit conversion in `d = <text>` differs from `CAST(<text> AS DATE)` for some literal shape, or Exasol Nano 2026.2 types a date-part call differently. The scan then returns other rows than native, or the CI Nano leg fails. Routed to `[UNSTATED_ASSUMPTION]` and `[HIDDEN_DEPENDENCY]`.
2. The split route's spec text stays out of step with current `main`, so the implementer builds against a stale requirement: the empty `selectList` clause that #632 changed. Routed to `[REQUIREMENT_CONFLICT]`.
3. The marker plumbing is wired incompletely: the "request holds a marker character" flag never reaches the typed pass, or the marker loses its characters in serialization. Every converted query then fails. Routed to `[EFFORT_MISESTIMATION]` and `[INFORMATION_COUPLING]`.

## Intent Fidelity

`[INTENT_DRIFT]` and `[SCOPE_CREEP]`: no objection, axis checked. Each of the four item-5 parts of #615 (DATE text literal, AND split, date-part types, #461 guard) has tasks. Every task traces to one of them, or to the `AGENTS.md` docs rule.

#### [SCOPE_REDUCTION] FINDINGS
- Location: plan.md § Context, open-question bullet; decision-log.md § Interview and Decision [2]
- Issue: #615 item 5 reads "Let Exasol convert constant text before the scan, so `d = '2024-01-02'` keeps the scan filter". It asks for constant text in general and gives a DATE example. The plan narrows the work to DATE (Decision [2]). It also states "Item 5 of #615 names them", but item 5 names no TIMESTAMP, DOUBLE, or DECIMAL. It states "The user confirmed the DATE-only scope", but `## Interview` records only "constant text keeps the scan filter". `Closes #615` then closes the issue, and only a `#TBD` issue that does not exist yet tracks the rest.
- Fix: In plan.md § Context, replace "Item 5 of #615 names them" with "Item 5 of #615 asks for constant text in general, with a DATE example". If the user confirmed the DATE-only scope, add that Q/A to decision-log.md § Interview. Otherwise, list the narrowing as an open question for the user. In both cases, keep the open question saying that a human opens the follow-up issue before the implementing commit says `Closes #615`.

## Feasibility

`[NFR_IGNORED]`: no objection, axis checked. Security: the literal travels hex-encoded and reaches SQL only inside one Exasol string literal, and a malformed-marker error quotes no spec text (3.4, 3.6). Performance: one CAST per marker per shard row. Checked against code: `build_fan_out_inner` (`support.rs:283`) is the only `to_common_json` call. The row scan, aggregate, `COUNT(DISTINCT)`, and grouped paths call it directly. The broadcast join (`joins/sql_builders.rs:463`), the N-scan legs (`:576`), and the wrapper (`:753`) reach it through `build_scan_driving_sql`. `Cargo.lock` pins DataFusion 55.2.0.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § 1.1, § 3.1, § Verification > Checklist
- Issue: CI runs the local E2E suite on three images: `exasol/docker-db:2025.1.16`, `exasol/docker-db:8.29.13`, and `exasol/nano:2026.2.0-nano.8` (`.github/workflows/ci.yml`, check `E2E (Nano, arm64)`). `ExasolVersion::of` maps 2026 to the `Exasol2025` arm. Task 1.1 measures date-part types, and task 3.1 measures CAST and `TO_CHAR`, only on 8.29.13 and 2025.1.16. The checklist omits Nano. The new `SPLIT_AGGREGATE_CASES` rows and `text_conversion.rs` cases still run on Nano in CI.
- Fix: Add `exasol/nano:2026.2.0-nano.8` to the measurements of 1.1 and 3.1. Add a checklist row "E2E on Exasol Nano | `make e2e-down; EXASOL_IMAGE=exasol/nano:2026.2.0-nano.8 make e2e-up test-e2e` | 0 failures". The image is multi-arch (`specs/testing.md` § Exasol image flavors).

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § 3.1; `pushdown-types/pushdown-planning-scan-text-conversion` delta, scenario "Exasol converts a text literal compared with a DATE before the scan", second AND step
- Issue: Decision [1] assumes that native `d = <text>`, `IN`, `BETWEEN`, and simple `CASE` compare with the value of `CAST(<text> AS DATE)` for every text literal. Task 3.1 checks only well-formed date literals under `DD.MM.YYYY` and `YYYY-DD-MM`. It does not check a literal with a time part (`'01.01.2024 10:00:00'`), a literal with surrounding blanks, one-digit day or month (`'1.1.2024'`), or a literal of blanks only. If native Exasol compares any of these differently, for example as a TIMESTAMP, the scan returns other rows. The scenario also requires "NULL where the literal converts to NULL", and task 3.4 builds a NULL branch. No task shows that a non-empty literal can convert to NULL, and Exasol sends `''` as `literal_null` (`file-planning/pruning-predicate-translation` Background).
- Fix: In 3.1, list these literal shapes. For each one, record whether native `=`, `IN`, `BETWEEN`, and simple `CASE` agree with the CAST. A shape that disagrees keeps declining, and the delta lists it. Add one E2E case per agreeing shape to 3.7. In 3.1, also record whether any non-empty literal converts to NULL. If none does, drop the NULL clause from the scenario and the NULL branch from 3.4 and 3.6.

#### [EFFORT_MISESTIMATION] FINDINGS
- Location: plan.md § 3.2, § 3.5
- Issue: Task 3.5 says "With one, no translator converts a literal", but no task says how the flag reaches the typed pass. The conversion happens in `compare_without_session_parse` inside `ScanTyping` (vs-expression). Code builds `ScanTyping` through `ScanTyping::new(columns, version)`, called from `ScanTranslator { columns, version }`. Five production sites build a translator or typing: `mod.rs:145`, `joins/mod.rs:103`, `joins/sql_builders.rs:85`, `joins/rendering.rs:36`, and `joins/rendering.rs:150`. About 35 test sites call `ScanTyping::new`. Task 3.5 keeps the signature of `build_fan_out_inner` but leaves the translator change unnamed.
- Fix: Add to 3.2 how the typed pass learns the flag. One option: `ScanTranslator::over` takes the flag from the pushdown context and sets a `ScanTyping` field, and conversion stays enabled by default so test call sites keep compiling. List the production construction sites the task edits.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § 2.4
- Issue: Task 2.4 says the golden `group_by_fallback_rendering_filter.sql` is "generated before 2.1", but it appears after 2.1 to 2.3. An implementer who works through the list in order generates the golden from post-change output, so it guards nothing.
- Fix: Renumber the task as 2.1, before the `split_where` change, and shift the later task numbers and the references to them in plan.md.

## Requirement Quality

Checked each delta against the current recorded spec. The split-aggregate-types, declined-filter, aggregate-extensions, and scan-text-conversion deltas build on the current scenario and Background text. Their unmarked Backgrounds match the recorded text word for word. `speq plan validate` passes. The pruning claim holds: `file-planning/pruning-predicate-translation` gives a date column only a literal of its own kind.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: specs/_plans/change-pushdown-keep-more-in-scan/pushdown/pushdown-planning/spec.md § Scenario "A declined WHERE filter routes the single-table request to the qualified wrapper", third AND step
- Issue: The CHANGED block keeps "an absent, JSON-null, or empty `selectList` SHALL return the FULL base row". On current `main`, an empty `selectList` returns one column. #632 (commit bdb25955) added `OutputContract::AnyOneColumn`, and `outer_wrapper_clauses` (`joins/sql_builders.rs`) takes one column for it: "Exasol counts the rows and rejects any width but one column". The recorded Background of the same spec says the same: "An empty `selectList` needs no column and accepts exactly one". The delta therefore restates behavior from before #632 in a block that this revision rewrote. The plan's NEW scenario cites the recorded scenario "A declined filter does not widen the wrapper's projection" (declined-filter-self-apply), which has the same stale clause ("an absent key, JSON null, an empty array, and a non-array value alike").
- Fix: In the pushdown-planning delta, rewrite that AND step as: "the wrapper's returned column count, order, and declared types SHALL equal what the request's `selectList` declares: the full base row for an absent or JSON-null `selectList`, and one column for an empty one, so the route never trips Exasol's positional `04000` validation". In the declined-filter-self-apply delta, add a DELTA:CHANGED block for "A declined filter does not widen the wrapper's projection" that states the same rule, and drop its implementation detail per `.speq/plan-hook.md`. In 2.5, add a case `SELECT RANDOM() FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1` whose row count matches the native copy. Map the changed scenario to `e2e_declined_filter_select_star_returns_full_row_shape` and that new case.

#### [COMPLETENESS_GAP] FINDINGS
- Location: scan-text-conversion delta, scenario "Exasol converts a text literal compared with a DATE before the scan", fourth AND step; plan.md § 3.8
- Issue: "a literal that does not parse SHALL fail the query ... whenever a shard scans" says nothing about a scan with no shard. When the table has no file, or other conjuncts prune every file, the adapter returns the empty-result SQL (`empty_result_sql`, `mod.rs`). That SQL holds no `common` argument and so no CAST, and the query returns zero rows. Native Exasol may raise the conversion error instead, and 3.1 measures this for an empty table. Realistic data can hit this difference in whether a query succeeds (mission Core Capability 12).
- Fix: Add an AND step that states the zero-file outcome as 3.1 measures it, for example "a query whose scan has no file SHALL return no rows and raise no conversion error". Add a 3.7 case whose other conjunct prunes every file. Make the section that 3.8 adds to `docs/semantic-differences.md` state the zero-file outcome.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: split-aggregate-types delta § Background (CHANGED); declined-filter-self-apply delta § Background (CHANGED)
- Issue: The changed split-aggregate-types Background no longer names "the typed pass". Unchanged recorded scenarios still use the term: "An unprojected group key with a derived exact type keeps the split" ("that the typed pass gives an exact type (Background)") and "An unprojected SECOND group key takes the type of its argument's precision" ("in the typed scan pass"). The changed declined-filter Background drops "the scan translator", which the unchanged N-scan scenario uses ("the scan translator renders it against that side's own column metadata"). After the merge, these steps refer to terms that nothing in their spec defines.
- Fix: In the split-aggregate-types delta, add DELTA:CHANGED blocks for those two scenarios. Replace "that the typed pass gives an exact type (Background)" with "that has an exact type (Background)", and "in the typed scan pass" with "as the Background derives it". In the declined-filter delta, change Background bullet 2 to "A conjunct declines when the scan translator, which decides what the scan computes, declines it (`pushdown-types/pushdown-planning-scan-text-conversion`)".

#### [IMPLEMENTATION_DETAIL] FINDINGS
- Location: pushdown-planning delta; recorded `specs/pushdown/pushdown-planning/spec.md` § Background, bullet "The plan-time file-pruning predicate has a per-format owner"
- Issue: The CHANGED scenario "Filter predicate is pushed into the scan spec" drops the only step that names `iceberg::expr::Predicate`. The unchanged Background bullet still says "Scenario clauses naming `iceberg::expr::Predicate` describe the Iceberg arm". After the merge, no scenario step depends on that bullet, and the bullet names a Rust type.
- Fix: Add a DELTA:CHANGED `## Background` block to the pushdown-planning delta that copies the current Background without that bullet. Per `.speq/plan-hook.md`, also leave out the bullets that are implementation detail and would otherwise be copied: the `dispatch` and tokio bullet, and the pushdown-context bullet. If that is too much for this plan, add the stale bullet to plan.md § Out of scope instead.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: recorded `specs/pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback/spec.md` § Background bullet 2; plan.md § 2.2
- Issue: That Background says the wrapper's inner raw scan projects "group keys, select-list aggregate arguments, filter columns, and HAVING and ORDER BY references". After task 2.2, the split route emits only the columns of the declined conjuncts. A grouped request with an undecomposable item and a partly declined filter then projects fewer columns than the filter names.
- Fix: Add a delta for that feature whose CHANGED Background says "the columns of the filter conjuncts the wrapper applies" in place of "filter columns". If that is too much for this plan, list the mismatch in plan.md § Out of scope.

## Task Breakdown

`[TASK_GRANULARITY]`: no objection, axis checked. Each task touches one module group for one reason. Groups B and C state their file-level dependencies on A and B.

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § 1.3, § 3.6, § 3.7, § Verification > Scenario Coverage
- Issue: Three mapped tests cannot fail when the behavior they cover breaks.
  - (a) The 3.7 `NScan(2)` case with `a.C_DATE = '…'` passes whether or not the leg keeps the converted filter. A declined conjunct moves to the outer WHERE and returns the same rows. `specs/testing.md` § Correctness oracles says: "When a declined filter would return the same rows as a pushed one, the test also asserts the SQL". `Case.path` holds one `PathCheck`, so the case cannot also check `Has(SCAN_FILTER)`.
  - (b) In 1.3, an unprojected key shows its declared type only in the `EMITS` clause. `PathCheck::Split` and the row oracle pass with any type wide enough, so the step "YEAR SHALL take DECIMAL(4,0)" is unproven for group keys.
  - (c) The 3.6 test `marker_character_in_the_request_disables_the_conversion` in `pushdown_tests.rs` proves, through a synthesized request, a state that plain SQL reaches (a literal that holds the marker character) and that `EXPLAIN VIRTUAL` shows. `specs/testing.md` § Coverage rule puts that at the E2E layer.
- Fix:
  - (a) In 3.7, add a second row with the same N-scan query and `Has(SCAN_FILTER)`.
  - (b) In 1.3, add a row per date-part key query with `Has(r#""GK_0" DECIMAL(4,0)"#)`, or `DECIMAL(2,0)` for the others, beside its `Split` row.
  - (c) Replace the `pushdown_tests.rs` test with a 3.7 E2E case: `C_DATE = '…' AND C_VARCHAR = '<marker character>'` matches native, and its plan `Lacks("CAST(")`.

#### [CLUSTER_INCOHERENCE] FINDINGS
- Location: plan.md § Parallelization row A; § 1.5
- Issue: Task 1.5 (the DataFusion guard in `raw_scan_tests.rs`) shares no spec delta and no source module with 1.1 to 1.4. Group A's Knowledge lists `floor_timestamp_literals`, which no group A task edits.
- Fix: Move 1.5 into a group D with "Depends on: none" and Knowledge `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`. Drop `floor_timestamp_literals` from group A's Knowledge.

## Design Depth

`[ADR_OVERPROMOTION]`: no objection, axis checked. Decision [1] is the only `yes` entry. Its Rationale names criteria 1 and 4 and states the ADR search result. Its Decision holds no path, signature, or flag.

`[ADR_CONFLICT]`: no objection, axis checked. Decision [1] supersedes `scan-never-converts-session-formatted-text` with `Supersedes:`. Decision [3] conforms to `typed-pass-infers-exasol-types` (one rule per function, exact only after a live check) and to `exact-types-follow-the-exasol-version`. Task 2.1 keeps the renderability screen at the render consumer (`screen-renderability-at-the-render-consumer-not-in-the-partition-classifier`).

The architecture delta's BASE matches `git hash-object specs/architecture.md` (a84eff5a). § Components and § Data Flow are copied in full, with the vs-expression line changed and one Data Flow line added.

#### [ARCHITECTURE_DRIFT] BLOCKER
- Location: plan.md § 3.5; specs/_plans/change-pushdown-keep-more-in-scan/architecture.md (no § Constraints block); decision-log.md Decision [1] `Architecture:` line
- Issue: Task 3.5 adds a value derived from the request to the pushdown context: "The pushdown context (`context.rs`) records once per request whether the request JSON holds a marker character". `specs/architecture.md` § Constraints says: "Each pushdown request builds one pushdown context from its UDF handshake and its parsed properties: the CONNECTION, the scan UDF names, the node count, the tuning values, and the Exasol version. Planning reads these values only from that context." The new value comes from neither the handshake nor the properties, and the list omits it. The delta has no § Constraints block, and Decision [1]'s `Architecture:` line names only § Data Flow and § Components.
- Fix: Add a DELTA:CHANGED `## Constraints` block to architecture.md that copies the full current section. Change the pushdown-context bullet to "builds one pushdown context from its UDF handshake, its parsed properties, and its request text: the CONNECTION, the scan UDF names, the node count, the tuning values, the Exasol version, and whether the request holds a converted-literal marker character". Add "§ Constraints" to Decision [1]'s `Architecture:` line.

#### [INFORMATION_COUPLING] FINDINGS
- Location: plan.md § 3.3 to 3.5; decision-log.md Decision [1] Consequences, first bullet
- Issue: Decision [1] says "the pushdown-planner does not know the encoding". In task 3.5, the pushdown context, which belongs to the pushdown-planner, tests the request JSON for "a marker character", so the planner must know the marker's characters. The flag also rides in `Shards`, whose doc reads "A table's files split into shards, with the UDFs that scan them". A property of the request text thus enters a file-sharding type, only so that one signature stays unchanged. The marker also relies on serde_json leaving its characters unescaped when `CommonScanSpec::to_common_json`, which scan-spec owns, serializes the filter. No test pins that round trip.
- Fix: In 3.3, have `converted_literal.rs` export the predicate "this text holds a marker character", and in 3.5 have the context call it. Pass the flag to the function of 3.4 as a parameter, or as a field of `FanOut`, not on `Shards`. Add to 3.6 one lakehouse-engine unit test: serialize a common spec whose filter holds a marker with `to_common_json`, and assert the concatenation that the 3.4 function returns.

## Prose Quality

No em dashes in the plan artifacts. plan.md § Context and the decision log state current facts.

#### [PROSE_UNCLEAR] FINDINGS
- Location: pushdown-planning delta § Scenario "Filter predicate is pushed into the scan spec", THEN step
- Issue: "SHALL translate a filter that does not render whole one top-level `AND` conjunct at a time" misleads on first reading: it reads as "render whole one top-level AND conjunct".
- Fix: Rewrite as "and, when the whole filter does not render, SHALL translate it one top-level `AND` conjunct at a time".
