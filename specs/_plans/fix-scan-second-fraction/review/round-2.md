# Plan Review Findings: fix-scan-second-fraction (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 7 (Blockers: 1, Advisory: 6)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Round-1 Blocker Recheck
- Resolved: [INTENT_DRIFT] SECOND and EXTRACT(SECOND) over a DATE departed from issue #603: decision-log.md `## Interview` now records both answers (`SECOND(<date>)` is 0 with no exact type; `EXTRACT(SECOND FROM <date>)` declines to match native). Decision [1] credits `DECIMAL(2,0)` to issue #603's proposed fix in its Alternatives line. plan.md § Open Questions no longer holds the DATE question. The delta scenarios "SECOND over a DATE computes 0 without a stated type" and "SECOND over any other argument, or EXTRACT(SECOND) over a DATE, runs in Exasol" match the answers.
- Resolved: [HIDDEN_DEPENDENCY] the declared-surface sweep would fail after task 1.3: task 1.7 renders the `SECOND` fixture's DataFusion assertion through `render_scan_expression` with `TS` typed `TIMESTAMP(6)`. Task 1.3 gives `SECOND` its own DataFusion arm that declines every call, so the backstop comment at `lib.rs` l.938 stays true. § Dead Code Removal lists the replaced untyped assertion. Checked against `lib_tests.rs` l.2083-2275: the panic on an untyped DataFusion decline is the sweep's only assertion that task 1.3 breaks. The fixture-completeness checks match on names and are unaffected.
- Resolved: [AMBIGUOUS_REQUIREMENT] two scenarios sent `render_expression` to a typed-only rule: both scenarios now say that `render_expression` SHALL decline `SECOND` and that `render_scan_expression` renders it per the new scenario. Task 1.6 asserts the decline in `renders_year_month_day_extract` and `renders_extract`. A GIVEN that still excludes `SECOND` remains, raised below as an ADVISORY.
- Resolved: [REQUIREMENT_CONFLICT] the unprojected-key decline scenario contradicted the new SECOND key scenario: the delta adds a `DELTA:CHANGED` block whose GIVEN reads "has no exact type (Background)". Background bullet 3 gives `SECOND(x)` over `TIMESTAMP(p)` the exact type `DECIMAL(2+p, p)`. § Scenario Coverage maps the scenario to `e2e_split_aggregate_columns_match_native_oracle`, which carries that scenario tag (`capability.rs` l.3342).

## Premortem

Six months from now this plan failed. Three ways it got there:

1. The verifier runs the checklist's E2E command. Five tests run in parallel and all rewrite `FILTER_ORACLE.NATIVE_COPY`, so one test compares against another test's copy and fails at random. The implementer then chases a parity defect that does not exist. Routed to `[HIDDEN_DEPENDENCY]`.
2. A later refactor lets the typed pass rewrite `SECOND(ts, n)` into the synthetic node and ignore `n`. The test tagged as the refused-arity scenario's proof still passes, because untyped `render_expression` now declines every `SECOND` call. Routed to `[COMPLETENESS_GAP]`.
3. A third function earns an exact type, for example another date part. ADR [3] lists "Two rules qualify", so that change needs another superseding ADR, the same step this plan had to take. Routed to `[ADR_OVERPROMOTION]` (ADVISORY).

## Intent Fidelity
No objection, axis checked. The plan renders `SECOND(ts)` and `EXTRACT(SECOND FROM ts)` through the typed scan translator (tasks 1.1-1.4). It takes p from the argument's declared type through `exact_expression_type`, scales with a decimal product to `DECIMAL(2+p, p)` (decision [2]), and declines when p is unknown (task 1.1). It compares E2E results with a native copy over `TIMESTAMP(6)` and `TIMESTAMP(9)` in every position the issue names (tasks 2.1-2.2). It pins the unit SQL for p = 0, 3, 6, 9 and DATE (task 1.5), and it keeps `SECOND(ts, n)` native (task 2.1 row). The DATE behavior follows the user's settled answers. Generalizing `date_part('nanosecond', …)` from p = 9 to p from 7 to 9 adds no scope.

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Verification › Checklist, row "E2E"; § Implementation Tasks 2.3
- Issue: The checklist runs `cargo test --workspace --features lakehouse-e2e/exasol-e2e --test e2e_local -- timestamp_precision split_aggregate` without `--test-threads=1`. `specs/testing.md` l.62 says "Each suite runs with `--test-threads=1`, because all tests of one binary share one provisioning", and `make test-e2e` passes the flag (Makefile l.90). No `RUST_TEST_THREADS` default exists in `.cargo/config.toml`, so libtest runs the selected tests in parallel. The filter selects the new test, three existing `timestamp_precision` tests that write the native copy (`iceberg_microsecond_timestamps_round_trip_at_the_declared_precision`, `cast_to_bare_timestamp_filters_at_milliseconds_like_native`, `cast_to_timestamp_with_local_time_zone_matches_native`), and `e2e_split_aggregate_columns_match_native_oracle`. Each of them runs `CREATE OR REPLACE TABLE FILTER_ORACLE.NATIVE_COPY` (`e2e_harness.rs` l.1203, l.1216-1220), and task 2.2 replaces that table in the middle of the new test. A test can therefore compare against another test's copy, so the expected result "0 failures" is not reliable as written.
- Fix: In plan.md § Verification › Checklist, change the E2E command to `cargo test --workspace --features lakehouse-e2e/exasol-e2e --test e2e_local -- --test-threads=1 timestamp_precision split_aggregate`. Name the same command in task 2.3.
- Escalation: MECHANICAL. `specs/testing.md` and the harness settle the fix. No requester judgment is needed.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: plan.md § Implementation Tasks 2.2; § Parallelization, Knowledge column
- Issue: Task 2.2 queries `LAKEHOUSE_VS`'s `E2E_ALL_TYPES_TABLE` from `timestamp_precision.rs`. That file's `setup()` provisions only `TS_PRECISION_VS` (l.25-38). Other tests make `LAKEHOUSE_VS` exist by calling `lakehouse_vs()` from `e2e_local/fixtures.rs` (l.77), for example `scan.rs` l.250. Neither the task nor the Knowledge column names that fixture. When the new test runs alone, it queries a virtual schema that may not exist yet.
- Fix: In task 2.2, state that the test calls `lakehouse_vs()` before it builds the second native copy. Add `crates/lakehouse-e2e/tests/e2e_local/fixtures.rs` (`lakehouse_vs`, `LAKEHOUSE_VS`) to group A's Knowledge column.

## Requirement Quality

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 1.5 and 1.6; § Scenario Coverage row "A refused argument count declines for DataFusion and renders for Exasol"
- Issue: Task 1.6 keeps `second_with_precision_declines_for_datafusion_renders_for_exasol` unchanged and tags it as the scenario's proof. That test asserts that untyped `render_expression` declines `SECOND(ts, 3)`. After task 1.3, `render_expression` declines every `SECOND` call at any arity, so the assertion no longer depends on the argument count. The arity check now lives in the typed pass (task 1.1: "`SECOND(x)` (one argument)"), and no unit row exercises it: task 1.5's decline rows hold no two-argument call. `specs/testing.md` l.10 says a unit test carries the scenario line only when it is the scenario's one proof. The E2E row `SECOND(TS, 2)` and the adapter tests that use `SECOND(C_TS, 3)` as a decline fixture catch a regression only indirectly.
- Fix: In task 1.5, add a decline row `SECOND(TS, 3)` that runs through `render_scan_expression` with `TS` typed `TIMESTAMP(6)`. Put the scenario tag on that test, not on the untyped one, and update the § Scenario Coverage row to name it.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 2.1, last sentence
- Issue: Task 2.1 says "The select-list and filter rows assert the scan marker with `PathCheck::Has`: `date_part('microsecond'` for a microsecond column". Applied literally, three select-list rows get a wrong check. `SECOND(TS, 2)` is computed by Exasol, so the scan has no `date_part`. `EXTRACT(SECOND FROM CAST(TS AS DATE))` declines and fails, and `native_copy_mismatch` runs `isolated_pushdown_statement` for any path other than `Unchecked`. `SECOND(CAST(TS AS DATE))` renders `date_part('second'`, not `date_part('microsecond'`. The task also does not say which check the projected GROUP BY rows over `TS` take.
- Fix: In task 2.1, state the check for each row that the marker rule does not cover: `PathCheck::Has("date_part('second'")` for `SECOND(CAST(TS AS DATE))`, `PathCheck::Lacks("date_part(")` for `SECOND(TS, 2)`, `PathCheck::Unchecked` for the `Expect::Error` row, and `PathCheck::Split` for every GROUP BY row.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Scenario Coverage; § Implementation Tasks 1.6
- Issue: Four changed date-fns scenarios map to existing unit tests that carry no `/// Scenario:` line: "EXTRACT translates to the DataFusion date_part call" (`renders_extract`), "EXTRACT renders Exasol's EXTRACT FROM form in the Exasol dialect", "Field-shortcut date functions translate to date_part of the matching field", and "Field-shortcut date functions render verbatim in the Exasol dialect". A grep of `crates/` finds none of these titles. `AGENTS.md` § Code style and `specs/testing.md` l.10 require the tag on the proving test. Task 1.6 edits all four tests but adds a tag only to `second_with_precision_declines_for_datafusion_renders_for_exasol`. In the other direction, the DATE scenario and the decline scenario have E2E rows in `second_and_extract_second_match_a_native_copy` (§ Verification note), so the unit tests are not their one proof.
- Fix: In task 1.6, add the four `/// Scenario:` lines to the four tests they map to. In task 2.1, tag `second_and_extract_second_match_a_native_copy` with "SECOND over a DATE computes 0 without a stated type" and "SECOND over any other argument, or EXTRACT(SECOND) over a DATE, runs in Exasol". Add those two E2E rows to § Scenario Coverage.

## Design Depth

No `[ADR_CONFLICT]`: decision [3] carries `Supersedes: split-aggregate-columns-take-exact-types-or-decline`. The ADRs `scan-never-converts-session-formatted-text` (p > 0 and the DATE case are classed `Number`), "The declared output column is the single authority for the emitted type" (the emit still casts to `EMITS`), and "There is no Exasol-side fallback for a predicate whose capability the adapter advertised" (declines are self-applied) all hold. No `[ARCHITECTURE_DRIFT]`: [3] states `Architecture: no change` with a reason, and `specs/architecture.md` l.92 already routes every scan type rule through one scan translator. Task 1.1 writes the classifier once for both `type_scan_tree` and `exact_expression_type`. The callers whose routing changes are `group_key_exasol_types` and `nested_aggregate_type`, which is what § Impact states. `output_select_list` and `select_item_type` only drop a now-redundant cast.

#### [ADR_OVERPROMOTION] ADVISORY
- Location: decision-log.md § Design Decisions [3], Decision line
- Issue: The policy in [3] qualifies for an ADR under rule-2 criterion 2. The Decision also lists the rules that qualify: "Two rules qualify: an aggregate's result type (`COUNT` is `DECIMAL(18,0)`, `SUM` over ... ), and `DECIMAL(2+p, p)` for `SECOND` over a `TIMESTAMP(p)` argument". That list is how the superseded ADR became wrong: it named "the one rule the adapter copies", so this plan had to supersede it. The type rules already live in `pushdown-planning-split-aggregate-types` Background bullets 3 and 4 and in the scenario "SECOND keeps the fraction of its argument's declared precision". `/speq:adr-rules` rule 3 excludes statements that already live in a feature spec. With the list in the Decision, a third qualifying rule needs a third ADR.
- Fix: In decision-log.md [3], end the Decision after the policy sentence ("The adapter copies a rule only when the rule depends on nothing but the exact types of its arguments and holds on every supported Exasol version"). Move the two example rules to the Consequences line as current instances, or drop them, because the specs hold them.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: spec delta `sql-functions/vs-expression-translator-date-fns` § Scenario "EXTRACT translates to the DataFusion date_part call" (GIVEN l.36, AND l.40-41) and § Scenario "Field-shortcut date functions translate to date_part of the matching field" (GIVEN l.57, AND l.61-62)
- Issue: Each GIVEN limits the node to `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE`, but each scenario ends with AND lines about a `SECOND` node, which its own GIVEN excludes. A reader who looks up how `render_expression` treats `SECOND` finds the rule inside a scenario whose precondition rules `SECOND` out.
- Fix: Move the two `SECOND` lines of each scenario into one new scenario, "The untyped DataFusion walker declines SECOND", with GIVEN "`SECOND(x)` or `EXTRACT(SECOND FROM x)`", WHEN "`render_expression` processes the node", THEN "it SHALL decline the node, because only the typed pass knows the argument's precision", and AND "`render_scan_expression` SHALL render it per `SECOND keeps the fraction of its argument's declared precision`". Map it in § Scenario Coverage to `renders_year_month_day_extract` and `renders_extract`.
