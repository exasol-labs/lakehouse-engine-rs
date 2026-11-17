# Plan Review Findings: add-clock-function-pushdown

## Summary
- Axes checked: 4/4
- Total findings: 12 (Blockers: 2, Findings: 10)
- Intent Fidelity blockers: 0

Live checks run for this review on the clone's Exasol 8.29.13 container:

- All 596 names of `SELECT TIME_ZONE_NAME FROM EXA_TIME_ZONES` resolve in the tz data of `chrono-tz` 0.10.4 (tzdb 2025b). The tz data holds one name Exasol 8.29.13 does not list: `AMERICA/COYHAIQUE`.
- D2 holds: in a session at `ASIA/TOKYO` with `DBTIMEZONE = EUROPE/BERLIN`, `TIMESTAMP '<Berlin evening>' < CURRENT_TIMESTAMP` compares against the Tokyo wall clock, and `CAST(TIMESTAMP 'x' AS TIMESTAMP WITH LOCAL TIME ZONE)` returns `x` unchanged.
- Exasol constant-folds `DATE '2024-01-10' - 7` and `ADD_DAYS(DATE '2024-01-10', -7)` before it sends a pushdown request: `EXPLAIN VIRTUAL` shows `literal_date` `2024-01-03` in the request's `filter`. The scan computes `EVENT_DATE - 7` and `ADD_DAYS(EVENT_DATE, -7)` correctly (both return 11 rows).

## Intent Fidelity

#### [SCOPE_REDUCTION] FINDINGS
- Location: plan.md § Out of scope
- Issue: Issue #641 names `WHERE ts > CURRENT_TIMESTAMP - INTERVAL '1' HOUR` as one of its two motivating filters. The plan states "`CURRENT_TIMESTAMP - INTERVAL '1' HOUR` stays unpushed, because `LITERAL_INTERVAL` is not advertised". The interview never asked whether this is acceptable. The acceptance criteria do not require it, so this is a boundary the user should confirm, not a defect.
- Fix: In intent.md § Directions, add a direction stating that interval arithmetic over a clock function stays evaluated by Exasol and that `ADD_HOURS`/`ADD_MINUTES` are the pushed forms, and name it in the plan report as a scope boundary for the user to confirm.

#### [INTENT_GAP] FINDINGS
- Location: pushdown/pushdown-planning-clock-functions/spec.md § Scenario "A filter on a clock function prunes files at plan time"
- Issue: The issue's criterion reads "A filter like `d = CURRENT_DATE` on a date-partitioned table prunes files down to the matching partition at plan time." The scenario instead uses "data files hold disjoint ranges of a `DATE` column `d`" and `d >= ADD_DAYS(CURRENT_DATE, -<n>)`, so it proves min/max range pruning only. Partition-equality pruning is a separate pruning shape (mission Core Capability 6), and no scenario covers it for a clock filter.
- Fix: Add a row to that scenario (or a second scenario) with a table partitioned on `d` and the filter `d = CURRENT_DATE`, asserting that `EXPLAIN VIRTUAL` lists only the partition of the folded date, and map it in plan.md § Scenario Coverage to a test in `crates/lakehouse-e2e/tests/e2e_local/pruning.rs`.

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Context ("every planning path handles `literal_date` and `literal_timestamp`, pruning included"); intent.md D1; pushdown/pushdown-planning-clock-functions/spec.md § Scenario "A filter on a clock function prunes files at plan time"
- Issue: Plan-time pruning reads a comparison only when one side is a column and the other a literal node (`pruning_filter.rs` takes the operand through `exact_literal`, which returns `None` for any non-`literal_*` node). The adapter has never received a function over a literal, because Exasol constant-folds `DATE '…' - 7` and `ADD_DAYS(DATE '…', -7)` before it sends the request (verified live). After D1's fold, `ADD_DAYS(CURRENT_DATE, -<n>)` becomes `ADD_DAYS(literal_date, -<n>)`, which no pruning path reads. The plan's own pruning scenario therefore fails as written, and so do the relative filters the issue cites as most common (`CURRENT_DATE - 7`, `ADD_HOURS(CURRENT_TIMESTAMP, -1)`): they run in the scan but prune no file at plan time.
- Fix: In plan.md § Implementation step 4, add that the fold also evaluates date arithmetic whose operands are all literals after folding (`ADD`/`SUB` of a `DATE` and an integer, `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, `ADD_MINUTES`, and any other date-arithmetic function Exasol advertises here) into one literal, with Exasol's semantics, and add an intent.md direction recording that choice. Add a unit test of that evaluation in `clock_tests.rs`, and add `d >= CURRENT_DATE - <n>` beside the `ADD_DAYS` form in the pruning scenario. If the plan keeps arithmetic unfolded instead, change the scenario to a bare `d >= CURRENT_DATE` and list relative-date pruning in § Out of scope for the user to confirm.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: intent.md D3; plan.md § Summary ("one clock reading per statement"); pushdown/pushdown-planning-clock-functions/spec.md § Background ("Exasol runs one adapter VM per statement")
- Issue: The recorded spec `file-planning/pushdown-statement-state` states only "One adapter VM serves every request of one statement on one virtual schema", and the statement-state code comments that "nothing establishes that one VM never serves two virtual schemas of a statement". Nothing establishes either that two virtual schemas of one statement share a VM. A statement over two virtual schemas may then fold two clock readings, which contradicts "one reading per statement". The scenario only covers one virtual schema, so no test catches it.
- Fix: Restate D3, the plan Summary, and the Background line as one reading per statement on one virtual schema, and add the cross-schema case to the `docs/semantic-differences.md` item of plan.md § Implementation step 7. Alternatively, verify live that a `UNION ALL` over two virtual schemas returns one distinct `SYSTIMESTAMP`, and record the result in D3.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: pushdown/pushdown-planning-clock-functions/spec.md § Background (second bullet); intent.md D6
- Issue: The Background states as fact that for a numeric zone Exasol sets `TZ` "as a POSIX `UTC±N` string with the sign inverted". Issue #641 says this "still has to be confirmed on a database started with a numeric OS time zone", and the interview only settled how to test the parse (`%env TZ`), not what Exasol sends. D6 makes an unreadable `TZ` fail every `SYSDATE`/`SYSTIMESTAMP` query, so if Exasol sends another form (`<+07>-7`, `+07:00`, `Etc/GMT-7`), every such query fails on those databases.
- Fix: Add to D6 that the `UTC±H[:MM]` form is unconfirmed on a database with a numeric OS zone, and either list the other POSIX and offset forms the parse also accepts or add an open question to plan.md. Drop the unconfirmed claim from the Background, or mark it as expected.

#### [NFR_IGNORED] FINDINGS
- Location: intent.md D3; plan.md § Implementation step 4 ("one `OnceLock` instant per adapter VM")
- Issue: A clock kept for the VM's life is correct only while each VM serves exactly one statement. Today's per-statement state shares that assumption, but a stale catalog snapshot is a visible staleness, while a stale clock silently returns yesterday's rows for `d = CURRENT_DATE`. The plan does not say whether a prepared statement executed several times re-runs pushdown, which decides whether a folded literal can outlive its execution. The SDK exposes `ctx.session_id()` and `ctx.statement_id()`, which can key the reading at no cost.
- Fix: In plan.md step 4, key the clock reading on the session and statement id, and read a new clock when either changes. Add to step 1 a live check that executing one prepared statement twice sends two pushdown requests, and record the result in D3.

## Requirement Quality

#### [IMPLEMENTATION_DETAIL] BLOCKER
- Location: sql-functions/vs-expression-translator-date-fns/spec.md § Background; vs-properties/property-value-validation/spec.md § Background; pushdown/pushdown-planning-clock-functions/spec.md § feature description
- Issue: The local planning rules (`.claude/CLAUDE.md` § Planning) say "Never copy implementation detail from an existing spec into a delta." The CHANGED date-fns Background copies "arms of the same recursive walker behind the public entry points of `crates/vs-expression`: `ScanTyping::render_expression` and `ScanTyping::render_filter` … `render_expression_exasol` and `ScanTyping::exasol_fragment`", "declared `VerbatimCall` in the crate's declaration of translated names", and "the typed scan pass computes only its one-argument call". Its new bullet adds internal ordering: "before it translates a request …, so neither dialect receives one". The property-value-validation delta copies "The adapter parses the properties once per request into one typed value". The new feature's description says the adapter replaces each call "before it plans the request", an internal step order.
- Fix: In the date-fns CHANGED Background, drop the first bullet and the `VerbatimCall` and typed-pass sentences, keeping only what Exasol and DataFusion show, and reword the clock bullet to "No clock function reaches the scan or the wrapper SQL: the pushed SQL holds a date or timestamp literal in its place (`pushdown/pushdown-planning-clock-functions`)." In the property-value-validation Background, drop the "parses … into one typed value" sentence and keep "`pushdown` and `getCapabilities` read a rejected value as absent and an unknown name as unset". In the new feature description, replace "before it plans the request" with "in the pushed SQL".

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: pushdown/pushdown-planning-clock-functions/spec.md § Scenario "TIME_ZONE accepts the zone names Exasol lists and rejects other values"; intent.md D5
- Issue: The scenario says the adapter "SHALL reject any other value" than the `EXA_TIME_ZONES` names, while D5 validates through the `chrono-tz` tz database. On Exasol 8.29.13 that database accepts `AMERICA/COYHAIQUE`, which `EXA_TIME_ZONES` does not list (verified), so the scenario's reject step is false for that name, and every tz database update can add more. The plan already captures the `EXA_TIME_ZONES` list for `every_exa_time_zones_name_resolves`.
- Fix: In plan.md § Implementation step 2, validate a `SESSION_TIME_ZONE` value against the captured `EXA_TIME_ZONES` name list (the union over Exasol 8 and 2025) and resolve it through `chrono-tz` only after it passes, and note this in D5. Keep `TZ` parsing on the full tz database.

#### [COMPLETENESS_GAP] FINDINGS
- Location: pushdown/pushdown-planning-clock-functions/spec.md § Scenario "An unreadable TZ fails only a query that needs it"; plan.md § Scenario Coverage
- Issue: The step "the same failure SHALL occur when `TZ` is unset" cannot be produced by the mapped E2E test: Exasol always sets `TZ` (plan.md § Impact), and `%env TZ=;` sets it empty, not unset. The unit test `unreadable_tz_is_refused` lists "empty", not unset.
- Fix: Change the step to "when `TZ` is empty or unset", add an unset case to `unreadable_tz_is_refused`, and note in § Scenario Coverage that the unit test proves the unset step, per `specs/testing.md` § Coverage rule (a state no Exasol request can reach).

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: specs/sql-functions/vs-expression-translator-date-diff-fns/spec.md § Background (no delta in this plan)
- Issue: The recorded Background says `sql-functions/vs-expression-translator-date-fns` "covers … `WEEK` and the now-family". This plan removes the date-fns now-family scenario and the term "now-family" from every other spec, so the line names coverage that no longer exists.
- Fix: Add a delta for `sql-functions/vs-expression-translator-date-diff-fns` whose CHANGED Background drops "and the now-family" from that bullet, and list it in plan.md § Features.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Dead Code Removal (`now_family_falls_through`)
- Issue: The test asserts that the translator refuses `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP`. The changed date-fns Background keeps that behavior ("The clock functions … are not translated"), and the plan adds no translator arm. `specs/testing.md` § Regression guards deletes only "A test whose assertion a change makes false", so deleting a test whose assertion stays true removes the guard that a missed fold falls back to a refusal instead of a scan-side clock.
- Fix: Remove the row from § Dead Code Removal, and add to § Implementation a task to extend the test's name list with `LOCALTIMESTAMP` and drop its scenario reference.

## Design Depth

#### [DESIGN_VIOLATION] FINDINGS
- Location: intent.md D2; plan.md § Implementation step 4
- Issue: Information hiding: which node is a constant is decided by `is_literal_selectlist_item` (grouped aggregation) and `is_constant_sort_key` (the constant `ORDER BY` drop), both keyed on `literal_*` node types. D2's select-list form, `CAST(literal_timestamp AS TIMESTAMP WITH LOCAL TIME ZONE)`, is a `function_scalar_cast` node that neither recognizes. `SELECT CURRENT_TIMESTAMP AS run_at, region, COUNT(*) FROM t GROUP BY region` then fails grouped detection (`classify_scalar_over_aggregate` returns `None` for an item without an aggregate) and routes to the qualified wrapper over raw rows. Today Exasol computes `CURRENT_TIMESTAMP` itself and the partial aggregation is pushed, so this is a regression. The same cast inside an aggregate argument such as `SUM(CASE WHEN ts > ADD_HOURS(CURRENT_TIMESTAMP, -24) THEN 1 ELSE 0 END)` declines in the DataFusion dialect and loses the split as well. The live check shows the plain literal is equivalent wherever the value only feeds a comparison or a function, not only under `filter`, `having`, and a join condition. Only a select-list item whose declared type is `TIMESTAMP WITH LOCAL TIME ZONE` needs the zone-typed form. For that item, `literal_timestamputc` carrying the UTC instant is an existing literal node: both constant checks recognize it, the existing wrapper renders it as `CAST(CONVERT_TZ(TIMESTAMP '<utc>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, and it returns the session's native value even when the session zone differs (ADR `route-non-emittable-selectlist-to-qualified-wrapper`, Consequences: "The wrapper returns the session-local value").
- Fix: Rewrite D2 so `CURRENT_TIMESTAMP` folds into the plain `literal_timestamp` everywhere except a select-list item whose `selectListDataTypes` entry is `TIMESTAMP WITH LOCAL TIME ZONE`, where it folds into `literal_timestamputc` of the instant (or, if the plan keeps the cast, make both constant checks recognize it). Update plan.md step 4, the unit test `current_timestamp_folds_plain_in_filters_and_cast_elsewhere`, and the scenario "A session zone that differs from the folding zone gets the folding zone's values" for the projected value the chosen form returns. Add a scenario that a grouped query selecting `CURRENT_TIMESTAMP` beside an aggregate keeps the partial-aggregate split in `EXPLAIN VIRTUAL`.

no objection — axis checked (ADR and architecture): D8 is the only `ADR: yes` direction; it names rule-2 criterion 4, states its ADR search result, and records a design decision rather than a signature or path, so no `[ADR_OVERPROMOTION]`. The architecture delta's BASE matches `git hash-object specs/architecture.md`, its single CHANGED block copies the whole current Constraints section with the two clock lines added, and plan.md states no component, interface, data flow, or external system the delta lacks (`chrono-tz` is a library, and the External Dependencies section lists only services), so no `[ARCHITECTURE_DRIFT]`. The ADR digest holds no clock or now-family decision, and `route-non-emittable-selectlist-to-qualified-wrapper` governs routing, which the plan keeps, so no `[ADR_CONFLICT]`.
