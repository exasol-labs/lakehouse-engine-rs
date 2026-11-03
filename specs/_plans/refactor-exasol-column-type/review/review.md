# Plan Review Findings: refactor-exasol-column-type

## Summary
- Axes checked: 6/6
- Total findings: 15 (Blockers: 4, Findings: 11)
- Intent Fidelity blockers: 0

Why this plan may fail:
1. Task 3.1 moves the per-column type map into format-readers' `ResolvedScan`. That creates a pushdown-planner and format-readers dependency cycle, and the architecture delta does not record it (Design Depth, `[ARCHITECTURE_DRIFT]`).
2. Done checks 1 and 2 have no mechanical proof. About 20 bare-name `(String, String)` signatures and two `String` type fields are missing from the census and from task 3.2. Task 5.1's greps still pass while those signatures and fields remain (Task Breakdown, `[TRACEABILITY_GAP]`).
3. After recording, the spec library contradicts the code. A recorded type-mapping scenario requires a "string-returning owner" that task 1.2 removes (Requirement Quality, `[REQUIREMENT_CONFLICT]`).
4. Done check 4 rests on a test that passes on unchanged code (Feasibility, `[UNSTATED_ASSUMPTION]`).

## Intent Fidelity

no objection — axis checked for the core ask: the plan covers every addendum direction of #404. It adds one owned type (task 1.1) and one per-column type that absorbs `BinaryColumns` and is keyed by `ColumnRef` (task 3.1). It adds `ProjectedColumn` (task 4.1), sequences the work after #578, and removes `BinaryColumns` and the parallel map (done check 1). `LogicalField.arrow_type` and the SDK `ExaType` are out of scope as the interview and the issue state. Decision-log [2] justifies keeping `arrow_to_exasol_type` against the issue's "dead" claim with two verified production callers.

#### [INTENT_DRIFT] FINDINGS
- Location: plan.md § Impact; plan.md task 1.2; `specs/_plans/refactor-exasol-column-type/vs-adapter/create-virtual-schema-declaration-details/spec.md` § Scenario "A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision"
- Issue: The interview answer reads "one JSON renderer, byte-exact". Task 1.2 changes the answer `exasol_type_to_json` gives for `CHAR(n)`, which today is `{"type":"varchar","size":2000000}`. It also changes the answer for an `ASCII` type, which today carries no `characterSet`. The delta adds a step that requires the new answers. Neither input is reachable, but the change departs from "byte-exact", and the user has not acknowledged that.
- Fix: In plan.md § Impact, add one sentence that names this as the only departure from the interview's byte-exact answer, with its reason: the JSON renderer becomes the inverse of the parser. The other option is to keep the recorded answers and delete the `CHAR(n)`/`ASCII` step from the delta.

#### [SCOPE_REDUCTION] FINDINGS
- Location: plan.md § Out of scope, bullets 3 ("`vs-expression`'s `render_cast_target` keeps rendering `CAST` targets ...") and 4 ("The pushdown type checks keep treating a `TIMESTAMP` column as an unknown type ...")
- Issue: The issue says "rendered once at each output edge" and "Strings should then appear only where SQL text is generated". Bullet 3 keeps a second JSON-to-SQL-type renderer, with its own `DOUBLE` spelling and no `CHAR` cap. Bullet 4 keeps the issue's TIMESTAMP evidence unresolved. AGENTS.md says: "Cite an existing issue as `(#83)`, otherwise mark `(#TBD)` and list it as an open question." Neither bullet carries an issue reference, and the user has not agreed to either deferral.
- Fix: Append `(#TBD)` to both bullets, and add an `## Open questions` list to plan.md that asks the user to confirm each deferral in one sentence.

## Feasibility

no objection — `[NFR_IGNORED]` checked: the scan-spec wire format and the `LogicalField` tag stay unchanged. The declared `dataType` JSON that Exasol persists stays byte-identical for every type a catalog produces. The change adds no I/O, credential path, or concurrency. The map is built after `admission.admit`, so the ADR `permission-check-runs-where-a-pushdown-first-reads-table-metadata` still holds.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Impact, § Requirements (Done check 4), § Verification; `specs/_plans/refactor-exasol-column-type/pushdown/pushdown-planning/spec.md`; decision-log.md [3]
- Issue: plan.md § Impact says that two inputs which no well-formed request reaches "change their answer", one of them being "a `DECIMAL` `dataType` without precision and scale". That answer does not change. On this branch, `exasol_type_from_json` (`types/mapping.rs`) already reads `{"type":"DECIMAL"}` as `DECIMAL(18,0)` (`unwrap_or(18)`, `unwrap_or(0)`). `is_numeric_exasol_type` accepts it (`starts_with("DECIMAL(")`), and `sum_emit_type` returns `DECIMAL(36,0)`. Every step of the NEW scenario holds today, so `sum_over_a_decimal_without_stated_precision_pushes_a_decimal_36_0_partial` passes on unchanged code and cannot prove Done check 4. `specs/testing.md` § Unit tests requires that "A unit test that pins a rule uses an input that fails under the wrong rule." The two checks disagree only on a bare `"DECIMAL"` string. No producer emits that string, and the planned enum cannot hold it. The scenario adds a requirement for a request Exasol never sends and changes no behavior, which `.speq/plan-hook.md` forbids: "No speculative requirements."
- Fix: Delete `specs/_plans/refactor-exasol-column-type/pushdown/pushdown-planning/spec.md`. Delete its row in plan.md § Features, its row in § Scenario Coverage, and both `pushdown-planning` rows of § Manual Testing. Those rows also create a `PLAN_CHECK` schema that is never dropped, and they expect a request `dataType` that `capture-pushdown-payload.sh` does not print. Rewrite Done check 4 as: "The DECIMAL disagreement cannot occur: `ExasolType::Decimal` always carries precision and scale, and the numeric check of task 2.2 matches every `Decimal` and `DoublePrecision`." Remove the `sum_over_a_decimal_...` test from task 2.2. In § Impact, name only the `dataType` JSON of a `CHAR` or `ASCII` type. Delete decision-log [3], because the interview's enum choice leaves it no real alternative (hook: "A decision that has no real alternative gets no entry"). Then remove the `(decision-log [3])` reference from Done check 4.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md task 3.1 ("One constructor takes the involved columns' Exasol types, the declared `LogicalField`s, and `binary_as_string`, with the width rule of `BinaryColumns::widths`")
- Issue: `BinaryColumns::widths` iterates the logical schema. The order of `ColumnTypes` matters, because `project_columns`' `full_row()` emits the map in its own order and the `EMITS` columns follow that order. A refused column has no `LogicalField` but must stay in the map. The task does not say whether the involved columns or the logical schema drive the iteration.
- Fix: In task 3.1, state that the map lists every involved column in request order. A `LogicalField` matched by uppercase name only sets `binary`, and a column without a match gets no `binary`.

#### [EFFORT_MISESTIMATION] FINDINGS
- Location: plan.md tasks 1.1, 2.1, 3.2
- Issue: `notes/planning.md` counts about 700 host test lines with type literals (`support_tests` 120, `grouped_agg_tests` 114, `joins/sql_builders_tests` 84, and others). Task 2.1 retypes every carrier but names no test update. No task says how a test builds an `ExasolType` once the production string parsers are gone. Stages 2 and 3 rewrite the same signatures and fixtures twice: first `(String, String)` becomes `(String, ExasolType)`, then the slice becomes `ColumnTypes`.
- Fix: In task 1.1, state how tests construct values: enum literals, or `exasol_type_from_json(&json!(..))` behind one helper in `adapter/pushdown/test_support.rs`, with no test-side string parser. In task 2.1, name the test files it updates. Alternatively, move the slice-to-`ColumnTypes` change of task 3.2 into task 2.1, so each test fixture changes once.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: recorded `specs/scan-types/type-mapping/spec.md` § Scenario "A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR" (lines 91-92); `specs/_plans/refactor-exasol-column-type/scan-types/type-mapping/spec.md`
- Issue: The recorded steps read "exactly one SHALL own the two returned STRINGS that branch on it" and "the string-returning owner SHALL be declared PRIVATE to `types/mapping.rs`". Task 1.2 makes `catalog_decimal_to_exasol` return `ExasolType`, and Done check 2 forbids a string carrier. After recording, the library requires a string-returning owner that the code no longer has. The plan's type-mapping delta changes only the neighbouring scenario "The Iceberg-to-Arrow logical mapping reads the same catalog-decimal guard".
- Fix: Add a DELTA:CHANGED block for "A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR" to `specs/_plans/refactor-exasol-column-type/scan-types/type-mapping/spec.md`. In it, replace "the two returned STRINGS" with "the two returned Exasol types" and "the string-returning owner" with "the type-returning owner". Keep every other step and every recorded pair byte-identical. Add the scenario to plan.md § Scenario Coverage with the unit test in `types/mapping_tests.rs` that proves it today.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: recorded specs that name refactored symbols or the string carrier
- Issue: Several recorded lines become false or inconsistent:
  - `pushdown-types/pushdown-planning-like-type-coercion/spec.md:16` says "`project_columns` has three callers: `extract_projection` (single-table), ...", but task 3.2 deletes `extract_projection`.
  - `pushdown-types/pushdown-planning-char-type-declaration/spec.md` lines 22, 30, 38, and 74 say "derives that ordinal's Exasol type string". This plan's delta rewrites the same feature's Background away from "type string".
  - `vs-adapter/create-virtual-schema-declaration-details/spec.md:35` says "a `col_types` entry", in a feature this plan already deltas.
  - `delta/delta-type-mapping/spec.md` lines 15 and 64 and `unity-catalog/unity-catalog-create-virtual-schema/spec.md:73` say "declaration strings" and "bare string".
  - `direct-storage/parquet-directory-seam/spec.md:58` says "the Exasol type string".
- Fix: Add a DELTA:CHANGED Background to `specs/_plans/refactor-exasol-column-type/pushdown-types/pushdown-planning-like-type-coercion/spec.md` that replaces "`extract_projection` (single-table)" with "the single-table path, after table resolution". Leave the wording-only lines unchanged, and add one sentence to plan.md § Out of scope stating that recorded "type string" wording outside the delta'd scenarios is not rewritten.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md task 1.2 ("the inverse of `exasol_type_from_json` for every variant"); `specs/_plans/refactor-exasol-column-type/vs-adapter/create-virtual-schema-declaration-details/spec.md`, step "a `CHAR(n)` type or an `ASCII` character type ... SHALL serialize to the `dataType` object that `exasol_type_from_json` reads back as the same type"
- Issue: Task 1.1 keeps `Decimal`'s scale signed. `exasol_type_from_json` reads `scale` with `as_u64`, so `Decimal { precision: 10, scale: -2 }` renders `"scale": -2` and reads back as `DECIMAL(10,0)`. The parser also caps a precision above 36, a `CHAR` size above 2,000, and a `VARCHAR` size above 2,000,000. The enum can hold all of these values, and none of them round-trips. A test named `exasol_type_to_json_round_trips_every_variant` either fails or narrows its inputs without saying so.
- Fix: In task 1.2 and in the delta step, limit the round trip to the values the input edges produce: precision 1 to 36 and scale 0 to 36 for a decimal, `CHAR` size up to 2,000, and `VARCHAR` size up to 2,000,000. List the excluded values in the test's doc comment.

#### [IMPLEMENTATION_DETAIL] FINDINGS
- Location: `specs/_plans/refactor-exasol-column-type/scan-types/type-relaxation/spec.md` § Background, bullet "The emit boundary needs no pair knowledge. ..."
- Issue: type-relaxation has one scenario, and none of its GIVEN/WHEN/THEN steps depends on the emit boundary. Its steps cover table-schema registration, the column-binding adapter, and the per-file cast. The plan rewrites the bullet only to remove `exasol_type_to_arrow`. The Background is already 10 bullets and a table, which predates this plan.
- Fix: Delete that bullet from the delta's Background instead of rewriting it. Leave the other bullets as recorded.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md § Census of Exasol type strings; § Implementation Tasks 2.1, 3.2, 5.1; § Requirements (Done checks 1 and 2)
- Issue: The #404 addendum asks: "Pass it as one named type, not as a slice of tuples". Done check 1 says "`ColumnTypes` is the one per-column map". Task 3.2 lists only the filter, binary, broadcast, empty-side, top-N, request-shape, and empty-result readers. About 20 more signatures take a bare-name `&[(String, String)]` or `Vec<Vec<(String, String)>>` today, and no task names them:
  - `adapter/pushdown/mod.rs`:278 (`build_dispatch_sql`)
  - `support.rs`:85, 170, 199, and 230 (the `EMITS` builders)
  - `grouped_agg.rs`:333, 426, 468, and 504 (`col_type_for`, `validate_agg_col_types`, `partial_emits_items`)
  - `joins/sql_builders.rs`:165, 231, 263, 327, 509, 770, 850, 904, and 938-949 (`EmptyRelation`)
  - `joins/rendering.rs`:159 and 259-269
  - `joins/planning.rs`:251

  Task 2.1 retypes these signatures to `(String, ExasolType)`, and no later task turns them into `ColumnTypes`. The census also misses three `String` type carriers: `SingleGroupItem::ScalarOverAggregate.declared_type` (`single_group_agg.rs`:21), `GroupedSelectItem::ScalarOverAggregate.declared_type` (`grouped_agg.rs`:44), and `cast_to_declared_type`'s `declared: Option<&str>` (`support.rs`:1032). Task 5.1 greps only for deleted names and type literals. It passes while the slices and `String` fields remain, so Done checks 1 and 2 have no proof.
- Fix: In plan.md § Census, add the two `declared_type` fields and `cast_to_declared_type`'s parameter as carriers. In task 3.2, name every signature listed above and state that each takes `&ColumnTypes` (one leg's map on the join paths) in place of a tuple slice. In task 5.1, add `rg -n '\(String, (String|ExasolType)\)|declared_type: String|_types: (Vec<String>|&\[String\])' crates/lakehouse-engine/src/adapter/pushdown -g '!*_tests.rs'` with the expected output "no output".

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § Verification § Scenario Coverage; tasks 1.2 and 2.2
- Issue: No test in `crates/` carries a `/// Scenario:` line for any of the four mapped scenarios. `catalog_decimal_guard_is_shared_by_both_source_kinds` and `cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_digit` carry none. `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` carries only "A timestamp filter compares at full precision". `specs/testing.md` § Coverage rule requires one verbatim line per scenario, and no task adds these lines.
- Fix: In task 1.2, add the verbatim `/// Scenario:` line for each mapped scenario to its mapped test. These are the unit test for the type-mapping scenario and the two tests in `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs`. In § Census, change "None of these files changes" to say that `timestamp_precision.rs` gains only these lines.

#### [CLUSTER_INCOHERENCE] FINDINGS
- Location: plan.md § Parallelization, group A Knowledge
- Issue: Task 1.2 says "Pushdown callers call `to_string()` at the boundary until stage 2". Group A therefore edits the `exasol_type_from_json` callers under `adapter/pushdown/` (`support.rs`, `grouped_agg.rs`, `empty_result.rs`, `joins/planning.rs`), but its Knowledge column names none of them. Group B then edits the same lines again.
- Fix: Add the `exasol_type_from_json` caller files under `adapter/pushdown/` to group A's Knowledge column.

## Design Depth

no objection — `[SHALLOW_DESIGN]`, `[INFORMATION_COUPLING]`, and `[TACTICAL_SHORTCUT]` checked: `ExasolType` puts five string parsers behind one `Display` and the family predicates. `ColumnType` moves the binary-width rule into one constructor. Task 2.1 removes the stage-1 `to_string()` calls.

no objection — `[ADR_OVERPROMOTION]` checked: all three decision-log entries are `Promotes to ADR: no`.

no objection — `[ADR_CONFLICT]` checked against `single-authoritative-datafusion-to-exasol-type-mapping-with-json-fallback`, `collapse-exasol-facing-timestamp-string-keep-internal-tz-aware-arrow-type`, `declared-output-column-single-authority-emit-type`, `binary-reads-as-lowercase-hex-text`, `permission-check-runs-where-a-pushdown-first-reads-table-metadata`, `one-per-request-scan-source-resolver`, and `join-shard-ranks-by-emitted-value`. Each keeps holding: one mapping, plain `TIMESTAMP` for `timestamptz`, raw-byte comparisons, the single admission point, and the single resolver.

#### [ARCHITECTURE_DRIFT] BLOCKER
- Location: plan.md task 3.1; decision-log.md [1]; architecture.md § Components (format-readers and pushdown-planner lines)
- Issue: Task 3.1 says the resolver "passes it to `ResolvedScan::resolve` in place of `binary_as_string`, and `ResolvedScan` and `ResolvedJoinSide` carry it". `ResolvedScan` is a format-readers type (`adapter/pushdown/format/mod.rs`). The delta makes pushdown-planner the owner of "the per-column type map" and leaves format-readers at "depends on: column-notes, lakehouse-catalog, type-mapping". The plan therefore adds a format-readers dependency on pushdown-planner that the delta does not record. That dependency closes a cycle, because pushdown-planner already depends on format-readers (`/speq:design-philosophy`: "No cycles in the module dependency graph"; `[BOUNDARY_VIOLATION]`). It also puts the `pub(super)` type `ColumnTypes` (`support.rs`) into the field list of the `pub struct ResolvedScan`.
- Fix: Keep the map out of format-readers. In task 3.1, have `TableScanResolver::resolve` (`scan_resolution.rs`, pushdown-planner) build the map, apply `with_binary_literals` to the filter with it, and pass the retyped filter to `ResolvedScan::resolve`. `ResolvedScan::resolve` then takes neither `binary_as_string` nor the map. Have `TableScanResolver::resolve` return the map beside the `ResolvedScan`, and let `ResolvedJoinSide`, a pushdown-planner type, carry it. Change decision-log [1]'s Decision line to match. Leave the format-readers line in architecture.md unchanged.

## Prose Quality

#### [PROSE_UNCLEAR] FINDINGS
- Location: plan.md § Context, bullet 2
- Issue: The bullet says "Implementation starts after PR #580 (#578) merges. The plan is written against branch `refactor/column-node-owner`". PR #580 is merged (`7f9a0ee9` on `main`). Writing-guardrails rule 8 asks for the current state.
- Fix: Replace the bullet with "Builds on #578 (PR #580, merged), which added `ColumnNode`, `ColumnRef`, and `ColumnTypes`."

#### [PROSE_UNCLEAR] FINDINGS
- Location: plan.md § Census of Exasol type strings
- Issue: Entries such as "S:470", "G:256", "J:938", and "R:157" make the reader decode four one-letter file aliases that are defined once. Line numbers also drift with every rebase (writing-guardrails rule 5).
- Fix: Replace each alias with the symbol name, for example `column_exa_type` in `support.rs`. Keep a line number only where no symbol exists.
