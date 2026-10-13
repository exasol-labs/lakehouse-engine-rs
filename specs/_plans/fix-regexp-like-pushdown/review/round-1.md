# Plan Review Findings: fix-regexp-like-pushdown (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 8 (Blockers: 1, Advisory: 7)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now this plan failed. Three ways it happens:

1. A contributor re-advertises `FN_PRED_REGEXP_LIKE` with an anchored rendering and updates `CAPABILITIES`, the docs Comparison row, and `reports_audited_capability_set` together. The docs-vs-code test passes because both lists agree. Its "Handled by Exasol" check misses the contradiction, because the regex row names `REGEXP_LIKE`, not `FN_PRED_REGEXP_LIKE`. Only the E2E alternation pattern catches the anchoring, and only because `event-01` is the one value the anchored form accepts. The plan never states that property, so a later edit drops it. Routed to Feasibility (task 3.5) and Requirement Quality (task 3.1).
2. The human PR reviewer, who has already asked twice for concise specs, rejects the PR. The re-authored Backgrounds still carry rationale and internal detail: a new four-bullet regex rationale in the scalar-fns spec that repeats ADR candidate [1], and function names and internal step order in the LIKE-coercion spec. The touched select-list scenario and the docs page still narrate history, against acceptance criterion (5). Routed to Requirement Quality and Prose Quality.
3. The `EVENTS` fixture is renamed. `e2e_regexp_like_returns_native_rows` keeps passing but no longer discriminates, because its native check runs on hard-coded synthetic names and never reads the fixture. Routed to Feasibility.

## Intent Fidelity

No objection, axis checked. Each acceptance criterion maps to a task:

- (1) `FN_PRED_REGEXP_LIKE` is removed by task 3.3. Tasks 1.2 and 2.1 remove the handling code, and grep 4.4 checks the result. I verified the only non-test sites: `capabilities.rs:29`, `vs-expression/src/lib.rs:34/171/572/751`, and `pushdown/support.rs:392/770`. The plan covers all of them.
- (2) Decision [1] supersedes ADR 018, as interview Q1 decided. The scalar-fns and string-conversion deltas list `REGEXP_LIKE` with the regex functions.
- (3) Task 3.4 adds the ten omitted names. I verified each one is in `CAPABILITIES` and absent from `docs/capabilities.md`. The task also adds the `REGEXP_LIKE` row and moves inner joins out of "Handled by Exasol".
- (4) Task 3.1 covers the docs test.
- (5) Decision [3] covers criterion (5). The remaining gaps are in Prose Quality.
- (6) Decision [2] swaps the issue's patterns for patterns that match the seeded data. The swap satisfies the criterion:
  - `'event-1'` reproduces the partial match: 10 rows through the defect, 0 natively.
  - `'(e|ev)(e|ent-01)?'` has the shape of `'(A|AR)(G|GENTINA)?'`. The first alternative `e` wins, the optional group matches empty at `v`, and the first PCRE match ends at offset 1. An anchored pattern backtracks to `ev` plus `ent-01` and accepts `event-01`.
  - The look-around pattern reproduces the scan error.
  - The issue's literal patterns match nothing in any seeded table (`seed.rs:507-510`, and `customer-01` to `customer-05`). Using them would break the discriminating-fixture rule in `specs/testing.md` § Correctness oracles.
  - The `EXPLAIN VIRTUAL` sibling-conjunct proof is task 3.6.

Interview Q3 decided to remove the code from the shared `vs-expression` crate.

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks, task 3.5
- Issue: The task claims that running each predicate natively over `SELECT 'event-' || LPAD(TO_CHAR(LEVEL), 2, '0') AS name FROM DUAL CONNECT BY LEVEL <= 20` makes "a fixture change that stops the test from discriminating fail with a message saying so". That query never reads the virtual-schema table, so it cannot detect a fixture change. Suppose `seed.rs` renamed the events to `evt-NN`. Then `'event-1'` returns no rows both through the virtual schema and natively. The expected-ids assertion ("none") still passes, and the test stops discriminating without any failure. The task also lists the defect results (10, 20, error) but not the anchored result for `'(e|ev)(e|ent-01)?'` (id 1, `event-01`). That result is what makes the pattern stand in for `'(A|AR)(G|GENTINA)?'` and guard against the rejected anchoring alternative of decision [1].
- Fix: In plan.md task 3.5, before the predicate checks, add an assertion that `SELECT name FROM <vs_table()> ORDER BY id` equals the 20 synthetic names. Its failure message should say that the fixture no longer reproduces the defect. Only then use the synthetic native counts as the oracle. In the `'(e|ev)(e|ent-01)?'` bullet, add "an anchored `^(?:<pat>)$` rendering would return id 1".

Verified without objection:
- Test helpers exist. `include_str!` of a workspace file from a test is established practice (`tests/build_convention.rs` reads `AGENTS.md`). `explain_virtual_pushdown_request` is at `e2e_capability_test.rs:2199`, and `EXPLAIN VIRTUAL` echoes the capability list (`e2e_advertises_inner_equi_join_capability`).
- I measured the character-class claim in decision [1] on the Docker Exasol 2025.1.16. `UNICODECHR(233) REGEXP_LIKE '\w'` is false, `UNICODECHR(8195) REGEXP_LIKE '\s'` is false, and `('a' || UNICODECHR(233)) REGEXP_LIKE 'a\b.'` is true. Positive controls `'a' REGEXP_LIKE '\w'` and `' ' REGEXP_LIKE '\s'` are true. Exasol's classes are ASCII-only, as the plan states.

## Requirement Quality

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: specs/_plans/fix-regexp-like-pushdown/sql-functions/vs-expression-translator-scalar-fns/spec.md § Background, the block from "Exasol evaluates every regular-expression function and predicate itself" through "would still be pushed at the DataFusion scan."
- Issue: The plan adds a rationale block that no scenario of this spec depends on. The block has four bullets: "DataFusion's `regexp_like` matches anywhere in the string. Exasol's `REGEXP_LIKE` takes the first PCRE match...", "Anchoring the pattern as `^(?:<pat>)$` does not reproduce that rule...", "The `regex` crate rejects backreferences and look-around... its `\d`, `\w`, `\s`, and `\b` match Unicode...", and "DataFusion has no `regexp_substr`...". A compile-check paragraph follows. The spec's only regex scenario, "Regexp scalar functions are deliberately not translated", asserts the decline error, the `None` result, both dialects, and the unadvertised capabilities. None of its steps depends on why the semantics differ. The anchoring bullet records a rejected alternative, which belongs to the decision log. Decision [1]'s Rationale and Alternatives already carry it, and [1] becomes the ADR. The Background rule makes this a BLOCKER, and spec-conciseness Rule 1 says a fact needed only to understand why belongs in the decision log.
- Fix: Replace that block with at most two sentences that state the current rule. Suggested text: "`REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, and `REGEXP_COUNT` are not translated in either dialect, and Exasol evaluates them (issue #106, #TBD), because DataFusion's Rust `regex` matching does not reproduce Exasol's PCRE matching." Delete the four bullets and the compile-check paragraph. ADR 018, which [1] supersedes, is the only active ADR that holds the `regexp_substr` and argument-shape fact. Add that fact as one clause to decision-log.md [1] Rationale so the superseding ADR keeps it.
- Escalation: MECHANICAL. Resolved by editing one Background block and one Rationale line. Decision [1] already holds the rationale, so no judgment call is needed.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md tasks 3.1 and 3.4; specs/_plans/fix-regexp-like-pushdown/vs-adapter/create-virtual-schema/spec.md, scenario "Adapter reports its pushdown capabilities"
- Issue: The scenario requires "`docs/capabilities.md` SHALL NOT list an advertised capability as one Exasol handles". Test 3.1 enforces this by comparing the backticked tokens in the first column of the "Handled by Exasol" table with `CAPABILITIES`. Task 3.4's new regex row names functions (`REGEXP_LIKE`, `REGEXP_REPLACE`, ...). The existing rows name `MEDIAN`, `LISTAGG`, and `CURRENT_DATE`. None of these is a capability identifier, so the check can never fire for those rows. A page that lists `FN_PRED_REGEXP_LIKE` in the Comparison row and `REGEXP_LIKE` under "Handled by Exasol" passes it. The scenario also says "outside its 'Handled by Exasol' section", but the test reads only the rows above that heading. The task does not say what the test does when the heading is missing.
- Fix: In task 3.4, put `FN_PRED_REGEXP_LIKE`, `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and `FN_REGEXP_COUNT` in the first column of the regex row, next to the function names. In task 3.1, make the test fail with a clear message when the `## Handled by Exasol` heading is missing. In the scenario, change "outside its" to "above its".

#### [IMPLEMENTATION_LEAKAGE] ADVISORY
- Location: specs/_plans/fix-regexp-like-pushdown/pushdown-types/pushdown-planning-like-type-coercion/spec.md § Background
- Issue: The delta replaces the whole Background with `DELTA:CHANGED` and keeps four statements that no scenario of the merged spec depends on. I checked the eight changed scenarios and the two unchanged nested-LIKE scenarios for each term:
  - "The type dispatch and rewrite happen in the adapter (`pushdown/support.rs`), not in `vs-expression`..."
  - "The primitive visits a `predicate_like` node's OWN `expression` and `pattern` children before the LIKE dispatch runs on that node. This is inert..." This is internal step order.
  - In the select-list bullet, the function names `like_subject_type_guard` and `guard_like_subject` and the pass order "(LIKE guard, then string-function-argument guard, then decimal-stringification rewriter)".
  - "The Unicode-versus-ASCII fold divergence between the two `col_types` builders..."

  These statements predate the plan, and the plan changes only the `REGEXP_LIKE` token. That is why this finding is ADVISORY, not a BLOCKER. Spec-conciseness Rule 4 still applies to a block this plan rewrites: "Do not describe the adapter's order of internal steps".
- Fix: Delete the first, second, and fourth statements. Cut the select-list bullet to its observable rule: "The select-list projection runs the same subject dispatch as the WHERE-clause filter, through one shared pipeline (issue #219)."

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: specs/_plans/fix-regexp-like-pushdown/sql-functions/vs-expression-translator-scalar-fns/spec.md § Background, "Verified on live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`)"
- Issue: `docker-compose.yml:77` pins `docker-db:2025.1.16` (commit 0f5291d, #401), and plan.md Context reports its measurements on 2025.1.16. The re-authored block states a pin that is false. Spec-conciseness Rule 4 also moves version numbers out of specs.
- Fix: Delete the parenthetical "(the image pinned in `docker-compose.yml`)". If the BLOCKER fix above rewrites this Background, make this change in the same edit.

## Task Breakdown

#### [SCOPE_CREEP] ADVISORY
- Location: plan.md task 3.6; § Scenario Coverage, row "Regular-expression capabilities are not advertised"
- Issue: Task 3.2's `reports_audited_capability_set` already proves that `FN_PRED_REGEXP_LIKE` is absent from `CAPABILITIES`. `EXPLAIN VIRTUAL` echoes that same constant. The E2E capability-echo assertion in 3.6 therefore tests the same fact again at a higher layer. The assertion "the serialized filter contains no `regexp` in any case" is implied by "the filter is a `predicate_less` over column `ID`". The test-strategy-review rule 3 says: test each behavior once, at the lowest layer that can prove it.
- Fix: Remove the `explain_virtual_sql` capability-echo assertion and the "no `regexp`" assertion from task 3.6. Remove that test's `/// Scenario: Regular-expression capabilities are not advertised` line. Map the scenario to `reports_audited_capability_set` only in § Scenario Coverage.

Verified without objection:
- Each of the nine deltas has an implementing task or a mapped existing test. All 14 test names cited in § Scenario Coverage exist.
- No `dispatch_golden` fixture, benchmark query, or Delta or Iceberg pruning path references the regex predicate.
- The A-before-B ordering is safe. One correction to the "Depends on" note: only the `support_tests.rs:3415` row fails once A lands. The `:3579` row still widens and passes. `like_guard_regexp_date_subject_wraps_cast` fails after task 2.1, not after A.

## Design Depth

No objection, axis checked:
- Decision [1] (`Promotes to ADR: yes`) passes `/speq:adr-rules`. It names rule-2 criteria 2 and 4 and states the ADR search result. It supersedes the live ADR 018; ADR 002 is already superseded by 018. Its Decision holds no paths or signatures. Its `Architecture: no change` line gives a reason, and `specs/architecture.md` mentions neither regular expressions nor the capability list (only the protocol line, line 57).
- The plan conforms to `declaration-gates-dispatch-and-sweep` (ADR 037: withdraw the declared name and the capability together), to `no-exasol-side-fallback-for-an-advertised-capability` (ADR 045), to the capability-invariant ADR, and to "Keep the pushable-node whitelist". It removes the node from the whitelist and does not invert it.
- The plan adds no module, interface, or boundary.
- Decision [2] is `no` and names a real rejected alternative.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: specs/_plans/fix-regexp-like-pushdown/pushdown/pushdown-planning-selectlist-expressions/spec.md; `docs/capabilities.md:38` (task 3.4)
- Issue: Acceptance criterion (5) asks specs and docs to describe current behavior only. Decision [3] applies this only to "This delta SUPERSEDES" and "REPLACING" passages. Other history narration stays in text this plan re-authors or rewrites:
  - Scenario "Scalar select-list expression is pushed into the scan-driving query": "it widened through the unknown-node arm before and widens through the probe now".
  - The same scenario: "none encodes the pre-guard behaviour, so THIS feature moves no fixture byte at all".
  - The Background bullet "**This feature moves no `dispatch_golden` fixture byte.**".
  - The docs page, which task 3.4 rewrites: "`LIMIT_WITH_OFFSET` is now advertised (issue #191)".
- Fix: In the select-list delta, restate the first item as "a top-level `function_aggregate` item widens through the same probe". Restate the fixture clause and bullet as the current rule: every `dispatch_golden` fixture stays byte-identical except its scan-spec `storage` value. In task 3.4, add a bullet that replaces "is now advertised (issue #191)" with "is advertised (issue #191)".

#### [PROSE_BLOAT] ADVISORY
- Location: decision-log.md § Design Decisions [3]
- Issue: Entry [3] records a writing convention and names no rejected alternative. `/speq:adr-rules` rule 4 gives a Design Decisions entry only to a choice with a rejected alternative. Spec-conciseness Rule 2 keeps writing conventions out of the decision log.
- Fix: Delete decision [3] from decision-log.md.
