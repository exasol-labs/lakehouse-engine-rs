# Decision Log: fix-vs-expression-dialect

## Interview

No live interview. The orchestrator ran `/speq:plan-pr` in headless mode and supplied issue #209's title and body verbatim as the authoritative scope, plus one instruction: follow the per-dialect convention issue #197 established for the `MOD` arm rather than inventing a second mechanism. Every decision below was made by the planner and is recorded here in place of a human answer, per the headless escalation rule. One question was later escalated and answered by a human; it is recorded last.

**Q (from the orchestrator's brief):** How should a dialect parameter be named or threaded for functions that do not currently take one?
**A (planner):** No new threading is needed. `Dialect` already reaches every arm of `render_expression_inner`; the arms simply never read it. See decision [1].

**Q (from the orchestrator's brief):** Fix all affected function families in one plan, or defer some?
**A (planner):** All of them, plus two further failure paths found during discovery. See decisions [2] and [3]. Nothing is deferred.

**Q (escalated to a human after plan-review round 1):** The plan presents the `decimal_to_varchar_exasol` Exasol-dialect rendering as one of three failure paths verified failing on live Exasol, but review found the node unreachable from every Exasol-dialect consumer. Keep it, relabelled as unreachable-today hardening, or drop it?
**A (human):** Drop it entirely. It is not reachable today, so it does not belong in this plan. Do not keep it as relabelled hardening. Track it separately if an adapter path later makes it reachable. Applied throughout; see the `[plan-review]` entry "Unreachable decimal-to-VARCHAR item dropped from scope".

## Design Decisions

### [1] One guarded match arm owns the Exasol-dialect rule

- **Decision:** State one rule, "in the Exasol dialect, render what Exasol sent", and give it a single owner: the `if dialect == Dialect::Exasol` guarded `function_scalar` arm that issue #210 already added for the string-function family. The math family, the field-shortcut date functions, `WEEK`, the `*_BETWEEN` family, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `GREATEST`, `LEAST`, `NULLIF`, `NULLIFZERO`, `ZEROIFNULL`, and the now-family join that arm, and the names it covers are declared once in a shared set (decision [12]). Constructs outside that set (`function_scalar_extract`, `predicate_like_regexp` and the `function_scalar` `REGEXP_LIKE` alternate encoding, the two timestamp literals) branch inline with `match dialect`, which is the shape issue #197's `MOD` fix uses.
- **Alternatives:** (a) A per-dialect name lookup table keyed by function name. Rejected: it adds a second mechanism and an indirection for a rule whose content is "do not translate", and a table cannot express the `EXTRACT`, `REGEXP_LIKE`, or timestamp-literal shape changes anyway. (b) A `Dialect` trait with two implementations. Rejected: the two dialects differ at roughly a dozen of some fifty arms, so two full implementations would duplicate the other thirty-eight and invite exactly the silent drift this plan exists to end. (c) A private `match dialect` inside each affected arm, following #197 literally everywhere. Rejected: it scatters one decision across ten arms, so a reader cannot see the rule and the next arm added has no obvious place to inherit it.
- **Rationale:** Per `/speq:design-philosophy`, the defect is a design decision with no owner: the same "which parser reads this fragment" question was answered independently in `render_cast_target`, in the `MOD` arm, and in the #210 string arm, and left unanswered in the rest. Giving the decision one home is the fix; the arm ordering that keeps `CAST`, `MOD`, `CONCAT`, and the operator names ahead of the guarded arm expresses the four exclusions without a negative-name list.
- **Promotes to ADR:** yes

### [2] Scope extended to two failure paths issue #209 does not name

- **Decision:** Fix, in the same plan, two further Exasol-dialect renderings verified during planning to fail on live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`): `predicate_like_regexp` — and its `function_scalar` `REGEXP_LIKE` alternate encoding — rendering `regexp_like(s, p)` (`syntax error, unexpected REGEXP_LIKE_`, since `REGEXP_LIKE` is an infix predicate in Exasol, not a function), and `literal_timestamp` / `literal_timestamp_utc` rendering `arrow_cast(...)` (`function or script ARROW_CAST not found`).
- **Alternatives:** Restrict the plan to the arms issue #209 enumerates and file two follow-up issues. Rejected: both are the same defect class on the same code path, both are reachable today (`FN_PRED_REGEXP_LIKE`, `LITERAL_TIMESTAMP`, and `LITERAL_TIMESTAMP_UTC` are all advertised), and each fix is a few lines inside an arm this plan already edits. Splitting them would ship a "systemic remainder" fix that still leaves a systemic remainder.
- **Rationale:** Issue #209 frames itself as the systemic fix for every renamed or re-shaped rendering, not as a list of seven queries. A fix that leaves two verified compilation errors in place would not satisfy that framing, and the regression test in decision [7] would fail on them anyway. A third candidate, the adapter-synthesized `decimal_to_varchar_exasol` node, was dropped from scope after review: it is unreachable from every Exasol-dialect consumer today (see the `[plan-review]` entry below).
- **Promotes to ADR:** no

### [3] Uniformity over minimal diff: functions that already parse in Exasol are folded into the verbatim rule

- **Decision:** Apply the verbatim rule to every Exasol-native scalar function, including the ones whose current rendering already parses in Exasol: `NULLIFZERO`, `ZEROIFNULL`, `GREATEST`, `LEAST`, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `DAYS_BETWEEN`, and the whole math family apart from `SIGN`. For most of these the emitted string changes only in name case; `NULLIFZERO` and `ZEROIFNULL` stop being rewritten to `nullif(x, 0)` and `coalesce(x, 0)`.
- **Alternatives:** Change only the arms that currently produce a compilation error, which is the smaller and lower-risk diff. Rejected on maintainability: a rule applied to some arms and not others is not a rule a future reader can apply. They would have to test each name against live Exasol to learn whether its current rendering is principled or merely lucky, which is how the `*_BETWEEN` family shipped broken in the first place.
- **Rationale:** The whole value of the verbatim rule is that it needs no per-function verification. Verified on live Exasol 2025.2.1 that every math name in the arm exists natively (`ABS`, `FLOOR`, `CEIL`, `SQRT`, `EXP`, `LN`, `DEGREES`, `RADIANS`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `SINH`, `COSH`, `TANH`, `COT`, `ROUND`, `TRUNC`, `LOG`, `POWER`, `ATAN2`), so folding them in changes no result.
- **Promotes to ADR:** yes

### [4] The now-family renders as bare Exasol keywords, correcting a latent wrong answer

- **Decision:** Render `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` as their own bare Exasol keywords in the Exasol dialect, with no parentheses and no collapsing of one name onto another.
- **Alternatives:** Leave `current_date()` and `now()` in place. Issue #209 claims Exasol rejects the parenthesised form, but that claim is false: both `SELECT current_date()` and `SELECT now()` execute successfully on live Exasol 2025.2.1, so neither is a compilation error and neither is required by the issue's stated scope.
- **Rationale:** The current mapping collapses `SYSDATE` onto `CURRENT_DATE` and `SYSTIMESTAMP` onto `CURRENT_TIMESTAMP`, erasing Exasol's database-time versus session-time distinction. That is a silent wrong answer rather than a loud failure, which is worse. The verbatim rule removes it for two lines of change, and this is the one behavior change in the plan that an operator could observe as a different value rather than as an error becoming a result. It is called out in `plan.md`'s Impact for that reason.
- **Residual gap and tracked follow-up:** The fix reaches the Exasol dialect only. The DataFusion dialect keeps `SYSDATE` → `current_date()` and `SYSTIMESTAMP` → `now()`, frozen by the "DataFusion output frozen" requirement, so the two dialects disagree for these four names and the wrong-answer path survives on the scan path. A GitHub issue MUST be filed for that residual collapse before this plan is recorded, per the no-silent-gap rule in `CLAUDE.md`. `plan.md` § Non-Goals names it as out of scope.
- **Promotes to ADR:** yes

### [5] The Exasol dialect imposes no arity check

- **Decision:** The Exasol-dialect verbatim arm forwards the argument list unchanged and does not validate argument count, even where the DataFusion-dialect arm does.
- **Alternatives:** Keep each family's existing arity check in both dialects. Rejected: Exasol's compiler emitted a call its own engine accepts, so a translator-side arity check on that path can only reject valid input.
- **Rationale:** This is not new; it is the rule the #210 string arm already follows, and it is load-bearing there. `vs-adapter/pushdown-planning-string-fn-type-coercion` declines a three-argument `INSTR(s, sub, start)` from the DataFusion scan precisely because the Exasol wrapper can still evaluate it verbatim. Extending the same rule to the other families keeps one behavior instead of two.
- **Promotes to ADR:** yes

### [6] No capability is added or withdrawn

- **Decision:** `capabilities.rs` is untouched. Every function affected by this plan stays advertised.
- **Alternatives:** Withdraw the capabilities whose Exasol-dialect rendering was broken until the fix is proven end to end. Rejected: the advertisement governs what Exasol may push to the node-local DataFusion scan, and the DataFusion-dialect rendering of every one of these functions is correct and unchanged. Withdrawing would remove working pushdown to fix a wrapper-only defect.
- **Rationale:** Keeping one capability set for both dialects is also why the declines stay symmetric: the delta specs state that an untranslated name falls through in both dialects, so the verbatim rule can never widen the translated set behind the capability list's back.
- **Promotes to ADR:** no

### [7] A per-node name-equality sweep test enforces the rule, so a future arm cannot silently forget it

- **Decision:** Add `exasol_dialect_renders_declared_verbatim_surface`: a table-driven unit test that renders one representative node per translated function name and per node type through `render_expression_exasol`. For each `function_scalar` row it asserts the rendering equals `<NAME>(<rendered args>)` built from the node's own uppercased `name`; for every other node type it asserts a per-dialect expected string. It also asserts every `function_scalar` name in the table is either a member of the declared `EXASOL_VERBATIM_FNS` set (decision [12]) or one of the five named exceptions — the four verbatim-rule exclusions (operator wire names, `MOD`, `CONCAT`, `CAST`) plus the `REGEXP_LIKE` alternate encoding, whose Exasol form is infix. The DataFusion-only token list (`signum`, `date_part`, `strpos`, `arrow_cast`, `character_length`, `octet_length`, `regexp_like(`, `current_date()`, `now()`, `nullif(`, `coalesce(`, bare `%`) is kept as a secondary assertion.
- **Alternatives:** (a) Rely on the per-family paired-dialect tests alone. Rejected: those only cover arms someone remembered to test, which is the failure mode that produced this issue. The `*_BETWEEN` family had per-function E2E parity tests and still shipped a broken Exasol rendering, because every one of those tests exercised the DataFusion path. (b) A deny-list of DataFusion-only tokens as the primary assertion, which was this plan's first shape. Rejected on review: a deny-list can only catch the arms already known today, so a future `SUBSTRING`, `NVL`, or `DATE_BIN` arm passes it. "Every DataFusion-only token" is also not a testable set, because it is unbounded.
- **Rationale:** Name equality is what makes the rule structural rather than reviewed: it is a property of the node, checkable without enumerating what the other dialect might emit, so it holds for names that do not exist yet. Driving the test from one table makes adding an arm one row. The guard still needs that row, but the declared-set membership assertion in decision [12] is what turns a forgotten row into a failure rather than a silent fall-through.
- **Promotes to ADR:** yes

### [8] Issue #209's code references are stale in three places, and its consumer-site list names the wrong site

- **Decision:** Record the corrections rather than carry them into the plan. (a) There is no `crates/lakehouse-engine/src/adapter/pushdown/joins.rs`; `joins` is a module directory and the functions the issue cites live in `joins/rendering.rs` and `joins/sql_builders.rs`. (b) The issue lists `LENGTH`, `UNICODE`, `UNICODECHR`, `INSTR`, and `LOCATE` as broken, but issue #210's fix (commit `3c0fe8a`, recorded in `e5a7e15`) already gave the whole string family an Exasol-verbatim arm; those names are correct today. (c) The issue's list of Exasol-dialect consumer sites is wrong. There are four production sites — `render_scalar_over_merge` (`grouped_agg.rs:424`), `render_expression_qualified` (`joins/rendering.rs:103`), `render_df_filter_qualified` (`joins/rendering.rs:116`), and `parse_declined_sort_key` (`topn.rs:138`) — and the one the issue omits is `parse_declined_sort_key`, which calls `render_expression_exasol_safe` for an expression ORDER BY element of the declined-ORDER-BY row-scan wrapper. The two further matches in `sql_builders.rs` are inside `mod tests`.
- **Alternatives:** Silently plan against the real code. Rejected: the stale references would otherwise be re-derived by the implementer and by the next planner reading the issue.
- **Rationale:** The omitted consumer site matters beyond bookkeeping: it means a declined ORDER BY over any affected expression is a further reachable instance of the same failure, so the fix's blast radius is wider than the issue states and the sweep test in decision [7] is the only practical way to cover all four sites at once. The `sql-comprehension/vs-expression-translator` delta lists all four so the next reader inherits the correct map.
- **Promotes to ADR:** no

### [9] Spec drift from #197 and #210 is corrected in the same deltas

- **Decision:** The `-scalar-fns` delta updates the `MOD` scenario to state the shipped `MOD(a, b)` Exasol form (#197) and the string-function scenario to state the shipped Exasol-verbatim arm (#210). Neither behavior was reflected in `sql-comprehension/vs-expression-translator-scalar-fns`.
- **Alternatives:** Leave the drift and document only new behavior. Rejected: the plan's whole subject is the per-dialect rendering rule for these exact scenarios, and leaving two shipped Exasol-dialect behaviors undocumented in the feature that owns them would leave the spec contradicting the code the plan is about to extend.
- **Rationale:** #210's spec deltas landed in `vs-adapter/pushdown-planning-string-fn-type-coercion`, the adapter-side feature, and never updated the translator-side feature whose scenario text still describes `strpos` as the only rendering. Correcting it costs one clause per scenario. The same drift left #210's Exasol string arm with no translator-side unit test, which task 1 now adds.
- **Promotes to ADR:** no

### [10] Translator scenarios map to unit tests, wrapper-compilation scenarios to E2E

- **Decision:** Every scenario that asserts a rendered string maps to a unit test in `crates/vs-expression/src/lib.rs`; every scenario whose truth depends on Exasol actually compiling the SQL maps to an integration test in `crates/lakehouse-engine/tests/e2e_capability_test.rs` using the in-session native-oracle idiom already used in that file's section 8.16.
- **Alternatives:** Integration tests for everything, per the default in `/speq:planning`. Rejected under that skill's own exception: `render_expression` and `render_expression_exasol` are pure `&Json -> Result<String>` functions with no I/O and no ambient state, which is exactly the pure-computation case unit tests are reserved for. The existing Exasol-dialect tests (`renders_mod_as_function_call_in_exasol_dialect`, `renders_cast_varchar_exasol_dialect_includes_length`) are unit tests for the same reason.
- **Rationale:** The paired-dialect assertion convention those tests use (same JSON node, one `assert_eq!` per dialect) is what freezes the DataFusion output, and it only works as a unit test. An E2E test only proves the fix when its SQL actually reaches the Exasol dialect, which is why the `REGEXP_LIKE` acceptance test is specified in a select-list position rather than a WHERE clause.
- **Promotes to ADR:** no

### [11] Same-file tasks are sequenced, not grouped as parallel

- **Decision:** Tasks 1 through 8 all edit `crates/vs-expression/src/lib.rs`, so each is its own group and they run in order: A, then B1 through B4 (tasks 2 to 5), then C1 through C3 (tasks 6 to 8). Only Group D holds two tasks that genuinely run concurrently.
- **Alternatives:** (a) Run the same-file arms concurrently as independent edits. Rejected: concurrent sub-agent edits to one 3,351-line file conflict, and task 1 restructures the guarded arm the others sit beside. (b) Keep them in one "parallel group" annotated as must-run-sequentially, which is how this plan first expressed it. Rejected on review: a group whose members must not run in parallel is a chain mislabelled as a group, and an orchestrator reading the table before the notes would fan out concurrent edits to one file.
- **Rationale:** The parallelism that is real here is across files: task 9 (`e2e_capability_test.rs`) and task 10 (golden fixtures) genuinely run concurrently. Expressing the chain as a chain removes the need for a caveat that contradicts the table.
- **Promotes to ADR:** no

### [12] One declared name set feeds both the Exasol guard and the sweep assertion

- **Decision:** Declare the verbatim-eligible Exasol `function_scalar` names exactly once — a `const EXASOL_VERBATIM_FNS: &[&str]` plus an `is_exasol_verbatim(name: &str) -> bool` guard helper in `crates/vs-expression/src/lib.rs`. The guarded Exasol arm's guard reads that set instead of carrying an inline pattern list, and decision [7]'s sweep test asserts that every `function_scalar` name in its table is either in the set or one of the five named exceptions.
- **Alternatives:** Spell the eligible names inline in the guarded arm's pattern list, which is the shape #210 shipped and which decision [1] originally carried forward. Rejected on review: an inline list is a second copy of the translated-name set. The guarded arm sits ahead of the DataFusion arms, so a name present in a DataFusion arm but absent from the Exasol list falls through to the DataFusion rendering with no error — the exact back-door that produced issue #209 and the day-one `*_BETWEEN` break.
- **Rationale:** Decision [1] claimed to remove the drifting mapping, but a second inline pattern list only relocates it: the question "which names does this translator translate" would have had two owners in one file, which `/speq:design-philosophy`'s one-owner-per-decision diagnostic rejects. One declaration read by both the guard and the test is what makes the earlier claim true. The cost is one const and one three-line helper, with no indirection at the call site, so the arm stays as shallow as the alternative the plan rejected a lookup table for.
- **Supersedes in part:** "One guarded match arm owns the Exasol-dialect rule" — that decision stands, but its inline-pattern-list mechanism is replaced by the declared set.
- **Promotes to ADR:** yes

## Review Findings

### [plan-review] Unreachable decimal-to-VARCHAR item dropped from scope

- **Finding:** Round 1, Intent Fidelity [SCOPE_CREEP] BLOCKER. The plan presented the `decimal_to_varchar_exasol` Exasol-dialect rendering as one of three failure paths "verified during planning to fail on live Exasol", and § Impact claimed all three "are reachable today". Independent re-verification found the node unreachable from every Exasol-dialect consumer: it is adapter-synthesized only, and both production producers terminate in the DataFusion dialect (`adapter/pushdown/mod.rs:213`, `adapter/pushdown/support.rs:1125`). All four Exasol-dialect consumers read raw wire JSON, which cannot carry an adapter-synthesized node. The § Manual Testing repro could not reproduce.
- **Direction change:** Escalated to a human, who chose full removal over relabelling as hardening. The item is gone from the plan: the `CAST(x AS VARCHAR)` row in § Context, the § Impact failure-path sentence, § Design Architecture and § Patterns, the § Consequences row, task 5, the § Manual Testing row, the two § Verification rows, decision-log [2], and decision-log [6] (the whole entry, since its only subject was that node). The `-scalar-ops` delta's Background ¶3 and its `decimal_to_varchar_exasol` scenario revert to the recorded library text, and its Exasol-dialect DELTA:NEW scenario is deleted. `plan.md` § Non-Goals now names the node as out of scope with the reachability evidence, so the next planner does not re-derive it. Decisions [7] through [11] are renumbered down by one from the previously logged [8] through [12]. Tasks 6, 7, and 8 renumber to 5, 6, and 7; the new dialect-invariant freeze-test task takes number 8; tasks 9, 10, and 11 keep their numbers. Round 1's findings were written against the pre-revision numbering, so its "task 5" is gone, its "task 6" is now task 5, its "task 7" is now task 6, and its proposed "task 12" is now task 8.
- **Promotes to ADR:** no

### [plan-review] REGEXP_LIKE has two encodings; only one was branched

- **Finding:** Round 1, Requirement Quality [COMPLETENESS_GAP] BLOCKER. `regexp_like(...)` is emitted at two sites. Task 3 branched only `predicate_like_regexp` (`lib.rs:497`); the `function_scalar` `REGEXP_LIKE` alternate encoding (`lib.rs:678`) is dialect-blind and would keep emitting a form Exasol rejects. It also conflicted with the sweep test, which must cover every translated function.
- **Direction change:** Task 3 now branches both sites and requires the two encodings to render byte-identically within a dialect. Both REGEXP_LIKE scenarios in the `vs-expression-translator` delta widen their GIVEN to either encoding and add that byte-identity step. The alternate encoding is named in § Design Architecture, § Patterns, the sweep table (task 6), and the sweep exception set.
- **Promotes to ADR:** no

### [plan-review] Both REGEXP_LIKE acceptance tests exercised a path the Exasol dialect never reaches

- **Finding:** Round 1, Requirement Quality [COMPLETENESS_GAP] BLOCKER. Both acceptance tests put the predicate in a WHERE clause. `build_qualified_single_table_fallback_sql` applies the WHERE filter inside the scan, where the DataFusion trio renders it, so both tests passed identically with and without the fix. The Exasol dialect sees `predicate_like_regexp` only in a select item, GROUP BY key, HAVING operand, ORDER BY element, or N-scan cross-side residual.
- **Direction change:** `e2e_count_distinct_regexp_like_matches_native_oracle` and § Manual Testing row 4 are respecified to a select-list position: `SELECT COUNT(DISTINCT (c_name REGEXP_LIKE '^C')) FROM <vs>.CUSTOMER WHERE c_custkey <= 10000`, which compiles natively on 2025.2.1. Both carry a note that a WHERE-clause `REGEXP_LIKE` is pushed into the scan and does not exercise the Exasol dialect, and the Exasol-dialect scenario in the `vs-expression-translator` delta states the reachable positions normatively.
- **Promotes to ADR:** no

### [plan-review] The sweep-test requirement was an unverifiable token deny-list

- **Finding:** Round 1, Requirement Quality [AMBIGUOUS_REQUIREMENT] BLOCKER. § Requirements row 3 asked for "the absence of every DataFusion-only token", which is not a testable set, and task 7 implemented it as twelve hardcoded tokens. A deny-list catches only arms already known today: a future `SUBSTRING`, `NVL`, `DATE_BIN`, or `TO_CHAR` arm passes it, so the requirement could not deliver "a future arm that forgets the dialect fails a test".
- **Direction change:** § Requirements row 3 and task 6 are rewritten as a per-node name-equality assertion with a named exception set, plus a declared-set membership assertion; the token list survives as a secondary assertion. The test is renamed `exasol_dialect_renders_declared_verbatim_surface`, since `exasol_dialect_renders_no_datafusion_only_token` described only the secondary assertion. Decision [7] records that name equality, not token absence, is what makes the rule structural, and drops the claim that keeping the guard current costs nothing.
- **Promotes to ADR:** no

### [plan-review] Five named tests in § Verification had no owning task

- **Finding:** Round 1, Task Breakdown [TRACEABILITY_GAP] BLOCKER. `renders_string_family_verbatim_in_exasol_dialect`, `renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect`, `arithmetic_operators_render_identically_in_both_dialects`, `non_timestamp_literals_render_identically_in_both_dialects`, and `exasol_df_filter_suppresses_trivially_true` were named in § Verification, exist nowhere in the crate today, and were created by no task. Two DELTA:NEW spec steps therefore had no implementing task, and the "DataFusion output frozen" requirement rested on paired assertions nobody was tasked with writing.
- **Direction change:** Task 1 now names the two string-family tests explicitly, covering the arm #210 shipped with no translator-side test. A new task 8 adds the three dialect-invariant freeze tests. § Parallelization gains Group C3 for task 8.
- **Promotes to ADR:** no

### [plan-review] The design duplicated the name mapping it claimed to eliminate

- **Finding:** Round 1, Design Depth [INFORMATION_LEAKAGE] BLOCKER. § Design Decision claimed "there is no mapping left that can drift", but after task 1 the translated-name set existed twice in `lib.rs`: once as the guarded arm's inline pattern list and once across the DataFusion arms. Nothing enforced agreement, and because the guarded arm precedes the DataFusion arms, a name in the DataFusion lists but missing from the Exasol list falls through to DataFusion rendering silently. Decision [1] rejected a name table for "adding a second mechanism" without noticing the guarded arm was itself the second copy.
- **Direction change:** New decision [12] declares the eligible names once (`EXASOL_VERBATIM_FNS` plus the `is_exasol_verbatim` guard helper), read by both the guarded arm and task 6's sweep assertion. Task 1 builds that set instead of an inline list, § Consequences gains a row for it, and § Design Decision now says both dialects read one declared name set so drift fails a test, rather than claiming no mapping remains. The same correction lands in the `vs-expression-translator` and `-scalar-fns` delta Backgrounds, which carried the "no mapping that can drift" claim into text destined for the permanent library.
- **Promotes to ADR:** yes

### [plan-review] Now-family divergence disclosed, DataFusion-side collapse named as a tracked non-goal

- **Finding:** Round 1, Intent Fidelity [SCOPE_CREEP] ADVISORY. Decision [4] conceded the now-family change is not a compilation fix, then made it anyway, and two consequences went undisclosed: the DataFusion dialect keeps the `SYSDATE`/`SYSTIMESTAMP` collapse with no tracked issue, and § Impact's claim "Results change only where a query previously returned an error" is false for these four names.
- **Direction change:** § Impact ¶3 now excepts the four now-family names, and ¶4 states that the DataFusion dialect keeps the collapse, that the two dialects therefore disagree for these names, and that no query is known to evaluate one such node on both paths. § Non-Goals names the DataFusion-side collapse as out of scope, and decision [4] gains a residual-gap clause recording that a GitHub issue MUST be filed for it before recording, per `CLAUDE.md`'s no-silent-gap rule.
- **Promotes to ADR:** no

### [plan-review] E2E task needs a rebuilt SLC, not just a code change

- **Finding:** Round 1, Feasibility [HIDDEN_DEPENDENCY] ADVISORY. Task 9's E2E parity tests exercise the VS adapter running inside Exasol, so they cannot pass until the `.so` carrying the new code is rebuilt and uploaded to BucketFS. That prerequisite appeared only in § Checklist, so an implementer reading § Parallelization would run task 9 against a stale SLC and read the failures as fix failures.
- **Direction change:** § Parallelization gains a sequential-dependency bullet: Group D's task 9 additionally requires `make cross-musl-udf-build` plus the BucketFS SLC upload after Group B4, and an E2E run against a stale `.so` tests the old rendering.
- **Promotes to ADR:** no

### [plan-review] Exasol version corrected to the pinned image

- **Finding:** Round 1, Feasibility [UNSTATED_ASSUMPTION] ADVISORY. The acceptance criterion named "live Exasol 2025.1.x", a version the project does not pin; `docker-compose.yml:115` and `Makefile:3` pin `exasol/docker-db:2025.2.1`. The version was being recorded into the permanent spec library as provenance an implementer could not reproduce. Review re-executed every claim on 2025.2.1 and all held.
- **Direction change:** Every occurrence of "live Exasol 2025.1.x" in `plan.md` and in the four spec deltas that carried it now reads "live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`)", with the SQL codes unchanged. The pre-existing 2025.1.3 provenance from issue #107 is untouched, since it records a different investigation.
- **Promotes to ADR:** no

### [plan-review] Consumer-site count corrected from five to four

- **Finding:** Round 1, Requirement Quality [COMPLETENESS_GAP] ADVISORY. The `vs-expression-translator` delta claimed "Five consumer sites" directly above a four-row table. Four is correct: `grouped_agg.rs:424`, `joins/rendering.rs:103`, `joins/rendering.rs:116`, `topn.rs:138`. Decision [9] (now [8]) derived five by adding `parse_declined_sort_key` to the issue's own four, but the issue's four collapse into three rows of that table.
- **Direction change:** The delta reads "Four consumer sites", `plan.md` § Context reads "Four consumer sites", and decision [8](c) now states that the issue's consumer-site list is wrong, that there are four production sites, and that the one it omits is `parse_declined_sort_key`. The entry title and rationale drop "a fifth consumer site exists".
- **Promotes to ADR:** no

### [plan-review] Exclusion-set membership made consistent at four constructs

- **Finding:** Round 1, Requirement Quality [REQUIREMENT_CONFLICT] ADVISORY. The exclusion set was stated three times with two memberships: the `-scalar-fns` table listed three (operators, `MOD`, `CONCAT`), `plan.md` listed four dedicated arms ahead of the verbatim arm (`CAST`, `MOD`, `CONCAT`, operators), and task 6 told the implementer to document "the three constructs excluded from it" without saying which three.
- **Direction change:** The `-scalar-fns` table now reads "Four constructs" and gains a `CAST` row pointing at `sql-comprehension/vs-expression-translator-cast`. Task 5 (formerly 6) names them explicitly: the operator wire names, `MOD`, `CONCAT`, and `CAST`. § Patterns reads "four constructs", and decision [1] reads "four exclusions". Where the sweep test needs a fifth exception — the `REGEXP_LIKE` alternate encoding, whose Exasol form is infix rather than a call — that is stated as four exclusions plus that encoding, never as a bare count.
- **Promotes to ADR:** no

### [plan-review] Group B split into a declared chain

- **Finding:** Round 1, Task Breakdown [TASK_GRANULARITY] ADVISORY. Group B was listed in the "Parallel Group" column while a bullet beneath said its members "MUST run sequentially, not concurrently". An orchestrator reading the table before the bullets would fan out concurrent edits to one 3,351-line file.
- **Direction change:** Group B is split into B1 through B4 (tasks 2, 3, 4, 5 in order) and the contradictory bullet is deleted. Group C is split the same way into C1 through C3, since tasks 6, 7, and 8 also all edit `crates/vs-expression/src/lib.rs` and had the same latent mislabelling. The table's column header is now "Group", and a sentence states that only Group D holds genuinely concurrent tasks. Decision [11] records the reasoning.
- **Promotes to ADR:** no

### [plan-review] Over-long sentences split

- **Finding:** Round 1, Prose Quality [PROSE_BLOAT] ADVISORY. § Summary's first sentence ran 41 words against `/speq:writing-guardrails`' 25-word cap and carried four ideas; § Impact ¶2 packed three failure paths into one 58-word sentence; § Consequences row 2's rationale ran one long sentence.
- **Direction change:** § Summary is three sentences, none over 25 words, keeping "Closes issue #209" as the closing clause. § Impact ¶2 is one sentence per failure path (two paths remain after the scope drop). § Consequences row 2's rationale is split into two sentences.
- **Promotes to ADR:** no
