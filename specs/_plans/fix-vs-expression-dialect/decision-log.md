# Decision Log: fix-vs-expression-dialect

## Interview

No live interview. The orchestrator ran `/speq:plan-pr` in headless mode and supplied issue #209's title and body verbatim as the authoritative scope, plus one instruction: follow the per-dialect convention issue #197 established for the `MOD` arm rather than inventing a second mechanism. Every decision below was made by the planner and is recorded here in place of a human answer, per the headless escalation rule.

**Q (from the orchestrator's brief):** How should a dialect parameter be named or threaded for functions that do not currently take one?
**A (planner):** No new threading is needed. `Dialect` already reaches every arm of `render_expression_inner`; the arms simply never read it. See decision [1].

**Q (from the orchestrator's brief):** Fix all affected function families in one plan, or defer some?
**A (planner):** All of them, plus three further failure paths found during discovery. See decisions [2] and [3]. Nothing is deferred.

## Design Decisions

### [1] One guarded match arm owns the Exasol-dialect rule

- **Decision:** State one rule, "in the Exasol dialect, render what Exasol sent", and give it a single owner: the `if dialect == Dialect::Exasol` guarded `function_scalar` arm that issue #210 already added for the string-function family. The math family, the field-shortcut date functions, `WEEK`, the `*_BETWEEN` family, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `GREATEST`, `LEAST`, `NULLIF`, `NULLIFZERO`, `ZEROIFNULL`, and the now-family join that arm. Node types outside `function_scalar` (`function_scalar_extract`, `predicate_like_regexp`, the two timestamp literals, `decimal_to_varchar_exasol`) branch inline with `match dialect`, which is the shape issue #197's `MOD` fix uses.
- **Alternatives:** (a) A per-dialect name lookup table keyed by function name. Rejected: it adds a second mechanism and an indirection for a rule whose content is "do not translate", and a table cannot express the `EXTRACT`, `REGEXP_LIKE`, or timestamp-literal shape changes anyway. (b) A `Dialect` trait with two implementations. Rejected: the two dialects differ at roughly a dozen of some fifty arms, so two full implementations would duplicate the other thirty-eight and invite exactly the silent drift this plan exists to end. (c) A private `match dialect` inside each affected arm, following #197 literally everywhere. Rejected: it scatters one decision across ten arms, so a reader cannot see the rule and the next arm added has no obvious place to inherit it.
- **Rationale:** Per `/speq:design-philosophy`, the defect is a design decision with no owner: the same "which parser reads this fragment" question was answered independently in `render_cast_target`, in the `MOD` arm, and in the #210 string arm, and left unanswered in the rest. Giving the decision one home is the fix; the arm ordering that keeps `CAST`, `MOD`, `CONCAT`, and the operator names ahead of the guarded arm expresses the three exclusions without a negative-name list.
- **Promotes to ADR:** yes

### [2] Scope extended to three failure paths issue #209 does not name

- **Decision:** Fix, in the same plan, three further Exasol-dialect renderings verified during planning to fail on live Exasol 2025.1.x: `predicate_like_regexp` rendering `regexp_like(s, p)` (`syntax error, unexpected REGEXP_LIKE_`, since `REGEXP_LIKE` is an infix predicate in Exasol, not a function), `literal_timestamp` and `literal_timestamp_utc` rendering `arrow_cast(...)` (`function or script ARROW_CAST not found`), and `decimal_to_varchar_exasol` rendering a hardcoded length-less `CAST(x AS VARCHAR)` (`syntax error, unexpected ')', expecting '('`).
- **Alternatives:** Restrict the plan to the arms issue #209 enumerates and file three follow-up issues. Rejected: all three are the same defect class on the same code path, all three are reachable today (`FN_PRED_REGEXP_LIKE`, `LITERAL_TIMESTAMP`, and `LITERAL_TIMESTAMP_UTC` are all advertised), and each fix is a few lines inside an arm this plan already edits. Splitting them would ship a "systemic remainder" fix that still leaves a systemic remainder.
- **Rationale:** Issue #209 frames itself as the systemic fix for every renamed or re-shaped rendering, not as a list of seven queries. A fix that leaves three verified compilation errors in place would not satisfy that framing, and the regression test in decision [8] would fail on them anyway.
- **Promotes to ADR:** no

### [3] Uniformity over minimal diff: functions that already parse in Exasol are folded into the verbatim rule

- **Decision:** Apply the verbatim rule to every Exasol-native scalar function, including the ones whose current rendering already parses in Exasol: `NULLIFZERO`, `ZEROIFNULL`, `GREATEST`, `LEAST`, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `DAYS_BETWEEN`, and the whole math family apart from `SIGN`. For most of these the emitted string changes only in name case; `NULLIFZERO` and `ZEROIFNULL` stop being rewritten to `nullif(x, 0)` and `coalesce(x, 0)`.
- **Alternatives:** Change only the arms that currently produce a compilation error, which is the smaller and lower-risk diff. Rejected on maintainability: a rule applied to some arms and not others is not a rule a future reader can apply. They would have to test each name against live Exasol to learn whether its current rendering is principled or merely lucky, which is how the `*_BETWEEN` family shipped broken in the first place.
- **Rationale:** The whole value of the verbatim rule is that it needs no per-function verification. Verified on live Exasol 2025.1.x that every math name in the arm exists natively (`ABS`, `FLOOR`, `CEIL`, `SQRT`, `EXP`, `LN`, `DEGREES`, `RADIANS`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `SINH`, `COSH`, `TANH`, `COT`, `ROUND`, `TRUNC`, `LOG`, `POWER`, `ATAN2`), so folding them in changes no result.
- **Promotes to ADR:** yes

### [4] The now-family renders as bare Exasol keywords, correcting a latent wrong answer

- **Decision:** Render `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` as their own bare Exasol keywords in the Exasol dialect, with no parentheses and no collapsing of one name onto another.
- **Alternatives:** Leave `current_date()` and `now()` in place. Issue #209 claims Exasol rejects the parenthesised form, but that claim is false: both `SELECT current_date()` and `SELECT now()` execute successfully on live Exasol 2025.1.x, so neither is a compilation error and neither is required by the issue's stated scope.
- **Rationale:** The current mapping collapses `SYSDATE` onto `CURRENT_DATE` and `SYSTIMESTAMP` onto `CURRENT_TIMESTAMP`, erasing Exasol's database-time versus session-time distinction. That is a silent wrong answer rather than a loud failure, which is worse. The verbatim rule removes it for two lines of change, and this is the one behavior change in the plan that an operator could observe as a different value rather than as an error becoming a result. It is called out in `plan.md`'s Impact for that reason.
- **Promotes to ADR:** yes

### [5] The Exasol dialect imposes no arity check

- **Decision:** The Exasol-dialect verbatim arm forwards the argument list unchanged and does not validate argument count, even where the DataFusion-dialect arm does.
- **Alternatives:** Keep each family's existing arity check in both dialects. Rejected: Exasol's compiler emitted a call its own engine accepts, so a translator-side arity check on that path can only reject valid input.
- **Rationale:** This is not new; it is the rule the #210 string arm already follows, and it is load-bearing there. `vs-adapter/pushdown-planning-string-fn-type-coercion` declines a three-argument `INSTR(s, sub, start)` from the DataFusion scan precisely because the Exasol wrapper can still evaluate it verbatim. Extending the same rule to the other families keeps one behavior instead of two.
- **Promotes to ADR:** yes

### [6] Exasol evaluates its own DECIMAL formatting rather than the translator emulating it

- **Decision:** In the Exasol dialect, `decimal_to_varchar_exasol` renders `CAST(<arg> AS VARCHAR(2000000))` and does not call `format_decimal_exasol_style`. The helper keeps its signature, its body, and its single DataFusion-dialect caller, and its doc comment records that it emits DataFusion-dialect SQL only.
- **Alternatives:** Thread a `Dialect` into `format_decimal_exasol_style` so it emits a length-qualified cast for Exasol. Rejected on two counts: it spreads the dialect decision into a pure string primitive that has no other reason to know about dialects, and it would have Exasol run two `regexp_replace` calls to reproduce a conversion Exasol already performs.
- **Rationale:** The helper exists to make DataFusion imitate Exasol. Verified on live Exasol 2025.1.x that the thing being imitated is already the native behavior: `CAST(1.500 AS VARCHAR(2000000))` returns `1.5` and `CAST(100.000 AS VARCHAR(2000000))` returns `100`. The absent-size width `VARCHAR(2000000)` matches the fallback `render_cast_target` already uses for an Exasol character CAST target, so the two paths agree. The general principle: never emulate Exasol inside SQL that Exasol will evaluate.
- **Promotes to ADR:** yes

### [7] No capability is added or withdrawn

- **Decision:** `capabilities.rs` is untouched. Every function affected by this plan stays advertised.
- **Alternatives:** Withdraw the capabilities whose Exasol-dialect rendering was broken until the fix is proven end to end. Rejected: the advertisement governs what Exasol may push to the node-local DataFusion scan, and the DataFusion-dialect rendering of every one of these functions is correct and unchanged. Withdrawing would remove working pushdown to fix a wrapper-only defect.
- **Rationale:** Keeping one capability set for both dialects is also why the declines stay symmetric: the delta specs state that an untranslated name falls through in both dialects, so the verbatim rule can never widen the translated set behind the capability list's back.
- **Promotes to ADR:** no

### [8] A single sweep test enforces the rule, so a future arm cannot silently forget it

- **Decision:** Add `exasol_dialect_renders_no_datafusion_only_token`: a table-driven unit test that renders one representative node per translated function and node type through `render_expression_exasol` and asserts the output contains none of `signum`, `date_part`, `strpos`, `arrow_cast`, `character_length`, `octet_length`, `regexp_like(`, `current_date()`, `now()`, `nullif(`, `coalesce(`, or a bare `%` operator.
- **Alternatives:** Rely on the per-family paired-dialect tests alone. Rejected: those only cover arms someone remembered to test, which is the failure mode that produced this issue. The `*_BETWEEN` family had per-function E2E parity tests and still shipped a broken Exasol rendering, because every one of those tests exercised the DataFusion path.
- **Rationale:** The rule from decision [1] is only durable if forgetting it fails a test rather than a review. Driving the test from one table makes adding a new arm one row, so the cost of keeping the guard current stays near zero.
- **Promotes to ADR:** yes

### [9] Issue #209's code references are stale in three places, and a fifth consumer site exists

- **Decision:** Record the corrections rather than carry them into the plan. (a) There is no `crates/lakehouse-engine/src/adapter/pushdown/joins.rs`; `joins` is a module directory and the functions the issue cites live in `joins/rendering.rs` and `joins/sql_builders.rs`. (b) The issue lists `LENGTH`, `UNICODE`, `UNICODECHR`, `INSTR`, and `LOCATE` as broken, but issue #210's fix (commit `3c0fe8a`, recorded in `e5a7e15`) already gave the whole string family an Exasol-verbatim arm; those names are correct today. (c) The issue names four Exasol-dialect consumer sites; there are five. `parse_declined_sort_key` in `adapter/pushdown/topn.rs` calls `render_expression_exasol_safe` for an expression ORDER BY element of the declined-ORDER-BY row-scan wrapper.
- **Alternatives:** Silently plan against the real code. Rejected: the stale references would otherwise be re-derived by the implementer and by the next planner reading the issue.
- **Rationale:** The fifth consumer site matters beyond bookkeeping: it means a declined ORDER BY over any affected expression is a further reachable instance of the same failure, so the fix's blast radius is wider than the issue states and the sweep test in decision [8] is the only practical way to cover all five sites at once. The `sql-comprehension/vs-expression-translator` delta lists all five so the next reader inherits the correct map.
- **Promotes to ADR:** no

### [10] Spec drift from #197 and #210 is corrected in the same deltas

- **Decision:** The `-scalar-fns` delta updates the `MOD` scenario to state the shipped `MOD(a, b)` Exasol form (#197) and the string-function scenario to state the shipped Exasol-verbatim arm (#210). Neither behavior was reflected in `sql-comprehension/vs-expression-translator-scalar-fns`.
- **Alternatives:** Leave the drift and document only new behavior. Rejected: the plan's whole subject is the per-dialect rendering rule for these exact scenarios, and leaving two shipped Exasol-dialect behaviors undocumented in the feature that owns them would leave the spec contradicting the code the plan is about to extend.
- **Rationale:** #210's spec deltas landed in `vs-adapter/pushdown-planning-string-fn-type-coercion`, the adapter-side feature, and never updated the translator-side feature whose scenario text still describes `strpos` as the only rendering. Correcting it costs one clause per scenario.
- **Promotes to ADR:** no

### [11] Translator scenarios map to unit tests, wrapper-compilation scenarios to E2E

- **Decision:** Every scenario that asserts a rendered string maps to a unit test in `crates/vs-expression/src/lib.rs`; every scenario whose truth depends on Exasol actually compiling the SQL maps to an integration test in `crates/lakehouse-engine/tests/e2e_capability_test.rs` using the in-session native-oracle idiom already used in that file's section 8.16.
- **Alternatives:** Integration tests for everything, per the default in `/speq:planning`. Rejected under that skill's own exception: `render_expression` and `render_expression_exasol` are pure `&Json -> Result<String>` functions with no I/O and no ambient state, which is exactly the pure-computation case unit tests are reserved for. The existing Exasol-dialect tests (`renders_mod_as_function_call_in_exasol_dialect`, `renders_cast_varchar_exasol_dialect_includes_length`) are unit tests for the same reason.
- **Rationale:** The paired-dialect assertion convention those tests use (same JSON node, one `assert_eq!` per dialect) is what freezes the DataFusion output, and it only works as a unit test.
- **Promotes to ADR:** no

### [12] Same-file tasks run sequentially

- **Decision:** Tasks 2 through 6 all edit `crates/vs-expression/src/lib.rs` and are grouped as parallel Group B but marked MUST-run-sequentially inside the group.
- **Alternatives:** Run them concurrently as independent arms. Rejected: concurrent sub-agent edits to one 3,300-line file conflict, and task 1 restructures the guarded arm the others sit beside.
- **Rationale:** The parallelism that is real here is across files: task 9 (`e2e_capability_test.rs`) and task 10 (golden fixtures) genuinely run concurrently, and Group C's tests depend only on the finished surface.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-plan-pr after plan-reviewer resolves a blocker, and by speq-implement after code review. -->
