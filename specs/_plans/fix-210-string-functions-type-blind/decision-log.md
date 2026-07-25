# Decision Log: fix-210-string-functions-type-blind

## Interview

Planned in headless mode. No live interview took place; the orchestrator supplied the decisions below as pre-made and authoritative, and every one was re-verified against the current source before this plan was written.

**Q:** Where does the fix live, given `vs-expression` renders the string functions?
**A:** In the adapter, as a new recursive guard `string_function_arg_type_guard` in `crates/lakehouse-engine/src/adapter/pushdown/support.rs`. `vs-expression` is a pure stateless JSON-to-SQL translator with no column-type context and is shared with a sibling VS-adapter project, so the fix cannot live there. Structure it like the existing `like_subject_type_guard` (`Option<Json>`, `None` declines the whole tree) crossed with `rewrite_decimal_stringifications`'s post-order recursion over the same child fields.

**Q:** Which argument indices of which functions are in string position?
**A:** All arguments for `CONCAT`/`TRIM`/`LTRIM`/`RTRIM`/`REPLACE`/`TRANSLATE`; index 0 only for `LOWER`/`UPPER`/`ASCII`/`INITCAP`/`REVERSE`/`LENGTH`/`OCTET_LENGTH`/`UNICODE`/`SUBSTR`/`REPEAT`/`LEFT`/`RIGHT`; indices 0 and 2 (when present) for `LPAD`/`RPAD`; indices 0 and 1 for `INSTR`/`LOCATE`. `CHR` and `UNICODECHR` are excluded entirely — their sole argument is genuinely an integer, so pushing them unchanged is correct, not a bug.

**Q:** How does each argument type dispatch?
**A:** VARCHAR/CHAR unchanged; DATE rewrapped as `CAST(<col> AS VARCHAR)` exactly as `guard_like_subject` does, carrying the same #216 `NLS_DATE_FORMAT` caveat; DECIMAL (including wire-encoded integers) wrapped in the existing `decimal_to_varchar_exasol` node via the existing `wrap_decimal_to_varchar` helper, reused verbatim rather than reimplemented; anything else (BOOLEAN, DOUBLE, TIMESTAMP) or an unresolved column name returns `None` and declines. A string-position argument that is not a bare column is left unchanged, the same tracked-exception convention #211 used.

**Q:** Which surfaces get wired?
**A:** Exactly the two #211 covers, no expansion: the single-table WHERE-clause filter chain in `pushdown/mod.rs` (slotted after `like_subject_type_guard` and before `rewrite_decimal_stringifications`, with the composition verified by a test rather than asserted), and the select-list projection path in `project_columns`, where a `None` decline sets the existing `needs_full_fallback` rather than propagating an error.

**Q:** What stays out of scope?
**A:** The broadcast-join per-leg filter path and any GROUP-BY-key-only occurrence, mirroring #211's own tracked #223 gap for exactly those two surfaces. Recorded as tracked exceptions, never silent gaps.

**Q:** Do the `FN_*` capability flags need touching?
**A:** No, unless the plan concludes otherwise and justifies it explicitly. The family is already advertised. This plan agrees — see decision [7].

## Design Decisions

### [1] Author a new feature rather than extend #207's or #211's

- **Decision:** Create `vs-adapter/pushdown-planning-string-fn-type-coercion` as a NEW feature, and add one CHANGED delta to `vs-adapter/pushdown-planning-decimal-string-format`.
- **Alternatives:** Extend `pushdown-planning-like-type-coercion` (rejected: it is scoped to LIKE/REGEXP_LIKE subjects, a different node shape and a different recursion) or extend `pushdown-planning-decimal-string-format` (rejected: it is scoped to DECIMAL formatting, while this feature governs DATE, BOOLEAN, DOUBLE, and TIMESTAMP arguments too and declines rather than formats).
- **Rationale:** The concern is argument typing across a whole function family. Folding it into either predecessor would make that feature's own scope statement false. The CHANGED delta is still required because #211's "CAST, CONCAT, or LENGTH over a non-DECIMAL column is left unchanged" scenario asserts the item "SHALL render exactly as it did before this change", which stops being true end to end once `CONCAT`/`LENGTH` over a DATE column is CAST-wrapped and over a DOUBLE column declines. The delta narrows that scenario's claim to `rewrite_decimal_stringifications` in isolation and names the new feature as the end-to-end owner.
- **Promotes to ADR:** no

### [2] Guard runs before `rewrite_decimal_stringifications`, verified by test

- **Decision:** Slot `string_function_arg_type_guard` between `like_subject_type_guard` and `rewrite_decimal_stringifications` at both surfaces.
- **Alternatives:** Run it after the decimal rewriter (rejected: both would then see a bare DECIMAL column under `CONCAT`/`LENGTH` and produce a double wrap).
- **Rationale:** Traced through the actual code rather than assumed. Once the new guard replaces a bare DECIMAL argument with a `decimal_to_varchar_exasol` node, `is_bare_decimal_column` returns false for it (its `type` is no longer `column`), so `rewrite_decimal_stringifications` leaves it alone and emits exactly one trim. The plan still requires a composition test (`where_filter_decimal_stringification_rewritten_to_trim`, extended to the four-stage chain) because a no-double-wrap property held only by reasoning is a property nobody notices breaking.
- **Promotes to ADR:** no

### [3] Decline, never cast, for BOOLEAN / DOUBLE / TIMESTAMP

- **Decision:** Return `None` for any resolvable string-position column type other than VARCHAR, CHAR, DATE, or DECIMAL.
- **Alternatives:** Emit `CAST(<col> AS VARCHAR)` for them (rejected).
- **Rationale:** The decline is not merely a fail-safe for the crash; it is the only branch that cannot silently change a result. The Iceberg table spec defines `boolean` as "True or false", `double` as "64-bit IEEE 754 floating point", and `timestamp` as "Timestamp, microsecond precision, without timezone" and assigns none of them a text form, so each engine picks its own: Exasol renders BOOLEAN as `TRUE`/`FALSE` where DataFusion renders `true`/`false`, and Exasol's TIMESTAMP text form is space-separated where DataFusion's is `T`-separated. A cast would convert a loud hard failure into a quiet wrong answer. This is the same reasoning #207 recorded for declining a DECIMAL LIKE subject before #211 supplied the faithful formatter.
- **Promotes to ADR:** yes

### [4] Split per-function argument knowledge into a pure index table

- **Decision:** Add `string_position_arg_indices(fn_name, arg_count) -> Option<Vec<usize>>` as a standalone pure function; `None` means "not a governed string function". **Signature superseded by review finding [2]:** the function is `string_position_args(fn_name, arg_count) -> StringPositionArgs { NotGoverned, Coerce(Vec<usize>), Decline }`. It remains one standalone pure function; only the return type gained a third outcome.
- **Alternatives:** Inline the per-function match inside the recursion (rejected: mixes a 22-name arity table into a tree walk and makes the arity edge cases reachable only through JSON fixtures).
- **Rationale:** The table is the part most likely to be wrong — `SUBSTR(str, start, length)` and `LPAD(str, length, pad)` each mix string-position and genuinely-numeric arguments, and `LPAD` has a 2-argument form with no index 2. Isolating it makes every entry directly assertable and lets `CHR`/`UNICODECHR` exclusion share the same code path as every other non-string function.
- **Promotes to ADR:** no

### [5] `LOCATE`'s render-time argument reorder does not affect index assignment

- **Decision:** Treat indices 0 and 1 as string-position for both `INSTR` and `LOCATE`.
- **Alternatives:** Compensate for the reorder in the index table (rejected).
- **Rationale:** Verified in `crates/vs-expression/src/lib.rs:741-772`: Exasol `INSTR(string, substring)` renders as `strpos(arg0, arg1)` and Exasol `LOCATE(substring, string)` as `strpos(arg1, arg0)`. The reorder swaps which rendered slot each argument fills, never which arguments are string-position — and since both of the first two arguments are string-position for both functions, no compensation is possible or needed. Recorded because it looks like it needs compensation.
- **Promotes to ADR:** no

### [6] Keep #211's `CONCAT`/`LENGTH` rewriter arms rather than delete them as dead

- **Decision:** Leave `rewrite_decimal_stringifications` untouched.
- **Alternatives:** Strip its `CONCAT`/`LENGTH` arms and narrow it to `function_scalar_cast`, since the new guard reaches those two functions first at both wired surfaces (rejected).
- **Rationale:** They are not dead. `rewrite_decimal_stringifications` remains the sole handler for `function_scalar_cast`-to-string over a DECIMAL column, which the new guard deliberately does not touch, and its `CONCAT`/`LENGTH` arms stay a correct idempotent backstop for the #223-tracked surfaces that do not yet run the new guard. Rewriting a spec that shipped one commit earlier for zero behavior change is not worth the regression risk.
- **Promotes to ADR:** no

### [7] No `FN_*` capability change

- **Decision:** Leave `crates/lakehouse-engine/src/adapter/capabilities.rs` untouched.
- **Alternatives:** Un-advertise the family, or advertise it conditionally per argument type (rejected).
- **Rationale:** After this fix the adapter handles every argument type — pass through, cast, trim, or decline — so type-blind advertisement is no longer a defect. Un-advertising would forfeit the common VARCHAR-argument pushdown that already works. Conditional advertisement is not expressible: Exasol's `getCapabilities` handshake carries no per-argument-type conditioning and runs before any query is known.
- **Promotes to ADR:** no

### [8] Extract `wrap_cast_to_varchar` and share it with `guard_like_subject`

- **Decision:** Move the DATE `CAST`-to-VARCHAR `json!` literal out of `guard_like_subject` into a private helper called from both guards.
- **Alternatives:** Duplicate the literal in the new guard (rejected).
- **Rationale:** Both guards must emit the identical node shape, because both rely on `render_cast_target`'s DataFusion arm rendering `{"type":"VARCHAR"}` as bare `VARCHAR`. A shared helper makes that identity structural instead of coincidental, and mirrors how #211 already factored `wrap_decimal_to_varchar`. The extraction is behavior-neutral, so #207's existing tests are the regression proof.
- **Promotes to ADR:** no

### [9] The recursion must be broader than `like_subject_type_guard`'s

- **Decision:** Recurse over `expressions`/`arguments`/`results` and `expression`/`pattern`/`left`/`right`/`basis`, copying `rewrite_decimal_stringifications`'s child-field set rather than `like_subject_type_guard`'s junction-only walk.
- **Alternatives:** Reuse the junction-only recursion (rejected: it would silently miss the primary filter shape).
- **Rationale:** A filter-side string function sits under a comparison predicate — `UPPER(c) = 'X'` is a `predicate_equal` with the function under `left` — and `like_subject_type_guard` descends only into `predicate_and`/`predicate_or`/`predicate_not`. A junction-only guard would leave the WHERE surface unfixed while appearing wired. The plan pins this with a `predicate_equal` test that the narrow recursion cannot pass.
- **Promotes to ADR:** no

### [10] `INSTR`/`LOCATE` dropped optional arguments filed as a separate issue

- **Decision:** File a new GitHub issue for it; keep it out of this plan's scope and cite it inline in the new feature spec.
- **Alternatives:** Fix it here (rejected: different defect class) or say nothing (rejected).
- **Rationale:** Found while verifying argument positions. `crates/vs-expression/src/lib.rs:741-772` reads only `args[0]` and `args[1]` for both functions, so Exasol's `INSTR(str, sub, start, occurrence)` and `LOCATE(sub, str, start)` render as a bare `strpos(str, sub)` with the extra arguments silently dropped — a wrong result, not a crash, and both functions are advertised unconditionally. It is an arity defect, not a typing defect, so it does not belong in this fix; leaving it unmentioned would be exactly the silent gap the project rules forbid. **Superseded in part by review finding [2]:** the issue is filed as #228 (no placeholder remains), and the plan no longer merely cites the defect — the guard DECLINES any `INSTR`/`LOCATE` call carrying more than two arguments, because coercing index 0 would have converted this loud failure into a silent wrong answer. A faithful rendering of the dropped arguments stays out of scope.
- **Promotes to ADR:** no

### [11] E2E decline test uses an in-session literal oracle

- **Decision:** Prove the decline path by comparing `UPPER(c_double)` over the virtual table against the identical expression over a plain Exasol literal of the same value, evaluated in the same session with no virtual schema.
- **Alternatives:** Hard-code Exasol's expected DOUBLE text form in Rust (rejected: Exasol's DOUBLE-to-VARCHAR formatting is not trivially reproducible) or assert only that the query does not error (rejected: too weak — it would pass even if DataFusion returned divergent text).
- **Rationale:** The literal form routes through Exasol's own conversion without touching the adapter, so it is an independent oracle rather than a tautology, and it fails loudly if the decline regresses into a pushed cast. The plan also instructs the implementer to drop the `c_ts`/`c_bool` variants and record why, rather than assert blind, if the live container rejects Exasol's own implicit conversion for either.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Two unguarded render surfaces were claimed fixed and are now named tracked exceptions (#227)

- **Finding:** `plan-reviewer` (round 1, SCOPE_REDUCTION) showed the Goals claim "every governed string function over any Exasol column type either pushes down … or declines" was false. Two production render surfaces are neither wired nor named: `detect_group_by_aggregates` (`grouped_agg.rs:184-245`) renders every `groupBy` element AND every non-aggregate select-list item with bare `render_expression` and matches them by rendered-SQL string equality, and `handle_pushdown` consumes that grouped SQL directly (the grouped arm's `projection` is inert, so `project_columns` never runs against it); `parse_agg_item`'s `arg_column_or_expr` (`single_group_agg.rs:160-169`, `213-277`) renders an aggregate's argument with no type guard. `SELECT UPPER(c_custkey), COUNT(*) … GROUP BY UPPER(c_custkey)`, `MAX(UPPER(c_custkey))`, and `COUNT(UPPER(c_custkey))` therefore still hard-fail. The Non-Goals wording "a GROUP-BY-key-only occurrence" was also wrong: the grouped path is unguarded whether or not the key is selected.
- **Direction change:** Narrowed the Goals claim to the two wired surfaces, named explicitly. Filed issue #227 covering both unguarded surfaces with repros and a fix shape, and added both to the new feature spec's out-of-scope list as separately named tracked exceptions citing it. Replaced the "GROUP-BY-key-only" wording with the plain statement that the ENTIRE grouped path — keys and select items — is out of scope. Verified in source before filing, rather than repeating the reviewer's claim.
- **Promotes to ADR:** no

### [2] [plan-review] INSTR/LOCATE beyond two arguments now declines instead of coercing a truncated call (#228)

- **Finding:** `plan-reviewer` (round 1, UNSTATED_ASSUMPTION) showed the plan's own decision [3] rule — "a cast would convert a loud hard failure into a quiet wrong answer" — was violated by its own index table. `crates/vs-expression/src/lib.rs:741-772` reads only `args[0]`/`args[1]` for `INSTR`/`LOCATE` and guards only `args.len() < 2`, dropping a 3rd (start position) or 4th (occurrence) argument. Today `INSTR(c_custkey, '1', 5)` over a DECIMAL column hard-fails at DataFusion planning, so the dropped argument is masked by a loud error. An unconditional `[0, 1]` coercion would make that node plan successfully and return a position computed from offset 1 — silently wrong.
- **Direction change:** Made the table arity-aware and gave it a third outcome. `string_position_arg_indices -> Option<Vec<usize>>` became `string_position_args -> StringPositionArgs { NotGoverned, Coerce(Vec<usize>), Decline }`; `INSTR`/`LOCATE` with `arg_count > 2` returns `Decline`, so the guard returns `None` and Exasol evaluates natively. The decline is unconditional on argument type, which also corrects the pre-existing silently-wrong all-VARCHAR case `INSTR(c_varchar, 'b', 3)` at the two wired surfaces. Added unit tests (task 1.3 and the guard-level and select-list cases), an *AND* clause set on the INSTR/LOCATE scenario, and an arity-decline E2E with an in-session native oracle (task 5.3). Filed the arity defect as issue #228, replacing the `#229` placeholder. Supersedes decision [4] ("Split per-function argument knowledge into a pure index table") on the return type only — the table stays one pure function, which is what made the third outcome cheap to add — and supersedes the placeholder instruction in decision [10].
- **Promotes to ADR:** yes

### [3] [plan-review] `project_columns` is shared with the broadcast join; the join SELECT list is in scope and now tested

- **Finding:** `plan-reviewer` (round 1, REQUIREMENT_CONFLICT) showed the spec's description of `project_columns` as one of "the two single-table scan surfaces" was wrong, and that listing the broadcast join as entirely out of scope contradicted the wiring. `extract_join_projection` (`joins/rendering.rs:29-37`) calls `project_columns` against the union of BOTH joined tables' columns, and `joins/mod.rs:138` calls it on the empty-side path. Unlike #211's rewriter, which never declines, this plan's guard can — so wiring it changed broadcast-join behavior on a surface the spec declared untouched, undisclosed and untested.
- **Direction change:** Dropped "single-table" as a description of `project_columns` in the Scope sentence and in Goals. Added a Background bullet mirroring #211's own disclosure (`pushdown-planning-decimal-string-format/spec.md:14`): the guard reaches the join SELECT list through the shared function, and a decline there sets `needs_full_fallback` over the union of both sides' columns — correct and already-reachable behavior, previously undisclosed. Narrowed the out-of-scope bullet to the join PER-LEG WHERE-clause filter path (`joins/sql_builders.rs`) only. Departed from #211's no-join-test precedent and added one `extract_join_projection` test (task 4.4) plus an *AND* clause on the select-list decline scenario, because #211's argument rested on its rewriter being unable to decline: a guard that can decline changes join control flow, so shared-function coverage no longer transfers.
- **Promotes to ADR:** no
