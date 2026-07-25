# Decision Log: fix-192-char-type-pushdown

## Interview

Planned in headless mode — no live interview. The orchestrator supplied the findings below as the
interview's stand-in; each was verified against the code and against the running Exasol 2025.2.1
container before being accepted.

**Q:** What exactly fails in issue #192, and in how many places?
**A:** Any pushed-down result column Exasol declares `CHAR(n)` is emitted by the adapter as
`VARCHAR(n)`, so Exasol's type checker rejects the pushdown with `Data type mismatch ... Expected
CHAR(n), but got VARCHAR(n)`. Three shapes fail live: a GROUP BY on a CASE-of-string-literals
bucketing expression (`Expected CHAR(3) ASCII`), an explicit `CAST(c_phone AS CHAR(20))` select-list
item (`Expected CHAR(20) UTF8`), and a bare string literal used as a GROUP BY key (`Expected CHAR(1)
ASCII`). A GROUP BY on a genuine VARCHAR base column is the control and already passes.

**Q:** Where is the root cause?
**A:** `exasol_type_from_json` in `crates/lakehouse-engine/src/adapter/pushdown/support.rs` (line
778) matches the lowercased `dt["type"]` string and has arms for `boolean`, `decimal`, `double`,
`date`, and `timestamp`, then a catch-all commented "VARCHAR, CHAR, and all others" that renders
every string-family type as `VARCHAR(size)`. There is no `"char"` arm. It has 8 non-test call sites,
6 of them live for CHAR. (Plan review found a SECOND root cause behind three Exasol-parsed wrapper
paths — see review findings [R1] and [R5].)

**Q:** What should the CHAR branch's size cap be — VARCHAR's 2,000,000, or CHAR's 2,000?
**A:** 2,000. Exasol's CHAR maximum is 2,000 characters, so reusing the VARCHAR cap would emit an
Exasol-invalid declaration if an out-of-range size ever arrived. Treat this as the conventional
choice, not an open question. (See design decision [2] — confirmed live.)

**Q:** What is explicitly out of scope?
**A:** `crates/vs-expression/src/lib.rs`'s `render_cast_target` and its `renders_cast_char_as_varchar`
test, which render the internal DataFusion-side CAST fragment and are correct as VARCHAR because
Arrow has no CHAR type. Also `specs/datafusion-scan/type-mapping/spec.md`, which governs the
Iceberg/Arrow source-column mapping rather than the pushdown-request-to-EMITS-type mapping.
*(Superseded in part by review finding [R1]: `render_cast_target` has TWO dialect arms, and the
Exasol arm is now in scope. Only the DataFusion arm remains out of scope.)*

**Q:** New feature spec or a delta to an existing one?
**A:** Planner's call, with `vs-adapter/pushdown-planning-like-type-coercion` as the structural and
prose model, including its inline issue-citation pattern. (See design decision [1].)

## Design Decisions

### [1] Carve out a new feature spec rather than delta an existing one

- **Decision:** Author `vs-adapter/pushdown-planning-char-type-declaration` as a NEW feature spec.
- **Alternatives:** A CHANGED delta on `vs-adapter/pushdown-planning` (the general single-table
  pushdown-planning feature); a delta on `vs-adapter/pushdown-planning-grouped-agg`; deltas on both.
- **Rationale:** The corrected behavior spans eight pushdown paths — row-scan projection, grouped
  aggregate, single-group aggregate, broadcast join, the empty-result `CAST(NULL AS …)` wrapper, the
  N-scan unaccelerated join wrapper, the qualified single-table aggregate fallback, and the
  grouped-merge scalar-over-aggregate wrapper. Five derive their declared types through the shared
  adapter seam; the last three derive theirs through `vs-expression`'s Exasol dialect (review findings
  [R1] and [R5]) and are covered by this plan's separate
  `sql-comprehension/vs-expression-translator-scalar-ops` delta. The adapter-side behavior belongs to
  none of the five exclusively, and splitting it across deltas would duplicate the same Background
  facts.
  This mirrors how `pushdown-planning-like-type-coercion` was carved out as its own small feature for
  an equivalently narrow "adapter dispatches on an Exasol-declared type read off pushdown-request
  JSON" fix.
- **Promotes to ADR:** no

### [2] Cap the CHAR branch's size at 2,000, not VARCHAR's 2,000,000

- **Decision:** The `"char"` arm caps `size` at 2000.
- **Alternatives:** Reuse the VARCHAR arm's 2,000,000 cap for symmetry; leave the size uncapped and
  echo whatever Exasol sent.
- **Rationale:** Exasol's CHAR maximum is 2,000 characters, confirmed live: `CAST('a' AS CHAR(2001))`
  fails with `specified length too long for char type - maximum is 2000`. A 2,000,000 or uncapped
  CHAR length would emit an Exasol-invalid declaration. The cap is defensive — Exasol cannot declare
  a CHAR above 2,000 in the first place — so it costs nothing. The cap is deliberately NOT mirrored
  into `vs-expression`'s Exasol-dialect CHAR case (review finding [R1]): that seam echoes a width
  Exasol just sent and its documented convention is to trust it, whereas `exasol_type_from_json`
  synthesizes a declaration.
- **Promotes to ADR:** no

### [3] Keep the DATAFUSION-side CAST rendering as bare VARCHAR

- **Decision:** `vs-expression`'s `render_cast_target` keeps rendering a CHAR target as a bare,
  length-less DataFusion `VARCHAR` in its `Dialect::DataFusion` arm. *(Amended by review finding
  [R1]: the `Dialect::Exasol` arm DOES change. This decision now covers the DataFusion arm only.)*
- **Alternatives:** Also render CHAR in the DataFusion-side CAST fragment; change the test.
- **Rationale:** DataFusion and Arrow have no CHAR type, only `Utf8`, and datafusion-sql rejects a
  length-qualified character target without `support_varchar_with_length`. The value must be computed
  as a string and the declared output type must be CHAR — two separate concerns at two separate
  boundaries. Exasol then space-pads the emitted value into the CHAR output column, verified live: a
  15-character value emitted into `EMITS (P CHAR(20))` came back as `25-989-741-2988     `, matching
  native `CAST(c_phone AS CHAR(20))`. Width normalization on the DataFusion side is still needed
  where a CHAR value sits in a grouping-equality position, but that is a blank pad on a `Utf8` value,
  not a CHAR CAST target — see review findings [R2] and [R4].
- **Promotes to ADR:** yes

### [4] Keep this out of `datafusion-scan/type-mapping`

- **Decision:** Do not touch `specs/datafusion-scan/type-mapping/spec.md`.
- **Alternatives:** Extend that spec's type table with a CHAR row.
- **Rationale:** Two distinct type mappings exist in this codebase and must not be conflated.
  `type-mapping` governs Iceberg/Arrow source columns → the `createVirtualSchema` schema declaration
  and Arrow→`Value` conversion, where CHAR never appears (no Arrow type maps to it). This fix governs
  Exasol-echoed pushdown-request `dataType` JSON → the EMITS declaration, where CHAR appears only as
  an Exasol-computed expression result type.
- **Promotes to ADR:** yes

### [5] Verify the Exasol-side preconditions live during planning rather than assume them

- **Decision:** Probe the running Exasol 2025.2.1 container during planning to confirm the four facts
  the fix depends on, and record them in the spec Background: the declared types for all four #192
  shapes; that `CHAR(n)` and `CHAR(n) ASCII` are valid dynamic UDF `EMITS` output types; that Exasol
  space-pads a shorter emitted value into a CHAR output column; and that `CAST(<expr> AS CHAR(n)
  ASCII)` parses.
- **Alternatives:** Plan from the issue text and the VARCHAR arm's precedent alone; defer all
  verification to implementation.
- **Rationale:** The fix is worthless if Exasol rejects `CHAR` as an EMITS type or if padding
  diverges from native semantics, and neither is documented. Each probe was a single SQL statement
  against an already-running container. The probe also surfaced a fact the issue did not report: the
  declared type depends on whether the CASE branches have equal length — `'NEG'`/`'POS'` yields
  `CHAR(3) ASCII` while `'high'`/`'low'` yields `VARCHAR(4) ASCII`. That explains why the existing
  E2E projection test at `e2e_capability_test.rs:978` passes today and is now stated in the spec.
- **Promotes to ADR:** yes

### [6] Keep the payload capture as an ADVISORY first task, gating nothing

- **Decision:** Task 1 captures the real pushdown payload for all four shapes with
  `scripts/capture-pushdown-payload.sh`, and widens coverage if a shape routes to an unexpected
  adapter path — but it is best-effort and gates no later task. *(Amended by review finding [R3];
  originally a hard gate on the commit.)*
- **Alternatives:** Skip the capture, since the declared types are already known from the native
  probe; run the capture during planning; keep it as a hard gate on the fix's commit.
- **Rationale:** This project's standing rule is to capture the actual Exasol pushdown JSON before
  changing pushdown logic rather than guess. The native probe settles what Exasol *declares* but not
  which adapter path each request routes to — the bare-literal GROUP BY key in particular may arrive
  as a grouped `Constant` item or, if Exasol sends an empty `groupBy`, on the single-group aggregate
  path. Running the capture during planning was rejected because it needs a full `.so` release build
  inside Docker, and a headless run must not stake itself on a long build completing. That same
  reasoning applies to the implement run, which is also headless: making the capture a hard commit
  gate would reintroduce exactly the dependency the planning-time rejection avoided. The unit tests
  in Tasks 2-3 and 5-6 pin every routing assumption independently of the capture.
- **Promotes to ADR:** no

### [7] Add an E2E regression test, not unit tests alone

- **Decision:** Cover FOUR shapes with an E2E test in `e2e_capability_test.rs` in addition to the
  unit tests — the three CHAR-declared #192 facets plus the VARCHAR control — and a fifth E2E test for
  CHAR grouping equality over trailing-whitespace data (review finding [R2]).
- **Alternatives:** Unit tests only, as `fix-207-like-non-string-column` did; cover only the three
  CHAR facets and treat the control as scaffolding.
- **Rationale:** A unit test can assert only the rendered type string. Exasol's own type checker is
  the thing that rejects the pushdown, CHAR blank padding is engine-side behavior, and CHAR grouping
  equality is a merge over engine-side padded values, so only an E2E run proves the fix. This defect
  also reached production precisely because no E2E covered an equal-length CASE — the existing test's
  literals differ in length and so land on the VARCHAR path. The VARCHAR control is counted as a
  tested shape, not scaffolding: it is what proves the CHAR branch did not disturb the VARCHAR path,
  so the scenario title, the test name, and the assertions all say four.
- **Promotes to ADR:** no

## Review Findings

### [R1] [plan-review] Facet (b) stayed broken on the two Exasol-parsed wrapper paths

- **Finding:** `plan-reviewer` (BLOCKER, `SCOPE_REDUCTION` + `UNSTATED_ASSUMPTION`) showed that
  plan.md's claim "every pushdown path derives its declared types through this one seam … the single
  arm fixes all three #192 facets and the join and empty-result paths" was false. Two reachable paths
  derive their select-list output types from `vs-expression`'s Exasol dialect, never from
  `exasol_type_from_json`: the N-scan unaccelerated join wrapper
  (`joins/sql_builders.rs:191` `n_scan_join_select_items` → `render_selectlist_item_qualified` →
  `render_expression_exasol_safe`) and the qualified single-table aggregate fallback
  (`joins/sql_builders.rs:773` → `outer_wrapper_clauses` at `:814` → the same renderer). The
  Non-Goal's rationale was also wrong on the facts: `render_cast_target` does not only render "the
  internal DataFusion-side CAST fragment" — it has two dialect arms, and the Exasol arm's output is
  parsed by Exasol's own core engine. "Arrow has no CHAR type" justifies the DataFusion arm only.
- **Re-verification:** Confirmed independently. `crates/vs-expression/src/lib.rs:116-131` matches
  `"VARCHAR" | "CHAR"` in one arm and its `Dialect::Exasol` branch returns `format!("VARCHAR({size})")`,
  so `SELECT CAST(c AS CHAR(20)), COUNT(DISTINCT x), COUNT(DISTINCT y) FROM t` still fails
  `Data type mismatch … Expected CHAR(20)` and loses its blank padding. Also confirmed the reviewer's
  claim that only the broadcast join path is already fixed: `joins/mod.rs:138` routes through
  `project_columns`. And confirmed the collateral the reviewer did not name: the existing test
  `qualified_count_distinct_cast_char_renders_length_qualified_exasol_varchar`
  (`joins/sql_builders.rs:1719`) actively ASSERTS the buggy
  `COUNT(DISTINCT CAST("LHS_T0"."C_VARCHAR" AS VARCHAR(20)))` rendering, so the fix must retarget it.
- **Direction change:** Took the reviewer's option (a). `render_cast_target`'s `Dialect::Exasol` arm
  is now IN scope and gains a `CHAR` case rendering `CHAR({size})`, plus ` ASCII` when the node's
  `dataType.characterSet` is `ASCII` — the suffix is required, not cosmetic, because rendering bare
  `CHAR(3)` for an ASCII-declared CHAR would merely trade a `VARCHAR(3) ASCII` mismatch for a
  `CHAR(3) UTF8` one. The `Dialect::DataFusion` arm and the Exasol `VARCHAR` rendering stay
  byte-identical; `crates/vs-expression` is shared with a sibling VS-adapter project, so the change is
  narrowly additive and the crate's documented "trust Exasol's `size`, do not clamp" rule is kept
  (the 2,000 cap stays in the adapter seam only). Added plan Task 5, a `CHANGED` + `NEW` delta on
  `sql-comprehension/vs-expression-translator-scalar-ops`, a new spec scenario "A CAST-to-CHAR item
  inside an Exasol-parsed wrapper declares a CHAR column", a Background bullet naming both wrapper
  paths, a corrected Non-Goals paragraph, and a Dead Code Removal row for the two stale tests.
- **Supersedes:** the clause "`CHAR(n)` also → `VARCHAR(n)` per the mission data-type table" in
  `specs/_decision/011-fix-count-distinct-shard-cap.md`'s follow-up **Exasol-dialect CAST for the
  qualified wrapper**. That ADR's core decision — the dialect split itself, and length-qualifying
  character targets on the Exasol side — stands unchanged; only the CHAR-collapses-to-VARCHAR clause
  is superseded.
- **Promotes to ADR:** yes

### [R2] [plan-review] A CHAR group key would have grouped on the unpadded value

- **Finding:** `plan-reviewer` (BLOCKER, `COMPLETENESS_GAP`) showed that making
  `SELECT CAST(c AS CHAR(20)), COUNT(*) FROM t GROUP BY 1` pushable would group on the UNPADDED value
  while Exasol groups on the PADDED CHAR value, so source values `'ab'` and `'ab   '` would yield TWO
  output rows with split counts that both render identically as `'ab'` + 18 spaces where native
  Exasol yields ONE merged row. The fix as planned would have converted a clean type-checker
  rejection into a silently wrong answer.
- **Re-verification:** Confirmed. `grouped_agg.rs:591` declares the inner EMITS as
  `"GK_{i}" VARCHAR(2000000)` unconditionally, `grouped_agg.rs:652` builds the outer
  `GROUP BY "GK_0"` over that raw staging column, and `grouped_agg.rs:605-612` applies
  `CAST("GK_{i}" AS {ty})` only in the SELECT list. Also independently bounded the exposure, which
  the reviewer left open: `ScanSpec.common.group_keys` is populated at exactly ONE site
  (`mod.rs:390`, the grouped arm) and set `None` everywhere else; the `COUNT(DISTINCT)` fan-out is
  reachable only for a lone bare-column argument (`support.rs:296-320`, `is_lone_count_distinct`
  requires `dc.column.is_some()`), and a bare base column can never be CHAR-declared;
  `constant_projection_sql` renders an Exasol-side expression with no DataFusion counterpart. So the
  single exposed position is a `CHAR`-declared grouped-aggregate group key.
- **Direction change:** Took the reviewer's option (a) — no override needed. Added plan Task 6: when a
  group key's declared type is `CHAR(n)`, pad its DataFusion-side fragment to the declared width.
  *(The pad EXPRESSION chosen here was `rpad(<fragment>, n)`; that choice is **superseded by review
  finding [R4]**, which found `rpad` truncates over-length values and would have reintroduced this
  entry's own failure class in the opposite direction. The pad is now
  `CASE WHEN character_length(<fragment>) < n THEN rpad(<fragment>, n) ELSE <fragment> END`. This
  entry's conclusion — that a CHAR group key MUST be width-normalized on the DataFusion side — stands;
  only the expression changed.)* Two implementation
  constraints the reviewer did not name, both verified and now written into the plan and spec: (i) the
  padded fragments MUST be a separate list used only for `ScanSpec.common.group_keys`, because
  `build_grouped_order_by_clause` (`grouped_agg.rs:540`) and `detect_group_by_aggregates`
  (`grouped_agg.rs:244`) both match group keys by UNPADDED rendered-SQL equality — padding in place
  would return `Unresolvable` and decline every `ORDER BY` on a CHAR group key; and (ii)
  `group_key_exasol_types` (`mod.rs:395`) must move above the `spec_template` construction
  (`mod.rs:385`), which currently runs first. Confirmed the padding survives the UDF:
  `value_to_gk_string` (`partial_agg.rs:243`) passes strings through unchanged, and
  `build_grouped_partial_agg_sql` splices the same fragment verbatim into both the DataFusion SELECT
  list and its GROUP BY (`partial_agg.rs:210,233`). Projection-only CHAR ordinals are explicitly NOT
  padded (a Non-Goal): they carry no equality semantics and Exasol pads on read. Added spec scenarios
  "A CHAR group key is padded to its declared width before grouping" and "A CHAR group key over
  trailing-space data groups identically to native Exasol".
- **Seed consequence:** The `events` seed's `name` values are `event-NN` — exactly 8 characters, so
  the existing data cannot exhibit padding divergence. Rather than add a row to `events`, `labels`, or
  `regions` (whose `SEED_TOTAL_ROWS` / `SEED_LABELS_ROWS` / partition-pruning constants are asserted
  by existing tests), Task 7 adds a minimal dedicated seed table holding `'ab'`, `'ab   '`, and
  `'cd'` via the existing `create_and_append_files` helper. *(Extended by review finding [R4] with a
  FOURTH value 25 characters long, so one table carries both E2E queries: `CHAR(30)` fits every value
  and isolates the merge case, `CHAR(20)` makes exactly that row over-length and must raise 22001.)*
- **Promotes to ADR:** yes

### [R3] [plan-review] Advisory findings folded in

- **Finding:** `plan-reviewer` raised seven non-blocking findings.
- **Direction change:** Applied all seven.
  1. **"Aggregates are never CHAR" was false.** Re-verified: `col_type_for` (`grouped_agg.rs:782-787`)
     returns the declared type verbatim when an aggregate's argument is an expression rather than a
     bare column, and `validate_agg_col_types` (`grouped_agg.rs:833-858`) gates only SUM and the
     STDDEV/VARIANCE family on a numeric type — its own doc says "MIN/MAX are valid over any
     comparable type". So `MIN(CAST(<col> AS CHAR(20)))` declares `PARTIAL_min_0 CHAR(20)`. Corrected
     the Task 4 claim, added a spec scenario and a unit test asserting the partial EMITS type and the
     merge cast.
  2. **Consumer count reconciled to 8** non-test call sites, enumerated in a plan.md table and in the
     spec Background, with the two inert ones named (`extract_all_column_types`,
     `involved_table_columns`) and the reason stated: both read `involvedTables[].columns`, which can
     never carry CHAR because no Arrow source type maps to it.
  3. **Task 1 demoted** to advisory/best-effort, gating nothing — see amended decision [6].
  4. **E2E scenario retitled** to four shapes — see amended decision [7].
  5. **"Three further Exasol-side facts"** corrected to four, matching its bullet count.
  6. **Redundant "optional polish, not part of this plan" parenthetical** removed from both plan.md
     and decision-log — moot now that the `vs-expression` change is in scope and the two stale tests
     are explicit Task 5 work.
  7. **"Removes a whole failure class"** dropped from decision [2], which contradicted its own next
     clause about the 2,000 cap being unreachable.
- **Promotes to ADR:** no

### [R4] [plan-review round 2] `rpad` truncation would have reintroduced the silent-wrong-answer class

- **Finding:** `plan-reviewer` (round 2, BLOCKER) showed that the round-1 fix for [R2] carried the same
  defect [R2] was raised about, in the opposite direction. Exasol does NOT truncate an over-length
  value into a `CHAR(n)` — it errors. But `rpad(<expr>, n)` in DataFusion truncates. So for an
  over-length source value the planned pad would silently shorten it, the outer
  `CAST("GK_0" AS CHAR(n))` would become a no-op on an already-`n`-character input, and the query
  would return a wrongly-merged group where native Exasol fails the statement outright — exactly the
  "clean rejection becomes silent wrong answer" failure class this plan exists to prevent.
- **Re-verification:** Confirmed on both sides, by execution rather than by reading.
  Exasol: `CAST('abcdefghij' AS CHAR(3))` on the running 2025.2.1 container fails with
  `data exception - string data, right truncation; Valuelength: 10 Maxlength: 3` (SQL state 22001).
  DataFusion 54.1: `rpad('abcdefghij', 3)` returns `'abc'` — truncated. The source confirms it
  (`unicode/rpad.rs`: `if target_len <= str_len { builder.append_value(&string[..target_len]) }`,
  and its own doc string says "If the input string is longer than this length, it is truncated"), so
  the round-1 plan's claim that `rpad` gives "exactly Exasol's `CHAR(n)` blank-padding semantics" was
  wrong on the over-length case. The BLOCKER is real.
- **Direction change:** Replaced the pad expression. The orchestrator proposed
  `concat(<frag>, repeat(' ', greatest(n - character_length(<frag>), 0)))`; every candidate was
  executed against the pinned DataFusion 54.1 before choosing, and **that candidate was rejected**:
  DataFusion's `concat` skips NULL arguments, so a NULL group key measured as `''` rather than NULL
  and would merge with a genuine all-blanks group — a different silent-wrong-answer bug. The chosen
  form is `CASE WHEN character_length(<frag>) < n THEN rpad(<frag>, n) ELSE <frag> END`. Measured for
  `n = 5`: NULL → NULL, `'ab'` → `'ab   '`, `'abc'` → `'abc  '`, `'abcdefghij'` → unmodified. Three
  further properties were measured rather than assumed: a NULL `WHEN` condition falls to `ELSE` so
  NULL survives; `character_length` and `rpad` both count CHARACTERS not bytes (`'äö'` → `'äö  '` at
  `n = 4`), matching Exasol's character-based `CHAR(n)`; and the expression parses and evaluates when
  `<frag>` is itself a `CASE` expression — the #192 primary shape — spliced into all three positions.
  Every "pads to n" / "rpad" / "exactly Exasol's CHAR(n) blank-padding semantics" claim in plan.md,
  this log, and the adapter spec was narrowed to the accurate statement: SHORT values are padded to
  `n` with trailing spaces; values at or above `n` are left UNCHANGED so Exasol's own 22001 fires from
  its merge-side `CAST("GK_0" AS CHAR(n))` evaluation rather than being masked by the pad.
- **Coverage added:** a spec scenario and unit test that the rendered pad carries an over-length value
  through unmodified (a plan-level test can assert only the SQL text, so the assertion is scoped to
  that and the limitation is stated); a spec scenario and E2E test that an over-length group-key value
  raises 22001 like native instead of returning a merged truncated group; and an over-length seed row
  (25 characters) sized so ONE seed table serves both E2E queries — `CHAR(30)` fits every value and
  isolates the merge case, `CHAR(20)` makes exactly that row over-length.
- **Projection facet corrected too:** the round-1 spec claimed unconditional equality with native
  `CAST(<col> AS CHAR(20))` for ALL values, but only a shorter-than-`n` case had been live-verified.
  Probed the over-length case rather than file a tracked exception for it: a LUA script emitting a
  10-character value into `EMITS (P CHAR(5))` fails with `Lua Error "string too long"` (SQL state
  22001), while 3 characters pad cleanly to `'abc  '`. Exasol enforces the declared CHAR width at
  emit, so the projection facet also fails cleanly rather than truncating and needs NO tracked
  exception. The claim was still qualified to "for values no longer than the declared width", with the
  over-length case stated separately and its differing error origin named (UDF emit vs. engine cast,
  same SQLSTATE 22001). Whether the Rust SLC's `emit_batch` Arrow IPC path behaves identically is
  written as an E2E assertion, not an assumption — and if it truncates there, the spec requires
  recording a cited tracked exception rather than leaving a silent gap.
- **Promotes to ADR:** yes

### [R5] [plan-review round 2] A third `render_cast_target` consumer and two more stale tests were missed

- **Finding:** `plan-reviewer` (round 2, BLOCKER) found that `render_scalar_over_merge` is a THIRD
  Exasol-parsed consumer of `render_cast_target`'s Exasol arm, beyond the two [R1] named, and that
  FIVE tests assert the old CHAR→VARCHAR collapsing behavior rather than the two Task 5 listed.
- **Re-verification:** Confirmed all of it. `render_scalar_over_merge` (`grouped_agg.rs:415`) calls
  `render_expression_exasol` directly and is reached from `build_grouped_aggregate_scan_sql`'s
  `ScalarOverAggregate` arm (`grouped_agg.rs:640-644`). All five tests exist at the cited lines with
  the cited assertions: `renders_cast_char_as_varchar` (`vs-expression/src/lib.rs:1610`),
  `qualified_count_distinct_cast_char_renders_length_qualified_exasol_varchar`
  (`joins/sql_builders.rs:1719`), `renders_cast_char_exasol_dialect_includes_length`
  (`lib.rs:2869`, asserting `CAST("X" AS VARCHAR(3))`), `cast_char_target_diverges_between_dialects`
  (`lib.rs:2889`, asserting Exasol-side `VARCHAR(20)`), and
  `scalar_over_merge_casts_to_length_qualified_exasol_varchar` (`grouped_agg.rs:1134`, asserting
  `sql.contains("VARCHAR(20)")`). Also confirmed the doc comment at `support.rs:891` cites
  `renders_cast_char_as_varchar` by name.
- **Direction change:** Because the code fix is the one shared `render_cast_target` Exasol arm, the
  third consumer needs no additional code change — but the test inventory did. Task 5 now carries a
  six-row table covering all five tests plus the `support.rs:891` doc-comment citation, each with its
  current assertion and its retarget, and each instructed to preserve its original guarding intent.
  `cast_char_target_diverges_between_dialects` is explicitly retained as a divergence guard with only
  its Exasol-side expectation retargeted; `scalar_over_merge_casts_to_length_qualified_exasol_varchar`
  keeps its length-qualification assertion because that is the invariant ADR 011 established. Named
  `render_scalar_over_merge` as the third seam-2 consumer in plan.md's Design (now a table), the
  Architecture diagram, the Patterns table, the adapter spec Background bullet, and the vs-expression
  delta Background bullet. Added a nested-CAST case
  (`CAST(CAST(SUM(x) AS CHAR(20) ASCII) AS CHAR(20) ASCII)`) to the Verification coverage table, both
  spec deltas, and Task 5's new tests. Added four Dead Code Removal rows for the additional stale
  tests and the stale doc comment.
- **Promotes to ADR:** no

### [R6] [plan-review round 2] Advisory findings folded in

- **Finding:** `plan-reviewer` raised five further non-blocking findings.
- **Direction change:** Applied all five.
  1. **Group-key padding claim qualified.** "SHALL apply to every group-key slot whose declared type
     is `CHAR(n)`" now adds "and which is itself a select-list ordinal", with the reason: an
     unreferenced group key has no `selectListDataTypes` entry, keeps the `VARCHAR(2000000)` default,
     and is pre-existing and out of scope.
  2. **"Cannot disagree" narrowed to CHAR.** The vs-expression delta's cross-seam agreement claim is
     now scoped to CHAR, and the pre-existing VARCHAR suffix asymmetry — the adapter appends ` ASCII`,
     this crate's Exasol VARCHAR rendering does not — is named as untouched and out of scope.
  3. **"Only DataFusion-side equality position" narrowed to GROUPING equality.** Filter/predicate
     equality on a CHAR-typed CAST is named as a separate, pre-existing, untouched divergence that
     this feature does not cover.
  4. **Task 6's width parse pinned.** It must read the digits between `(` and `)` so BOTH `CHAR(3)`
     and `CHAR(3) ASCII` yield `n`; trimming a trailing `)` would fail on the suffix and silently skip
     padding on every ASCII-declared CHAR key — the #192 primary shape. Added a unit test for it.
  5. **plan.md Summary tightened** from three sentences to two, dropping the path enumeration Design
     already covers.
- **Promotes to ADR:** no
