# Feature: Pushdown Planning — String-Function Type Coercion

Makes the string-position arguments of pushed-down string functions type-aware, so a non-string
argument is coerced, declined, or passed through rather than hard-failing the DataFusion scan.

## Background

* This feature's recorded out-of-scope bullet "The broadcast-join PER-LEG WHERE-clause filter path
  (`pushdown/joins/sql_builders.rs`), a render surface distinct from the join SELECT list (issue
  #223)" is REPLACED: that surface is now IN scope. Both join WHERE-filter render surfaces — the
  broadcast join's combined filter and the N-scan fallback's per-leg filter — run the guard through
  the shared type-rewrite pipeline, so the guard's decline and coercion reach them exactly as they
  reach the single-table WHERE filter. See `vs-adapter/pushdown-planning-join-filter-type-coercion`
  (issue #215). Issue #223's slice 2 closes with it; #223's slices 1 (computed-expression arguments)
  and 3 (GROUP-BY-only keys) remain open and out of scope here.
* The guard itself is untouched — no dispatch-table, arity-table, or traversal change. Only its
  reachable surface set grows, and it grows by wiring, not by new guard code.
* Issue #228's exposure NARROWS as a direct consequence: the `INSTR`/`LOCATE`-beyond-two-arguments
  decline now also covers the two join WHERE surfaces, so those surfaces return Exasol's native
  result instead of a position computed from a rendering that silently drops the third and fourth
  arguments. #228 is NOT closed — its root cause is the rendering defect in
  `crates/vs-expression`, untouched here, and any render surface still unwired to the guard remains
  exposed. Nothing in this delta should be read as having adjudicated #228.
* The grouped-aggregate render path, the aggregate-argument render path, `CHR`/`UNICODECHR`, and a
  non-bare-column string-position argument all remain out of scope, unchanged by this delta.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: INSTR and LOCATE coerce their first two arguments and decline beyond two

* *GIVEN* a `pushdown` request carrying `INSTR(a, b)` or `LOCATE(a, b)` where either bare-column argument is a non-string column — for example issue #210's repro `INSTR(c_custkey, '1')`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL treat indices 0 and 1 as string-position for both functions, coercing or declining each independently
* *AND* the index assignment SHALL be independent of the translator's render-time argument reorder, because `vs-expression` renders Exasol `INSTR(string, substring)` as `strpos(arg0, arg1)` and Exasol `LOCATE(substring, string)` as `strpos(arg1, arg0)` — the reorder swaps which rendered slot each argument fills, never which arguments are string-position
* *AND* the previously hard-failing `Function 'strpos' requires String, but received Int64` planning error SHALL no longer occur for this shape
* *AND* an `INSTR` or `LOCATE` call carrying MORE than two arguments — `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)` — SHALL instead make the guard return `None`, declining the whole tree for EVERY argument type including all-VARCHAR, because `vs-expression` reads only `args[0]` and `args[1]` and drops the rest (issue #228): coercing index 0 would let an incompletely rendered call plan successfully, converting today's loud DataFusion type error into a silently wrong position, and it SHALL therefore also correct the pre-existing wrong result for an all-string `INSTR(c_varchar, 'b', 3)`, which pushed down as `strpos("C_VARCHAR", 'b')` and ignored the start position
* *AND* that beyond-two decline SHALL be reached at the broadcast join's combined WHERE filter and at the N-scan fallback's per-leg WHERE filter as well as at the single-table WHERE filter and the select-list projection, each routing the decline through its OWN already-existing self-application outcome — REPLACING this feature's recorded out-of-scope bullet naming the join per-leg WHERE-filter path as a deferred surface (issue #223 slice 2, wired by issue #215)
* *AND* narrowing #228's exposure this way SHALL NOT be recorded as closing #228, whose root cause is the `crates/vs-expression` rendering defect this delta does not touch
<!-- /DELTA:CHANGED -->
