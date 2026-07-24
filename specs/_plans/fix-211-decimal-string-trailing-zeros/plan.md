# Plan: fix-211-decimal-string-trailing-zeros

## Summary

Make pushed-down DECIMAL→string conversions reproduce Exasol's shortest-form formatting (trailing scale zeros trimmed) in both the single-table select-list projection and the single-table WHERE-clause filter, fixing the silent wrong-result divergence in issue #211 including its headline `LENGTH(c_acctbal)>5` COUNT repro. A pure `format_decimal_exasol_style` helper, an adapter-synthesized `decimal_to_varchar_exasol` node, and one shared recursive rewriter carry the trim; the adapter decides where to inject it from resolved column types, keeping `vs-expression` type-blind.

## Design

### Context

Exasol converts a DECIMAL to its shortest string form, trimming trailing scale zeros (`2912.00`→`'2912'`). The pushed-down path delegates to DataFusion, whose `CAST(decimal AS VARCHAR)` and implicit decimal→utf8 coercion both render the full declared scale (`'2912.00'`). Every pushed-down expression that stringifies a DECIMAL column can silently return a different result — including a demonstrated aggregate COUNT divergence. This is silent wrong data, not an error.

A stringified column's `dataType` never crosses the wire; column Exasol types exist only in `involvedTables[0].columns`. `crates/vs-expression` is a pure, stateless, sibling-shared JSON→SQL translator with no column-type context, so it cannot decide type-dependent formatting itself. Issue #207's LIKE fix established the precedent: the type-aware decision lives in the adapter (`pushdown/support.rs`, which already resolves column types via `extract_all_column_types`), which rewrites the expression JSON before `vs-expression` renders it.

- **Goals** — reproduce Exasol's trimmed DECIMAL→string formatting for `CAST`/`CONCAT`/`LENGTH` over a bare DECIMAL column in the single-table select-list AND WHERE-clause filter; fix issue #211's headline `LENGTH(c_acctbal)>5` COUNT repro; ship a reusable trim primitive issue #210 can consume; keep `vs-expression` type-blind.
- **Non-Goals** — a stringified computed (non-bare-column) expression; the broadcast-join per-leg filter path; a GROUP-BY key absent from the select list; string functions that hard-fail on a DECIMAL argument (issue #210); changing the emit boundary.

### Decision

Add a pure `format_decimal_exasol_style(expr_sql: &str) -> String` helper and a `decimal_to_varchar_exasol` node type to `vs-expression`. In the adapter, one shared recursive rewriter (`rewrite_decimal_stringifications`) walks any expression or predicate tree and, at each stringifier node (`CAST` to VARCHAR/CHAR, `CONCAT`, `LENGTH`), rewrites every directly-stringified bare DECIMAL-column argument into a `decimal_to_varchar_exasol` node — descending through nested `CONCAT` (`a||b||c` is nested `CONCAT(a,CONCAT(b,c))`) and never wrapping a DECIMAL column in a non-stringifying context. The rewriter runs on each select-list item in `project_columns` and, composed after `like_subject_type_guard`, on the WHERE-clause filter tree at the single production filter site (`handle_pushdown`, mod.rs:188). `vs-expression` renders the node by rendering its argument then wrapping it with `format_decimal_exasol_style`, with no type inspection of its own.

#### Architecture

```
pushdown request (JSON)
  ├─ project_columns (also reused by the broadcast-join select-list)
  │     per select-list item: rewrite_decimal_stringifications(item, col_types)
  │     project_columns item_type match recognizes decimal_to_varchar_exasol
  │
  └─ handle_pushdown filter chain (mod.rs:188, single production site)
        like_subject_type_guard(f)  →  rewrite_decimal_stringifications(f, col_types)
        (DataFusion filter tree only; raw filter for Iceberg pruning untouched)
        │
        ▼
  vs-expression render_expression
        │  decimal_to_varchar_exasol → format_decimal_exasol_style(render(arg))
        ▼
  scan SQL  ── regexp_replace(regexp_replace(CAST(<col> AS VARCHAR),
        │        '(\.[0-9]*[1-9])0+$','\1'), '\.0+$','')  → Utf8View
        ▼
  emit.rs coerce_batch_to_exa_types  ── Utf8View → Utf8 for VARCHAR-declared column
        ▼
  Exasol EMITS / native predicate (trimmed text)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Type-aware guard in adapter, translator stays type-blind | `pushdown/support.rs` guard + `vs-expression` node | Mirrors #207 `like_subject_type_guard`; preserves `vs-expression` reuse by the sibling project |
| Adapter-synthesized JSON node rendered blindly | `decimal_to_varchar_exasol` | Injects the trim at a nested argument position without teaching `vs-expression` column types; #210 reuses the same node |
| Reusable pure string primitive | `format_decimal_exasol_style` | Single crate-visible, unit-tested trim source #210 imports directly |
| One recursive rewriter shared by projection and filter | `rewrite_decimal_stringifications` | Nested `CONCAT` and the WHERE path fall out of the same tree walk; a bare-column check leaves non-resolvable (computed) arguments untouched, never guessed |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Trim decision in the adapter; `vs-expression` type-blind | Add column-type awareness inside `vs-expression` | `vs-expression` has no column-type context and is sibling-shared (#207 ADR `like-guard-in-adapter-not-vs-expression`) |
| Inject a `decimal_to_varchar_exasol` node | Post-process rendered SQL strings; add a raw-SQL passthrough node | A synthetic node keeps nesting correct inside `CONCAT` and is reused verbatim by #210; string surgery is fragile |
| `regexp_replace`-based trim | Arrow-side re-formatting at emit; a DataFusion UDF | The value may be consumed inside the scan (e.g. `LENGTH`), so emit-side reformatting cannot fix it; `regexp_replace` needs no new UDF and is verified against DataFusion 54 |
| Scope to single-table select-list AND WHERE-clause bare-DECIMAL-column CAST/CONCAT/LENGTH | (a) select-list only, deferring WHERE — rejected because issue #211's only real-data-quantified symptom is a WHERE filter; (b) also fix computed args, join per-leg filter, GROUP-BY-only keys now | The WHERE path is one production filter site reusing the same rewriter, so fixing it earns a true `Closes #211`; computed-arg types are not on the wire and the join per-leg filter is a separate surface (#215 split for LIKE) — both deferred as tracked exception #223, not a silent gap |

### Iceberg spec compliance

Apache Iceberg carries decimals as the `decimal(P, S)` primitive: "Fixed-point decimal; precision P, scale S. Scale is fixed and precision must be 38 or less" (Iceberg table spec, Primitive Types). Because scale S is fixed, a stored value's trailing-zero digits in that scale are a formatting artifact of S, not part of the decimal value. Trimming them for the string form therefore changes presentation only, never the numeric value, and aligns the pushed-down string with Exasol's own DECIMAL→string conversion. No Iceberg-spec deviation is introduced; the residual out-of-scope paths are tracked in #223.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `sql-comprehension/vs-expression-translator-scalar-ops` | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| `vs-adapter/pushdown-planning-decimal-string-format` | NEW | `vs-adapter/pushdown-planning-decimal-string-format/spec.md` |

## Dependencies

- Branch base: `fix/207-like-non-string-column-v2` (PR #217, open, CI green — verified present on origin). PR stacks on it; do not merge #217.
- Reuses `extract_all_column_types` and the `#207` guard pattern in `crates/lakehouse-engine/src/adapter/pushdown/support.rs`.

## Implementation Tasks

1. Add `format_decimal_exasol_style(expr_sql: &str) -> String` to `crates/vs-expression/src/lib.rs`, next to `render_cast_target`, emitting `regexp_replace(regexp_replace(CAST(<expr> AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', '')`; add a unit test that evaluates the emitted SQL over literal decimals against a `datafusion::prelude::SessionContext` covering `2912.00`→`2912`, `-272.60`→`-272.6`, `868.90`→`868.9`, `0.00`→`0`, `100.00`→`100`, `12.350`→`12.35`, the `40.99`→`40.99` no-op, a scale-0 integer (`100`→`100`, `-7`→`-7`), and a NULL input (→ NULL).
2. Add a `decimal_to_varchar_exasol` arm to `render_expression_inner` that renders its single argument recursively then applies `format_decimal_exasol_style`, erroring (raising) / `None` (safe) on a non-unary argument list; add a unit test asserting the rendered SQL for a `column` argument.
3. Add `rewrite_decimal_stringifications(node: &Json, col_types) -> Json` to `crates/lakehouse-engine/src/adapter/pushdown/support.rs` — a recursive tree walk that, at each stringifier node (`function_scalar_cast` to VARCHAR/CHAR, `function_scalar` named `CONCAT`, `function_scalar` named `LENGTH`), replaces each directly-stringified bare-DECIMAL-column argument with a `decimal_to_varchar_exasol` node (whole-node replacement for the CAST case), and at every other node recurses into child expressions WITHOUT wrapping — so nested `CONCAT` args are reached but a DECIMAL column in a non-stringifying context (arithmetic, comparison operand) is never wrapped; leaves non-DECIMAL columns and non-bare-column arguments unchanged. [expert]
4. Wire the rewriter into `project_columns` so it runs on each select-list item before `render_expression_safe`, AND add `decimal_to_varchar_exasol` to the recognized-scalar `item_type` match arm (support.rs ~663-673) so a CAST rewritten to a top-level `decimal_to_varchar_exasol` node routes through `render_expression_safe` instead of the `_ =>` full-row fallback. [expert]
5. Wire the rewriter into the WHERE-clause filter chain at `handle_pushdown` (mod.rs:188), composed AFTER `like_subject_type_guard` and before `render_df_filter_safe`, on the DataFusion filter tree only — leaving `filter_json_raw` (the Iceberg-pruning tree) untouched; the rewriter never declines. [expert]
6. Add unit tests in `support.rs`: explicit CAST rewrite (and its `project_columns` routing, not full-row fallback), nested-CONCAT (`id||'-'||c_decimal_a`) rewrite, LENGTH rewrite, non-DECIMAL passthrough, computed-argument (`c_decimal_a*2`) untouched, WHERE-clause `LENGTH(c_decimal_a)>5` rewritten, and DECIMAL in a non-stringifying filter context (`c_decimal_a>5`) NOT rewritten.
7. Confirm `crates/lakehouse-engine/src/scan/emit.rs` coerces a `Utf8View` projected-expression column declared `VARCHAR(2000000)` to `Utf8` (already handled by `target_arrow_type`, `emit.rs:176,182`); add a focused emit unit test for an expression column at that declared type.
8. Add an E2E regression test in `crates/lakehouse-engine/tests/e2e_capability_test.rs` (Docker Exasol) over the `typed_distinct_probe` seed asserting: select-list `CAST(c_decimal_a AS VARCHAR(20))`, `id||'-'||c_decimal_a`, and `LENGTH(c_decimal_a)` return Exasol-trimmed results, AND the WHERE-clause headline shape `SELECT COUNT(*) ... WHERE LENGTH(c_decimal_a)>N` matches native Exasol — failing on current code, passing on the fix.
9. Run `cargo clippy --all-targets && cargo fmt`; ensure host `cargo test` is green.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1, Task 7 |
| Group B | Task 2 |
| Group C | Task 3 |
| Group D | Task 4, Task 5 |
| Group E | Task 6 |
| Group F | Task 8 |

Sequential dependencies:
- Group A (Task 1) → Group B (Task 2 wraps the Task 1 helper)
- Group B → Group C (the rewriter injects the Task 2 node)
- Group C → Group D (both wirings call the Task 3 rewriter; Task 4 and Task 5 are independent of each other)
- Group D → Group E (Task 6 tests the wired projection and filter paths)
- Group E → Group F (E2E exercises the fully wired path)
- Task 9 runs last.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | Additive change; no existing function or test is obsoleted |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Decimal-to-VARCHAR node renders Exasol-trimmed string | Unit | `crates/vs-expression/src/lib.rs` | `decimal_to_varchar_exasol_node_renders_trim` |
| format_decimal_exasol_style reproduces Exasol shortest-form decimal formatting | Unit | `crates/vs-expression/src/lib.rs` | `format_decimal_exasol_style_edge_cases` |
| Explicit CAST of a DECIMAL column to VARCHAR renders the trimmed form | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_decimal_cast_rewritten_and_routed` |
| Implicit CONCAT over a DECIMAL column renders the trimmed form, including nested concatenation | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_nested_concat_decimal_arg_rewritten` |
| Implicit LENGTH over a DECIMAL column renders the trimmed form | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_length_decimal_arg_rewritten` |
| WHERE-clause stringification of a DECIMAL column renders the trimmed form | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `filter_length_decimal_rewritten_to_trim` |
| A DECIMAL column in a non-stringifying filter context is left unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `filter_decimal_comparison_not_rewritten` |
| CAST, CONCAT, or LENGTH over a non-DECIMAL column is left unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `stringify_nondecimal_column_unchanged` |
| A stringified computed expression is left unchanged as a tracked exception | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `stringify_computed_decimal_arg_untouched` |
| Utf8View projected-expression column coerces to Utf8 at emit | Unit | `crates/lakehouse-engine/src/scan/emit.rs` | `expr_column_utf8view_coerces_to_utf8` |
| End-to-end trimmed DECIMAL→string select-list and WHERE-clause results | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_decimal_to_string_trims_trailing_zeros` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `vs-adapter/pushdown-planning-decimal-string-format` | `./scripts/capture-pushdown-payload.sh` then run `SELECT id, CAST(c_decimal_a AS VARCHAR(20)) FROM <vs>.typed_distinct_probe WHERE id IN (1,4,6)` against the Docker stack | `10.5` / `30` / `40.99` (not `10.50` / `30.00`) |
| `vs-adapter/pushdown-planning-decimal-string-format` | `SELECT id||'-'||c_decimal_a FROM <vs>.typed_distinct_probe WHERE id IN (1,4)` | `1-10.5` / `4-30` (not `1-10.50` / `4-30.00`) |
| `sql-comprehension/vs-expression-translator-scalar-ops` | `SELECT LENGTH(c_decimal_a) FROM <vs>.typed_distinct_probe WHERE id IN (1,4,6)` | `4` / `2` / `5` (not `5` / `5` / `5`) |
| `vs-adapter/pushdown-planning-decimal-string-format` (WHERE path) | `SELECT COUNT(*) FROM <vs>.typed_distinct_probe WHERE LENGTH(c_decimal_a) > 4` | Count matches native Exasol `LENGTH` (only `id=6`, `40.99`), not the untrimmed over-count |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures (fails, not skips, without Docker Exasol) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
