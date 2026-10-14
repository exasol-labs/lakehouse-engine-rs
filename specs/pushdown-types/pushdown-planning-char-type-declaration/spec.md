# Feature: Pushdown Planning — CHAR Type Declaration

The adapter declares a pushed-down result column as Exasol `CHAR(n)` whenever Exasol declared that ordinal `CHAR` on the wire. Exasol's type checker then accepts the pushdown instead of rejecting it with `Data type mismatch ... Expected CHAR(n), but got VARCHAR(n)` (#192). Before this feature the adapter rendered every string-family declared type as `VARCHAR(n)` and never emitted a genuine `CHAR` type.

This feature covers how the shared type derivation renders a `CHAR` type string: the ASCII suffix, the 2,000-character cap, the constant projection of a bare string literal, `CAST` targets inside the Exasol-parsed wrappers, and `MIN` and `MAX` partial columns. It also states that the `VARCHAR` path and the `LIKE` subject guard are unchanged. It specializes the type derivation shared by `pushdown/pushdown-planning`, `pushdown-grouped-agg/pushdown-planning-grouped-agg`, `file-planning/pushdown-planning-empty-result`, and `pushdown-joins/pushdown-planning-join-fallback`.

## Background

* `exasol_type_from_json` (`crates/lakehouse-engine/src/adapter/pushdown/support.rs`) is the single seam that maps an Exasol `dataType` JSON object to the Exasol type string used in the pushdown response — both the query-side `EMITS` clause and the outer-wrapper `CAST` targets. It has **8 non-test call sites**: `support.rs`'s `extract_all_column_types`, `project_columns`, and `aggregate_exasol_types`; `grouped_agg.rs`'s `constant_projection_sql`, `detect_group_by_aggregates`, and `group_key_exasol_types`; `joins/planning.rs`'s `involved_table_columns`; and `file_resolution.rs`'s `empty_select_list_typed_sql`. Two of the eight are inert for `CHAR`: `extract_all_column_types` and `involved_table_columns` both read `involvedTables[].columns`, which can never carry `CHAR` (see the next bullet).
* No Iceberg or Arrow source type maps to Exasol `CHAR` — Iceberg `string` maps to `VARCHAR` per this crate's type table. A `CHAR` type therefore reaches the adapter only as an Exasol-computed expression result in `selectListDataTypes`, never as an `involvedTables[].columns` base-column type (`crates/lakehouse-engine/tests/e2e_count_distinct_test.rs:511`).
* Exasol declares a string-literal expression `CHAR`, not `VARCHAR`, when every branch yields the same length. Verified live on Exasol 2025.2.1: `CASE WHEN c_acctbal<0 THEN 'NEG' ELSE 'POS' END` → `CHAR(3) ASCII`, while `CASE WHEN id>10 THEN 'high' ELSE 'low' END` (lengths 4 and 3) → `VARCHAR(4) ASCII`. A one-character difference in a literal flips the declared type, which is why the existing `'high'`/`'low'` E2E projection test passes while #192's `'NEG'`/`'POS'` shape fails.
* Exasol's `CHAR` maximum length is 2,000 characters. Verified live: `CAST('a' AS CHAR(2001))` fails with `specified length too long for char type - maximum is 2000`. VARCHAR's 2,000,000 cap is therefore not reusable for the CHAR branch.
* `CHAR(n)` and `CHAR(n) ASCII` are valid dynamic UDF `EMITS` output types, and Exasol space-pads a shorter emitted value into a `CHAR(n)` output column. Verified live with a LUA probe script: emitting the 15-character `25-989-741-2988` into `EMITS (P CHAR(20))` yields `25-989-741-2988     `, matching native `CAST(<col> AS CHAR(20))`.
* `CAST(<expr> AS CHAR(n) ASCII)` is valid Exasol CAST syntax (verified live), which the grouped-aggregate outer wrapper needs for `CAST("GK_i" AS <declared type>)`, and which the qualified single-table and N-scan join wrappers need for a `CAST(… AS CHAR(n))` select item.
* The character-set suffix rule is the one already established for VARCHAR (issue #136 follow-up): append ` ASCII` when the `dataType` JSON's `characterSet` equals `ASCII` case-insensitively. A `UTF8` or absent `characterSet` renders no suffix, which Exasol reads as its UTF8 default.
* **THREE Exasol-parsed wrapper paths derive their select-list column types from `vs-expression`, not from `exasol_type_from_json`**: the N-scan unaccelerated join wrapper (`joins/sql_builders.rs`'s `n_scan_join_select_items`, the recorded `pushdown-joins/pushdown-planning-join-fallback` behavior); the qualified single-table aggregate fallback (`joins/sql_builders.rs`'s `build_qualified_single_table_fallback_sql`, which serves undecomposable grouped shapes and multi/mixed `COUNT(DISTINCT)`); and the grouped-merge scalar-over-aggregate wrapper (`grouped_agg.rs`'s `render_scalar_over_merge`, reached from `build_grouped_aggregate_scan_sql`'s `ScalarOverAggregate` arm, which renders shapes such as `CAST(SUM(x) AS CHAR(20))` over the merged partials). The first two build their SELECT list via `render_selectlist_item_qualified` → `render_expression_exasol_safe`; the third calls `render_expression_exasol` directly. All three land on `render_cast_target` in the Exasol dialect, whose character arm rendered a `CHAR` target as `VARCHAR({size})`. Those wrappers are therefore fixed at the `vs-expression` seam, not at the adapter seam — one shared arm fixes all three — see the `sql-comprehension/vs-expression-translator-scalar-ops` delta in this plan. The broadcast join path is unaffected: it resolves its EMITS types through `project_columns` (`joins/mod.rs`).
* **`spec.common.group_keys` is the only DataFusion-side GROUPING-equality position a `CHAR` declared type can reach.** This claim is scoped to grouping equality specifically. Equality inside a pushed-down FILTER predicate over a `CHAR`-typed CAST is a separate, pre-existing divergence that this feature does not touch and does not cover: the DataFusion dialect renders such a CAST as a bare `VARCHAR`, so a filter comparison runs on the unpadded value. That path is unchanged by this feature and is out of scope here. `spec.common.group_keys` is populated at exactly one site (`adapter/pushdown/mod.rs`, the grouped-aggregate arm); every other `ScanSpec` construction sets it `None`. The `COUNT(DISTINCT)` fan-out (`build_distinct_fan_out`) is reachable only for a lone **bare-column** argument — `is_lone_count_distinct` requires `dc.column.is_some()`, and an expression argument declines to the qualified wrapper — so its `"V"` column always carries a base-column type, never `CHAR`. `constant_projection_sql` renders an Exasol-side outer-wrapper expression only and has no DataFusion-side counterpart; a bare literal is classified `Constant`, not a group key, and is already exactly `n` characters wide. All THREE Exasol-parsed wrapper paths let Exasol perform the grouping natively over the padded `CHAR` value, so they are correct by construction once their CAST target renders `CHAR(n)`.
* An aggregate CAN carry a `CHAR` declared type. `col_type_for` (`grouped_agg.rs`) returns the declared type verbatim for an aggregate whose argument is an expression rather than a bare column, and `validate_agg_col_types` gates only SUM and the STDDEV/VARIANCE family on a numeric type — "MIN/MAX are valid over any comparable type". So `MIN(CAST(<col> AS CHAR(20)))` on the grouped path declares `PARTIAL_min_i CHAR(20)`, which is valid because `CHAR(n)` is a valid `EMITS` type and a valid `CAST` target.
* See `pushdown-types/pushdown-planning-char-type-declaration-padding` for `CHAR` projection and group-key values: blank padding of group keys, over-length values, and the end-to-end #192 query shapes.

## Scenarios

### Scenario: A CHAR-declared type renders as CHAR

* *GIVEN* a `dataType` JSON object `{"type":"CHAR","size":20,"characterSet":"UTF8"}`, the shape Exasol sends for an explicit `CAST(<col> AS CHAR(20))` select-list ordinal
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `CHAR(20)`
* *AND* the adapter MUST NOT render `VARCHAR(20)`
* *AND* the match on the `type` field SHALL be case-insensitive, so both `"CHAR"` and `"char"` resolve, mirroring the existing lowercase-normalized dispatch

### Scenario: A CHAR-declared ASCII type carries the ASCII suffix

* *GIVEN* a `dataType` JSON object `{"type":"CHAR","size":3,"characterSet":"ASCII"}`, the shape Exasol sends for an equal-length CASE-of-string-literals ordinal
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `CHAR(3) ASCII`
* *AND* the `characterSet` comparison SHALL be case-insensitive, mirroring the VARCHAR rule
* *AND* the same object without a `characterSet` field SHALL render bare `CHAR(3)`, which Exasol reads as its UTF8 default

### Scenario: A CHAR size above Exasol's maximum is capped at 2,000

* *GIVEN* a `dataType` JSON object of type `CHAR` whose `size` exceeds 2,000
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `CHAR(2000)`
* *AND* the adapter MUST NOT apply VARCHAR's 2,000,000 cap to a CHAR type, because Exasol rejects any CHAR length above 2,000 with `specified length too long for char type - maximum is 2000`
* *AND* a `CHAR` object with no `size` field SHALL still render a valid CHAR length rather than an out-of-range one

### Scenario: A bare string-literal group-key projection casts to CHAR

* *GIVEN* a grouped-aggregate `pushdown` request whose `selectList` carries a `literal_string` item alongside an aggregate
* *AND* whose `selectListDataTypes` declares that literal's ordinal `{"type":"CHAR","size":1,"characterSet":"ASCII"}`
* *WHEN* the adapter builds that ordinal's constant projection for the outer wrapper
* *THEN* the emitted expression SHALL be `CAST('X' AS CHAR(1) ASCII)`
* *AND* the same declared type SHALL be resolved through the shared type-derivation seam whichever path Exasol's request routes to, so the emitted type matches on the grouped, single-group, and row-scan paths alike
* *AND* the constant SHALL need no padding, because it is classified as a `Constant` projection rather than a group key, has no DataFusion-side counterpart expression, and is already exactly `n` characters wide

### Scenario: A CAST-to-CHAR item inside an Exasol-parsed wrapper declares a CHAR column

* *GIVEN* an aggregate `pushdown` request reaching any of the THREE Exasol-parsed wrapper paths — the qualified single-table fallback (for example two `COUNT(DISTINCT …)` items alongside a `CAST(<col> AS CHAR(20))` select item), the N-scan unaccelerated join wrapper carrying the same select item, or the grouped-merge scalar-over-aggregate wrapper carrying a `CAST(SUM(<col>) AS CHAR(20))` item
* *AND* whose `selectListDataTypes` declares that ordinal `{"type":"CHAR","size":20,...}`
* *WHEN* the wrapper's SELECT list is rendered
* *THEN* that item SHALL render a length-qualified `CHAR` CAST target, carrying the ` ASCII` suffix exactly when the node's own `dataType` declares `characterSet` `ASCII`
* *AND* it MUST NOT render `VARCHAR({size})`, which Exasol rejects as `Data type mismatch ... Expected CHAR(20)` and which also strips the value's blank padding, nor a bare length-less `VARCHAR` or `CHAR`, which Exasol's parser rejects outright
* *AND* all three wrapper paths SHALL be corrected by the single shared `render_cast_target` Exasol-dialect case, so no per-wrapper rendering rule is introduced
* *AND* a NESTED CHAR CAST — `CAST(CAST(SUM(<col>) AS CHAR(20) ASCII) AS CHAR(20) ASCII)` on the grouped-merge path — SHALL render `CHAR(20) ASCII` at BOTH levels, because the renderer recurses into itself
* *AND* Exasol SHALL perform the grouping and DISTINCT evaluation natively over the padded `CHAR` value in these wrappers, so no adapter-side padding is required on this path

### Scenario: A MIN or MAX over a CHAR-typed expression declares a CHAR partial column

* *GIVEN* a `group_by` `pushdown` request whose `selectList` carries `MIN(CAST(<col> AS CHAR(20)))` — an aggregate over an expression argument, not a bare column — with that ordinal's `selectListDataTypes` entry declaring `{"type":"CHAR","size":20}`
* *WHEN* the adapter builds the partial `EMITS` clause and the outer merge SELECT
* *THEN* the partial column SHALL be declared `"PARTIAL_min_0" CHAR(20)`, because the declared type is carried through verbatim for an expression-argument aggregate and MIN/MAX are not gated on a numeric type
* *AND* the outer merge item SHALL cast the merged value to `CHAR(20)`
* *AND* the pushdown MUST NOT be declined for this shape, because `CHAR(n)` is both a valid `EMITS` output type and a valid `CAST` target

### Scenario: A VARCHAR-declared type is unaffected

* *GIVEN* a `dataType` JSON object `{"type":"VARCHAR","size":10,"characterSet":"UTF8"}`, the shape Exasol sends for a GROUP BY on a genuine VARCHAR base column
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `VARCHAR(10)`, unchanged by the new CHAR branch
* *AND* the `VARCHAR(2000000)` default SHALL remain the fallback when no declared type is locatable for an ordinal
* *AND* the `boolean`, `decimal`, `double`, `date`, and `timestamp` branches SHALL keep their current renderings
* *AND* a VARCHAR-declared group key SHALL receive no blank padding at all, because VARCHAR carries no fixed-width equality semantics

### Scenario: A CHAR-typed LIKE subject keeps pushing down unchanged

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` over a bare `column` whose type in `involvedTables[0].columns` resolves to `CHAR(n)`
* *WHEN* the LIKE subject type guard dispatches on that type (see `pushdown-types/pushdown-planning-like-type-coercion`)
* *THEN* the guard SHALL classify `CHAR(n)` as a string subject and leave the predicate unchanged
* *AND* the guard MUST NOT decline the filter, because a CHAR subject needs no coercion
* *AND* this SHALL be a forward-compatibility guard only, because no Iceberg or Arrow source type produces a CHAR base column today
