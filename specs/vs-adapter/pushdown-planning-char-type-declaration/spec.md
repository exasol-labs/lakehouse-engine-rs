# Feature: Pushdown Planning — CHAR Type Declaration

Declares a pushed-down result column as Exasol `CHAR(n)` whenever Exasol declared that ordinal
`CHAR` on the wire, so Exasol's type checker accepts the pushdown instead of rejecting it with
`Data type mismatch ... Expected CHAR(n), but got VARCHAR(n)` (#192), and makes a `CHAR`-declared
GROUP BY key group on the space-padded value so a pushed-down grouping merges exactly the rows
native Exasol merges. Before this feature the adapter rendered every string-family declared type as
`VARCHAR(n)` and never emitted a genuine `CHAR` type, which broke three real query shapes: a GROUP
BY on an equal-length CASE-of-string-literals bucketing expression, an explicit
`CAST(<col> AS CHAR(n))` select-list item, and a bare string literal used as a GROUP BY key. This
feature specializes the type derivation shared by `vs-adapter/pushdown-planning`,
`vs-adapter/pushdown-planning-grouped-agg`, `vs-adapter/pushdown-planning-empty-result`, and
`vs-adapter/pushdown-planning-join-fallback`. It changes the externally declared Exasol type, and —
only where a `CHAR`-declared expression sits in a grouping-equality position — the DataFusion-side
value expression, where a value shorter than the declared width is blank-padded to it so grouping
equality matches Exasol's. A value at or above the declared width is left unmodified, so Exasol's own
over-length truncation error still surfaces instead of being masked by the pad.

## Background

* `exasol_type_from_json` (`crates/lakehouse-engine/src/adapter/pushdown/support.rs`) is the single seam that maps an Exasol `dataType` JSON object to the Exasol type string used in the pushdown response — both the query-side `EMITS` clause and the outer-wrapper `CAST` targets. It has **8 non-test call sites**: `support.rs`'s `extract_all_column_types`, `project_columns`, and `aggregate_exasol_types`; `grouped_agg.rs`'s `constant_projection_sql`, `detect_group_by_aggregates`, and `group_key_exasol_types`; `joins/planning.rs`'s `involved_table_columns`; and `file_resolution.rs`'s `empty_select_list_typed_sql`. Two of the eight are inert for `CHAR`: `extract_all_column_types` and `involved_table_columns` both read `involvedTables[].columns`, which can never carry `CHAR` (see the next bullet).
* No Iceberg or Arrow source type maps to Exasol `CHAR` — Iceberg `string` maps to `VARCHAR` per this crate's type table. A `CHAR` type therefore reaches the adapter only as an Exasol-computed expression result in `selectListDataTypes`, never as an `involvedTables[].columns` base-column type (`crates/lakehouse-engine/tests/e2e_count_distinct_test.rs:511`).
* Exasol declares a string-literal expression `CHAR`, not `VARCHAR`, when every branch yields the same length. Verified live on Exasol 2025.2.1: `CASE WHEN c_acctbal<0 THEN 'NEG' ELSE 'POS' END` → `CHAR(3) ASCII`, while `CASE WHEN id>10 THEN 'high' ELSE 'low' END` (lengths 4 and 3) → `VARCHAR(4) ASCII`. A one-character difference in a literal flips the declared type, which is why the existing `'high'`/`'low'` E2E projection test passes while #192's `'NEG'`/`'POS'` shape fails.
* Exasol's `CHAR` maximum length is 2,000 characters. Verified live: `CAST('a' AS CHAR(2001))` fails with `specified length too long for char type - maximum is 2000`. VARCHAR's 2,000,000 cap is therefore not reusable for the CHAR branch.
* `CHAR(n)` and `CHAR(n) ASCII` are valid dynamic UDF `EMITS` output types, and Exasol space-pads a shorter emitted value into a `CHAR(n)` output column. Verified live with a LUA probe script: emitting the 15-character `25-989-741-2988` into `EMITS (P CHAR(20))` yields `25-989-741-2988     `, matching native `CAST(<col> AS CHAR(20))`.
* `CAST(<expr> AS CHAR(n) ASCII)` is valid Exasol CAST syntax (verified live), which the grouped-aggregate outer wrapper needs for `CAST("GK_i" AS <declared type>)`, and which the qualified single-table and N-scan join wrappers need for a `CAST(… AS CHAR(n))` select item.
* The character-set suffix rule is the one already established for VARCHAR (issue #136 follow-up): append ` ASCII` when the `dataType` JSON's `characterSet` equals `ASCII` case-insensitively. A `UTF8` or absent `characterSet` renders no suffix, which Exasol reads as its UTF8 default.
* The raw-scan emit path already tolerates a `CHAR(n)` declared type. `coerce_batch_to_exa_types` (`crates/lakehouse-engine/src/scan/emit.rs`) routes any type for which `exasol_type_to_arrow` returns `None` — explicitly "VARCHAR / CHAR" — to the `Utf8` string path, so a `CHAR(n)` entry in `emit_exa_types` feeds the strict `emit_batch` Arrow IPC path a `Utf8` column and needs no change.
* **THREE Exasol-parsed wrapper paths derive their select-list column types from `vs-expression`, not from `exasol_type_from_json`**: the N-scan unaccelerated join wrapper (`joins/sql_builders.rs`'s `n_scan_join_select_items`, the recorded `vs-adapter/pushdown-planning-join-fallback` behavior); the qualified single-table aggregate fallback (`joins/sql_builders.rs`'s `build_qualified_single_table_fallback_sql`, which serves undecomposable grouped shapes and multi/mixed `COUNT(DISTINCT)`); and the grouped-merge scalar-over-aggregate wrapper (`grouped_agg.rs`'s `render_scalar_over_merge`, reached from `build_grouped_aggregate_scan_sql`'s `ScalarOverAggregate` arm, which renders shapes such as `CAST(SUM(x) AS CHAR(20))` over the merged partials). The first two build their SELECT list via `render_selectlist_item_qualified` → `render_expression_exasol_safe`; the third calls `render_expression_exasol` directly. All three land on `render_cast_target` in the Exasol dialect, whose character arm rendered a `CHAR` target as `VARCHAR({size})`. Those wrappers are therefore fixed at the `vs-expression` seam, not at the adapter seam — one shared arm fixes all three — see the `sql-comprehension/vs-expression-translator-scalar-ops` delta in this plan. The broadcast join path is unaffected: it resolves its EMITS types through `project_columns` (`joins/mod.rs`).
* **`spec.common.group_keys` is the only DataFusion-side GROUPING-equality position a `CHAR` declared type can reach.** This claim is scoped to grouping equality specifically. Equality inside a pushed-down FILTER predicate over a `CHAR`-typed CAST is a separate, pre-existing divergence that this feature does not touch and does not cover: the DataFusion dialect renders such a CAST as a bare `VARCHAR`, so a filter comparison runs on the unpadded value. That path is unchanged by this feature and is out of scope here. `spec.common.group_keys` is populated at exactly one site (`adapter/pushdown/mod.rs`, the grouped-aggregate arm); every other `ScanSpec` construction sets it `None`. The `COUNT(DISTINCT)` fan-out (`build_distinct_fan_out`) is reachable only for a lone **bare-column** argument — `is_lone_count_distinct` requires `dc.column.is_some()`, and an expression argument declines to the qualified wrapper — so its `"V"` column always carries a base-column type, never `CHAR`. `constant_projection_sql` renders an Exasol-side outer-wrapper expression only and has no DataFusion-side counterpart; a bare literal is classified `Constant`, not a group key, and is already exactly `n` characters wide. All THREE Exasol-parsed wrapper paths let Exasol perform the grouping natively over the padded `CHAR` value, so they are correct by construction once their CAST target renders `CHAR(n)`.
* The grouped-aggregate merge groups on the **unpadded staging string**, not on the declared `CHAR` value. The inner `EMITS` always declares `"GK_i" VARCHAR(2000000)` and the outer wrapper is `GROUP BY "GK_0"` over that raw staging column, with `CAST("GK_0" AS <declared type>)` applied only in the SELECT list (`grouped_agg.rs`'s `build_grouped_aggregate_scan_sql`). Declaring a group key `CHAR(n)` without padding the DataFusion-side expression would therefore split source values that differ only in trailing whitespace into separate output rows that then render identically — a silently wrong answer where the pre-fix code produced a clean type-checker rejection.
* **Exasol does not truncate an over-length value into a `CHAR(n)` — it raises an error.** Verified live on Exasol 2025.2.1: `CAST('abcdefghij' AS CHAR(3))` fails with `data exception - string data, right truncation; Valuelength: 10 Maxlength: 3` (SQL state 22001). The DataFusion-side pad must therefore NOT truncate either. A truncating pad would silently shorten the value, leave the outer `CAST("GK_0" AS CHAR(n))` a no-op on an already-`n`-character input, and return a wrongly-merged group where native Exasol fails outright — the same clean-rejection-becomes-silent-wrong-answer failure this feature exists to prevent, reintroduced from the other direction.
* **The pad is `CASE WHEN character_length(<expr>) < n THEN rpad(<expr>, n) ELSE <expr> END`.** Every candidate was executed against the pinned DataFusion 54.1 rather than reasoned about. Bare `rpad(<expr>, n)` is NOT usable: it truncates (`rpad('abcdefghij', 3)` → `'abc'`; `unicode/rpad.rs` does `builder.append_value(&string[..target_len])` when `target_len <= str_len`, and its own doc states "If the input string is longer than this length, it is truncated"). `concat(<expr>, repeat(' ', greatest(n - character_length(<expr>), 0)))` is also NOT usable: DataFusion's `concat` skips NULL arguments, so a NULL group key measured as `''` rather than NULL and would merge with a genuine all-blanks group. The `CASE`-guarded form pads a SHORT value to exactly `n` with trailing spaces and leaves a value of `n` characters or more UNCHANGED. Measured for `n = 5`: NULL → NULL, `'ab'` → `'ab   '`, `'abc'` → `'abc  '`, `'abcdefghij'` → `'abcdefghij'` (unmodified).
* Three further properties of the chosen pad were measured, not assumed: a NULL `WHEN` condition falls through to `ELSE`, so a NULL group key stays NULL instead of becoming a run of spaces; `character_length` and `rpad` both count CHARACTERS rather than bytes (`'äö'` padded to 4 → `'äö  '`), matching Exasol's character-based `CHAR(n)`; and the expression parses and evaluates when `<expr>` is itself a `CASE` expression — the #192 primary shape — spliced into all three of its positions.
* An over-length value on the **projection** facet also fails rather than truncating: Exasol enforces the declared CHAR width at UDF emit. Verified with a LUA probe — emitting a 10-character value into `EMITS (P CHAR(5))` failed with `Lua Error "string too long"` (SQL state 22001), while a 3-character value padded cleanly to `'abc  '`. That the Rust SLC's `emit_batch` Arrow IPC path behaves the same is asserted by an E2E scenario below, not assumed.
* The unpadded rendered group-key SQL remains the identity key for group-key matching. `detect_group_by_aggregates` matches each select item to a group-key slot by rendered-SQL equality, and `build_grouped_order_by_clause` matches each `orderBy` element's freshly rendered (unpadded) SQL against the same list. Padding that shared list in place would make every `ORDER BY` on a `CHAR` group key unresolvable and decline the pushdown, so the padded form must be a separate list used only for `ScanSpec.common.group_keys`.
* The UDF preserves the padding. `value_to_gk_string` (`crates/lakehouse-engine/src/scan/partial_agg.rs`) passes string values through unchanged into the `GK_i VARCHAR(2000000)` column, and `build_grouped_partial_agg_sql` splices the same group-key fragment verbatim into both the DataFusion `SELECT` list and its `GROUP BY`, so one padded fragment covers both.
* An aggregate CAN carry a `CHAR` declared type. `col_type_for` (`grouped_agg.rs`) returns the declared type verbatim for an aggregate whose argument is an expression rather than a bare column, and `validate_agg_col_types` gates only SUM and the STDDEV/VARIANCE family on a numeric type — "MIN/MAX are valid over any comparable type". So `MIN(CAST(<col> AS CHAR(20)))` on the grouped path declares `PARTIAL_min_i CHAR(20)`, which is valid because `CHAR(n)` is a valid `EMITS` type and a valid `CAST` target.
* **Known, scoped exception: an unprojected group key whose CHAR type comes from an expression with no usable top-level `dataType` is shipped unpadded (#293).** `group_key_exasol_types` resolves an unprojected group key's declared type from `groupBy[slot]["dataType"]`. That resolves a bare CAST-to-CHAR node and an equal-length CASE-of-string-literals node (both carry a matching top-level `dataType`), but not a CASE whose *branches* are themselves CAST-to-CHAR expressions — Exasol still declares that overall expression `CHAR(n)`, but the `groupBy` node for it carries no top-level `dataType` the adapter can read positionally. Verified live and via a throwaway dispatcher test: `GROUP BY CASE WHEN id<0 THEN CAST(val AS CHAR(3)) ELSE 'abc' END`, unprojected, is pushed down unpadded. This affects only the unprojected sub-case of this narrower shape — every scenario in this spec, including the equal-length CASE-of-literals and bare CAST-to-CHAR keys, is unaffected and pads correctly whether projected or not. Deliberately not defensively patched: the candidate fallbacks (recursive `dataType` search, declining the pushdown, re-implementing Exasol's CHAR/VARCHAR type-unification lattice) each introduce a worse regression than the gap they'd close. Tracked in #293.

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

### Scenario: An explicit CAST-to-CHAR select-list item projects with a CHAR EMITS type

* *GIVEN* a row-scan `pushdown` request whose `selectList` carries a bare `column` item and a `function_scalar_cast` item targeting `CHAR(20)`
* *AND* whose parallel `selectListDataTypes` declares that second ordinal `{"type":"CHAR","size":20}`
* *WHEN* the adapter resolves the projection and its positionally-aligned EMITS types
* *THEN* the resolved EMITS type at that ordinal SHALL be `CHAR(20)`
* *AND* the projection SHALL remain a rendered expression item (`_LH_PROJ_<i>`), not a full-base-row fallback, because `CHAR(n)` is a valid EMITS output type
* *AND* the DataFusion-side value expression SHALL stay an unpadded `VARCHAR` cast, because Arrow has no CHAR type and a projection ordinal carries no grouping semantics
* *AND* for every value no longer than the declared width, the pushed-down result SHALL equal native `CAST(<col> AS CHAR(20))`, because Exasol space-pads each shorter emitted value to 20 characters (live-verified: a 15-character value emitted into `EMITS (P CHAR(20))` came back padded to 20)
* *AND* for a value LONGER than the declared width, the statement SHALL fail cleanly rather than return a silently truncated value, because Exasol enforces the declared CHAR width at emit — a 10-character value emitted into `EMITS (P CHAR(5))` failed with SQL state 22001 in a LUA probe. This is the same error class native Exasol raises for `CAST(<over-length> AS CHAR(5))` (also 22001), though the message text and origin differ: the pushed-down failure originates at the UDF emit, the native failure at the engine's cast. The equivalence that MUST hold is "clean failure, never a silently truncated value"; the E2E scenario below asserts it on the Rust SLC's `emit_batch` path, and any divergence found there MUST be recorded as a cited tracked exception rather than left silent

### Scenario: An equal-length CASE group key resolves to a CHAR group-key type

* *GIVEN* a `group_by` `pushdown` request whose `groupBy` holds a `function_scalar_case` over same-length string literals and whose `selectList` repeats that expression alongside an aggregate
* *AND* whose `selectListDataTypes` declares the group-key ordinal `{"type":"CHAR","size":3,"characterSet":"ASCII"}`
* *WHEN* the adapter resolves the group-key Exasol types
* *THEN* the group-key slot's type SHALL be `CHAR(3) ASCII`
* *AND* the outer merge wrapper SHALL cast the stringified staging column as `CAST("GK_0" AS CHAR(3) ASCII)`, which is valid Exasol CAST syntax
* *AND* the emitted group-key column type SHALL match what Exasol validates positionally against `selectListDataTypes`, so the pushdown is accepted rather than rejected as a `VARCHAR(3) ASCII` mismatch

### Scenario: A CHAR group key is blank-padded to its declared width before grouping

* *GIVEN* a `group_by` `pushdown` request whose group-key ordinal's `selectListDataTypes` entry resolves to `CHAR(20)`, over a source expression that can yield values of differing length
* *WHEN* the adapter builds the shard `ScanSpec`
* *THEN* that key's `ScanSpec.common.group_keys` fragment SHALL pad a value SHORTER than 20 characters to exactly 20 with trailing spaces, rendered as `CASE WHEN character_length(<unpadded fragment>) < 20 THEN rpad(<unpadded fragment>, 20) ELSE <unpadded fragment> END`
* *AND* the pad MUST NOT truncate: a value of 20 or more characters SHALL pass through UNMODIFIED, so it reaches Exasol's own `CAST("GK_i" AS CHAR(20))` intact and Exasol's over-length truncation error still fires
* *AND* the pad SHALL render a bare `rpad(<fragment>, 20)` NOWHERE, because `rpad` truncates an over-length input
* *AND* the outer wrapper SHALL keep emitting `CAST("GK_i" AS CHAR(20))` for that ordinal, which is a no-op re-cast of an already-20-character staging value and remains the declaration Exasol validates positionally

### Scenario: The CHAR group-key pad preserves NULL, parses the declared width, and leaves the identity list unpadded

* *GIVEN* a `group_by` `pushdown` request carrying a `CHAR`-declared group key
* *WHEN* the adapter derives the padded group-key fragment
* *THEN* a NULL group-key value SHALL stay NULL rather than becoming a run of `n` spaces, so a NULL group is not merged with an all-blanks group
* *AND* the declared width SHALL be parsed correctly from BOTH the `CHAR(20)` and the `CHAR(3) ASCII` forms — by reading the digits between `(` and `)` rather than trimming a trailing `)` — so an ASCII-declared CHAR key, the #192 primary shape, is padded rather than silently skipped
* *AND* the padding SHALL apply to every group-key slot whose declared type is `CHAR(n)` **and which is itself a select-list ordinal**, and to no slot whose declared type is anything else; a group key not projected as its own select-list ordinal has no `selectListDataTypes` entry to resolve, keeps the `VARCHAR(2000000)` default, and is unaffected, which is pre-existing behavior and out of scope here
* *AND* the group-key list used for select-item and `ORDER BY` slot matching SHALL remain the UNPADDED rendered SQL, so an `ORDER BY` on a `CHAR` group key still resolves to its output ordinal instead of declining the pushdown

### Scenario: An over-length CHAR group-key value raises Exasol's truncation error rather than merging a truncated group

* *GIVEN* a seeded Virtual Schema table holding a string value LONGER than the width a `CAST(<col> AS CHAR(n))` group key declares — for example a 25-character value under `CHAR(20)`
* *WHEN* `SELECT CAST(<col> AS CHAR(20)) g, COUNT(*) FROM <vs>.<table> GROUP BY 1` is executed through the Virtual Schema
* *THEN* the statement SHALL fail with Exasol's own truncation error — SQL state 22001, `data exception - string data, right truncation` — raised by Exasol's evaluation of `CAST("GK_0" AS CHAR(20))` on the merge side
* *AND* the statement MUST NOT return rows, because a returned result set would mean the pad silently truncated the over-length value into a wrong, merged group
* *AND* the failure SHALL match what native Exasol raises for the same statement over the same values, so pushing the shape down neither introduces nor suppresses an error
* *AND* the pad MUST NOT suppress this error, which is why it leaves values at or above the declared width unmodified instead of shortening them

### Scenario: An over-length CHAR projection value fails cleanly rather than truncating

* *GIVEN* the same seeded table holding a value longer than the declared CHAR width
* *WHEN* `SELECT CAST(<col> AS CHAR(20)) FROM <vs>.<table>` is executed through the Virtual Schema — the projection facet, with no GROUP BY, where the width is enforced at UDF emit rather than by an outer cast
* *THEN* the statement SHALL fail cleanly with a truncation error rather than return a silently truncated 20-character value
* *AND* this SHALL confirm on the Rust SLC's `emit_batch` Arrow IPC path what the LUA probe established for the classic emit path
* *AND* if the Rust path is found to truncate instead of erroring, that divergence MUST be recorded as an explicit, cited tracked exception in this spec rather than left as a silent gap

### Scenario: A CHAR group key over trailing-space data groups identically to native Exasol

* *GIVEN* a seeded Virtual Schema table holding two source rows whose string column differs only in trailing whitespace — `'ab'` and `'ab   '` — plus at least one unrelated value
* *WHEN* `SELECT CAST(<col> AS CHAR(30)) g, COUNT(*) FROM <vs>.<table> GROUP BY 1` is executed through the Virtual Schema, at a declared width that every seeded value fits so this scenario isolates merge behavior from truncation behavior
* *THEN* the two trailing-whitespace-only variants SHALL merge into exactly ONE output row whose count is 2
* *AND* the pushed-down result SHALL equal the result Exasol computes natively over the same values, row for row
* *AND* the adapter MUST NOT return two separate rows that render identically as `'ab'` followed by 28 spaces, which is what an unpadded `CHAR`-declared group key would produce

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
* *WHEN* the LIKE subject type guard dispatches on that type (see `vs-adapter/pushdown-planning-like-type-coercion`)
* *THEN* the guard SHALL classify `CHAR(n)` as a string subject and leave the predicate unchanged
* *AND* the guard MUST NOT decline the filter, because a CHAR subject needs no coercion
* *AND* this SHALL be a forward-compatibility guard only, because no Iceberg or Arrow source type produces a CHAR base column today

### Scenario: The four #192 query shapes execute end to end

* *GIVEN* the local Exasol, MinIO, and Iceberg REST Docker stack with the seeded Virtual Schema table
* *WHEN* each of the four shapes is executed through the Virtual Schema — a GROUP BY on an equal-length CASE-of-string-literals expression, a select list containing `CAST(<varchar col> AS CHAR(20))`, a bare string literal used as a GROUP BY key, and a GROUP BY on a genuine VARCHAR column as the control
* *THEN* each statement SHALL return rows rather than fail with `Data type mismatch ... Expected CHAR(n)`
* *AND* the `CAST(<varchar col> AS CHAR(20))` shape SHALL return values space-padded to exactly 20 characters
* *AND* the VARCHAR control SHALL keep returning its current result, proving the CHAR branch did not disturb the VARCHAR path
