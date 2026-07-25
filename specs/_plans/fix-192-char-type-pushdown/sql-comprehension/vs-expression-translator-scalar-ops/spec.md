# Feature: VS Expression Translator — Scalar Operators (DELTA)

Delta for `fix-192-char-type-pushdown`. Only the CAST scenario changes: the CAST-target renderer's
Exasol dialect arm stops mapping a `CHAR` target to `VARCHAR({size})` and renders `CHAR({size})`
instead, so a `CAST(<expr> AS CHAR(n))` select item inside an Exasol-parsed wrapper declares the
type Exasol validates against and keeps the value's blank padding (#192). The DataFusion dialect arm
is unchanged — Arrow has only `Utf8` and no CHAR type.

## Background

* `render_cast_target` (`crates/vs-expression/src/lib.rs`) has TWO dialect arms, threaded through the shared recursive translator by a private `Dialect` parameter (`specs/_decision/011-fix-count-distinct-shard-cap.md`, follow-up "Exasol-dialect CAST for the qualified wrapper"). The `DataFusion` arm feeds fragments embedded in a `ScanSpec` (`filter`/`projection`/`group_keys`) that datafusion-sql parses inside the scan UDF; the `Exasol` arm feeds wrapper SQL text that Exasol's own core engine parses.
* Both arms collapsed a `CHAR` target into `VARCHAR`: bare and length-less on the DataFusion side, `VARCHAR({size})` on the Exasol side. The Exasol-side collapse is the #192 defect on THREE wrapper paths: `joins/sql_builders.rs`'s `n_scan_join_select_items` (the N-scan unaccelerated join wrapper) and `build_qualified_single_table_fallback_sql` (the qualified single-table aggregate fallback, serving undecomposable grouped shapes and multi/mixed `COUNT(DISTINCT)`), both of which reach the renderer via `render_selectlist_item_qualified` → `render_expression_exasol_safe`; and `grouped_agg.rs`'s `render_scalar_over_merge` (the grouped-merge scalar-over-aggregate wrapper, reached from `build_grouped_aggregate_scan_sql`'s `ScalarOverAggregate` arm), which calls `render_expression_exasol` directly. Correcting the one shared Exasol-dialect arm fixes all three; each has its own existing test asserting the old collapsing behavior, so all three tests are retargeted.
* The DataFusion-side collapse is correct and stays: Arrow has only `Utf8` and no fixed-width CHAR type, and datafusion-sql rejects a length-qualified character target unless `support_varchar_with_length` is enabled, which this project does not enable.
* Exasol validates a wrapper's result column types positionally against `selectListDataTypes`, including character set. Rendering an ASCII-declared CHAR target as bare `CHAR({size})` would therefore trade a `VARCHAR(n) ASCII` mismatch for a `CHAR(n) UTF8` one, so the ` ASCII` suffix is required for correctness rather than cosmetic. `CAST(<expr> AS CHAR(n) ASCII)` is valid Exasol CAST syntax, verified live on Exasol 2025.2.1.
* The suffix rule mirrors the adapter's `exasol_type_from_json` CHAR rule so the two independent seams that declare a pushed-down column's type cannot disagree **on a CHAR target**. The claim is scoped to CHAR deliberately: on a VARCHAR target the two seams already differ in suffix handling — the adapter's VARCHAR arm appends ` ASCII` for an ASCII `characterSet`, while this crate's Exasol-dialect VARCHAR rendering emits `VARCHAR({size})` with no suffix. That asymmetry is pre-existing, untouched by this delta, and out of scope here; only the new CHAR case is held to cross-seam agreement.
* This crate is shared with a sibling VS-adapter project (`specs/mission.md`), so the change is a narrowly additive dialect case that leaves the `Dialect::DataFusion` behavior and the Exasol `VARCHAR` rendering byte-identical.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CAST translates to DataFusion CAST syntax

* *GIVEN* a VS expression node of type `function_scalar_cast` with `name` equal to `CAST` — the top-level node type Exasol's engine serializer emits for CAST (verified against the Exasol engine source; `function_scalar`+`name=CAST` is retained only as a defensive nested/alternate encoding, not the primary wire shape)
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"CHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)` where `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name, rendering `DECIMAL(p,s)` as `DECIMAL(p,s)`; both `VARCHAR` and `CHAR` as a bare, length-less `VARCHAR` — a DataFusion-dialect-specific rendering, because datafusion-sql rejects a length-qualified character target without `support_varchar_with_length` and Arrow has only `Utf8`, with no CHAR type for a fixed-width target to map to; `DOUBLE` as `DOUBLE`; `BOOLEAN` as `BOOLEAN`; `DATE` as `DATE`; `TIMESTAMP` as `TIMESTAMP`
* *AND* a `dataType` whose `type` is an Exasol target with no faithful DataFusion mapping — `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE` — SHALL return an error in raising mode and `None` in the safe variants, so the adapter omits the CAST and Exasol evaluates it as a correctness backstop
* *AND* the set of CAST target types the translator renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result, so `FN_CAST` (advertised per `vs-adapter/pushdown-planning-capability-extensions`) is never advertised for a target the translator would render divergently
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR

* *GIVEN* a `function_scalar_cast` node whose `dataType` is `{"type":"CHAR","size":20,"characterSet":"ASCII"}`
* *WHEN* the node is rendered through the Exasol-dialect entry points (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`) — the ones whose output Exasol's own core engine parses in the qualified single-table wrapper, the N-scan join wrapper, and the grouped-merge wrapper
* *THEN* the CAST target SHALL render as `CHAR(20) ASCII`, appending the ` ASCII` suffix exactly when `characterSet` equals `ASCII` case-insensitively and no suffix otherwise
* *AND* the target MUST NOT render as `VARCHAR({size})`, which Exasol rejects as `Data type mismatch ... Expected CHAR(20) ASCII` and which also strips the value's blank padding, nor as a bare length-less `CHAR` or `VARCHAR`, which Exasol's parser rejects outright (`sqlCode 04000`, "unexpected ')', expecting '('") — the regression this dialect split was introduced to fix
* *AND* the Exasol dialect SHALL keep rendering a `VARCHAR` target as `VARCHAR({size})` with no character-set suffix, unchanged
* *AND* the Exasol dialect SHALL keep trusting the `size` Exasol sent without clamping it, unchanged — the defensive 2,000 CHAR cap belongs to the adapter's `exasol_type_from_json`, the seam that synthesizes a declared type rather than echoing one Exasol just sent
* *AND* a NESTED CHAR CAST — `CAST(CAST(<agg> AS CHAR(20) ASCII) AS CHAR(20) ASCII)`, the shape the grouped-merge wrapper can produce — SHALL render `CHAR(20) ASCII` at BOTH levels, because the renderer recurses into itself and the CHAR case therefore applies at every level
* *AND* the two dialects SHALL still DIVERGE on the same CHAR node — bare `VARCHAR` in the DataFusion dialect, `CHAR({size})` plus any suffix in the Exasol dialect — so the existing divergence guard remains a guard, its Exasol-side expectation retargeted rather than removed
<!-- /DELTA:NEW -->
