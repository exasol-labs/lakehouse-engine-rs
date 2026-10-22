# Feature: VS Expression Translator, CAST

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with CAST target-type rendering.

## Background

* A CAST target the translator refuses (an error in raising mode, `None` in the safe variants) is one whose DataFusion result would diverge from Exasol's. `FN_CAST` is advertised, so no other component evaluates a refused CAST. The five refused targets are refused in both dialects, so a WHERE predicate carrying one cannot be self-applied either, and the result is a clean client-facing error (`pushdown/pushdown-declined-filter-self-apply`).
* Exasol emits CAST as the top-level node type `function_scalar_cast`. The translator also keeps a nested `function_scalar` with `name=CAST` arm for an alternate encoding.
* `render_expression` renders CAST for the DataFusion dialect (scan SQL parsed by DataFusion), and `render_expression_exasol` renders it for the Exasol dialect (wrapper SQL parsed by Exasol). The two parsers have different TIMESTAMP precision constraints.
* Exasol serializes a TIMESTAMP dataType as `{"type":"TIMESTAMP","withLocalTimeZone":<bool>,"fractionalSecondsPrecision":<0-9>}`. Both fields are optional, with defaults `false` and `3`.
* The two dialects diverge on a `CHAR` target. The DataFusion dialect renders a bare, length-less `VARCHAR`, because Arrow has only `Utf8` and datafusion-sql rejects a length-qualified character target. The Exasol dialect renders `CHAR({size})`, plus ` ASCII` when `dataType.characterSet` is `ASCII` case-insensitively, because Exasol validates the result positionally against `selectListDataTypes`.
* The DataFusion dialect never approximates a requested type: it renders the type or declines the node. A decline routes the node into SQL that the adapter writes in the Exasol dialect and Exasol executes. It is not an Exasol-side re-evaluation of a delegated node.
* The Exasol dialect renders `TIMESTAMP(p)` verbatim for every `p` in 0-9.

## Scenarios

### Scenario: CAST renders the mapped target type per dialect

* *GIVEN* a VS expression node of type `function_scalar_cast` with `name` equal to `CAST`, the top-level node type Exasol's engine serializer emits for CAST (`function_scalar`+`name=CAST` is retained only as a defensive nested/alternate encoding, not the primary wire shape)
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"CHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)` where `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name, rendering `DECIMAL(p,s)` as `DECIMAL(p,s)`; both `VARCHAR` and `CHAR` as a bare, length-less `VARCHAR`, a DataFusion-dialect-specific rendering, because datafusion-sql rejects a length-qualified character target without `support_varchar_with_length` and Arrow has only `Utf8`, with no CHAR type for a fixed-width target to map to (the Exasol dialect diverges, see the CHAR scenario below); `DOUBLE` as `DOUBLE`; `BOOLEAN` as `BOOLEAN`; `DATE` as `DATE`; and `TIMESTAMP` as `TIMESTAMP` or `TIMESTAMP(p)` per the fractional-seconds-precision rule
* *AND* a `dataType` whose `type` is an Exasol target with no faithful DataFusion mapping, `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE`, SHALL return an error in raising mode and `None` in the safe variants, in BOTH dialects
* *AND* such a refusal in a WHERE predicate SHALL cause the adapter to return a clean client-facing error, because the predicate can be applied neither by DataFusion nor by the adapter's own Exasol-dialect wrapper
* *AND* the adapter MUST NOT omit a refused CAST from a WHERE predicate and return rows, because the omitted predicate would be evaluated by nobody
* *AND* the set of CAST target types the translator renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result, so `FN_CAST` is never advertised for a target the translator would render divergently

### Scenario: CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect

* *GIVEN* a `function_scalar_cast` node whose `dataType` is `{"type":"TIMESTAMP", ...}` carrying an OPTIONAL `fractionalSecondsPrecision` integer in 0-9 and an OPTIONAL `withLocalTimeZone` flag
* *AND* the precision field name is `fractionalSecondsPrecision`, Exasol's documented data-type field for a TIMESTAMP's fractional-seconds precision, verified against Exasol's virtual-schema data-type API and the reference pushdown fixture `pushdown_request_alltypes.json` (`C_TIMESTAMP_4` = `{"type":"TIMESTAMP","fractionalSecondsPrecision":7}`); it is NOT `precision`, which Exasol uses only for `DECIMAL` (with `scale`) and `INTERVAL` (with `fraction`)
* *WHEN* `render_expression` (DataFusion dialect) or `render_expression_exasol` (Exasol dialect) processes the node
* *THEN* a `withLocalTimeZone: true` dataType SHALL be declined (`Err` in raising mode, `None` in the safe variants) BEFORE any precision handling runs, because DataFusion's plain TIMESTAMP cannot reproduce its session-timezone / UTC-normalisation semantics
* *AND* when `fractionalSecondsPrecision` is absent the translator SHALL render bare `TIMESTAMP` in BOTH dialects, since bare `TIMESTAMP` equals Exasol's default `TIMESTAMP(3)`
* *AND* when `fractionalSecondsPrecision` is present the Exasol dialect SHALL render `TIMESTAMP(p)` with the declared precision verbatim for every `p` in 0-9, because Exasol's parser accepts all of them
* *AND* when `fractionalSecondsPrecision` is present and `p` is in `{0, 3, 6, 9}` the DataFusion dialect SHALL render `TIMESTAMP(p)` VERBATIM, because the DataFusion SQL frontend parses exactly those four (Second/Millisecond/Microsecond/Nanosecond) and the rendered target then matches the requested one exactly
* *AND* when `p` is in `{1, 2, 4, 5, 7, 8}` the DataFusion dialect SHALL DECLINE the node (`Err` in raising mode, `None` in the safe variants), and MUST NOT render an approximated `TIMESTAMP(p')` for any `p'` in `{0, 3, 6, 9}`
* *AND* the DECLINE MUST NOT be read as Exasol independently re-evaluating the node: it routes the node into SQL the ADAPTER writes in the Exasol dialect, where the literal `p` renders verbatim and Exasol computes the value natively with no DataFusion or Arrow precision ceiling
* *AND* the declared EMITS type for such an item MUST still echo the literal `fractionalSecondsPrecision` Exasol sent, because Exasol validates a pushdown response positionally against `selectListDataTypes`; the decline changes which component COMPUTES the value, never which type is DECLARED
* *AND* the decline SHALL NOT produce a client-facing error for any of `{1, 2, 4, 5, 7, 8}`, unlike the five refused CAST TARGET TYPES of this feature's other scenario, because those are refused in BOTH dialects while this one renders in the Exasol dialect
* *AND* a declined select-list item SHALL widen the whole select list and route the whole request to the qualified single-table wrapper, so Exasol computes EVERY select-list item and the per-shard `LIMIT` and bounded top-N are given up, while the sharded fan-out, the referenced-column projection narrowing, and the scan-side WHERE predicate are retained

### Scenario: The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR

* *GIVEN* a `function_scalar_cast` node whose `dataType` is `{"type":"CHAR","size":20,"characterSet":"ASCII"}`
* *WHEN* the node is rendered through the Exasol-dialect entry points (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`), the ones whose output Exasol's own core engine parses in the qualified single-table wrapper, the N-scan join wrapper, and the grouped-merge wrapper
* *THEN* the CAST target SHALL render as `CHAR(20) ASCII`, appending the ` ASCII` suffix exactly when `characterSet` equals `ASCII` case-insensitively and no suffix otherwise
* *AND* the target MUST NOT render as `VARCHAR({size})`, which Exasol rejects as `Data type mismatch ... Expected CHAR(20) ASCII` and which also strips the value's blank padding, nor as a bare length-less `CHAR` or `VARCHAR`, which Exasol's parser rejects outright (`sqlCode 04000`, "unexpected ')', expecting '('")
* *AND* the Exasol dialect SHALL keep rendering a `VARCHAR` target as `VARCHAR({size})` with no character-set suffix
* *AND* the Exasol dialect SHALL keep trusting the `size` Exasol sent without clamping it: the defensive 2,000 CHAR cap belongs to the adapter's `exasol_type_from_json`, the seam that synthesizes a declared type rather than echoing one Exasol just sent
* *AND* a NESTED CHAR CAST, `CAST(CAST(<agg> AS CHAR(20) ASCII) AS CHAR(20) ASCII)`, the shape the grouped-merge wrapper can produce, SHALL render `CHAR(20) ASCII` at BOTH levels, because the renderer recurses into itself and the CHAR case therefore applies at every level
* *AND* the two dialects SHALL still DIVERGE on the same CHAR node, bare `VARCHAR` in the DataFusion dialect, `CHAR({size})` plus any suffix in the Exasol dialect, so the existing divergence guard remains a guard, its Exasol-side expectation retargeted rather than removed
