# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators, CAST, and the safe/fallback entry points. Named math/string/conditional scalar functions are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in `sql-comprehension/vs-expression-translator-date-fns`.

## Background

* This delta changes only the CAST target-type rendering for TIMESTAMP; every other scalar-operation scenario in the recorded feature is unchanged.
* `render_cast_target` renders a CAST target for two dialects: `Dialect::DataFusion` (the node-local scan SQL DataFusion 54 parses) and `Dialect::Exasol` (the wrapper SQL Exasol parses directly). The two parsers have different precision constraints for TIMESTAMP.
* Exasol serialises a TIMESTAMP dataType as `{"type":"TIMESTAMP","withLocalTimeZone":<bool>,"fractionalSecondsPrecision":<0-9>}`; both optional (defaults `false` and `3`). `fractionalSecondsPrecision` — not `precision` — is the fractional-seconds field.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CAST translates to DataFusion CAST syntax

* *GIVEN* a VS expression node of type `function_scalar_cast` with `name` equal to `CAST` — the top-level node type Exasol's engine serializer emits for CAST (verified against the Exasol engine source; `function_scalar`+`name=CAST` is retained only as a defensive nested/alternate encoding, not the primary wire shape)
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"CHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)` where `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name, rendering `DECIMAL(p,s)` as `DECIMAL(p,s)`; `VARCHAR` and `CHAR` as `VARCHAR`; `DOUBLE` as `DOUBLE`; `BOOLEAN` as `BOOLEAN`; `DATE` as `DATE`; and `TIMESTAMP` as `TIMESTAMP` or `TIMESTAMP(p)` per the fractional-seconds-precision rule in the scenario below
* *AND* a `dataType` whose `type` is an Exasol target with no faithful DataFusion mapping — `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE` — SHALL return an error in raising mode and `None` in the safe variants, so the adapter omits the CAST and Exasol evaluates it as a correctness backstop
* *AND* the set of CAST target types the translator renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result, so `FN_CAST` (advertised per `vs-adapter/pushdown-planning-capability-extensions`) is never advertised for a target the translator would render divergently
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect

* *GIVEN* a `function_scalar_cast` node whose `dataType` is `{"type":"TIMESTAMP", ...}` carrying an OPTIONAL `fractionalSecondsPrecision` integer in 0-9 and an OPTIONAL `withLocalTimeZone` flag
* *AND* the precision field name is `fractionalSecondsPrecision` — Exasol's documented data-type field for a TIMESTAMP's fractional-seconds precision, verified against Exasol's virtual-schema data-type API and the reference pushdown fixture `pushdown_request_alltypes.json` (`C_TIMESTAMP_4` = `{"type":"TIMESTAMP","fractionalSecondsPrecision":7}`); it is NOT `precision`, which Exasol uses only for `DECIMAL` (with `scale`) and `INTERVAL` (with `fraction`)
* *WHEN* `render_expression` (DataFusion dialect) or `render_expression_exasol` (Exasol dialect) processes the node
* *THEN* a `withLocalTimeZone: true` dataType SHALL be declined (`Err` in raising mode, `None` in the safe variants) BEFORE any precision handling runs, unchanged by this scenario, because DataFusion's plain TIMESTAMP cannot reproduce its session-timezone / UTC-normalisation semantics
* *AND* when `fractionalSecondsPrecision` is absent the translator SHALL render bare `TIMESTAMP` in BOTH dialects, preserving the pre-change rendering, since bare `TIMESTAMP` equals Exasol's default `TIMESTAMP(3)`
* *AND* when `fractionalSecondsPrecision` is present the Exasol dialect SHALL render `TIMESTAMP(p)` with the declared precision verbatim for every `p` in 0-9, because Exasol's parser accepts all of them
* *AND* when `fractionalSecondsPrecision` is present the DataFusion dialect SHALL render `TIMESTAMP(p')` where `p'` is the unique nearest value in `{0,3,6,9}` (`0→0`, `1→0`, `2→3`, `4→3`, `5→6`, `7→6`, `8→9`; identity on `0/3/6/9`; a precision above 9 clamps to 9), because DataFusion 54's SQL frontend parses `CAST(x AS TIMESTAMP(p))` only for `p` in `{0,3,6,9}` (Second/Millisecond/Microsecond/Nanosecond) and rejects 1, 2, 4, 5, 7, 8 with a parse error — the gaps 0-3, 3-6, 6-9 have non-integer midpoints 1.5, 4.5, 7.5, so no integer precision is equidistant
* *AND* an UP-snap (`2→3`, `5→6`, `8→9`) produces a finer DataFusion timestamp that the EMITS-declared Exasol column `TIMESTAMP(p)` (see `vs-adapter/pushdown-planning`) SHALL truncate back to the requested `p`, keeping the round-trip faithful, whereas the single DOWN-snap `1→0` SHALL be accepted as a named precision trade-off for the exotic `TIMESTAMP(1)` cast — recorded rather than left silent, because the Iceberg source stores microsecond-precision timestamps (Apache Iceberg table spec, Primitive Types: "`timestamp` — Timestamp, microsecond precision, without timezone") and DataFusion 54 cannot parse `TIMESTAMP(1)`
<!-- /DELTA:NEW -->
