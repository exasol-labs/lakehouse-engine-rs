# Feature: VS Expression Translator — CAST

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/sql-comprehension/vs-expression-translator-cast/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/sql-comprehension/vs-expression-translator-cast/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CAST renders the mapped target type per dialect

* *GIVEN* a VS expression node of type `function_scalar_cast` with `name` equal to `CAST` — the top-level node type Exasol's engine serializer emits for CAST (`function_scalar`+`name=CAST` is retained only as a defensive nested/alternate encoding, not the primary wire shape)
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"CHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)`, where `<expr>` is the rendered source, converted for a `VARCHAR` or `CHAR` target per `sql-comprehension/vs-expression-translator-string-conversion` (which also owns the boolean-source exception), and `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name, rendering `DECIMAL(p,s)` as `DECIMAL(p,s)`; both `VARCHAR` and `CHAR` as a bare, length-less `VARCHAR` — a DataFusion-dialect-specific rendering, because datafusion-sql rejects a length-qualified character target without `support_varchar_with_length` and Arrow has only `Utf8`, with no CHAR type for a fixed-width target to map to (the Exasol dialect diverges — see the CHAR scenario below); `DOUBLE` as `DOUBLE`; `BOOLEAN` as `BOOLEAN`; `DATE` as `DATE`; and `TIMESTAMP` as `TIMESTAMP` or `TIMESTAMP(p)` per the fractional-seconds-precision rule
* *AND* a `dataType` whose `type` is an Exasol target with no faithful DataFusion mapping — `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE` — SHALL return an error in raising mode and `None` in the safe variants, in BOTH dialects
* *AND* such a refusal in a WHERE predicate SHALL cause the adapter to return a clean client-facing error, because the predicate can be applied neither by DataFusion nor by the adapter's own Exasol-dialect wrapper — REPLACING the recorded "so the adapter omits the CAST and Exasol evaluates it as a correctness backstop", which assumed an Exasol-side re-check of a delegated `FN_CAST` that does not occur
* *AND* the adapter MUST NOT omit a refused CAST from a WHERE predicate and return rows, because the omitted predicate would be evaluated by nobody
* *AND* the set of CAST target types the translator renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result, so `FN_CAST` is never advertised for a target the translator would render divergently
<!-- /DELTA:CHANGED -->
