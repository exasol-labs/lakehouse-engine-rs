# Feature: VS Expression Translator — CAST

Renders a `function_scalar_cast` node to `CAST(<expr> AS <target>)` per dialect, and refuses every
Exasol target type whose DataFusion result would diverge from Exasol's.

## Background

* This delta corrects what a refused CAST target MEANS for the caller. The refusal itself is
  unchanged — an error in raising mode, `None` in the safe variants, for exactly the targets whose
  DataFusion result would diverge. What is corrected is the claim that the adapter can therefore
  omit the CAST and let Exasol evaluate it: `FN_CAST` IS advertised, so Exasol delegates the CAST and
  re-applies nothing. See `vs-adapter/pushdown-declined-filter-self-apply`.
* These five targets are refused in BOTH dialects, so a WHERE predicate carrying one cannot be
  self-applied either. That predicate is the terminal case: a clean client-facing error, never a
  result computed without it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CAST renders the mapped target type per dialect

* *GIVEN* a VS expression node of type `function_scalar_cast` with `name` equal to `CAST` — the top-level node type Exasol's engine serializer emits for CAST (`function_scalar`+`name=CAST` is retained only as a defensive nested/alternate encoding, not the primary wire shape)
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"CHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)` where `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name, rendering `DECIMAL(p,s)` as `DECIMAL(p,s)`; `VARCHAR` and `CHAR` as `VARCHAR`; `DOUBLE` as `DOUBLE`; `BOOLEAN` as `BOOLEAN`; `DATE` as `DATE`; and `TIMESTAMP` as `TIMESTAMP` or `TIMESTAMP(p)` per the fractional-seconds-precision rule
* *AND* a `dataType` whose `type` is an Exasol target with no faithful DataFusion mapping — `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE` — SHALL return an error in raising mode and `None` in the safe variants, in BOTH dialects
* *AND* such a refusal in a WHERE predicate SHALL cause the adapter to return a clean client-facing error, because the predicate can be applied neither by DataFusion nor by the adapter's own Exasol-dialect wrapper — REPLACING the recorded "so the adapter omits the CAST and Exasol evaluates it as a correctness backstop", which assumed an Exasol-side re-check of a delegated `FN_CAST` that does not occur
* *AND* the adapter MUST NOT omit a refused CAST from a WHERE predicate and return rows, because the omitted predicate would be evaluated by nobody
* *AND* the set of CAST target types the translator renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result, so `FN_CAST` is never advertised for a target the translator would render divergently
<!-- /DELTA:CHANGED -->
