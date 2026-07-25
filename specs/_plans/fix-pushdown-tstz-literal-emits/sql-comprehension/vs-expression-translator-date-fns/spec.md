# Feature: vs-expression Translator — Date Functions

Translates Exasol Virtual Schema date/time `function_scalar` nodes into rendered SQL
fragments for the scan-side DataFusion dialect and the wrapper-side Exasol dialect.

## Background

* The translator has two dialects (`Dialect::DataFusion`, `Dialect::Exasol`). The Exasol
  dialect exists because the qualified single-table wrapper and the N-scan join wrapper
  splice rendered fragments into SQL that Exasol's OWN core engine parses.
* The now-family arm is currently dialect-INSENSITIVE: it renders DataFusion syntax in
  both dialects. That is wrong on both sides of the seam, and this delta fixes the
  Exasol side.
* Wrapper side: `vs-adapter/pushdown-planning-capability-extensions` now routes a
  session-context-dependent projected item to the qualified wrapper so Exasol evaluates
  it in the caller's session. That only works if the Exasol dialect renders the ORIGINAL
  Exasol function, not `now()`.
* Scan side: the DataFusion rendering ships a UTC instant where Exasol would compute the
  session-local wall clock. For a projected item that is now fixed by the routing above
  (`(#238)`). For a pushed-down FILTER predicate it is NOT fixed here — a filter is
  executed inside DataFusion by design, so there is no Exasol-side seam. That remains a
  deliberate, accurately-scoped tracked exception, `(#239)`.
* The projected defect is captured, not inferred. The live request for
  `SELECT SYSTIMESTAMP FROM <vs_table> WHERE ID = 1` carries select-list node
  `{"name":"SYSTIMESTAMP","type":"function_scalar"}` with `selectListDataTypes`
  `{"type":"TIMESTAMP","fractionalSecondsPrecision":3}` — plain TIMESTAMP, NOT
  with-local-time-zone — and the adapter pushes `"projection":[{"expr":"now()"}]` emitting
  `("_LH_PROJ_0" TIMESTAMP)`. So the UDF computes the UTC instant and Exasol surfaces it
  verbatim: the declared-type gate alone cannot detect this item, which is why the routing
  classifier needs a session-context-dependence test (`(#238)`).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CURRENT_DATE and CURRENT_TIMESTAMP translate per dialect

* *GIVEN* a VS expression node of type `function_scalar` named `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `SYSDATE`, or `SYSTIMESTAMP` with no datetime-dependent arguments
* *WHEN* `render_expression` processes the node in the DataFusion dialect
* *THEN* `CURRENT_DATE`/`SYSDATE` SHALL render as `current_date()` and `CURRENT_TIMESTAMP`/`SYSTIMESTAMP` SHALL render as `now()`
* *AND* the translator MUST NOT depend on any Exasol session state to render these nodes
* *WHEN* `render_expression` processes the node in the Exasol dialect
* *THEN* each name SHALL render as ITSELF — `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `SYSDATE`, `SYSTIMESTAMP` — so Exasol's core engine evaluates it in the CALLER's session and the fragment carries that name's exact Exasol result type
* *AND* the Exasol-dialect rendering MUST NOT emit `now()` or `current_date()`, because those resolve to different values than the Exasol function they replace and, in `now()`'s case, silently parse as Exasol's own `NOW()` rather than failing
<!-- /DELTA:CHANGED -->
