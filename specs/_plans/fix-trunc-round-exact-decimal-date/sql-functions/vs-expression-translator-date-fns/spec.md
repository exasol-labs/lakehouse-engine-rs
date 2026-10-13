# Feature: VS Expression Translator — Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the
Exasol date/time scalar functions that DataFusion 54 can evaluate, so date-valued filters,
select-list expressions, and group keys push down to the node-local DataFusion scan instead of
being post-processed in Exasol. Kept as a separate feature so the scalar-ops spec stays focused on
arithmetic, string, and conditional functions. The date-difference (`*_BETWEEN`) family and the
divergent date-arithmetic functions issue #107 examined but declined to translate are covered by
the sibling feature `sql-functions/vs-expression-translator-date-diff-fns`.

## Background

* This feature shares the six public entry points of `crates/vs-expression` — the DataFusion trio
  (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) and the Exasol trio
  (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`);
  the date functions are additional arms inside the same recursive walker.

* **Exasol-dialect rendering is verbatim.** The Exasol dialect exists because the rendered fragment
  becomes wrapper SQL text parsed and evaluated by Exasol's own core engine, not by DataFusion.
  Every date/time function in this feature is an Exasol function that Exasol's compiler itself
  emitted, so the Exasol dialect renders the original name, the original argument order, and the
  original argument count — no name mapping, no re-shaping. Every date/time `function_scalar` name
  this feature translates is declared `VerbatimCall` in the crate's one declaration of translated
  names (see `sql-comprehension/vs-expression-translator`). A declared name's Exasol rendering is
  produced by the declaration's own
  branch, ahead of every per-name arm, so no per-name arm can reach it and it cannot diverge from the
  name Exasol sent. `EXTRACT` is the one exception in this feature: it is the separate node type
  `function_scalar_extract`, so it branches on dialect inside its own arm and is held in place by its
  own sweep row instead. Only the DataFusion dialect needs the `date_part`/epoch-arithmetic
  translations below (issue #209). Verified on live Exasol 2025.2.1 (the image pinned in
  `docker-compose.yml`):
  `DATE_PART` is not an Exasol function
  (`function or script DATE_PART not found`, SQL code 42000), so every `date_part`-based rendering
  is a hard compilation error in Exasol-dialect wrapper SQL.
* The dialect split is a rendering-time concern for every function below except the now-family.
  Every other function named below stays advertised, because the DataFusion dialect still governs
  what the node-local scan can evaluate, and each takes its datetime from its own arguments.
* **The now-family is withdrawn from pushdown instead of re-rendered.** `CURRENT_DATE`, `SYSDATE`,
  `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` are no longer translated in either dialect, and
  `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` are no longer
  advertised (see `pushdown-capabilities/pushdown-planning-capability-extensions`). Renaming them would not
  have made them correct. Exasol's four names are three semantics over one instant:
  `CURRENT_TIMESTAMP` reads it in the session time zone, `SYSTIMESTAMP` reads the same instant in the
  database time zone, and `CURRENT_DATE`/`SYSDATE` are `TO_DATE` of each. The scan UDF receives
  neither zone, opens no connect-back session, and gets no statement anchor, so it can only read its
  own container clock in UTC — once per shard, G times per statement, while Exasol's now-family is
  statement-constant. Exasol never delegates an unadvertised capability, so withdrawal makes all four
  correct by handing them back to the engine that owns the clock.

* Exasol sends EXTRACT as node type `function_scalar_extract` with the field in a `toExtract`
  property (not as `function_scalar` + `dateTimeField`). DataFusion 54 renders EXTRACT via
  `date_part('<FIELD>', expr)` — not `EXTRACT(<FIELD> FROM expr)` — because DataFusion 54's default
  feature set does not include an EXTRACT ExprPlanner. Both forms are semantically equivalent;
  the DataFusion dialect uses `date_part` to match DataFusion's execution path, while the Exasol
  dialect renders Exasol's own `EXTRACT(<FIELD> FROM expr)` form.
* This delta SUPERSEDES the preceding Background bullet "Only date/time functions whose DataFusion semantics match Exasol's are translated. Functions that depend on Exasol session state or whose DataFusion equivalent diverges in result are left unsupported (the node returns an error in raising mode, `None` in the safe variants), so the adapter omits them and Exasol post-processes them as a correctness backstop. This parity gate governs the DataFusion dialect, which decides what the node-local scan evaluates. It does not govern the Exasol dialect, which renders the name Exasol sent and so has nothing to reach parity with." The parity gate itself is unchanged and still correct. The claim that the adapter can omit an unsupported node and let Exasol post-process it holds ONLY while the corresponding capability is unadvertised. Once the capability IS advertised, the caller must apply the node itself — see ADR `specs/_decision/045` for why, `pushdown-capabilities/pushdown-planning-capability-extensions` for the safe direction of that trade, and `pushdown/pushdown-declined-filter-self-apply` for what a caller does with a delegated node it cannot render.
* An ARITY the DataFusion dialect refuses is the same case as a name it refuses, and it is reachable under an advertised capability: the Exasol dialect renders a declared verbatim-call name at any arity, while the DataFusion dialect checks each name's arity in its own arm. A pushed call whose arity no DataFusion arm accepts therefore declines in the DataFusion dialect and renders in the Exasol dialect, which is exactly the shape the caller self-applies.
* `WEEK` is translated because Exasol `WEEK` and DataFusion 54 `date_part('week', …)` are both
  ISO-8601. The Exasol dialect renders `WEEK(…)` and inherits Exasol's own week numbering directly.
* The date-difference (`*_BETWEEN`) family and the divergent date-arithmetic functions issue #107
  examined but declined to translate are specified in the sibling feature
  `sql-functions/vs-expression-translator-date-diff-fns`, split out to keep this feature's
  scenario count within the domain's convention.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: DATE_TRUNC translates to the DataFusion date_trunc call

* *GIVEN* a VS expression node of type `function_scalar` named `DATE_TRUNC` with a precision/unit literal argument and a source datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_trunc(<unit_sql>, <source_sql>)` with both arguments rendered recursively
* *AND* the unit literal MUST be passed through as a string argument DataFusion's `date_trunc` accepts (e.g. `'year'`, `'month'`, `'day'`, `'hour'`)
* *AND* `render_expression_exasol` SHALL return `DATE_TRUNC(<unit_sql>, <source_sql>)` — Exasol's own PostgreSQL-compatible `DATE_TRUNC` takes the same argument order, so the unit literal Exasol sent is forwarded unchanged and Exasol applies its own `NLS_FIRST_DAY_OF_WEEK` for the `'week'` unit
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: DATE_TRUNC renders as a call to the scan's Exasol DATE_TRUNC function in the DataFusion dialect

* *GIVEN* a VS expression node of type `function_scalar` named `DATE_TRUNC` with a unit argument and a source datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* a unit that is a `literal_string` from the accepted list (`microseconds`, `milliseconds`, `second`, `minute`, `hour`, `day`, `month`, `quarter`, `year`, `decade`, `century`, `millennium`), in any letter case, SHALL render as `exa_date_trunc('<unit>', <source_sql>)` with the unit in lower case and the source rendered recursively, reading the function name from the constant `crates/vs-expression` exports
* *AND* the translator MUST NOT render DataFusion's own `date_trunc`, because it has no `decade`, `century`, or `millennium` unit and its values are specified in `sql-functions/vs-expression-translator-trunc-round` instead
* *AND* a `week` unit, a unit spelling outside the accepted list, and a non-literal unit SHALL return `None` in the safe variants and an error naming `DATE_TRUNC` in raising mode, because `week` follows the session's `NLS_FIRST_DAY_OF_WEEK`, which the scan never receives, and a non-literal unit cannot be checked before the scan runs
* *AND* `render_expression_exasol` SHALL return `DATE_TRUNC(<unit_sql>, <source_sql>)` byte-identical to the rendering before this change, so Exasol's own PostgreSQL-compatible `DATE_TRUNC` evaluates every unit Exasol sent, applying its own `NLS_FIRST_DAY_OF_WEEK` for the `week` unit
<!-- /DELTA:NEW -->
