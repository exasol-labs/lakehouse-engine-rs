# Feature: VS Expression Translator — Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the
Exasol date/time scalar functions that DataFusion 54 can evaluate, so date-valued filters,
select-list expressions, and group keys push down to the node-local DataFusion scan instead of
being post-processed in Exasol. Kept as a separate feature so the scalar-ops spec stays focused on
arithmetic, string, and conditional functions. The date-difference (`*_BETWEEN`) family and the
divergent date-arithmetic functions issue #107 examined but declined to translate are covered by
the sibling feature `sql-comprehension/vs-expression-translator-date-diff-fns`.

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
  advertised (see `vs-adapter/pushdown-planning-capability-extensions`). Renaming them would not
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
* This delta SUPERSEDES the preceding Background bullet "Only date/time functions whose DataFusion semantics match Exasol's are translated. Functions that depend on Exasol session state or whose DataFusion equivalent diverges in result are left unsupported (the node returns an error in raising mode, `None` in the safe variants), so the adapter omits them and Exasol post-processes them as a correctness backstop. This parity gate governs the DataFusion dialect, which decides what the node-local scan evaluates. It does not govern the Exasol dialect, which renders the name Exasol sent and so has nothing to reach parity with." The parity gate itself is unchanged and still correct. The claim that the adapter can omit an unsupported node and let Exasol post-process it holds ONLY while the corresponding capability is unadvertised — an unadvertised function is never delegated, so Exasol keeps it. Once the capability IS advertised, Exasol delegates the node and re-applies nothing; the caller must then apply it itself. See `vs-adapter/pushdown-planning-capability-extensions` for the safe direction of that trade and `vs-adapter/pushdown-declined-filter-self-apply` for what a caller does with a delegated node it cannot render.
* An ARITY the DataFusion dialect refuses is the same case as a name it refuses, and it is reachable under an advertised capability: the Exasol dialect renders a declared verbatim-call name at any arity, while the DataFusion dialect checks each name's arity in its own arm. A pushed call whose arity no DataFusion arm accepts therefore declines in the DataFusion dialect and renders in the Exasol dialect, which is exactly the shape the caller self-applies.
* `WEEK` is translated because Exasol `WEEK` and DataFusion 54 `date_part('week', …)` are both
  ISO-8601. The Exasol dialect renders `WEEK(…)` and inherits Exasol's own week numbering directly.
* The date-difference (`*_BETWEEN`) family and the divergent date-arithmetic functions issue #107
  examined but declined to translate are specified in the sibling feature
  `sql-comprehension/vs-expression-translator-date-diff-fns`, split out to keep this feature's
  scenario count within the domain's convention.

## Scenarios

### Scenario: A refused argument count declines for DataFusion and renders for Exasol

* *GIVEN* a `function_scalar` node whose name is a declared verbatim-call date/time function and whose argument count exceeds what the DataFusion dialect's per-name arm accepts — for example `SECOND(<datetime>, <precision>)`, whose Exasol signature takes an optional precision and whose DataFusion arm accepts exactly one argument
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error in raising mode and `None` in the safe variants, because no DataFusion arm expresses that call
* *AND* the Exasol dialect SHALL render the call verbatim — the name, argument order, and argument count Exasol sent — because Exasol's own compiler emitted it
* *AND* the caller SHALL treat that asymmetry as a decline to self-apply, not as an omission, because the function's capability is advertised and Exasol therefore delegated the call

### Scenario: EXTRACT translates to the DataFusion date_part call

* *GIVEN* a VS expression node of type `function_scalar_extract` carrying a `toExtract` field with a datetime field name (e.g. `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`) and an `arguments` array with a single source datetime expression
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <source_sql>)` with the field name single-quoted and the source rendered recursively
* *AND* the rendered field name MUST be one DataFusion's `date_part` function recognises

### Scenario: EXTRACT renders Exasol's EXTRACT FROM form in the Exasol dialect

* *GIVEN* the same `function_scalar_extract` node as the preceding scenario
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `EXTRACT(<FIELD> FROM <source_sql>)` with the field name rendered as a bare keyword — NOT single-quoted — and the source rendered recursively in the Exasol dialect
* *AND* the rendered fragment MUST NOT contain `date_part`, because Exasol has no `DATE_PART` function and the fragment is parsed by Exasol's own core engine (`function or script DATE_PART not found`, SQL code 42000)
* *AND* the DataFusion-dialect rendering of the same node MUST remain byte-identical to the preceding scenario

### Scenario: Field-shortcut date functions translate to date_part of the matching field

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `SECOND` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <arg_sql>)` where `<FIELD>` is the function's own name, single-quoted
* *AND* the argument SHALL be rendered recursively

### Scenario: Field-shortcut date functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `SECOND` with a single datetime argument
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg_sql>)` using the function's own uppercased Exasol name, with the argument rendered recursively in the Exasol dialect
* *AND* the rendered fragment MUST NOT contain `date_part`
* *AND* the DataFusion-dialect rendering of the same node MUST remain byte-identical to the preceding scenario

### Scenario: DATE_TRUNC translates to the DataFusion date_trunc call

* *GIVEN* a VS expression node of type `function_scalar` named `DATE_TRUNC` with a precision/unit literal argument and a source datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_trunc(<unit_sql>, <source_sql>)` with both arguments rendered recursively
* *AND* the unit literal MUST be passed through as a string argument DataFusion's `date_trunc` accepts (e.g. `'year'`, `'month'`, `'day'`, `'hour'`)
* *AND* `render_expression_exasol` SHALL return `DATE_TRUNC(<unit_sql>, <source_sql>)` — Exasol's own PostgreSQL-compatible `DATE_TRUNC` takes the same argument order, so the unit literal Exasol sent is forwarded unchanged and Exasol applies its own `NLS_FIRST_DAY_OF_WEEK` for the `'week'` unit

### Scenario: The now-family is not translated in either dialect

* *GIVEN* a VS expression node of type `function_scalar` named `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, or `SYSTIMESTAMP`
* *WHEN* `render_expression` or `render_expression_exasol` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the unsupported function, identically in both dialects
* *AND* `render_expression_safe` and `render_expression_exasol_safe` SHALL each return `None` for the same node without panicking
* *AND* the four names MUST NOT appear in the crate's declaration of translated `function_scalar` names, so the gate declines them before any per-name arm is reached and no per-name arm for them remains reachable
* *AND* `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` MUST NOT be advertised, so Exasol never pushes a now-family node and evaluates its own clock instead (see `vs-adapter/pushdown-planning-capability-extensions`)
* *AND* the reason SHALL be the scan's missing clock context, not a rendering defect: the scan UDF receives neither `SESSIONTIMEZONE` nor `DBTIMEZONE`, holds no statement anchor, and reads its container clock in UTC once per shard, so no rendering of these four names on the scan path can match Exasol

### Scenario: TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls

* *GIVEN* a VS expression node of type `function_scalar` named `TO_DATE` or `TO_TIMESTAMP` with a string source argument and an optional format argument
* *WHEN* `render_expression` processes the node
* *THEN* `TO_DATE` SHALL render as `to_date(<args>)` and `TO_TIMESTAMP` SHALL render as `to_timestamp(<args>)` over the recursively rendered arguments
* *AND* when a format argument is present it SHALL be forwarded as the corresponding DataFusion format argument
* *AND* `render_expression_exasol` SHALL render `TO_DATE(<args>)` / `TO_TIMESTAMP(<args>)`, forwarding the format argument unchanged, so Exasol interprets the format model it sent rather than the translator forwarding an Exasol format string to a DataFusion parser

### Scenario: WEEK translates to the DataFusion date_part('week') ISO-8601 call

* *GIVEN* a VS expression node of type `function_scalar` named `WEEK` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('week', <arg_sql>)` with the argument rendered recursively
* *AND* the rendered call SHALL yield the ISO-8601 week number (1-53, weeks beginning Monday, week 1 containing the year's first Thursday) matching Exasol `WEEK`, including at year boundaries
* *AND* `render_expression_exasol` SHALL return `WEEK(<arg_sql>)`, so Exasol computes its own week number and the ISO-8601 equivalence above is not relied upon on that path
* *AND* `FN_WEEK` (advertised per `vs-adapter/pushdown-planning-capability-extensions`) SHALL be advertised only while the DataFusion-dialect ISO-8601 parity holds; if a year-boundary case diverges, the DataFusion-dialect `WEEK` rendering SHALL be withdrawn and `FN_WEEK` left unadvertised
