# Feature: VS Expression Translator, Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the Exasol date and time scalar functions that DataFusion 54 can evaluate, so date-valued filters, select-list expressions and group keys run in the node-local scan. `sql-functions/vs-expression-translator-date-diff-fns` covers the `*_BETWEEN` family and the date-arithmetic functions that stay unsupported.

## Background

* The date functions are arms of the same recursive walker behind the six public entry points of `crates/vs-expression`: `render_expression`, `render_expression_safe` and `render_df_filter_safe` for DataFusion, and `render_expression_exasol`, `render_expression_exasol_safe` and `render_df_filter_exasol_safe` for Exasol.
* The Exasol dialect renders wrapper SQL text that Exasol's own engine parses. It renders the original name, argument order and argument count of every date or time function, because Exasol's compiler emitted them. Each such name is declared `VerbatimCall` in the crate's declaration of translated names (see `sql-comprehension/vs-expression-translator`), and the declaration's own branch renders it ahead of every per-name arm. `EXTRACT` is the exception: it is the node type `function_scalar_extract` and branches on dialect inside its own arm. Only the DataFusion dialect uses the `date_part` and epoch-arithmetic translations. `DATE_PART` is not an Exasol function (`function or script DATE_PART not found`, SQL code 42000), so a `date_part` rendering in Exasol-dialect wrapper SQL fails to compile.
* The now-family `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP` and `SYSTIMESTAMP` is translated in neither dialect, and `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE` and `FN_SYSTIMESTAMP` are not advertised: see `pushdown-capabilities/pushdown-planning-capability-extensions`. Exasol reads the instant in the session time zone (`CURRENT_TIMESTAMP`) or the database time zone (`SYSTIMESTAMP`), and `CURRENT_DATE` and `SYSDATE` are `TO_DATE` of each. The scan UDF receives neither zone and has no statement anchor, so it can only read its container clock in UTC, once per shard. Exasol's now-family is statement-constant. Exasol never delegates an unadvertised capability, so withdrawal hands the four names back to the engine that owns the clock.
* Exasol sends EXTRACT as node type `function_scalar_extract` with the field in a `toExtract` property. The DataFusion dialect renders `date_part('<FIELD>', expr)`, because DataFusion 54's default feature set has no EXTRACT planner. The Exasol dialect renders `EXTRACT(<FIELD> FROM expr)`.
* The DataFusion dialect translates only functions whose DataFusion semantics match Exasol's. A node it refuses returns an error in raising mode and `None` in the safe variants. An omitted node is post-processed by Exasol only while its capability is unadvertised. For an advertised capability the caller applies the node itself: see `pushdown-capabilities/pushdown-planning-capability-extensions` and `pushdown/pushdown-declined-filter-self-apply`.
* A refused arity is the same case as a refused name. The Exasol dialect renders a declared verbatim-call name at any arity, while the DataFusion dialect checks arity in each name's arm. A call with an arity no DataFusion arm accepts declines in the DataFusion dialect and renders in the Exasol dialect, and the caller self-applies it.
* `WEEK` is translated because Exasol `WEEK` and DataFusion 54 `date_part('week', …)` are both ISO-8601. The Exasol dialect renders `WEEK(…)` and inherits Exasol's week numbering.

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
* *AND* `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` MUST NOT be advertised, so Exasol never pushes a now-family node and evaluates its own clock instead (see `pushdown-capabilities/pushdown-planning-capability-extensions`)
* *AND* the reason SHALL be the scan's missing clock context, not a rendering defect: the scan UDF receives neither `SESSIONTIMEZONE` nor `DBTIMEZONE`, holds no statement anchor, and reads its container clock in UTC once per shard, so no rendering of these four names on the scan path can match Exasol

### Scenario: TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls

* *GIVEN* a VS expression node of type `function_scalar` named `TO_DATE` or `TO_TIMESTAMP` with a string source argument and an optional format argument
* *WHEN* `render_expression` processes the node
* *THEN* with one argument `TO_DATE` SHALL render as `to_date(<source_sql>)` and `TO_TIMESTAMP` SHALL render as `to_timestamp(<source_sql>)`
* *AND* with a format argument the DataFusion dialect SHALL translate the format or decline the node, as `sql-functions/vs-expression-translator-datetime-format` specifies, and MUST NOT forward an Exasol format model to a DataFusion parser
* *AND* `render_expression_exasol` SHALL render `TO_DATE(<args>)` / `TO_TIMESTAMP(<args>)`, forwarding the format argument unchanged, so Exasol interprets the format model it sent

### Scenario: WEEK translates to the DataFusion date_part('week') ISO-8601 call

* *GIVEN* a VS expression node of type `function_scalar` named `WEEK` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('week', <arg_sql>)` with the argument rendered recursively
* *AND* the rendered call SHALL yield the ISO-8601 week number (1-53, weeks beginning Monday, week 1 containing the year's first Thursday) matching Exasol `WEEK`, including at year boundaries
* *AND* `render_expression_exasol` SHALL return `WEEK(<arg_sql>)`, so Exasol computes its own week number and the ISO-8601 equivalence above is not relied upon on that path
* *AND* `FN_WEEK` (advertised per `pushdown-capabilities/pushdown-planning-capability-extensions`) SHALL be advertised only while the DataFusion-dialect ISO-8601 parity holds; if a year-boundary case diverges, the DataFusion-dialect `WEEK` rendering SHALL be withdrawn and `FN_WEEK` left unadvertised
