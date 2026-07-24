# Feature: Pushdown Planning — LIKE Type Coercion

Makes pushed-down `LIKE` and `REGEXP_LIKE` predicates type-aware in the single-table pushdown
path (see `vs-adapter/pushdown-planning`'s "Filter predicate is pushed into the scan spec"
scenario, which this feature specializes). A LIKE predicate's `column` subject never carries a
`dataType` on the wire, and DataFusion performs no implicit non-string-to-VARCHAR coercion the
way Exasol does — so a pushed-down LIKE over a non-string column previously hard-failed the
DataFusion scan at execution time. This feature dispatches on the column's Exasol type, read from
`involvedTables[0].columns`, before rendering the filter: string subjects pass through unchanged,
DATE subjects are rewrapped in an explicit CAST-to-VARCHAR, and every other non-string subject
declines pushdown of the whole top-level filter so Exasol evaluates it natively. This is a
single-table scan-spec-filter-path fix only; the broadcast-join per-leg filter path
(`crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`) and the SELECT-list/
projection path have the same latent gap and are tracked separately (issues #215 and #219).

## Background

* A LIKE predicate's `column` subject carries no `dataType` on the wire; column Exasol types are read from `involvedTables[0].columns`.
* The type dispatch and rewrite happen in the adapter (`pushdown/support.rs`), not in `vs-expression`, because `vs-expression` is a pure syntactic JSON-to-SQL translator with no external column-type context and is shared with a sibling VS-adapter project.
* The rewrite (or decline) applies ONLY to the JSON tree fed to the DataFusion filter renderer; the raw filter tree forwarded to Iceberg file pruning is left untouched.
* A non-string LIKE subject anywhere in the filter tree declines the WHOLE top-level filter (not just the offending conjunct), mirroring the existing all-or-nothing untranslatable-predicate correctness backstop — Exasol then evaluates the entire predicate natively.
* The DATE CAST-to-VARCHAR is Exasol-faithful only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`); a session with an altered NLS date format is an accepted, tracked exception (#216), not a silent gap.

## Scenarios

### Scenario: LIKE on a VARCHAR or CHAR column pushes down unchanged

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the predicate subject unchanged, rendering `(<column> LIKE <pattern>)` exactly as before this change
* *AND* the rendered filter SHALL be carried in the common spec, because a string subject needs no coercion

### Scenario: LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL rewrap the subject as an explicit CAST-to-VARCHAR node (`function_scalar_cast` with target `{"type":"VARCHAR"}`) before rendering, so the emitted DataFusion SQL casts the DATE column to text and the LIKE matches against the `YYYY-MM-DD` string form
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to the DataFusion filter renderer, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR cast under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 date text form both engines render for the Iceberg `date` primitive (calendar date without timezone, days from 1970-01-01)
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from the `YYYY-MM-DD` default, the pushed-down match MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format for the adapter to reproduce — an accepted, accurately-scoped tracked exception (#216), not a silent gap

### Scenario: LIKE on a DECIMAL column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DECIMAL(p,s)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (emit no `filter` in the common spec), so Exasol evaluates the entire predicate natively
* *AND* the adapter SHALL NOT inject a CAST for the DECIMAL subject, because DataFusion's decimal-to-string formatting keeps trailing scale zeros that diverge from Exasol's trimmed formatting and would silently change which rows match — correct trimmed-decimal formatting is tracked in issue #211
* *AND* the returned scan-driving SQL SHALL remain valid with the filter omitted, exercising the existing untranslatable-predicate correctness backstop

### Scenario: LIKE on an integer column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column is an Exasol integer, carried on the wire as `DECIMAL(p,0)` (Exasol has no distinct integer type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter exactly as for any other `DECIMAL(p,s)` subject, so Exasol evaluates the predicate natively
* *AND* the adapter SHALL NOT inject a CAST for the integer subject

### Scenario: LIKE on a non-column subject is left untouched

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is NOT a bare `column` node (for example a computed scalar expression)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the node unchanged, applying neither the CAST rewrite nor the decline, because the subject's type is not resolvable from `involvedTables[0].columns`
* *AND* the adapter SHALL preserve the pre-existing behavior for non-column LIKE subjects, which remains outside this change's scope

### Scenario: LIKE on a bare column whose type cannot be resolved declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's name is NOT found in `involvedTables[0].columns` (a lookup miss — no resolvable Exasol type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (fail-safe), because it cannot prove the subject is a string and a non-string subject would hard-fail the DataFusion scan
* *AND* the name lookup SHALL be case-normalized by uppercasing the subject column name before matching, mirroring `extract_all_column_types`'s existing uppercasing of column names (`support.rs:411`), so a case-mismatched name resolves rather than spuriously declining

### Scenario: A nested non-string LIKE declines the entire enclosing filter

* *GIVEN* a `pushdown` request whose filter nests a non-string-column `predicate_like` inside a `predicate_and`, `predicate_or`, or `predicate_not`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter, not only the offending conjunct, mirroring the all-or-nothing untranslatable-predicate backstop
* *AND* a DATE-column LIKE nested in the same connectives SHALL instead be rewritten in place to its CAST-to-VARCHAR form while the surrounding tree is preserved
