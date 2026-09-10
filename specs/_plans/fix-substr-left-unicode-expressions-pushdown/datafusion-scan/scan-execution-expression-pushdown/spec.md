# Feature: DataFusion Scan Execution — Expression Pushdown

Extends `datafusion-scan/scan-execution` with expression pushdown execution capabilities.

## Background

<!-- DELTA:NEW -->
* `sqlparser` turns `SUBSTR(...)` and `SUBSTRING(...)` into the `Substring` AST node, which resolves only through a registered `ExprPlanner`, not the scalar-function registry.
* `UnicodeFunctionPlanner` is the sole planner for `Substring`, registered only when `unicode_expressions` is enabled (issue #187).
* Plain calls (`left(...)`, `right(...)`, `character_length(...)`, `strpos(...)`) resolve through the scalar-function registry and are unaffected.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Scan plans a rendered SUBSTR fragment in select-list and filter positions

* *GIVEN* a scan spec whose projection carries `substr("NAME", 1, 5)` and whose filter carries `substr("NAME", 1, 5) = '<literal>'`
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL plan both fragments and emit the evaluated substring value for matching rows
* *AND* the UDF SHALL NOT fail with `Substring could not be planned by registered expr planner`
* *AND* a rendered `left(...)` fragment in the same select list SHALL plan and evaluate, unchanged by this delta
<!-- /DELTA:NEW -->
