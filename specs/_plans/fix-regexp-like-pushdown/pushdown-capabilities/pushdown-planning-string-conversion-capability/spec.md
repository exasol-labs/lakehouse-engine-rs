<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — String Function and Type-Conversion Capability Extensions

Extends pushdown planning (`pushdown/pushdown-planning`) with `FN_CAST`/`FN_NEG` and
`FN_SUBSTR`/`FN_LEFT` capability advertisements. Split from
`pushdown-capabilities/pushdown-planning-capability-extensions`.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* A scalar-function capability is advertised only once a `crates/vs-expression` arm renders
  it and the DataFusion result matches Exasol. `FN_CAST`, `FN_NEG`, `FN_SUBSTR`, and
  `FN_LEFT` meet this bar.
* Iceberg spec compliance: checked, not engaged. This feature changes only which scalar capabilities the adapter advertises, touching no manifest, snapshot, field-id, delete, or type mapping. No normative requirement applies.
* Exasol delegates an advertised capability fully and never re-applies it. A capability the
  scan UDF cannot plan fails the query with no fallback (issue #187).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Regexp scalar function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`
* *AND* Exasol SHALL post-process regexp scalar functions because no faithful translation exists at DataFusion 54 / `regex` 1.12 (PCRE dialect mismatch, missing `regexp_substr`, argument-shape gaps — see issue #106 and `sql-functions/vs-expression-translator-scalar-fns`)
* *AND* the pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement SHALL remain unchanged
<!-- /DELTA:REMOVED -->
