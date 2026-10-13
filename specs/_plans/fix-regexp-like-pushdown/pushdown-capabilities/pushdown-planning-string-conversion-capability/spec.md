<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — String Function and Type-Conversion Capability Extensions

Extends pushdown planning (`pushdown/pushdown-planning`) with `FN_CAST`/`FN_NEG` and
`FN_SUBSTR`/`FN_LEFT` capability advertisements, plus the deliberately absent regular-expression
functions and `REGEXP_LIKE` predicate. Split from
`pushdown-capabilities/pushdown-planning-capability-extensions`.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* A scalar-function capability is advertised only once a `crates/vs-expression` arm renders
  it and the DataFusion result matches Exasol. `FN_CAST`, `FN_NEG`, `FN_SUBSTR`, and
  `FN_LEFT` meet this bar. The regular-expression functions and the `REGEXP_LIKE` predicate do
  not, because DataFusion's regex matching cannot reproduce Exasol's PCRE matching (see
  `sql-functions/vs-expression-translator-scalar-fns`), so they stay unadvertised.
* Iceberg spec compliance: checked, not engaged. This feature changes only which scalar capabilities the adapter advertises, touching no manifest, snapshot, field-id, delete, or type mapping. No normative requirement applies.
* Exasol delegates an advertised capability fully and never re-applies it. A capability the
  scan UDF cannot plan fails the query with no fallback (issue #187).
* Exasol never delegates an unadvertised predicate, and it applies that predicate itself over
  the rows the scan returns. The other conjuncts of an `AND` and the projection still push down.
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

<!-- DELTA:NEW -->
### Scenario: Regular-expression capabilities are not advertised

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_PRED_REGEXP_LIKE`, `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`
* *AND* Exasol SHALL evaluate `REGEXP_LIKE` and the regular-expression scalar functions itself (issue #106, (#TBD))
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: REGEXP_LIKE returns the rows native Exasol returns

* *GIVEN* a virtual-schema table with a string column
* *WHEN* a query filters that column with `REGEXP_LIKE` or `NOT (... REGEXP_LIKE ...)`, using a pattern that matches only part of a value, a pattern whose first match from the start of a value ends before the value's end (such as `'ARGENTINA' REGEXP_LIKE '(A|AR)(G|GENTINA)?'`), or a look-around pattern
* *THEN* the query SHALL return the same rows as native Exasol evaluation of the same predicate over the same values
* *AND* a look-around pattern SHALL NOT fail the query
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A REGEXP_LIKE conjunct stays in Exasol while its sibling conjunct is pushed

* *GIVEN* a query whose `WHERE` clause is a pushable comparison `AND` a `REGEXP_LIKE` predicate over the same virtual-schema table
* *WHEN* a user runs `EXPLAIN VIRTUAL` on the query
* *THEN* the pushdown request's filter SHALL contain the comparison and SHALL NOT contain the `REGEXP_LIKE` predicate
* *AND* the query SHALL return only the rows that satisfy both conjuncts
<!-- /DELTA:NEW -->
