# Feature: Pushdown Planning — LIKE Type Coercion

Makes pushed-down `LIKE` and `REGEXP_LIKE` predicates type-aware on the render surfaces that carry a
pushed expression tree, dispatching on the subject column's Exasol type before rendering.

## Background

* This feature's opening enumeration of "both render surfaces that carry a pushed expression tree:
  the single-table WHERE-clause filter and the select-list projection" is REPLACED by FOUR surfaces.
  `vs-adapter/pushdown-planning-join-filter-type-coercion` adds the broadcast join's combined WHERE
  filter and the N-scan fallback's per-leg WHERE filter (issue #215). This feature keeps ownership of
  the subject TYPE DISPATCH itself — pass through a string subject, rewrap a DATE subject as
  CAST-to-VARCHAR, decline everything else — unchanged and shared verbatim by all four surfaces; the
  join feature owns only which column-type universe each join surface screens against and what a
  decline means there.
* The subject dispatch, its traversal, and every clause of every scenario below are unchanged by that
  extension. A reader looking for what a decline MEANS at a join surface is directed to the join
  feature; a reader looking for what triggers one stays here.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a VARCHAR or CHAR column pushes down unchanged

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in the involved-table column metadata of the table that owns the column is `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the predicate subject unchanged, rendering `(<column> LIKE <pattern>)` exactly as before this change
* *AND* the rendered filter SHALL be carried in the common spec, because a string subject needs no coercion
* *AND* this pass-through SHALL hold identically at all FOUR surfaces that now run the dispatch — the single-table WHERE filter, the select-list projection, the broadcast join's combined WHERE filter, and the N-scan fallback's per-leg WHERE filter — REPLACING this feature's recorded enumeration of "both render surfaces", which named only the first two (see `vs-adapter/pushdown-planning-join-filter-type-coercion`, issue #215)
<!-- /DELTA:CHANGED -->
