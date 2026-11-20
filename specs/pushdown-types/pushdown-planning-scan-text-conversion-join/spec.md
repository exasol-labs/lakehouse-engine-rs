# Feature: Pushdown Planning, Scan Text Conversion in Joins

Applies the scan text conversion rule (`pushdown-types/pushdown-planning-scan-text-conversion`) to a join's filters and join keys. A filter the scan declines runs in Exasol over the broadcast fan-out, and a join key the scan declines keeps its pair off the broadcast.

## Background

* Which expressions the scan computes, and the fallback of each path for an expression it declines, are defined in `pushdown-types/pushdown-planning-scan-text-conversion`.

## Scenarios

### Scenario: A broadcast join keeps its plan for an integer LIKE

* *GIVEN* a broadcast-eligible inner equi-join whose WHERE filter holds `LIKE` over an integer column of one leg
* *WHEN* the adapter renders the join
* *THEN* the join SHALL keep the broadcast plan, and the scan SHALL apply the filter
* *AND* the rows SHALL equal the same filter applied to that leg alone, then joined

### Scenario: A join filter that converts a date to text runs in Exasol, and such a join key keeps the N-scan

* *GIVEN* an inner equi-join whose WHERE filter or `ON` filter converts a DATE to text, such as `o_orderdate LIKE '2024-01-0%'`, or whose join key does, such as `ON TO_CHAR(o.o_orderdate) = c.c_label`
* *WHEN* the adapter renders the join
* *THEN* a filter that converts a DATE to text SHALL keep the broadcast plan and run in Exasol's `WHERE` over the broadcast fan-out (`pushdown/pushdown-declined-filter-self-apply`)
* *AND* a join key that converts a DATE to text SHALL keep its pair off the broadcast, so a two-leg join takes the N-scan fallback, whose `ON` applies the conversion in Exasol, and a join of three or more legs broadcasts the next eligible pair (`pushdown-joins/pushdown-planning-join-multi-way`)
* *AND* the rows SHALL equal native Exasol under any `NLS_DATE_FORMAT`
