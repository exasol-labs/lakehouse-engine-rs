# Feature: Pushdown Planning: CHAR Padding and Width Enforcement

A `CHAR(n)` value returns what native Exasol returns: blank-padded to `n`, trailing blanks beyond `n` cut, and a non-blank excess rejected. Exasol applies a CAST to `CHAR(n)` itself, and pads a `CHAR(n)` value the scan emits.

## Background

* Exasol declares a string-literal expression `CHAR`, not `VARCHAR`, when every branch has the same length. On Exasol 2025.2.1, `CASE WHEN c_acctbal<0 THEN 'NEG' ELSE 'POS' END` is `CHAR(3) ASCII` and `CASE WHEN id>10 THEN 'high' ELSE 'low' END` is `VARCHAR(4) ASCII`.
* `CHAR(n)` and `CHAR(n) ASCII` are valid UDF `EMITS` output types. Exasol space-pads a shorter value emitted into a `CHAR(n)` column and rejects a longer one.
* Exasol's `CAST(x AS CHAR(n))` drops trailing blanks beyond `n` and fails with SQL state 22001 for non-blank excess.
* The scan does not blank-pad, so a CAST of text to `CHAR(n)` runs in Exasol (`pushdown-types/pushdown-planning-scan-text-conversion`). A `CHAR` group key that the scan computes therefore comes only from a `CHAR` column or a `CHAR` literal expression, whose values are already `n` characters wide, and travels as a typed `CHAR(n)` `GK_i` column (`pushdown-aggregates/pushdown-planning-split-aggregate-types`).
* `pushdown-types/pushdown-planning-char-type-declaration` covers how the `CHAR` type string is rendered.

## Scenarios

### Scenario: An equal-length CASE group key resolves to a CHAR group-key type

* *GIVEN* a `group_by` `pushdown` request whose `groupBy` holds a `function_scalar_case` over same-length string literals and whose `selectList` repeats that expression alongside an aggregate
* *AND* whose `selectListDataTypes` declares the group-key ordinal `{"type":"CHAR","size":3,"characterSet":"ASCII"}`
* *WHEN* the adapter resolves the group-key Exasol types
* *THEN* the scan SHALL declare the group-key column `"GK_0" CHAR(3) ASCII` in its `EMITS`
* *AND* the group-key column type SHALL match what Exasol validates positionally against `selectListDataTypes`

### Scenario: A CAST to CHAR returns Exasol's padded value

* *GIVEN* a select list, group key, or aggregate argument holding `CAST(<varchar col> AS CHAR(n))`
* *WHEN* the query runs through the Virtual Schema
* *THEN* Exasol SHALL apply the CAST, so each shorter value is space-padded to `n`
* *AND* values that differ only in trailing blanks SHALL form one group, including blanks beyond `n`
* *AND* a value with non-blank characters beyond `n` SHALL fail the statement with SQL state 22001, as natively

### Scenario: The CHAR declaration query shapes execute end to end through the Virtual Schema

* *GIVEN* the local Exasol, SeaweedFS, and Iceberg REST Docker stack with the seeded Virtual Schema table
* *WHEN* each of the four shapes is executed through the Virtual Schema, a GROUP BY on an equal-length CASE-of-string-literals expression, a select list containing `CAST(<varchar col> AS CHAR(20))`, a bare string literal used as a GROUP BY key, and a GROUP BY on a genuine VARCHAR column as the control
* *THEN* each statement SHALL return rows rather than fail with `Data type mismatch ... Expected CHAR(n)`
* *AND* the `CAST(<varchar col> AS CHAR(20))` shape SHALL return values space-padded to exactly 20 characters
