# Feature: DataFusion Scan Execution — Field-Id-Based Column Projection

Extends the scan UDF to bind columns by Iceberg field-id (with a physical-name fallback)
when the scan spec carries a logical schema, so projection is correct across Iceberg
schema evolution — renamed, dropped, and added columns all resolve correctly per file
without rewriting the schema adapter. An added column absent from an older data file
returns its defined Iceberg `initial-default` value, falling back to NULL only when the
column is nullable and defines no default.

## Background

* This delta adds three scenarios, split from one for scenario size, and amends no recorded clause.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An identity-bound field binds a file column whose name differs only in letter case

* *GIVEN* a scan spec whose logical schema carries an identity-bound field `CustomerId`, which declares neither a field-id nor a physical name
* *AND* one assigned file whose physical column is named `customerid`, and a second assigned file that carries both `CustomerId` and `CUSTOMERID`
* *WHEN* the scan UDF reads both files
* *THEN* the UDF SHALL bind `CustomerId` to the first file's `customerid` column, matched by the uppercase fold, and SHALL emit that column's real values, never NULL, because Spark resolves Parquet columns case-insensitively by default (`spark.sql.caseSensitive=false`) and neither the Delta protocol nor the Iceberg table spec states a column-name case rule
* *AND* in the second file the physical column whose name equals the logical name exactly SHALL bind, and `CUSTOMERID` SHALL stay unclaimed, because an exact match takes precedence over a folded one
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: An ambiguous case fold fails the query loud, in either direction

* *GIVEN* an identity-bound field, and a file carrying two or more physical columns that fold onto its name with none equal to it exactly
* *AND* a second such field, and a file carrying one physical column that folds onto two or more identity-bound names with none equal to it exactly
* *WHEN* the scan UDF reads each file
* *THEN* the UDF SHALL fail each query with an error naming the logical column and every candidate, rather than binding one of them arbitrarily
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The case fold is scoped to identity-bound fields and applies uniformly across formats

* *GIVEN* a field-id-bound field's physical-name fallback and a field bound by a declared physical name
* *WHEN* the scan UDF binds either field against a file whose matching column differs only in letter case
* *THEN* the UDF SHALL NOT apply the case fold and SHALL require an exact match, case included
* *AND* the case fold behavior for identity-bound fields SHALL be identical for every table format whose logical schema carries them, because the scan side MUST NOT branch on table format
<!-- /DELTA:NEW -->
