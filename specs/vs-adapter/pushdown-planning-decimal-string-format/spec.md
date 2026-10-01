# Feature: Pushdown Planning — Decimal String Formatting

Makes every pushed-down DECIMAL→string conversion reproduce Exasol's shortest form. Exasol trims
trailing scale zeros when it converts a DECIMAL to text (`2912.00`→`'2912'`,
`-272.60`→`'-272.6'`). DataFusion's `CAST(decimal AS VARCHAR)` and its implicit decimal→utf8
coercion keep the full declared scale, so their text differs from Exasol's (issue #211). The
DataFusion dialect routes every string-converted argument through `exa_to_varchar`, which trims a
DECIMAL by its Arrow type, on every surface that renders DataFusion SQL and for a computed DECIMAL
argument as well as a bare column (issue #227).

## Background

* The stringifications this feature covers are the string CAST (`CAST(<x> AS VARCHAR/CHAR)`) and
  the implicit conversions of `CONCAT` (Exasol's `||`) and `LENGTH`, the two string functions that
  convert silently rather than fail.
* `sql-comprehension/vs-expression-translator-string-conversion` owns which positions convert: every
  `CONCAT` level of Exasol's nested `a || b || c`, and no non-stringifying position such as
  arithmetic, a comparison operand, or a CAST to a non-string target.
* `datafusion-scan/scan-execution-exa-to-varchar` owns the trim, its DECIMAL scenario pins the text,
  and its Background records the Iceberg and Delta spec check and the 37- and 38-digit decimal
  trade-off.
* An Exasol integer arrives as `DECIMAL(p,0)`. The trim is a no-op on it.

## Scenarios

### Scenario: A DECIMAL stringification in a GROUP BY key or an aggregate argument renders the trimmed form

* *GIVEN* issue #227's repros over a `DECIMAL(12,2)` column holding `0.50` and `100.10`: `SELECT CAST(c_acctbal AS VARCHAR(20)) k, COUNT(*) ... GROUP BY CAST(c_acctbal AS VARCHAR(20))` and `SELECT MAX(CAST(c_acctbal AS VARCHAR(20))) ...`
* *WHEN* the adapter pushes the grouped or the single-group aggregate down
* *THEN* the group keys SHALL be `0.5` and `100.1` and the maximum SHALL be `100.1`, equal to native Exasol, NOT `0.50` and `100.10`
* *AND* the select-list key SHALL match its GROUP BY key by rendered text, so the request decomposes into the grouped partial/merge scan
