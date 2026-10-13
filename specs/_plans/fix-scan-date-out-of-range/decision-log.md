# Decision Log: fix-scan-date-out-of-range

## Interview

- Q: Should YEAR(D) and other pushed functions over an out-of-range date fail too? → A: No, out of scope; document in docs/semantic-differences.md
- Q: What counts as out of range, and what does the error name? → A: Exasol DATE range 0001-01-01..9999-12-31; error names the output column and the raw day count; one guard shared by raw-row and aggregate paths

## Design Decisions

### [1] The guard checks every Date32 column at the emit boundary, keyed on the Arrow type

- **Decision:** The guard runs on each column that leaves the scan as Arrow Date32, after coercion, on both emit paths. It does not key on a declared `ExaType::Date`.
- **Rationale:** A `GROUP BY` key column is declared `VARCHAR(2000000)` and passes `coerce_partial_agg_columns` uncoerced, as Date32. A check keyed on the declaration would miss it and still emit a 1970-01-01 group.
- **Alternatives:** Put the check inside `coerce_column`. Rejected: the raw path skips `coerce_column` when a batch already matches its declaration, and group keys skip it too.
- **Promotes to ADR:** no

### [2] The error names the declared output column

- **Decision:** The error names the output column as the call-site `EMITS` clause declares it. On the raw path that is the table's column name. On the aggregate path it is the adapter's partial alias, for example `PARTIAL_max_0` or `GK_0`.
- **Rationale:** The declared output column is the one authority for an emitted column (ADR `declared-output-column-single-authority-emit-type`), and the existing NUMERIC precision guard already names it on both paths. Together with the day count, the alias identifies the value.
- **Alternatives:** Map each aggregate column back to its source column. Rejected: the grouped loop would need a second walk over the partial-column widths, which `partial_row_from_batch` documents as a source of shifted values when re-derived.
- **Promotes to ADR:** no

### [3] The lower bound is -719162 days, the proleptic Gregorian 0001-01-01

- **Decision:** The guard's bounds are Date32 day counts -719162 (0001-01-01) and 2932896 (9999-12-31), both inclusive.
- **Rationale:** Arrow counts Date32 days in the proleptic Gregorian calendar. Exasol's `DAYS_BETWEEN(DATE '0001-01-01', DATE '1970-01-01')` returns -719164, because Exasol counts dates before 1582-10-15 in the Julian calendar (`DAYS_BETWEEN(DATE '1582-10-15', DATE '1582-10-04')` returns 1), so a bound derived from Exasol date arithmetic would be two days off. The E2E round trip of row 2 (task 1.3) confirms the bound.
- **Promotes to ADR:** no

## Review Findings
