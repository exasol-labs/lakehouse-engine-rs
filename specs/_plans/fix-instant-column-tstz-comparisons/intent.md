# Intent: fix-instant-column-tstz-comparisons

## Interview

- Q: Scope of #658: full fix or docs only? → A: Full fix
- Q: Breaking change on REFRESH? → A: Release note only (moot after the design change below)
- Q: Which source columns count as instants? → A: Every UTC-adjusted column
- Q: Pruning for comparisons with plain TIMESTAMP or DATE values? → A: Use `SESSION_TIME_ZONE`, already documented (moot after the design change)
- Q: Expressions over an instant column? → A: Scan, in the folding zone (moot after the design change)
- Q: Verify the TSTZ declaration before planning? → A: Probe now
- Q: Benchmark the per-row TSTZ conversion? → A: Out of scope
- Q: Expressions over an instant column that return TSTZ? → A: Exasol evaluates them (moot after the design change)
- Q: TSTZ declaration breaks EXPORT and UDF input of instant columns: how to handle it? → A: Backtrack, keep declaring `TIMESTAMP`
- Q: Design: TSTZ declaration, plain TIMESTAMP in UTC with exact instant comparisons, or docs only? → A: Plain TIMESTAMP in UTC, compare instants exactly
- Q: Does INT96 count as an instant column? → A: Yes

## Directions

- [D1] A Glue Hive `timestamp` is no instant column; Glue declares it zone-less, as the adapter already maps it.
- [D2] The UTC fold applies only where a comparison operator, `BETWEEN`, or `IN` sets the value directly against an instant column, in a filter, `HAVING`, or a select-list item Exasol does not type TSTZ; an expression over the column, and a comparison inside a TSTZ-typed select-list item, keep the folding-zone rule, the smallest rule that covers realistic filters.
- [D5] Plain `TIMESTAMP` and `DATE` values compare with an instant column's UTC date and time, so #658's `ts_tz > TIMESTAMP '...'` repro row stays a difference that `docs/semantic-differences.md` lists; confirm with the user.
- [D6] A comparison Exasol evaluates itself (a join with a native TSTZ column, a non-pushed subquery, or under `EXCLUDED_CAPABILITIES`) still reads the instant column as session-local; the "Instant columns" doc section names it with the `CONVERT_TZ(tz, 'UTC', SESSIONTIMEZONE)` workaround.
- [D3] Exasol's own type of the compared value decides: `CURRENT_TIMESTAMP`, Exasol 8 `LOCALTIMESTAMP`, and TSTZ literals compare as instants, and `SYSTIMESTAMP`, `CURRENT_DATE`, `SYSDATE`, and Exasol 2025 `LOCALTIMESTAMP` as UTC date and time; Exasol folds `CURRENT_DATE - 7` into a plain literal the adapter cannot tell apart, so a bare call behaves like its folded forms.
- [D4] One fold rule: clock functions and compared TSTZ values fold in the `SESSION_TIME_ZONE` zone, else `DBTIMEZONE` (`SYSDATE`, `SYSTIMESTAMP` in `TZ`), except a TSTZ value compared with an instant column, which folds to its UTC date and time; rejected option: declaring instant columns TSTZ (Exasol refuses it as an `EXPORT ... INTO SCRIPT` source column, and `REFRESH` would change every column's type); consequence: `EXCLUDED_CAPABILITIES` no longer restores native results for instant columns. ADR: yes, criterion 4, digest search found `fold-clock-values-in-virtual-schema-zone` and `collapse-exasol-facing-timestamp-string-keep-internal-tz-aware-arrow-type`, supersedes fold-clock-values-in-virtual-schema-zone, § Constraints
