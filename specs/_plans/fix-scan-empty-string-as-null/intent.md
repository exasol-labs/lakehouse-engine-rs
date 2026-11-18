# Intent: fix-scan-empty-string-as-null

## Interview

- Q: Should a stored `''` in a string column read as NULL, or only string functions that return `''`? → A: Both.
- Q: `nullif(col, '') = 'x'` loses row-group pruning in the scan. Keep pruning or accept the loss? → A: Keep pruning: a comparison, `LIKE`, `IN`, or `BETWEEN` over a bare string column that is not under a `NOT` keeps the bare column.
- Q: How much of the NULL-argument part of #203 belongs here? → A: All text functions. `REPLACE` is fixed, and the others are checked live and fixed or declined, so one PR closes #203.

## Directions

- [D1] The translator normalizes at two producers only, string columns and text-returning functions; a normalizing base select in the scan was rejected because every string filter would lose row-group pruning. ADR: yes, criterion 4, no ADR covers empty strings.
- [D2] `IS NULL` and `IS NOT NULL` over a string column render through `nullif` and lose scan-side row-group pruning; a second rewrite rule for a rarely selective filter is not worth it. Plan-time file pruning stays.
- [D3] A column-to-column comparison other than `=` renders through `nullif`; only `=` gets the bare form, because it carries the join keys and the hash join needs bare columns.
- [D4] The rule covers every column the per-column type map declares as a character type, a `BINARY_AS_STRING` column included. Over a hex-rendered binary column or a JSON column `nullif` changes nothing.
- [D5] A text function whose NULL-argument result differs from DataFusion's renders a NULL guard for that argument, as `REPLACE` does. Where no guard reproduces it, the call declines unless that argument is a non-NULL literal.
- [D6] The plan-time null test is proved on the Iceberg copy only, because every pruning path reads the one parsed filter.
- [D7] The empty-string node takes the exact type of its argument, so declared types, split aggregates, and branch unification are unchanged.
