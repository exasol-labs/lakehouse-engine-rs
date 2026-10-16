# Feature: Pushdown Planning: CHAR Padding and Width Enforcement

This feature covers what a `CHAR(n)` declaration does to pushed-down values. Exasol space-pads a shorter value at UDF emit. The scan UDF drops trailing blanks beyond `n` before the emit, as Exasol's own `CAST(x AS CHAR(n))` does, and a value with non-blank excess fails. A `CHAR`-declared GROUP BY key travels as a typed `CHAR(n)` group-key column, so the merge groups exactly the rows native Exasol groups.

End-to-end scenarios check trailing-space grouping, blank and non-blank excess on projection and group-key values, and the four #192 query shapes against native Exasol.

## Background

* `exasol_type_from_json` is the single seam that maps an Exasol `dataType` JSON object to the Exasol type string used in `EMITS` clauses and wrapper `CAST` targets. `involvedTables[].columns` never carries `CHAR` (see the next bullet).
* Exasol declares a string-literal expression `CHAR`, not `VARCHAR`, when every branch yields the same length. Verified live on Exasol 2025.2.1: `CASE WHEN c_acctbal<0 THEN 'NEG' ELSE 'POS' END` → `CHAR(3) ASCII`, while `CASE WHEN id>10 THEN 'high' ELSE 'low' END` (lengths 4 and 3) → `VARCHAR(4) ASCII`.
* `CHAR(n)` and `CHAR(n) ASCII` are valid dynamic UDF `EMITS` output types, and Exasol space-pads a shorter emitted value into a `CHAR(n)` output column. Verified live with a LUA probe script: emitting the 15-character `25-989-741-2988` into `EMITS (P CHAR(20))` yields `25-989-741-2988     `, matching native `CAST(<col> AS CHAR(20))`.
* `CAST(<expr> AS CHAR(n) ASCII)` is valid Exasol CAST syntax (verified live), which the qualified single-table and N-scan join wrappers need for a `CAST(… AS CHAR(n))` select item.
* The character-set suffix rule is the one already established for VARCHAR (issue #136 follow-up): append ` ASCII` when the `dataType` JSON's `characterSet` equals `ASCII` case-insensitively. A `UTF8` or absent `characterSet` renders no suffix, which Exasol reads as its UTF8 default.
* The emit path maps a `CHAR(n)` declaration to an Arrow `Utf8` column.
* Exasol's `CAST(x AS CHAR(n))` drops trailing blanks beyond `n` and fails for non-blank excess. Verified live: `CAST('ab     ' AS CHAR(4))` returns `'ab  '`, and `CAST('abcdefghij' AS CHAR(3))` fails with SQL state 22001.
* The UDF emit rejects any value longer than `n`, blanks included. Verified with a LUA probe: emitting `'ab     '` into `EMITS (C CHAR(4))` fails with `string too long`. The scan UDF therefore cuts blank excess before the emit and leaves non-blank excess for the emit to reject.
* The grouped merge groups on the typed `GK_i` column (`pushdown-aggregates/pushdown-planning-split-aggregate-types`). Exasol pads each emitted `CHAR(n)` key, so values that differ only in trailing blanks form one group, as natively.
* Equality inside a pushed-down FILTER predicate over a `CHAR`-typed CAST runs on the unpadded value, because the DataFusion dialect renders that CAST as a bare `VARCHAR`. That divergence is out of scope here.
* An unprojected group key whose `CHAR` type Exasol does not send, such as a CASE whose branches are CASTs to `CHAR` (#293), has no exact type and routes to the qualified wrapper, where Exasol groups natively.
* See `pushdown-types/pushdown-planning-char-type-declaration` for how the `CHAR` type string is rendered, including the ASCII suffix, the 2,000-character cap, wrapper `CAST` targets, and `MIN` and `MAX` partial columns.

## Scenarios

### Scenario: An explicit CAST-to-CHAR select-list item projects with a CHAR EMITS type

* *GIVEN* a row-scan `pushdown` request whose `selectList` carries a bare `column` item and a `function_scalar_cast` item targeting `CHAR(20)`
* *AND* whose parallel `selectListDataTypes` declares that second ordinal `{"type":"CHAR","size":20}`
* *WHEN* the adapter resolves the projection and its positionally-aligned EMITS types
* *THEN* the resolved EMITS type at that ordinal SHALL be `CHAR(20)`
* *AND* the projection SHALL remain a rendered expression item (`_LH_PROJ_<i>`), not a full-base-row fallback, because `CHAR(n)` is a valid EMITS output type
* *AND* the DataFusion-side value expression SHALL stay an unpadded `VARCHAR` cast, because Arrow has no CHAR type and a projection ordinal carries no grouping semantics
* *AND* for every value no longer than the declared width, the pushed-down result SHALL equal native `CAST(<col> AS CHAR(20))`, because Exasol space-pads each shorter emitted value to 20 characters (live-verified: a 15-character value emitted into `EMITS (P CHAR(20))` came back padded to 20)
* *AND* for a value with non-blank characters beyond the declared width, the statement SHALL fail cleanly rather than return a silently truncated value. The pushed-down failure originates at the UDF emit, the native failure (SQL state 22001) at the engine's cast

### Scenario: An equal-length CASE group key resolves to a CHAR group-key type

* *GIVEN* a `group_by` `pushdown` request whose `groupBy` holds a `function_scalar_case` over same-length string literals and whose `selectList` repeats that expression alongside an aggregate
* *AND* whose `selectListDataTypes` declares the group-key ordinal `{"type":"CHAR","size":3,"characterSet":"ASCII"}`
* *WHEN* the adapter resolves the group-key Exasol types
* *THEN* the group-key slot's type SHALL be `CHAR(3) ASCII`
* *AND* the scan SHALL declare the group-key column `"GK_0" CHAR(3) ASCII` in its `EMITS`
* *AND* the group-key column type SHALL match what Exasol validates positionally against `selectListDataTypes`, so the pushdown is accepted rather than rejected as a `VARCHAR(3) ASCII` mismatch

### Scenario: A CHAR(n) emit drops trailing blanks beyond n

* *GIVEN* a scan UDF output column declared `CHAR(n)` and values longer than `n`
* *WHEN* the UDF emits the column
* *THEN* a value whose characters beyond `n` are all blanks SHALL be emitted cut to its first `n` characters
* *AND* a value with a non-blank character beyond `n` SHALL be emitted unchanged, so the emit rejects it
* *AND* the cut SHALL count characters, not bytes, and SHALL keep NULL as NULL
* *AND* `SELECT CAST(<col> AS CHAR(4)) FROM <vs>.<table>` over `'ab   '` SHALL return `'ab  '`, as native Exasol does

### Scenario: An over-length CHAR group-key value fails rather than merging a truncated group

* *GIVEN* a seeded Virtual Schema table holding a string value with non-blank characters beyond the width a `CAST(<col> AS CHAR(n))` group key declares, for example a 25-character value under `CHAR(20)`
* *WHEN* `SELECT CAST(<col> AS CHAR(20)) g, COUNT(*) FROM <vs>.<table> GROUP BY 1` or `SELECT COUNT(*) FROM <vs>.<table> GROUP BY CAST(<col> AS CHAR(20))` is executed through the Virtual Schema
* *THEN* the statement SHALL fail with a truncation error raised at the UDF emit of the `CHAR(20)` group-key column, as native Exasol fails for the same values
* *AND* the statement MUST NOT return rows

### Scenario: An over-length CHAR projection value fails cleanly rather than truncating

* *GIVEN* the same seeded table holding a value longer than the declared CHAR width
* *WHEN* `SELECT CAST(<col> AS CHAR(20)) FROM <vs>.<table>` is executed through the Virtual Schema — the projection facet, with no GROUP BY, where the width is enforced at UDF emit rather than by an outer cast
* *THEN* the statement SHALL fail cleanly with a truncation error rather than return a silently truncated 20-character value
* *AND* this SHALL hold on the Rust SLC's `emit_batch` Arrow IPC path

### Scenario: A CHAR group key over trailing-space data groups identically to native Exasol

* *GIVEN* a seeded Virtual Schema table holding two source rows whose string column differs only in trailing whitespace — `'ab'` and `'ab   '` — plus at least one unrelated value
* *WHEN* `SELECT CAST(<col> AS CHAR(30)) g, COUNT(*) FROM <vs>.<table> GROUP BY 1` is executed through the Virtual Schema, at a declared width that every seeded value fits so this scenario isolates merge behavior from truncation behavior
* *THEN* the two trailing-whitespace-only variants SHALL merge into exactly ONE output row whose count is 2
* *AND* the pushed-down result SHALL equal the result Exasol computes natively over the same values, row for row
* *AND* the adapter MUST NOT return two separate rows that render identically as `'ab'` followed by 28 spaces
* *AND* under `CAST(<col> AS CHAR(4))`, where `'ab   '` exceeds the width by blanks only, the variants SHALL also merge into one row with count 2

### Scenario: The four #192 query shapes execute end to end

* *GIVEN* the local Exasol, SeaweedFS, and Iceberg REST Docker stack with the seeded Virtual Schema table
* *WHEN* each of the four shapes is executed through the Virtual Schema — a GROUP BY on an equal-length CASE-of-string-literals expression, a select list containing `CAST(<varchar col> AS CHAR(20))`, a bare string literal used as a GROUP BY key, and a GROUP BY on a genuine VARCHAR column as the control
* *THEN* each statement SHALL return rows rather than fail with `Data type mismatch ... Expected CHAR(n)`
* *AND* the `CAST(<varchar col> AS CHAR(20))` shape SHALL return values space-padded to exactly 20 characters
* *AND* the VARCHAR control SHALL keep returning its current result, proving the CHAR branch did not disturb the VARCHAR path
