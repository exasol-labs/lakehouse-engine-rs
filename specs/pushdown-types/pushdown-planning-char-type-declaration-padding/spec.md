# Feature: Pushdown Planning: CHAR Padding and Width Enforcement

A pushed-down `CHAR(n)` projection relies on Exasol to space-pad a shorter value at UDF emit and to reject a longer one. A pushed-down `CHAR(n)` GROUP BY key is blank-padded on the DataFusion side to its declared width, so the grouping merges exactly the rows native Exasol merges.

## Background

* Exasol declares a string-literal expression `CHAR`, not `VARCHAR`, when every branch has the same length. On Exasol 2025.2.1, `CASE WHEN c_acctbal<0 THEN 'NEG' ELSE 'POS' END` is `CHAR(3) ASCII` and `CASE WHEN id>10 THEN 'high' ELSE 'low' END` is `VARCHAR(4) ASCII`.
* `CHAR(n)` and `CHAR(n) ASCII` are valid UDF `EMITS` output types, and `CAST(<expr> AS CHAR(n) ASCII)` is valid CAST syntax. The adapter appends ` ASCII` when the `dataType` JSON's `characterSet` equals `ASCII` case-insensitively. A `UTF8` or absent `characterSet` adds no suffix.
* Exasol space-pads a shorter value emitted into a `CHAR(n)` column and raises an error for a longer one: SQL state 22001, `data exception - string data, right truncation`. It does not truncate.
* The grouped-aggregate merge groups on the unpadded staging column `"GK_i" VARCHAR(2000000)` and applies `CAST("GK_i" AS <declared type>)` only in the SELECT list. A `CHAR(n)` key that is not padded on the DataFusion side would split values that differ only in trailing whitespace into rows that render identically.
* `spec.common.group_keys` is the only DataFusion-side grouping-equality position a `CHAR` type can reach. The Exasol-parsed wrapper paths group natively over the padded `CHAR` value.
* The pad is `CASE WHEN character_length(<expr>) < n THEN rpad(<expr>, n) ELSE <expr> END`, measured against DataFusion 54.1. Bare `rpad` truncates a longer value. `concat(<expr>, repeat(...))` turns NULL into `''`. The `CASE` form keeps NULL as NULL, leaves a value of `n` characters or more unchanged, and counts characters, not bytes.
* The unpadded rendered group-key SQL is the identity key that matches select items and `orderBy` elements to group-key slots. The padded fragments are a separate list used only for `ScanSpec.common.group_keys`.
* A group key that is not projected as its own select-list ordinal has no `selectListDataTypes` entry, keeps the `VARCHAR(2000000)` default, and is not padded. An unprojected key whose `CHAR` type comes from a CASE of CAST-to-CHAR branches has no top-level `dataType` to read, so it is also not padded.
* `pushdown-types/pushdown-planning-char-type-declaration` covers how the `CHAR` type string is rendered.

## Scenarios

### Scenario: An explicit CAST-to-CHAR select-list item projects with a CHAR EMITS type

* *GIVEN* a row-scan `pushdown` request whose `selectList` carries a bare `column` item and a `function_scalar_cast` item targeting `CHAR(20)`
* *AND* whose parallel `selectListDataTypes` declares that second ordinal `{"type":"CHAR","size":20}`
* *WHEN* the adapter resolves the projection and its positionally-aligned EMITS types
* *THEN* the resolved EMITS type at that ordinal SHALL be `CHAR(20)`
* *AND* the projection SHALL remain a rendered expression item (`_LH_PROJ_<i>`), not a full-base-row fallback, because `CHAR(n)` is a valid EMITS output type
* *AND* the DataFusion-side value expression SHALL stay an unpadded `VARCHAR` cast, because Arrow has no CHAR type and a projection ordinal carries no grouping semantics
* *AND* for every value no longer than the declared width, the pushed-down result SHALL equal native `CAST(<col> AS CHAR(20))`, because Exasol space-pads each shorter emitted value to 20 characters (live-verified: a 15-character value emitted into `EMITS (P CHAR(20))` came back padded to 20)
* *AND* for a value LONGER than the declared width, the statement SHALL fail cleanly rather than return a silently truncated value, because Exasol enforces the declared CHAR width at emit — a 10-character value emitted into `EMITS (P CHAR(5))` failed with SQL state 22001 in a LUA probe. This is the same error class native Exasol raises for `CAST(<over-length> AS CHAR(5))` (also 22001), though the message text and origin differ: the pushed-down failure originates at the UDF emit, the native failure at the engine's cast. The equivalence that MUST hold is "clean failure, never a silently truncated value"; the E2E scenario below asserts it on the Rust SLC's `emit_batch` path, and any divergence found there MUST be recorded as a cited tracked exception rather than left silent

### Scenario: An equal-length CASE group key resolves to a CHAR group-key type

* *GIVEN* a `group_by` `pushdown` request whose `groupBy` holds a `function_scalar_case` over same-length string literals and whose `selectList` repeats that expression alongside an aggregate
* *AND* whose `selectListDataTypes` declares the group-key ordinal `{"type":"CHAR","size":3,"characterSet":"ASCII"}`
* *WHEN* the adapter resolves the group-key Exasol types
* *THEN* the group-key slot's type SHALL be `CHAR(3) ASCII`
* *AND* the outer merge wrapper SHALL cast the stringified staging column as `CAST("GK_0" AS CHAR(3) ASCII)`, which is valid Exasol CAST syntax
* *AND* the emitted group-key column type SHALL match what Exasol validates positionally against `selectListDataTypes`, so the pushdown is accepted rather than rejected as a `VARCHAR(3) ASCII` mismatch

### Scenario: A CHAR group key is blank-padded to its declared width before grouping

* *GIVEN* a `group_by` `pushdown` request whose group-key ordinal's `selectListDataTypes` entry resolves to `CHAR(20)`, over a source expression that can yield values of differing length
* *WHEN* the adapter builds the shard `ScanSpec`
* *THEN* that key's `ScanSpec.common.group_keys` fragment SHALL pad a value SHORTER than 20 characters to exactly 20 with trailing spaces, rendered as `CASE WHEN character_length(<unpadded fragment>) < 20 THEN rpad(<unpadded fragment>, 20) ELSE <unpadded fragment> END`
* *AND* the pad MUST NOT truncate: a value of 20 or more characters SHALL pass through UNMODIFIED, so it reaches Exasol's own `CAST("GK_i" AS CHAR(20))` intact and Exasol's over-length truncation error still fires
* *AND* the pad SHALL render a bare `rpad(<fragment>, 20)` NOWHERE, because `rpad` truncates an over-length input
* *AND* the outer wrapper SHALL keep emitting `CAST("GK_i" AS CHAR(20))` for that ordinal, which is a no-op re-cast of an already-20-character staging value and remains the declaration Exasol validates positionally

### Scenario: The CHAR group-key pad preserves NULL, parses the declared width, and leaves the identity list unpadded

* *GIVEN* a `group_by` `pushdown` request carrying a `CHAR`-declared group key
* *WHEN* the adapter derives the padded group-key fragment
* *THEN* a NULL group-key value SHALL stay NULL rather than becoming a run of `n` spaces, so a NULL group is not merged with an all-blanks group
* *AND* the declared width SHALL be parsed correctly from BOTH the `CHAR(20)` and the `CHAR(3) ASCII` forms — by reading the digits between `(` and `)` rather than trimming a trailing `)` — so an ASCII-declared CHAR key, the #192 primary shape, is padded rather than silently skipped
* *AND* the padding SHALL apply to every group-key slot whose declared type is `CHAR(n)` **and which is itself a select-list ordinal**, and to no slot whose declared type is anything else; a group key not projected as its own select-list ordinal has no `selectListDataTypes` entry to resolve, keeps the `VARCHAR(2000000)` default, and is unaffected, which is pre-existing behavior and out of scope here
* *AND* the group-key list used for select-item and `ORDER BY` slot matching SHALL remain the UNPADDED rendered SQL, so an `ORDER BY` on a `CHAR` group key still resolves to its output ordinal instead of declining the pushdown

### Scenario: An over-length CHAR group-key value raises Exasol's truncation error rather than merging a truncated group

* *GIVEN* a seeded Virtual Schema table holding a string value LONGER than the width a `CAST(<col> AS CHAR(n))` group key declares — for example a 25-character value under `CHAR(20)`
* *WHEN* `SELECT CAST(<col> AS CHAR(20)) g, COUNT(*) FROM <vs>.<table> GROUP BY 1` is executed through the Virtual Schema
* *THEN* the statement SHALL fail with Exasol's own truncation error — SQL state 22001, `data exception - string data, right truncation` — raised by Exasol's evaluation of `CAST("GK_0" AS CHAR(20))` on the merge side
* *AND* the statement MUST NOT return rows, because a returned result set would mean the pad silently truncated the over-length value into a wrong, merged group
* *AND* the failure SHALL match what native Exasol raises for the same statement over the same values, so pushing the shape down neither introduces nor suppresses an error
* *AND* the pad MUST NOT suppress this error, which is why it leaves values at or above the declared width unmodified instead of shortening them

### Scenario: An over-length CHAR projection value fails cleanly rather than truncating

* *GIVEN* the same seeded table holding a value longer than the declared CHAR width
* *WHEN* `SELECT CAST(<col> AS CHAR(20)) FROM <vs>.<table>` is executed through the Virtual Schema — the projection facet, with no GROUP BY, where the width is enforced at UDF emit rather than by an outer cast
* *THEN* the statement SHALL fail cleanly with a truncation error rather than return a silently truncated 20-character value
* *AND* this SHALL confirm on the Rust SLC's `emit_batch` Arrow IPC path what the LUA probe established for the classic emit path
* *AND* if the Rust path is found to truncate instead of erroring, that divergence MUST be recorded as an explicit, cited tracked exception in this spec rather than left as a silent gap

### Scenario: A CHAR group key over trailing-space data groups identically to native Exasol

* *GIVEN* a seeded Virtual Schema table holding two source rows whose string column differs only in trailing whitespace — `'ab'` and `'ab   '` — plus at least one unrelated value
* *WHEN* `SELECT CAST(<col> AS CHAR(30)) g, COUNT(*) FROM <vs>.<table> GROUP BY 1` is executed through the Virtual Schema, at a declared width that every seeded value fits so this scenario isolates merge behavior from truncation behavior
* *THEN* the two trailing-whitespace-only variants SHALL merge into exactly ONE output row whose count is 2
* *AND* the pushed-down result SHALL equal the result Exasol computes natively over the same values, row for row
* *AND* the adapter MUST NOT return two separate rows that render identically as `'ab'` followed by 28 spaces, which is what an unpadded `CHAR`-declared group key would produce

### Scenario: The four #192 query shapes execute end to end

* *GIVEN* the local Exasol, SeaweedFS, and Iceberg REST Docker stack with the seeded Virtual Schema table
* *WHEN* each of the four shapes is executed through the Virtual Schema — a GROUP BY on an equal-length CASE-of-string-literals expression, a select list containing `CAST(<varchar col> AS CHAR(20))`, a bare string literal used as a GROUP BY key, and a GROUP BY on a genuine VARCHAR column as the control
* *THEN* each statement SHALL return rows rather than fail with `Data type mismatch ... Expected CHAR(n)`
* *AND* the `CAST(<varchar col> AS CHAR(20))` shape SHALL return values space-padded to exactly 20 characters
* *AND* the VARCHAR control SHALL keep returning its current result, proving the CHAR branch did not disturb the VARCHAR path
