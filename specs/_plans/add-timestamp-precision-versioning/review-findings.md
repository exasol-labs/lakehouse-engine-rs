# Code Review Findings: add-timestamp-precision-versioning

## Summary
- Files reviewed: 14
- Total findings: 11 (standard: 11, expert: 0)

Verified clean (no finding raised, recorded so a re-review does not re-litigate them):

- **Call-site census (task 3) is complete.** `iceberg_primitive_to_exasol`, `iceberg_type_to_exasol`,
  and `column_source_type_to_exasol` have no caller anywhere in the workspace — including the
  feature-gated `crates/lakehouse-engine/tests/` binaries — outside `types/mapping.rs`,
  `types/mapping_tests.rs`, `adapter/mod.rs`, `adapter/adapter_tests.rs`, and
  `adapter/catalog_client_tests.rs`, all of which were threaded.
- **`exasol_type_to_json` / `exasol_type_from_json` are a matched pair.** The new
  `TIMESTAMP(` → `fractionalSecondsPrecision` arm inverts the pre-existing `"timestamp"` arm at
  `mapping.rs:657-673`, and `exasol_type_to_json_renders_timestamp_fractional_seconds_precision`
  asserts the round trip for `TIMESTAMP`, `TIMESTAMP(6)`, and `TIMESTAMP WITH LOCAL TIME ZONE`.
  `exasol_type_to_arrow` (`mapping.rs:141`) and `classify_exa_type` already tolerate `TIMESTAMP(p)`.
- **Single owner of the version read.** `refresh` and `setProperties` both route through
  `handle_create_virtual_schema` (`adapter/mod.rs:136-137`), so `ctx.database_version()` is read at
  exactly one site and no second owner exists.

## Standard fixes

### crates/lakehouse-engine/src/types/mapping.rs

#### [TOO_MANY_ARGUMENTS] `unity_type_name_to_exasol` now takes four parameters
- Location: lines 517-522
- Issue: threading `TimestampPrecision` pushed the signature from three parameters to four
  (`type_name`, `precision`, `scale`, `timestamp_precision`), over the guardrail limit of three.
  `precision` and `scale` are only meaningful together and only for the `"DECIMAL"` arm — they are
  one concept split across two parameters, which is what left no room for the fourth.
- Fix: In `crates/lakehouse-engine/src/types/mapping.rs`, add
  `#[derive(Debug, Clone, Copy)] struct CatalogDecimal { precision: u32, scale: u32 }` next to
  `catalog_decimal_to_exasol`; change `unity_type_name_to_exasol` to
  `fn unity_type_name_to_exasol(type_name: &str, decimal: CatalogDecimal, timestamp_precision: TimestampPrecision) -> String`
  and have its `"DECIMAL"` arm call `catalog_decimal_to_exasol(decimal.precision, decimal.scale)`;
  construct the `CatalogDecimal` at the single call site in `column_source_type_to_exasol`
  (line 501-505) from the `ColumnSourceType::Unity` fields. No test calls
  `unity_type_name_to_exasol` directly, so no test edit is required.

#### [OUTDATED_COMMENT] `TimestampPrecision` doc claims 8.x rejects parameterized `TIMESTAMP(p)`
- Location: lines 275-277
- Issue: the doc states "only the calendar-versioned line accepts the parameterized `TIMESTAMP(p)`".
  decision-log.md `[C3]` records the opposite from a live capture: "Exasol 8.29.13 accepts
  `TIMESTAMP(p)` **only** for `p` in `{3, 6}`" — `TIMESTAMP(6)` is accepted syntactically and clamped
  to milliseconds semantically. `[C1]` adds that 8.29.13 also accepts `fractionalSecondsPrecision: 6`
  in a `createVirtualSchema` column `dataType` and silently downgrades it to `TIMESTAMP(3)`. The
  distinguishing property is *honoring*, not *accepting*.
- Fix: In `crates/lakehouse-engine/src/types/mapping.rs`, rewrite the `TimestampPrecision` doc
  sentence at lines 275-277 to say that both engine lines accept `TIMESTAMP(6)` but only the
  calendar-versioned line *honors* it — 8.x clamps the declaration to `TIMESTAMP(3)` and strips
  `fractionalSecondsPrecision` from the pushdown echo — so the gate exists to keep
  `SYS.EXA_ALL_COLUMNS` honest about what the adapter asked for, not to avoid a rejection. Cite
  decision-log.md `[C1]`/`[C3]`.

#### [OUTDATED_COMMENT] `from_database_version` rationale claims a loud failure that cannot happen
- Location: lines 293-298
- Issue: the doc justifies the microsecond default with "an unrecognised engine that rejects
  `TIMESTAMP(6)` fails loudly at `createVirtualSchema`, which beats silently truncating every
  timestamp value". decision-log.md `[C1]` consequence 2 explicitly retracts this: "The plan's stated
  risk that an engine 'rejects `TIMESTAMP(6)` and fails visibly at `createVirtualSchema`' (decision
  [5]) does **not** materialize on 8.29.13: it accepts and clamps. The unparseable-version default
  therefore fails silently, not loudly." `[C6]` repeats the instruction: "the recorded rationale
  should not claim the gate prevents a failure on 8.x". The implementation carried forward the plan's
  superseded text.
- Fix: In `crates/lakehouse-engine/src/types/mapping.rs`, replace the second paragraph of the
  `from_database_version` doc comment (lines 293-298) with the corrected rationale: the microsecond
  default is the user's recorded deliberate choice (decision-log.md interview Q2); on an 8.x-like
  engine it is not loud — the engine accepts and clamps (`[C1]`) — so the choice trades a silent
  misdeclaration in `SYS.EXA_ALL_COLUMNS` against silently truncating every value on an
  unrecognised engine that would have honored the precision. Keep it to two or three lines.

#### [REDUNDANT_COMMENT] Two-line rationale block restating what a dedicated test already pins
- Location: lines 68-70
- Issue: the block "Deliberately outside the `TimestampPrecision` version gate: this resolver answers
  for an Arrow input, not a catalog declaration." states the same fact three other artefacts already
  own: the function's own doc comment ("The Exasol type for an **Arrow type**…"), decision-log.md
  `[1]`, and the dedicated test `arrow_input_resolver_stays_outside_the_timestamp_version_gate`
  (`mapping_tests.rs:1321-1340`), whose function-pointer binding makes the exclusion a compile-time
  assertion. The project rule is one necessary WHY-line, not a rationale block.
- Fix: In `crates/lakehouse-engine/src/types/mapping.rs`, collapse lines 68-70 to a single line
  appended inside the existing comment block above `DataType::Timestamp(_, _)`, reading
  `// Not version-gated: an Arrow input type, not a catalog declaration.`

### crates/lakehouse-engine/src/types/mapping_tests.rs

#### [MISSING_BOUNDARY_TEST] Malformed `TIMESTAMP(p)` silently falls through to VARCHAR
- Location: `mapping.rs:596-602` (arm under test), `mapping_tests.rs:1367-1402` (test)
- Issue: the new `exasol_type_to_json` arm parses `p` with `.parse::<u64>().ok()` and, on failure,
  falls through past the decimal arm into the VARCHAR catch-all — so `TIMESTAMP()`,
  `TIMESTAMP(abc)`, and `TIMESTAMP(-1)` are declared `{"type":"varchar","size":2000000}`, silently
  reclassifying a timestamp column as a string with no error. Only the well-formed inputs
  `TIMESTAMP(0)`, `TIMESTAMP(6)`, `TIMESTAMP(9)` are covered.
- Fix: In `crates/lakehouse-engine/src/types/mapping_tests.rs`, extend
  `exasol_type_to_json_renders_timestamp_fractional_seconds_precision` with the malformed-argument
  boundary: assert the current behavior for `"TIMESTAMP()"`, `"TIMESTAMP(abc)"`, and
  `"TIMESTAMP(-1)"`, and add a one-line doc note that the VARCHAR catch-all is the recorded answer
  because `TimestampPrecision::declaration()` is the only producer and emits only `TIMESTAMP` and
  `TIMESTAMP(6)`.

### crates/lakehouse-engine/tests/common/timestamp_precision.rs

#### [INFORMATION_LEAKAGE] The `TIMESTAMP(p)` string format is decoded again in a consumer
- Location: `timestamp_precision.rs:20-34`; `e2e_timestamp_precision_test.rs:86-101`
- Issue: `ExpectedTimestampPrecision` exposes the precision only as a display string
  (`"TIMESTAMP(6)"` / `"TIMESTAMP(3)"`), so `e2e_timestamp_precision_test::declared_precision` has to
  strip the `TIMESTAMP(` prefix and `)` suffix back off it to get the integer that `retained_at`
  needs, and panics if the oracle's string is not parameterized. That makes the string format an
  undocumented contract between the oracle and one of its consumers: correcting
  `MILLISECOND.declared_column_type` to the bare `"TIMESTAMP"` the adapter actually declares would
  panic the consumer rather than fail an assertion.
- Fix: In `crates/lakehouse-engine/tests/common/timestamp_precision.rs`, add a
  `pub retained_fractional_digits: u32` field to `ExpectedTimestampPrecision`, set it to `6` on
  `MICROSECOND` and `3` on `MILLISECOND`, and document it as the digits the engine actually keeps.
  In `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs`, delete the
  `declared_precision` helper and replace `let precision = declared_precision(expected.declared_column_type);`
  with `let precision = expected.retained_fractional_digits;`.

#### [MISSING_DESIGN_INTENT] `declared_column_type` is documented for one use but spliced into SQL
- Location: `timestamp_precision.rs:21` and its doc at lines 14-19
- Issue: the doc pins the field as "the exact `SYS.EXA_ALL_COLUMNS.COLUMN_TYPE` string", but
  `e2e_capability_test.rs:2756-2758` splices it into a `CAST(... AS {declared_column_type})` SQL
  target. That second contract only holds because of decision-log.md `[C3]`: "Exasol 8.29.13 accepts
  `TIMESTAMP(p)` **only** for `p` in `{3, 6}`; every other precision is rejected as `0A000 Feature
  not supported`". Nothing in the oracle records that a future value has to satisfy both contracts,
  so adding e.g. a nanosecond arm would compile, read as correct, and fail live on 8.x.
- Fix: In `crates/lakehouse-engine/tests/common/timestamp_precision.rs`, extend the
  `declared_column_type` doc comment to state both contracts — it is the exact
  `SYS.EXA_ALL_COLUMNS.COLUMN_TYPE` string *and* a legal `CAST` target on both supported engines —
  and record the constraint that any value must stay within `p in {3, 6}`, citing decision-log.md
  `[C3]`.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [INFORMATION_LEAKAGE] Exact `COLUMN_TYPE` comparison skips the file's own whitespace normalization
- Location: lines 1466-1491
- Issue: `unity_delta_timestamp_columns_declare_the_exact_gated_precision` `assert_eq!`s the raw
  string from `column_types` (lines 170-183, which only upper-cases) against the oracle constant. The
  same file's `assert_col_type` (lines 185-189) exists precisely because Exasol's `COLUMN_TYPE`
  rendering is "tolerant of Exasol's `COLUMN_TYPE` rendering (whitespace, …) by matching on the
  space-stripped prefix", and the sibling consumer
  `e2e_timestamp_precision_test::declared_type` (lines 74-84) strips whitespace before comparing for
  this exact assertion. The normalization rule now lives in two places and is missing in the third,
  so the same oracle is compared three different ways.
- Fix: In `crates/lakehouse-engine/tests/e2e_unity_test.rs`, strip whitespace from the value read out
  of `column_types` before each `assert_eq!` in
  `unity_delta_timestamp_columns_declare_the_exact_gated_precision` — e.g. bind
  `let actual: String = raw.chars().filter(|c| !c.is_whitespace()).collect();` — matching
  `e2e_timestamp_precision_test::declared_type`. Do this for all three columns
  (`TIMESTAMP_COL`, `TIMESTAMP_NTZ_COL`, `DATE_TIMESTAMP_NTZ`).

### crates/lakehouse-engine/tests/e2e_capability_test.rs

#### [OUTDATED_COMMENT] "only by coincidence" contradicts the recorded capture
- Location: lines 2731-2737
- Issue: the doc says the previous hardcoded bare-`TIMESTAMP` cast "happened to render identically
  for this millisecond-only fixture, but only by coincidence". decision-log.md `[C4]` records it as
  deterministic, not coincidental: "The existing `.100` fixture renders identically under both CAST
  targets on both engines", and its table shows
  `UPPER(CAST(TIMESTAMP '…100' AS TIMESTAMP))` and `UPPER(CAST(… AS TIMESTAMP(6)))` both returning
  `VARCHAR(26)` `...100000` on 2025.2.1 and 8.29.13 — because `.100` carries no sub-millisecond
  digits. As written the comment implies the old assertion was fragile when it was in fact
  unconditionally correct for this fixture, and hides that the change is a behavioral no-op kept as
  hardening.
- Fix: In `crates/lakehouse-engine/tests/e2e_capability_test.rs`, rewrite lines 2731-2737 to state
  that the `.100` fixture renders identically under both CAST targets on both engines
  (decision-log.md `[C4]`), so reading the target from `expected_timestamp_precision` changes no
  assertion today and exists so the oracle stays correct if the fixture ever gains sub-millisecond
  digits. Drop the word "coincidence".

### CLAUDE.md

#### [OUTDATED_COMMENT] Version gate attributed to the Arrow row, pointing at a path `/speq:record` will delete
- Location: line 245
- Issue: two defects on one row. (1) The row sits in the "DataFusion/Arrow → Exasol" table, which the
  heading says applies "in both `createVirtualSchema` schema mapping and Arrow→Value conversion" —
  but decision-log.md `[1]` records that the Arrow-input resolver (`compatible_exasol_type` /
  `arrow_to_exasol_type`) is *deliberately left ungated*, and `mapping_tests.rs`'s
  `arrow_input_resolver_stays_outside_the_timestamp_version_gate` pins that. As written the row says
  an Arrow `Timestamp(_, _)` becomes `TIMESTAMP(6)` on 2025.x, which is false for the Arrow→Value
  half. (2) It links `specs/_plans/add-timestamp-precision-versioning/datafusion-scan/type-mapping/spec.md`
  — a plan-scoped delta path that `/speq:record` archives, leaving a dead link in a checked-in
  project rules file. The permanent home is `specs/datafusion-scan/type-mapping/`, which already
  exists.
- Fix: In `CLAUDE.md`, rewrite the `Timestamp(_, _)` row (line 245) to separate the two directions:
  the Arrow→Value/EMITS direction stays bare `TIMESTAMP` at every engine version, while a
  *catalog-declared* Iceberg or Delta timestamp is declared `TIMESTAMP(6)` on Exasol 2025.x+ (and on
  an unrecognized version) versus bare `TIMESTAMP` on 8.x. Change the link target to
  `specs/datafusion-scan/type-mapping/spec.md`.

### .github/workflows/ci.yml

#### [TACTICAL_SHORTCUT] The new 8.29.x E2E leg is not a required check and nothing tracks making it one
- Location: lines 453-460, 478-486
- Issue: the comment records that "The 8.29.x leg's check name is new and NOT yet in the ruleset
  (separate manual ops step)", and `fail-fast: false` means the 8.29.x leg reports independently. Until
  the ruleset is updated, `E2E (8.29.x)` can go red without blocking a merge — so the leg this plan
  exists to add gates nothing. The shortcut is named but has no scheduled follow-up, and the project
  rule is that new work is tracked as a GitHub issue.
- Fix: Create a GitHub issue titled along the lines of "Add `E2E (8.29.x)` to main's required-checks
  ruleset" describing the manual ops step, then edit the comment above the `e2e` job in
  `.github/workflows/ci.yml` (lines 458-460) to cite that issue number inline, following the repo's
  `(#27)` inline-reference convention.

## Expert fixes
[none]
