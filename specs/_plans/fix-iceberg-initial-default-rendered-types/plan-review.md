# Plan Review Findings: fix-iceberg-initial-default-rendered-types

## Summary
- Axes checked: 4/4
- Total findings: 9 (Blockers: 0, Findings: 9)
- Intent Fidelity blockers: 0

## Intent Fidelity
No objection — axis checked: issue #367 asks for the default on pre-add rows for `time`, `decimal(p>36)`, `binary`, `fixed(L)`, and `uuid`, rendered as a stored value renders, with the five types (one REQUIRED) in the `initdef` E2E fixture and an updated `LogicalField.initial_default` table. `plan.md` § Summary, § Verification, and § Dead Code Removal cover each item. Interview answers are honored: raw encoding stays for existing types (Background bullet 3 keeps the raw form for native tags), unrenderable defaults fail cleanly instead of NULL (new refusal scenario, D1), nested defaults stay out of scope (Context last bullet). No scope creep: the only extra, the refusal path, follows from interview answer 2.

## Feasibility

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Impact
- Issue: The encoded default lives in the CREATE-time column note (`adapter/pushdown/format/mod.rs` `declare_columns`, `vs-adapter/column-source-notes`), and pushdown plans only from notes. A virtual schema created before this fix keeps notes with no default for these five types, so it still returns NULL (or the required-absent error) until `REFRESH`. The Impact states the refresh requirement only for the refusal ("becomes a refused column after the next `CREATE`, `REFRESH`, or `SET PROPERTIES`"), while "Rows of pre-add files return the declared `initial-default` ... where they returned NULL" reads as immediate.
- Fix: In plan.md § Impact, state that the rendered default also takes effect only after the next `CREATE`, `REFRESH`, or `SET PROPERTIES`, and add a direction to intent.md § Directions that names this as accepted upgrade behavior.

#### [NFR_IGNORED] FINDINGS
- Location: plan.md § Impact; spec delta § Background bullet 3
- Issue: A `binary` default is now carried as hex, which doubles its length, inside the column note. `checked_note` (`adapter/column_notes.rs`) fails the whole `CREATE`/`REFRESH`/`SET PROPERTIES` statement when a note exceeds `MAX_COLUMN_NOTE_CHARS` (2,000,000). Before this plan the default encoded `None`, so a table with a binary default above about 1 MB was creatable; after it, the entire virtual schema fails to create or refresh. The plan does not mention this.
- Fix: Decide and record the behavior in intent.md § Directions: either refuse the column (D1 mechanism) when the encoded default would push the note over the limit, or accept the statement failure as recorded in `vs-adapter/column-source-notes` "A column note longer than the per-column limit fails the statement". If refusing, add the case to the refusal scenario and its unit test.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: intent.md § Directions D2; plan.md § Context bullet 4
- Issue: D2 says the encoding "reuses ... the scan's partition-value parser". `scan::partition_scalar` (`scan/partition_values.rs`) parses every non-binary type with `ScalarValue::try_from_string`, which reads decimal text, not the raw `i128` mantissa, and date/timestamp text, not raw days or ticks. Routing all of `reconstruct_initial_default` through it would silently change every existing raw default (for example mantissa `12345` under `decimal128(10,2)` becomes `12345.00`, not `123.45`), which the interview forbids ("keep raw encoding for existing types").
- Fix: In D2, state that the scan reuses only the hex-decode branch of `partition_scalar` (or a helper extracted from it) for the `binary` and `fixed_size_binary(L)` tags, and that every other tag keeps its current raw parse in `reconstruct_initial_default`.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: spec delta § "Every supported primitive initial-default survives the scan-spec serialization round-trip", AND step "a `fixed_size_binary(L)` default whose hex does not decode to exactly L bytes SHALL fail reconstruction with a clean error"; intent.md D1; spec delta § "A primitive initial-default that cannot be read at its column's declared type refuses the column"
- Issue: D1 and the refusal scenario say an unreadable default refuses only its column, so "a query that reads only other columns of the table SHALL succeed". A wrong-length `fixed` default instead reaches the scan, because `identity_hex` (`adapter/pushdown/format/iceberg.rs`) hex-encodes any `Binary` literal without a length check. `reconstruct_initial_defaults` runs over the whole logical schema in `FieldIdResolution::for_logical_schema`, so the failure breaks every query on the table, including ones that never read the column.
- Fix: Add to the refusal scenario a GIVEN for a `fixed(L)` field whose `initial-default` does not hold exactly L bytes, and require the VS to refuse that column at declaration. Keep the scan-side length check in the round-trip scenario only as a defense for a hand-written spec, and say so in that AND step.

#### [COMPLETENESS_GAP] FINDINGS
- Location: spec delta § Background bullet 3 ("a decimal outside `1 ≤ p ≤ 36`, `0 ≤ s ≤ p` map to the `utf8` tag and carry the default as the text a stored value of the column reads as")
- Issue: The domain also covers `s > p` (the removed scenario's example `decimal(5,10)`). `render_identity_value` builds a `Decimal128Array` with `with_precision_and_scale`, which Arrow rejects when scale exceeds precision, so `file_column_value` returns `None` and the render fails. The column is then refused, not filled with text as the Background claims. No scenario or test covers it.
- Fix: Narrow Background bullet 3 to the decimals the renderer handles (`37 ≤ p ≤ 38`, `0 ≤ s ≤ p`), add `decimal(5,10)` with a default as a GIVEN of the refusal scenario, and add it to `declaration_refuses_a_column_whose_initial_default_cannot_be_rendered`. Verify the Arrow rejection in that unit test rather than assume it.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: spec delta § "A primitive initial-default that cannot be read at its column's declared type refuses the column", GIVEN
- Issue: The only example is "a literal of another primitive type than the field". iceberg-rust parses `initial-default` JSON against the field's own type, so that case is reachable only by building a `NestedField` by hand. The scenario names no reachable input, so it gives no guidance on which real tables are refused.
- Fix: Replace the example with the reachable cases (a `decimal` with `s > p`, a `fixed(L)` default of the wrong length, and the over-limit binary default if the NFR finding above chooses refusal). Keep the mismatched-literal case as a unit-test-only guard.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: `specs/scan-types/type-mapping-iceberg-logical/spec.md` line 28 (recorded), not touched by the plan
- Issue: The recorded spec says the DECIMAL domain check is read by "the `initial-default` encoding gate (`scan-read-path/scan-execution-field-id-projection-absent-fields`)". This plan removes the gate scenario: the domain now picks raw mantissa or rendered text, not whether a default is encoded. The cross-reference goes stale.
- Fix: Add a CHANGED delta for `scan-types/type-mapping-iceberg-logical` that rewords the reference to "the `initial-default` encoding, which picks the raw mantissa or the rendered text", or confirm in plan.md § Context that the wording still holds and why.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Verification § Scenario Coverage, row 3
- Issue: The E2E test `e2e_added_columns_initial_default_fill_all_types` currently carries `/// Scenario: Scan-driving UDF invocations are schema-qualified from the running adapter script's schema`, not this feature's scenario. AGENTS.md requires one `/// Scenario: <title>` line per implemented scenario. Also, `ExpectedValue` in `crates/lakehouse-e2e/tests/common/seed.rs` has no variant for the post-add value equaling the pre-add value by construction; the scenario's "rendered exactly as a stored value ... renders in a row of the second file" needs a row-to-row comparison, not two fixed literals.
- Fix: In row 3, state that the test gains `/// Scenario: An initial-default of a type without a native Arrow tag reads as a stored value of its column reads`, and that the new columns assert each pre-add value equals the post-add value of the same column in the same result set, in addition to the expected literal.

## Design Depth

#### [DESIGN_VIOLATION] FINDINGS
- Location: plan.md § Context bullet 4; intent.md D2
- Issue: Principle: information hiding and general-purpose interfaces. `render_identity_value` and `scan::partition_scalar` embed their partition context in error text ("the partition struct holds a value of another type than the ... column", "a binary partition value must be hex"). Reusing them for defaults puts those strings into the refusal reason of a default-bearing column, which the new scenario requires to "state that its `initial-default` cannot be read at the column's declared type". The plan does not say how the shared renderer's errors are made neutral.
- Fix: In plan.md, state that the shared renderer returns a context-free cause (or takes the subject as a parameter) and that each caller wraps it with its own subject (partition value or `initial-default`). Rename `render_identity_value` to a neutral name if it now serves both callers.

No objection on ADRs — axis checked: intent.md has no `ADR: yes` direction (no overpromotion surface); no `architecture.md` delta is present and the plan adds no component, boundary, or external dependency (`hex` and the renderer already exist), so no architecture drift; checked the accepted ADRs `implement-iceberg-column-projection-rule-3-for-all-absent-fields`, `a-files-identity-partition-value-wins-over-its-column-ahead-of-the-initial-default`, and `binary-reads-as-lowercase-hex-text`, which the plan conforms to, so no ADR conflict.
