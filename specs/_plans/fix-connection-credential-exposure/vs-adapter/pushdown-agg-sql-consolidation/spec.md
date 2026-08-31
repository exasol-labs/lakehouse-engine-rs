# Feature: Pushdown Aggregate SQL Consolidation

Gives three hand-repeated pieces of the aggregate-pushdown SQL assembly one owner each — the König–Huygens sufficient-statistics fragments behind the STDDEV/VARIANCE merge expressions, the "CAST to the declared type unless it is the `VARCHAR(2000000)` default" rule, and the aggregate-function-name to `AggKind` mapping — split out of `vs-adapter/pushdown-planning-grouped-agg` and `vs-adapter/pushdown-planning-single-group-agg` as issue [#179](https://github.com/exasol-labs/lakehouse-engine-rs/issues/179)'s SQL-assembly scope. Every merge formula, cast decision, and detection outcome those features record is unchanged and is consumed here, not restated: this feature records only who owns each repeated construction, and that the generated SQL is byte-identical.

## Background

* **This delta is issue #135. It adds ONE scenario and changes no consolidation rule.** The single-owner sufficient-statistics fragments, the `stddev_of` guard, the `GREATEST(0.0, …)` clamp, the corrected NULL-path rationale, the single declared-type CAST owner, the two unconditional `CAST(NULL AS …)` arms, and the aggregate-name-to-`AggKind` tables are all UNCHANGED.
* **SUPERSEDES this feature's two byte-identity gates for the `storage` value alone.** The recorded clauses are "the correction SHALL change NO rendered SQL: every golden fixture under `testdata/dispatch_golden/` MUST stay byte-identical and every existing merge assertion MUST pass with NO change to any expected value" and "the generated SQL for every single-group, grouped, and empty-result pushdown request MUST be byte-identical to its pre-consolidation output, and every existing `dispatch_golden` fixture and grouped-aggregate assertion MUST pass with NO change to any expected value". Both hold for every byte of the rendered SQL EXCEPT the `storage` value of the common scan-spec literal, which becomes the tagged wrapper of `vs-adapter/scan-spec-credential-reference`.
* **The carve-out is one value, not a waiver, and the fixture split is exact.** Eighteen of the twenty-four committed `dispatch_golden` fixtures carry a `storage` value and are REGENERATED; the six `empty_*` fixtures carry none and MUST stay byte-identical. Naming the split is what stops "regenerate the goldens" from silently accepting a diff in a fixture that must not change.
* **Every merge assertion and every aggregate-name assertion is unaffected**, because none of them reads the `storage` value: the consolidation this feature records is about the outer merge expression and the declared-type CAST, neither of which touches the common scan-spec literal.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract and the fixture-regeneration requirement.** This feature CITES it and restates none of it.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The aggregate byte-identity gate carves out the storage value and nothing else

* *GIVEN* this feature's two recorded byte-identity gates over the `dispatch_golden` fixtures and the grouped-aggregate and merge assertions
* *WHEN* the scan-spec `storage` value becomes the tagged wrapper of `vs-adapter/scan-spec-credential-reference`
* *THEN* the rendered SQL for every single-group, grouped, and empty-result pushdown request MUST stay byte-identical to its pre-change output EXCEPT for that one value, and every merge, aggregate-name, and declared-type-CAST assertion MUST pass with NO change to any expected value
* *AND* the eighteen `dispatch_golden` fixtures that carry a `storage` value SHALL be REGENERATED and each SHALL then contain the reference encoding and no credential value, while the six `empty_*` fixtures carry no `storage` value and SHALL stay BYTE-IDENTICAL and be asserted unchanged
* *AND* a diff in any of those six, or a diff outside the `storage` value in any of the eighteen, SHALL be treated as a regression rather than an expected update, which is the contract the golden module's own header states
<!-- /DELTA:NEW -->
