# Plan Review Findings: fix-path-style-default (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 2 (Blockers: 1, Advisory: 1)
- Intent Fidelity blockers: 0

## Premortem

Six months from now this plan failed catastrophically. Why?

1. The Test Disposition table names a test that does not exist at the cited location. The implementer searches for `resolve_vended_storage_signature_takes_only_a_credential_free_store_address` at `catalog_public_surface.rs:331-363`, finds a different test, and either edits the wrong test or wastes time tracing the mismatch. The Iceberg-path arity pin test goes unedited, and the third field is never proven reachable from outside the crate on that path.

2. The "Fourteen sites" count in the mechanical-literal section contradicts the sixteen individual line references the list enumerates. The implementer trusts the count, stops after fourteen edits, and the workspace fails to compile on the remaining two. The error is self-correcting (the compiler catches it), but the count plants doubt about whether the list itself is complete.

## Intent Fidelity

No objection -- axis checked. The plan delivers the three parts the user chose in the interview: the default flip (`false` on the non-vended path), the `Option<bool>` widening, and the vended CONNECTION-wins admission. The `validate_creds` guard (decision [3]) is a necessary safety consequence of the default flip, grounded in a verified code fact (`build_undecorated_store` discards `endpoint` when `path_style` is `false`), not scope creep. Four spec deltas are justified by decision [8]: two additional features carry normative clauses naming the store-address field count as exactly two, so leaving them standing beside a three-field type would be a spec-library contradiction.

## Feasibility

No objection -- axis checked. Every code fact the plan builds on was verified against the codebase: `StorageCreds::from_json` defaulting to `true` at `creds.rs:214-217`; `build_undecorated_store`'s `if storage.path_style` gate at `object_store.rs:227-231`; `StaticStoreAddress` declaring two fields at `storage.rs:283-287` with a doc comment anticipating deliberate widening; `CatalogConnectionPassword` always serializing `path_style` at `stack.rs:337` (so no E2E fixture trips the new guard); the installer template at `install.sh:1478-1484` naming no `endpoint` and no `path_style`. Version numbers are current: `lakehouse-engine` at `0.45.0` (bump to `0.46.0`), `lakehouse-catalog` at `0.2.0` (bump to `0.3.0`). The Iceberg REST spec and Delta protocol were fetched and searched rather than recalled from memory. The external-document evidence is sound.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: `plan.md` line 167, section "Test Disposition / Must change"
- Issue: The table row cites `resolve_vended_storage_signature_takes_only_a_credential_free_store_address` at `crates/lakehouse-catalog/tests/catalog_public_surface.rs:331-363`. The test at line 341 (within that range) is `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend`. The name the plan cites is the UC variant, which lives at line 377 (`resolve_uc_vended_storage_signature_takes_only_a_credential_free_store_address`). Both tests pin the `StaticStoreAddress` parameter shape and both doc comments name the field count, so both need to name the third field, but the plan references only one and gives it the wrong name at the wrong location. Task 2.6 ("confirm the two source-level probes cover the third field") depends on the implementer finding both tests.
- Fix: Replace the test name with `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend` and keep the line range `:331-363`. Add a second row for the UC variant: `resolve_uc_vended_storage_signature_takes_only_a_credential_free_store_address` at `:377`, with disposition "Its doc comment names the parameter shape and must name the third field", so both arity-pin tests are traceable to task 2.6.

## Task Breakdown

No objection -- axis checked. Every spec delta traces to at least one task group (connection-credentials to group A; pushdown-planning-cloud-credentials, storage-backend-enum, and catalog-crate-public-surface-extensions to group B). The A-then-B-then-C sequence is correct: the `Option<bool>` type change must land before the vended merge compiles, and documentation documents both paths. Group B is tagged expert for task 2.2, which composes three precedence sources against the `register_side_store` coupling. The "Must NOT change" characterization gate (plan.md lines 170-178) is a sound verification strategy that pins the pre-change behavior of every vended fixture omitting `path_style`.

## Design Depth

No objection -- axis checked. The plan introduces no new module, interface, or boundary. Resolution ownership is clean: `StorageCreds::backend` is the single non-vended selector, `s3_backend` is the single vended selector, and each owns exactly one resolution of an unstated `path_style`. The `StaticStoreAddress` widening follows the type's own doc comment, which anticipated a deliberate edit to its one conversion. `StorageProps` stays unchanged (decision [5]), avoiding a 30-fixture ripple for no behavioral gain. The tri-state is preserved from parse to resolution, so the two resolution points read the distinction rather than inventing it.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: `plan.md` line 182, section "Mechanical literal updates, no assertion change"
- Issue: The text reads "ConnectionCreds literals stating path_style become Some(...). Fourteen sites:" but the enumerated list that follows contains sixteen individual line references (two entries each carry an "and :NNN" appendix for a second site in the same file). Task 1.6 (line 219) says "the sixteen ConnectionCreds and StorageCreds literal sites", which sums to a different total depending on whether "fourteen" counts files or edit locations. The count mismatch plants doubt about list completeness.
- Fix: Change "Fourteen sites:" to "Sixteen sites (in fourteen files):" so the count matches the enumerated line references and aligns with task 1.6's combined total of sixteen ConnectionCreds sites plus two StorageCreds sites.
