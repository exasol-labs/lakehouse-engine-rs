# Plan Review Findings: fix-path-style-default (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 1 (Blockers: 0, Advisory: 1)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- Resolved: [AMBIGUOUS_REQUIREMENT] Test Disposition cited the wrong test name and omitted the UC variant. The revised plan.md lines 166-167 now list both tests by their correct names: `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend` at `:331-363` and `resolve_uc_vended_storage_signature_takes_only_a_credential_free_store_address` at `:377`. Both names and both line ranges match the live codebase (verified against `crates/lakehouse-catalog/tests/catalog_public_surface.rs`). The Scenario Coverage table (lines 290-291) carries matching entries. The mechanical-literal count at line 183 now reads "Sixteen sites (in fourteen files):" which agrees with the enumerated list and with task 1.6's reference. The decision-log entry at lines 160-164 records the finding and direction change.

## Premortem

Six months from now this plan failed catastrophically. Why?

1. The `validate_creds` guard rejects a legitimate CONNECTION that an operator migrated from an older version. The operator's MinIO CONNECTION carried `endpoint` and relied on the old `true` default. After upgrading, the guard fires with a named error telling the operator to add `path_style: true`. The operator adds the field, and the CONNECTION works. Failure duration: one query attempt. This is the intended design (decision [3]), and the guard's error message is specified to name both values and what each reaches (connection-credentials delta, new scenario, clause 2). Not a plan defect.

2. The three-step vended chain silently changes behaviour for a Lakekeeper or self-hosted catalog that vends `s3.path-style-access: true` beside a CONNECTION that stated `path_style: false`. The CONNECTION now wins, overriding the catalog. The operator gets virtual-hosted addressing against a path-style store and requests 404. The plan names this in Migration row 6 ("Remove `path_style` to keep the vended value") and in the manual testing table (lines 303-304, the characterization step). The spec delta (pushdown-planning, amended clause) says a stated `false` SHALL WIN over a vended `true`. This is the user's chosen scope, not a defect. No plan change needed.

3. A future plan widens `StaticStoreAddress` again (a fourth field) and forgets to update the two source-level probes. The probes read the struct's declaration rather than a hardcoded field list (storage-backend-enum delta, clause "that probe SHALL read the field list out of the type's own declaration"), so they cover any added field automatically. Not a plan defect.

No premortem story identified a plan defect. All three routed to design decisions the plan already documents.

## Intent Fidelity

No objection -- axis checked. The plan delivers all three parts the user chose in the interview: the default flip (decision [2], `false` on the non-vended path), the `Option<bool>` widening (decision [1]), and the vended CONNECTION-wins admission (decision [4]). The `validate_creds` guard (decision [3]) is a safety consequence grounded in a verified code fact. Four deltas rather than two (decision [8]) prevent spec-library contradictions. No part of the ask is dropped, deferred, or reinterpreted.

## Feasibility

No objection -- axis checked. Every code fact verified against the live codebase: `StorageCreds::from_json` defaulting at `creds.rs:214-217` (confirmed: `json.get("path_style")` with a `.unwrap_or(true)` chain); `build_undecorated_store`'s `if storage.path_style` gate at `object_store.rs:227` (confirmed: endpoint and virtual-hosted style set only inside that branch); `StaticStoreAddress` declaring two fields at `storage.rs:284` (confirmed: `endpoint` and `region` only); `s3_backend` at `storage.rs:350` with current derivation `vended.path_style.unwrap_or(!endpoint.is_empty())` at `:372` (confirmed); the stale comment at `connection_tests.rs:56` (confirmed: "path_style defaults to true (MinIO behaviour preserved)"). Version numbers are current: `lakehouse-engine` at `0.45.0`, `lakehouse-catalog` at `0.2.0` (both confirmed from Cargo.toml). The Iceberg REST spec and Delta protocol were fetched as external documents (planning.md lines 63-67) rather than cited from memory. The sixteen ConnectionCreds literal sites (in fourteen files) and two StorageCreds sites match the enumerated list (count verified against grep output).

## Requirement Quality

No objection -- axis checked. Each spec delta's normative clauses are testable: the tri-state parse (absent/true/false/non-boolean mapped to None/Some(true)/Some(false)/None), the `backend` resolution (None to `false`, Some(v) to v), the guard's five edge cases (fires, does not fire under explicit value, no endpoint, vending, and no credential leak), and the three-step vended chain (connection stated beats vended, vended beats derivation, derivation still works when neither source states). The four supersession clauses in the connection-credentials and pushdown-planning deltas each quote the text they replace and name both the old premise and why it no longer holds. The two additional deltas (storage-backend-enum and catalog-crate-public-surface) widen recorded "EXACTLY two" field counts to "EXACTLY three" and name the field being added. No clause conflicts with an unedited recorded spec: the only two-field-count clauses in the library are the ones these deltas supersede (`storage-backend-enum/spec.md:215` and `catalog-crate-public-surface-extensions/spec.md:86`). The characterization gate (plan.md lines 169-179, "Must NOT change") pins every vended fixture's pre-change behaviour, which protects the Iceberg REST compliance evidence.

## Task Breakdown

No objection -- axis checked. Every delta traces to at least one task group: connection-credentials to group A (1.1-1.10), pushdown-planning-cloud-credentials and storage-backend-enum and catalog-crate-public-surface-extensions to group B (2.1-2.6), documentation to group C (3.1-3.2). The A-then-B-then-C sequence is grounded in a compilation dependency: the `Option<bool>` type change in `creds.rs` (group A) must land before the vended merge (group B) compiles, and documentation (group C) documents both resolution paths. Group B is tagged expert for task 2.2, which composes three precedence sources against the `register_side_store` coupling. Task 2.3 (confirmation that Unity needs no edit) is justified by Unity's existing reduction to `path_style: None` in the shared `s3_backend`, so it is a verification task rather than hidden work. No task is too large to verify as one unit: the largest (1.6, sixteen mechanical literal changes) is bounded by an enumerated list.

## Design Depth

No objection -- axis checked. The plan introduces no new module, interface, or boundary. The tri-state is preserved from parse to resolution, so the two resolution points (non-vended: `StorageCreds::backend`, vended: `s3_backend`) each read the distinction rather than reinventing it. `StorageProps` keeps its `bool` because it is the resolved type, not a second owner of the "what does unstated mean" decision (decision [5]). `StaticStoreAddress` widens following its own doc comment's anticipated edit pattern, and the one-construction and field-privacy rules cover the third field without structural change. No tactical shortcut is taken: the plan delivers the full scope rather than deferring the vended admission.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: `plan.md` lines 38 and 41, Design section
- Issue: Two em dashes serve as label-content separators: "**Goals** -- Make an unstated..." and "**Non-Goals** -- Changing `StorageProps`..." The writing guardrails ban em dashes and prescribe a comma, period, colon, parentheses, or a new sentence.
- Fix: Replace each em dash with a colon: "**Goals**: Make an unstated..." and "**Non-Goals**: Changing `StorageProps`..."
