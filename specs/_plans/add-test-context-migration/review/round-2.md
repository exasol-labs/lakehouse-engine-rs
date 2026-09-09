# Plan Review Findings: add-test-context-migration (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 0 (Blockers: 0, Advisory: 0)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- Resolved: [CLUSTER_INCOHERENCE] Task 2.8 crate-wide gate false-fails under parallel groups — Task 2.8 now reads "scoped to group B's territory" and asserts only "no `impl UdfContext` under `crates/lakehouse-engine/tests/`" and "exactly 2 under `crates/lakehouse-engine/src/scan/`." The crate-wide "exactly 2 total" assertion moved to the Manual Testing table row 5 ("Both (residual census)"), which runs after both groups complete. Decision-log entry `[plan-review] Task 2.8 crate-wide gate false-fails under parallel groups` records the direction change. The fix is implemented as directed.
- Resolved: [PROSE_UNCLEAR] Summary double count corrected from 20 to 19 — plan.md Summary now reads "replaces 19 of the repository's 21 hand-rolled `UdfContext` test doubles." The count matches the Dead Code Removal table (11 + 1 + 5 + 1 + 1 = 19) and the Migration table (19 replacements + 2 retentions = 21). Decision-log entry `[plan-review] Summary double count corrected from 20 to 19` records the change.

## Premortem

Six months from now this plan failed catastrophically.

1. An implementer migrating one of the 7 uniform raw-scan tests in task 2.2 supplied fewer `Value::Null` columns than the production code indexes. `TestContext::get` returned an out-of-range error instead of the `Ok(None)` the deleted double answered at any index. The test failed with a confusing error rather than the intended assertion. Mitigated: task 2.2 states the obligation ("supply one column per index the test's production path reads") and explains the mechanism. The entire group B routes to the expert implementer, who has the context to determine per-file index counts.
2. A contributor replaced `DefaultsCtx` with `TestContext` in `adapter_tests.rs`, unaware that `TestContext` overrides `node_count`. The trait-default fallback test kept passing (both return 0 today) but stopped asserting the trait default. The coverage trap held until a future SDK changed its default. Mitigated: the adapter spec delta normatively requires `DefaultsCtx` and prohibits `TestContext` for that call site. Decision [3] promotes to ADR and records the reasoning.
3. A manually provisioned staging environment pushed the 0.24.0 `.so` without reinstalling the SLC. UDF load failed with a fingerprint mismatch. Mitigated: the Impact section documents the deploy consequence explicitly, and the automated paths (Makefile, E2E harness) retarget by construction.

All three stories are handled by existing plan defenses.

## Intent Fidelity

No objection. The plan targets issue #383 item 1 (test-double consolidation via SDK 0.24.0), drops item 2 (resolved by commit f1c1954, recorded in decision [9] and the Dead Code Removal section), and excludes item 3 (user-agreed out of scope, recorded in the interview). The 19 migrations, 2 retentions, and one shared wrapper trace to the user's interview answers. No scope creep beyond what the user scoped.

## Feasibility

No objection. All four 0.24.0 artifacts are verified published (3 crates on crates.io, both SLC release assets on GitHub). The planner checked `context.rs` stability from v0.23.1 to HEAD; the 0.23.0 to 0.23.1 gap is immaterial because task 1.4 compiles all 21 existing doubles against the bump before any edit, catching any new required method regardless of the diff range checked. The `Makefile` SLC_VERSION sed and the E2E harness compile-time const both derive from the workspace pin. Task dependencies are sound: A gates B and C, and B and C share no source file, no spec delta, and no knowledge entry.

## Requirement Quality

No objection. Both spec deltas validate with 0 errors (`speq feature validate` confirmed). The scan delta pins per-call capture grouping, column-count obligation, `NextPolicy::Reject` scalar-path proof, both spelling forms, and dev-dependency feature gating. The adapter delta pins the `DefaultsCtx` normative requirement and the `TestContext` prohibition for the trait-default assertion site. The `StubConnections` migration scopes its allowed observable change to the unasserted error cause (decision [7]). No conflict with the permanent spec library: the existing "Each scan submodule owns its tests" scenario requires "a test helper shared across submodules MUST live in one shared module rather than being duplicated," which aligns with this plan's `BatchCapturingCtx` consolidation.

## Task Breakdown

No objection. Task 2.8 is now properly scoped to group B's territory (no impls under `tests/`, exactly 2 under `src/scan/`). The crate-wide census runs in Manual Testing after both groups complete. Task 3.6 covers the adapter side independently. Groups B and C share no source file and no spec delta. Expert tagging on group B is appropriate: the wrapper in 2.1 gates every migration, and 4 of 7 migrations encode behavioral traps (telemetry counting, scalar-path rejection, two-argument reconstitution, connection error wrapping) that require reading the deleted double's semantics rather than copying a shape.

## Design Depth

No objection. `BatchCapturingCtx` passes the design-philosophy Quick Diagnostic, answered in plan.md. The wrapper is deep (replaces a 40-line impl per consumer with one constructor call). The IPC decode is the one decision it hides and it is the sole owner. No information leakage: the decode format is confined to the wrapper. The retained in-crate doubles are a stated residual in the spec delta, not a silent gap, and neither duplicates the decode this plan consolidates. The `DefaultsCtx` constraint is pinned normatively in the spec delta rather than left to task prose.

## Prose Quality

No objection. Sentences are within the 25-word descriptive cap. Active voice throughout. No em dashes, no semicolons, no filler or hedging. RFC keywords (SHALL, MUST, MAY) in the spec deltas follow RFC 2119 usage. Vocabulary is consistent (the plan uses "double" for test doubles, "wrapper" for BatchCapturingCtx, "pin" for version pinning throughout). The Summary leads with the outcome. The Consequences table puts the rationale after the decision. No process narration.
