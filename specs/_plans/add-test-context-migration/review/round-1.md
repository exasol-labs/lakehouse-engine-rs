# Plan Review Findings: add-test-context-migration (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 2 (Blockers: 1, Advisory: 1)
- Intent Fidelity blockers: 0

## Premortem

Six months from now this plan failed catastrophically.

1. The crate-wide census gate in task 2.8 ran at the end of group B while group C had not yet removed the 7 adapter-side `impl UdfContext` blocks. The gate saw 9 matches instead of 2, reported a false failure, and the expert implementer spent a cycle diagnosing it before discovering the cross-group ordering assumption.
2. A future SDK version added a new provided method with behavior that differs from the trait default. `BatchCapturingCtx` delegated only the required methods, so scan tests silently ran against the trait default instead of the SDK override. (Mitigated: the plan says "delegate every declared UdfContext method," covering provided methods too.)
3. The SLC reinstall requirement went unnoticed on staging, and the first post-merge deploy pushed a 0.24.0 `.so` into an instance running the 0.23.0 SLC. UDF load failed with a fingerprint mismatch. (Mitigated: the plan documents this consequence in the Impact section.)

Story 1 routes to Task Breakdown. Stories 2 and 3 are handled by the plan as noted.

## Intent Fidelity

No objection. The plan targets issue #383 item 1 (test-double consolidation), drops item 2 (already resolved by commit f1c1954), and excludes item 3 (comment discipline, user-agreed out of scope). The `BatchCapturingCtx` wrapper, the per-file migrations, and the `DefaultsCtx` choice all trace to the user's interview answers.

## Feasibility

No objection. All four 0.24.0 artifacts (`exasol-udf-sdk`, `exasol-udf-macros`, `cargo-exasol-udf` on crates.io; `lc-rust-0.24.0` release assets on GitHub) are verified published. The planner checked the SDK source diff (0.23.1 to HEAD): `context.rs` is unchanged, so no new required method breaks the 2 retained doubles. The Makefile `SLC_VERSION` sed (line 129) and the E2E harness `SLC_VERSION` const both derive from the workspace pin, so one edit moves all three consumers. Task 1.3 checks the derivation. Task 1.4 proves the bump alone compiles.

## Requirement Quality

No objection. Both spec deltas are testable as written. The scan delta pins the per-call capture grouping contract (payload count distinguishable from batch count), the column-count coverage obligation, and the `NextPolicy::Reject` scalar-path proof. The adapter delta pins the `DefaultsCtx` requirement and its reasoning (decision [3], promoted to ADR). The `StubConnections` migration scopes its allowed observable change to the unasserted error cause (decision [7]). The scan delta's retained-doubles scenario names the reachability boundary and the `SinkCtx` limitation. Checked against the permanent spec library: `speq feature validate datafusion-scan/scan-module-structure` and `speq feature validate vs-adapter/adapter-module-structure` both pass with 0 errors.

## Task Breakdown

#### [CLUSTER_INCOHERENCE] BLOCKER
- Location: plan.md, task 2.8 and Parallelization table (groups B and C declared parallel)
- Issue: Task 2.8 asserts "exactly 2 [impl UdfContext] elsewhere, at `src/scan/emit_tests.rs` and `src/scan/test_support_tests.rs`." That "elsewhere" spans the whole `crates/` tree. Groups B and C run in parallel after A. When group B completes task 2.8 while group C is still running, `src/adapter/` still holds 7 `impl UdfContext` blocks (verified: `grep -rn 'UdfContext for' crates/lakehouse-engine/src/adapter/` returns 7 hits today). The gate would see 9 matches, not 2, and fail.
- Fix: Narrow task 2.8's assertion to group B's own scope: it MUST assert "no `impl UdfContext` under `crates/lakehouse-engine/tests/`" and "exactly 2 under `crates/lakehouse-engine/src/scan/`" (the retained doubles). Move the crate-wide "exactly 2 total" assertion to the Verification section's Manual Testing table, which runs after both groups complete. Alternatively, add a sequenced post-B+C task in its own group that depends on both B and C.

## Design Depth

No objection. `BatchCapturingCtx` passes the design-philosophy Quick Diagnostic (answered in plan.md). The wrapper is deep: it replaces a 40-line impl per consumer with one constructor call, and the IPC decode is the one decision it hides as sole owner. No information leakage: the decode format is confined to the wrapper. The retained in-crate doubles are a stated residual in the spec delta (not a silent gap), and neither duplicates the decode this plan consolidates. The `DefaultsCtx` choice in decision [3] is the one cross-module design constraint, and it is pinned normatively in the spec delta rather than left to task prose.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md, Summary (line 1)
- Issue: "replaces 20 of the repository's 21 hand-rolled `UdfContext` test doubles" does not match the plan's own evidence. The Dead Code Removal table lists 19 items (11 scan-test doubles + 1 `StubConnections` + 5 adapter_tests doubles + 1 connection_tests `StubCtx` + 1 unity_schema_tests `UnityConnCtx` = 19). The Migration table also shows 19 replacements and 2 retentions (19 + 2 = 21). The "20" appears inherited from the user brief, which predated the census correction in decision [1].
- Fix: Change the Summary sentence from "replaces 20" to "replaces 19."
