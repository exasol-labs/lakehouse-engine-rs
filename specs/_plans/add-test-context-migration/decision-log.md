# Decision Log: add-test-context-migration

## Interview

**Q:** `ConnectionCreds` already has `#[derive(Clone, Default)]` (commit f1c1954). Include as verification task?
**A:** Drop — already resolved before this plan. No task.

**Q:** `TestContext` (SDK 0.24.0) has no `emit_record_batch_ipc` override. How handle the 11 scan doubles?
**A:** One shared `BatchCapturingCtx` wrapping `TestContext`, overriding only `emit_record_batch_ipc`. No upstream issue.

**Q:** Issue #383 item 3 (comment discipline)?
**A:** Out of scope. Existing CLAUDE.md rule covers it.

## Design Decisions

### [1] Per-call capture grouping in `BatchCapturingCtx`

- **Decision:** Group captures per `emit_record_batch_ipc` call; expose flattened batches, call count, and row total.
- **Alternatives:** Flat `Vec<RecordBatch>`. Rejected: `scan_telemetry.rs` asserts payload count, and one payload may decode to several batches.
- **Promotes to ADR:** no

### [2] `DefaultsCtx` replaces `NoopCtx`, not `TestContext`

- **Decision:** `NoopCtx`'s call sites use `DefaultsCtx`.
- **Alternatives:** `TestContext` everywhere. Rejected: `TestContext` overrides `node_count` (returns 0 from its own metadata), so the `0 → 1` fallback test would assert the double instead of the trait default. The trap is invisible in a green run.
- **Promotes to ADR:** yes

### [3] Wrapper lives under `tests/`; 2 in-crate doubles retained

- **Decision:** `BatchCapturingCtx` in `tests/scan_fixture/`. `CapturingCtx` (`emit_tests.rs`) and `SinkCtx` (`test_support_tests.rs`) stay hand-rolled.
- **Alternatives:** Cross-boundary `#[path]` or feature-gated `pub mod test_support`. Rejected: permanent structural cost for 2 call sites that duplicate no decode. `SinkCtx` also cannot use `DefaultsCtx` (needs accepting `emit_record_batch_ipc`).
- **Promotes to ADR:** yes

### [4] `StubConnections` migrates; unasserted error cause changes

- **Decision:** Replace with `TestContext`. The wrapped cause changes from `ConnectBack` to `Unimplemented`, but `connection_password` wraps any variant and no assertion reads the cause substring.
- **Alternatives:** Retain `StubConnections`. Rejected: the fidelity loss is confined to an unasserted substring.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Task 2.8 scope narrowed for parallel execution

- **Finding:** BLOCKER — task 2.8 asserted a crate-wide census while groups B and C run in parallel, causing false failures.
- **Fix:** Narrowed to group B's scope (no impls under `tests/`, exactly 2 under `src/scan/`). Crate-wide check moved to Manual Testing (post-both-groups).

### [plan-review] Summary count corrected

- **Finding:** ADVISORY — Summary said "20" but Dead Code Removal lists 19.
- **Fix:** Changed to "19".
