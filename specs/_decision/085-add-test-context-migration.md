# Decisions: add-test-context-migration

## ADR: DefaultsCtx replaces NoopCtx, not TestContext

**ID:** defaultsctx-replaces-noopctx-not-testcontext
**Plan:** add-test-context-migration
**Status:** Accepted

### Context
`exasol-udf-sdk` 0.24.0's `TestContext` overrides `node_count` and returns 0 from its own metadata. The `cluster_nodes_from_context_defaults_to_one_when_node_count_zero` test drives the adapter's `0 → 1` fallback and must exercise the `UdfContext` trait default, not a double's return value. Using `TestContext` everywhere would make the test assert the double instead of the trait default, a trap invisible in a green run.

### Decision
`NoopCtx`'s call sites migrate to `DefaultsCtx`, a double that does not override `node_count`, so the fallback test keeps asserting the trait behavior.

### Options Considered
`TestContext` everywhere. Rejected because it overrides `node_count` and would silently swap the test's target from the trait default to the double's own value.

### Consequences
The node-count fallback test stays a valid characterization of the trait default rather than a tautology against a double.

## ADR: Wrapper lives under tests/; two in-crate doubles are retained

**ID:** batchcapturingctx-under-tests-two-doubles-retained
**Plan:** add-test-context-migration
**Status:** Accepted

### Context
Eleven scan-test `UdfContext` doubles duplicate an identical `emit_record_batch_ipc` decode loop. `crates/lakehouse-engine/src/scan/emit_tests.rs`'s `CapturingCtx` and `src/scan/test_support_tests.rs`'s `SinkCtx` each hand-roll a variant of this shape, but `tests/` modules are not nameable from `src/`, so a single shared double cannot reach both sides of the crate boundary without a permanent cross-boundary export.

### Decision
`BatchCapturingCtx` lives in `tests/scan_fixture/` and owns the decode for every integration test under `tests/`. `CapturingCtx` and `SinkCtx` stay hand-rolled in `src/scan/`, since together they cover only two call sites and neither duplicates the decode this delta consolidates.

### Options Considered
A cross-boundary `#[path]` include or a feature-gated `pub mod test_support` re-exporting the `tests/` double into `src/`. Rejected because it adds a permanent structural cost to save two call sites, and `SinkCtx` cannot reuse `DefaultsCtx` since it needs to accept `emit_record_batch_ipc` itself.

### Consequences
Exactly two hand-rolled `impl UdfContext` doubles remain in the crate, both under `src/scan/`, and the crate-wide gate in `tests/scan_udf_context_surface.rs` asserts that count explicitly.
