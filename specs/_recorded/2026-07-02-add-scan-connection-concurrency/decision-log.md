# Decision Log: add-scan-connection-concurrency

Date: 2026-07-02

GitHub issues: https://github.com/exasol-labs/lakehouse-engine-rs/issues/47 (this plan) and https://github.com/exasol-labs/lakehouse-engine-rs/issues/43 (pre-existing bug report for the exact `ctx.node_count()==0` handshake symptom the dependency bump fixes). Reference both as `Closes #47, Closes #43` in the implementing commit.

## Interview

**Q1 (plan shape):** Two of the three hypothesis bullets (#shards=#nodes, one DataFusion instance per node getting max resources) are already expressible today via existing VS properties (`PARALLELISM_FACTOR=1` → `G = node_count`; `DATAFUSION_THREADING_MODE=AUTO`/`FIXED` gives that single instance all the node's cores). Only "max file connections per node to saturate network/IO" has no existing knob. Options: (a) benchmark-first, gate the new knob on results [recommended]; (b) build the knob now and benchmark alongside it; (c) benchmark only, no new knob.
**A1:** "Bump to lc-rs v0.20.1 first because the number of nodes/cores was wrongfully exposed; add the knob and benchmark; goal of the plan shall be to get close to the performance of the native IMPORT (see benchmark results) → benchmarks/e2e/benches shall not be part of the spec deltas." → Resolved as option (b): build the knob now AND benchmark, gated on the 0.20.1 dependency-version prerequisite, with benchmark/e2e work excluded from spec deltas (supporting verification only, referenced in plan.md's validation/ADR narrative).

**Q2 (knob placement):** VS property like `PARALLELISM_FACTOR` [recommended], or a fixed hardcoded default with no operator-facing property?
**A2:** VS property, like `PARALLELISM_FACTOR`.

### Amendment interview (2026-07-02, new 180M-row benchmark evidence)

**Q3 (evidence incorporation):** A 2026-07-01 full-`lineitem` run (60 files, 180M rows) found the VS full-emit `CREATE TABLE AS SELECT *` (~151 s) is ~1.9× slower than native `IMPORT INTO` (~80.4 s) — flipping the original aggregate-path finding — but the run recorded the confounded `CLUSTER_NODES=1` (the pre-0.20.1 `node_count()==0` bug that Task 1 fixes). Should we (a) expand this plan to build emit-path optimization now, (b) incorporate the evidence as rationale only + add a named re-gate task + document emit-path work as evidence-gated deferred work, or (c) ignore it?
**A3:** No user response received within the interview timeout. Proceeded on the project's established "evidence-gated future work" convention already codified in `docs/performance.md` §"Future engine work (deferred, evidence-gated)" → resolved as option (b): evidence-only, plus a named re-gate task (plan.md Task 10) and a documented, evidence-gated deferred-work item (plan.md §Deferred work). No code/scope expansion.

## Design Decisions

### [1] Prerequisite dependency bump 0.20.0 → 0.20.1 ships without a spec delta

- **Decision:** Bump `exasol-udf-sdk`/`exasol-udf-macros` `0.20.0 → 0.20.1` (fixes upstream `language-container-rs` issue #41: `ctx.node_count()` returned `0` on the single-call VS-adapter path). Ship it as a task only, with no Given/When/Then scenario. Closes this repo's own bug report of the same symptom, **issue #43** ("`CLUSTER_NODES` always 1 on multi-node clusters"), which had left the fix undecided pending #41 and flagged `create_vs_records_cluster_nodes_property` as an assertion to revisit.
- **Alternatives:** Author a `resolve_cluster_nodes` / `create-virtual-schema-adapter-notes` CHANGED delta (as the prior `2026-07-01-fix-createvs-cores-nodecount` plan did for its 0.19.1→0.20.0 bump, because that plan rewrote `resolve_cluster_nodes`).
- **Rationale:** This repo's `resolve_cluster_nodes` (`adapter/mod.rs:693-702`) is unchanged — it already maps `node_count()==0 → 1` and passes positive counts through verbatim. The observable contract is already fully covered by `cluster_nodes_passes_through_reported_node_count` and the `0→1` fallback scenarios; the fix lives entirely in the upstream SDK handshake threading. Inventing a scenario for a pure version pin with zero contract change would be spec noise. Issue #43's own "not yet verified" list resolves cleanly to option (a) it posed ("fix consumed via a new SLC release") now that #41 shipped.
- **Promotes to ADR:** no

### [2] One `S3_MAX_CONNECTIONS` knob, not a dual per-file/per-node pair

- **Decision:** Expose a single operator VS property `S3_MAX_CONNECTIONS` (mirroring the native `IMPORT FROM PARQUET` `MaxConnections` vocabulary) rather than mirroring the native importer's full dual model (`MaxConnections` = parallel reads within a file + `MaxConcurrentReads` = files in parallel per node).
- **Alternatives:** Ship both axes as two properties now.
- **Rationale:** The user's hypothesis is a single lever ("max file connections per node to saturate"). One knob covers it; a second axis is unproven complexity. Defer the split until the benchmark shows one knob is insufficient (YAGNI). Establishes the project convention that new tuning axes ship as one operator knob until a benchmark proves a second is needed.
- **Promotes to ADR:** yes

### [3] Explicit-wins-else-AUTO, with no separate MODE property

- **Decision:** Resolve `S3_MAX_CONNECTIONS` as: explicit positive integer verbatim (FIXED-like), else AUTO-derive from `nr_of_cores` and the per-node UDF-instance share (mirroring `auto_threads_per_udf`), with `0` cores → built-in default. No `..._MODE` property.
- **Alternatives:** A full `DATAFUSION_THREADING_MODE`-style AUTO/FIXED mode property.
- **Rationale:** The threading MODE property exists only because partitions and threads are two coupled fields needing a shared selector. Connection concurrency is one field, so a mode property is redundant machinery — the `PARALLELISM_FACTOR` single-property-with-computed-default pattern fits better.
- **Promotes to ADR:** no

### [4] Apply the budget via object_store ClientOptions, not DataFusion target_partitions

- **Decision:** Size the budget onto the S3 client through `AmazonS3Builder::with_client_options(ClientOptions)` (object_store 0.13.2, method confirmed present), targeting the HTTP connection pool — the axis independent of CPU thread count.
- **Alternatives:** DataFusion `target_partitions` file-group splitting (rejected: that is the CPU/threading axis, already a knob); `datafusion.execution.meta_fetch_concurrency` (rejected: only affects schema/stats reads, not data-scan throughput).
- **Rationale:** The object-store HTTP client pool is what genuinely maps to "how many concurrent fetches from S3 per UDF instance". The exact pooling call (`with_pool_max_idle_per_host` and/or companions) and whether it must be paired with file-group splitting is the expert-tagged mechanism decision (Task 5); the AUTO-derivation formula is the expert-tagged decision in Task 3. Records that object-store connection concurrency is a first-class tuning axis distinct from the DataFusion thread/partition budget.
- **Promotes to ADR:** yes

### [5] Incorporate 180M-row full-emit evidence as rationale + re-gate task + deferred-work doc; do NOT expand scope to build emit-path work

- **Decision:** Fold the 2026-07-01 180M-row / 60-file finding (native `IMPORT INTO` ~80.4 s vs. VS full-emit CTAS ~151 s, ~1.9× — a full raw-row `SELECT *` workload, differently shaped from the original aggregate-path benchmark; `docs/performance.md` §"Larger-scale validation") into plan.md as reinforcing rationale for *both* existing deliverables. Add one named validation task (plan.md Task 10) to re-run that exact 60-file comparison *after* Task 1's dep bump lands and confirm whether the gap narrows once `CLUSTER_NODES` is real. Document the emit-path `Int64→Decimal128` coercion optimization as evidence-gated deferred work (plan.md §Deferred work), conditioned on Task 10's outcome. Do NOT add code beyond the plan's existing scope (0.20.1 dep bump + `S3_MAX_CONNECTIONS` knob). The feature spec (`datafusion-scan/scan-execution-connection-concurrency/spec.md`) is unchanged — the knob's contract is unaffected; only the surrounding rationale/validation narrative grows.
- **Alternatives:** (a) Expand this plan to build the emit-path `Int64→Decimal128` optimization now — rejected. (c) Ignore the new evidence — rejected (it materially reshapes the rationale and surfaces a real open question).
- **Rationale:** The 151 s/80.4 s gap is confounded by the pre-0.20.1 `CLUSTER_NODES=1` under-sharding bug that Task 1 already fixes, so it is unknown whether the gap is under-sharding (would close on the dep bump) or a genuine emit-path bottleneck. Building emit-path coercion work now would be YAGNI — there is no confirmed emit-bound root cause, only a confounded measurement. Task 10 supplies the isolating measurement; the deferred-work doc records the optimization so it is not lost, gated on that measurement. Keeping benchmark/re-gate work out of Given/When/Then deltas follows the same rule as the existing Task 9 (bench work is validation, not spec contract). Resolution followed the project's "evidence-gated future work" convention after the amendment interview (Q3) received no response within the timeout.
- **ADR note:** Codifies the project rule — new benchmark evidence that is confounded by an in-flight fix is incorporated as rationale + a named post-fix re-gate task + evidence-gated deferred-work docs, never as immediate scope expansion, until the confound is isolated.
- **Promotes to ADR:** yes

## Review Findings

<!-- Populated by speq-implement after code review. -->
