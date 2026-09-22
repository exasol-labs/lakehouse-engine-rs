# Decision Log: refactor-nr-of-cores-detection

## Interview

**Q1 (Bench harness):** `bench/run.sh`, `bench/batch_size_sweep.sh`, `bench/emit_s3conn_sweep.sh`, and `bench/batch_size_aggcheck.sh` all pin `NR_OF_CORES` to simulate specific core counts for reproducible sweeps, and `run.sh` even asserts exact property strings. Removing the property breaks all of these. How should the plan handle it?
**A1:** Remove the property and update the bench scripts. Drop `NR_OF_CORES` everywhere, including `bench/*.sh` and the docs (`bench/README.md`, `bench/.env.example`). Core-count-driven derivation becomes tied to the real host or container core count, because no override remains. This is in scope for this plan. `bench/.env.example` already sets `LH_EXASOL_CPUS=4`, which constrains the Docker container's real cgroup CPU quota, and `available_parallelism()` on Linux respects cgroup quota. For docker-mode bench, `LH_EXASOL_CPUS` alone therefore drives the effective detected core count, making `BENCH_NR_OF_CORES` redundant there. Remote mode has no equivalent lever, so removing `BENCH_NR_OF_CORES` there means the remote bench run uses the real cluster's auto-detected core count instead of an artificial override. That is a real, user-visible behavior change and must be called out in `bench/README.md`.

**Q2 (Testability):** Should `resolve_parallelism_factor`, `resolve_df_threading`, `auto_threads_per_udf`, and `resolve_s3_max_connections` keep taking a resolved core-count value as an explicit parameter, or should each call `available_parallelism()` internally?
**A2:** Keep threading a resolved value as a parameter. Call `available_parallelism()` once, at the same call site `resolve_nr_of_cores` occupies today inside `handle_create_virtual_schema`, and pass the resolved value down as today. This mirrors the #184 precedent for `cluster_nodes_from_context(ctx) -> usize`, which is called once in `dispatch` and threaded as a plain `cluster_nodes: usize` parameter into `handle_pushdown_request` and onward. It also keeps today's unit tests for these pure derivation functions able to inject arbitrary core counts deterministically, for example 8 cores giving factor 16.

**Q3 (Unknown-cores fallback):** Today `resolve_s3_max_connections` falls back to a distinct `DEFAULT_S3_MAX_CONNECTIONS` of 16 when cores are unknown (`nr_of_cores == 0`), while every other knob's `0` fallback collapses to the same numbers as `nr_of_cores == 1` via `max(1, ...)`. Verified: `resolve_parallelism_factor` and `auto_threads_per_udf` produce identical output for cores=0 and cores=1 today. Dropping the `0` sentinel makes every knob use a uniform `nr_of_cores = 1` when `available_parallelism()` errors, changing the unknown-case S3 default from 16 to 4.
**A3:** Accept the uniform `nr_of_cores = 1` fallback everywhere. It is the simplest option and it matches the instruction to drop the `0` branches. It changes behavior only in the already-rare case where the OS cannot report a core count at all. That is a real `io::Error` path in `std::thread::available_parallelism()`, essentially never hit on Linux, Docker, or production hardware.

**Q4 (Verification scope):** CLAUDE.md's verification-first rule requires confirming, before removing the property, that `available_parallelism()` inside the adapter VM actually reports the per-node core count on Docker and on a multi-node cluster.
**A4:** Docker E2E check plus a manual multi-node checklist. Add an E2E task comparing the adapter's derived core count, surfaced through an adapterNotes-visible signal such as `DF_THREADS_PER_UDF` under a known `LH_EXASOL_CPUS`-constrained container, to the Docker container's real core count. Treat multi-node confirmation as a manual pre-merge checklist item, because no multi-node cluster is available in CI. File a follow-up GitHub issue only if the manual multi-node check finds a mismatch. In that case the right fix is a different source such as UDF metadata, not resurrecting the VS property.

## Design Decisions

### [1] The core count is a resolved input, not configuration or state

- **Decision:** Remove the `NR_OF_CORES` VS property and the `NOTE_NR_OF_CORES` adapterNotes entry. Read the per-node core count from `std::thread::available_parallelism()` at one call site in `handle_create_virtual_schema`, then pass it to the four derivations as a plain `u32`.
- **Alternatives:** Keep the property as an undocumented escape hatch. Rejected: a property the adapter reads is a supported property whatever the docs say, and no recorded need for the override exists. Keep the note for future readers. Rejected: nothing reads it, and an unread persisted value drifts from the node that produced it.
- **Rationale:** This is the shape #184 established for the cluster node count. One live read per request replaces a create-time capture that no consumer used, so the schema stops carrying a value that can go stale against the node executing the query.
- **Consequences:** A `CREATE VIRTUAL SCHEMA` statement that still supplies `NR_OF_CORES` succeeds and the property has no effect, because the adapter reads properties by name and ignores unknown names. A schema created under an earlier version keeps its persisted `NR_OF_CORES` entry, which survives unread and inert through refresh. The four derivations keep their `nr_of_cores` parameter, per decision [2].
- **Promotes to ADR:** yes

### [2] Derivations keep an injected core count rather than reading the host

- **Decision:** `resolve_parallelism_factor`, `resolve_df_threading`, `auto_threads_per_udf`, `resolve_df_fixed_count`, and `resolve_s3_max_connections` keep their `nr_of_cores: u32` parameter. None of them calls `available_parallelism()`.
- **Alternatives:** Have each derivation call `available_parallelism()` internally, which would delete the parameter from five signatures.
- **Rationale:** An internal call makes each derivation read ambient state. Its unit tests could then assert only what the build host happens to report, so a test such as "8 cores gives factor 16" becomes unwritable. It would also read the host four times per request instead of once.
- **Consequences:** The parameter survives a refactor whose stated goal is removing the value's ceremony. That is intentional: what this plan removes is the property, the note, and the sentinel, not the dependency injection that makes the derivations testable.
- **Promotes to ADR:** no

### [3] One fallback value of 1, replacing the 0 sentinel everywhere

- **Decision:** When `std::thread::available_parallelism()` returns an error, the adapter uses a core count of `1`. Delete the `nr_of_cores == 0` early return in `resolve_s3_max_connections`, which returned `DEFAULT_S3_MAX_CONNECTIONS`.
- **Alternatives:** Keep the `0` sentinel for `resolve_s3_max_connections` alone. Rejected: one sentinel consumer forces every caller to keep producing the sentinel, which is the ceremony this plan removes.
- **Rationale:** Two of the three sentinel consumers already collapse `0` into their own `max(1, ...)` floor, so their `0` branch was dead by construction. Making the third uniform leaves one rule rather than two.
- **Consequences:** The unknown-core S3 connection budget changes from 16 to 4, because the uniform AUTO formula gives `1 × S3_CONNECTIONS_PER_THREAD`. This is the one behavior change in the plan and it fires only on a platform that cannot report a core count. `DEFAULT_S3_MAX_CONNECTIONS` keeps its two other roles, as the serde default for a `ScanSpec` payload that omits the field and as the pushdown-side fallback at `crates/lakehouse-engine/src/adapter/mod.rs:404` for an absent or invalid `S3_MAX_CONNECTIONS` adapterNote.
- **Promotes to ADR:** no

### [4] A pure seam makes the detection-failure branch testable

- **Decision:** Split the resolver in two. `resolve_nr_of_cores()` calls `std::thread::available_parallelism()`. A pure `core_count_or_default(detected: std::io::Result<NonZeroUsize>) -> u32` maps `Ok(n)` to `n` and `Err` to `1`.
- **Alternatives:** One function with the fallback inline. Rejected: the `Err` branch is then unreachable from a test, so the scenario that specifies it would carry no evidence.
- **Rationale:** The failure branch is the one place this refactor changes behavior in a way a reader cannot check by inspection. A two-line pure function makes it assertable at zero runtime cost.
- **Promotes to ADR:** no

### [5] Docker E2E asserts the quota, and the multi-node check stays manual

- **Decision:** Add `adapter_detects_container_cpu_quota` to the Docker E2E suite. It creates a probe virtual schema with `PARALLELISM_FACTOR = '1'` so the AUTO derivation records the detected core count unchanged as `DF_THREADS_PER_UDF`, then asserts that entry equals the CPU quota read back from the running container's own `/sys/fs/cgroup/cpu.max`. Record the multi-node check as a mandatory pre-merge gate in plan.md § Manual Testing, not as an implementation task.
- **Alternatives:** Compare against `nproc` inside the container. Rejected: Docker's `cpus:` sets a CFS quota rather than a cpuset, so `nproc` reports the host core count while `available_parallelism()` reports the quota. Comparing against `nproc` would assert the wrong number. Read `DF_THREADS_PER_UDF` from the default virtual schema. Rejected: at the default parallelism factor of 8, the AUTO derivation floors to 1 for every core count from 1 to 8, so the note carries no signal.
- **Rationale:** `PARALLELISM_FACTOR = '1'` sets `udf_instances_per_node` to 1, which makes `DF_THREADS_PER_UDF` an exact readout of the detected count. This is the only observable channel: the adapter writes no core count of its own, and CLAUDE.md forbids trusting code inspection over a live run.
- **Consequences:** If the E2E test shows the host core count rather than the container quota, the removal is wrong and the implementer escalates rather than weakening the assertion. Task 3.3 states that stop condition. Reading the quota from the container rather than from `LH_EXASOL_CPUS` covers a developer machine with fewer cores than the compose file requests, because the container receives the quota the host could grant. The assertion discriminates only when the quota is strictly below the test host's own core count, so the test fails naming that precondition when it is not met. CI does not satisfy it as configured: `.github/workflows/ci.yml` sets `LH_EXASOL_CPUS: "2"` at lines 537, 647, 750, and 853 under a comment asserting a 2-vCPU runner, which makes quota and host count equal. Any host reporting more cores than the configured quota satisfies it, such as the compose default of `4` on a developer box of eight cores or more, or a runner whose measured `nproc` exceeds `LH_EXASOL_CPUS`. Task 3.4 measures the runner and lowers `LH_EXASOL_CPUS` when the measurement does not clear the bar.
- **Promotes to ADR:** no

### [6] The bench harness loses the override on both targets

- **Decision:** Remove `NR_OF_CORES` and `BENCH_NR_OF_CORES` from `bench/run.sh`, the three sweep scripts, `bench/.env.example`, `deploy/scripts/secrets.sh`, `bench/README.md`, and `docs/benchmark.md`.
- **Alternatives:** Keep the property for the bench path only. Rejected per decision [1].
- **Rationale:** `LH_EXASOL_CPUS` already sets the docker container's real CPU quota, so docker-mode sweeps keep an equivalent and more honest lever. Remote mode loses the lever outright, which is the accepted cost of removing the property.
- **Consequences:** A remote bench comparison across this change is not directly comparable unless `BENCH_PARALLELISM_FACTOR` and the `DATAFUSION_*` properties are pinned. `bench/README.md` must state this. `deploy/scripts/secrets.sh` writes `BENCH_NR_OF_CORES=8` into every generated `bench/.env`, so a stale generated file would otherwise carry a now-inert variable.
- **Promotes to ADR:** no

### [7] Four conceptual NR_OF_CORES mentions stay unchanged

- **Decision:** Leave `CLAUDE.md:150`, `docs/architecture.md:43`, `specs/mission.md:80`, and `specs/udf-context.md:11` as they stand. Reword only the two code doc comments that name the removed property rather than the Exasol parameter: `crates/lakehouse-engine/src/scan/diagnostics.rs:48` and `crates/lakehouse-engine/src/scan/mod.rs:109`.
- **Alternatives:** Reword all six for consistency. Rejected: the four left alone name Exasol's own per-node core-count parameter, which sizes each node's UDF VM pool and which this plan does not change.
- **Rationale:** The VS property borrowed an existing Exasol name. Removing the property does not remove the engine parameter, so rewording those four would replace a correct statement with a vaguer one.
- **Consequences:** `git grep -n "NR_OF_CORES"` keeps returning those four matches after this plan lands. The residual-sweep checklist row states that expectation, so a reviewer does not read them as leftovers.
- **Promotes to ADR:** no

### [8] Sequencing the bench and documentation cluster after the adapter cluster

- **Decision:** Two parallelization groups. Group A covers the adapter, its unit tests, and the Docker E2E observation. Group B covers the bench harness and the operator documentation, and depends on A.
- **Alternatives:** Run both groups in parallel, since they share no file. Rejected: task 5.1 rewrites the `docs/tuning.md` bullet that states the connection-budget number group A changes, so B's author should see A's landed code.
- **Rationale:** Every adapter-side spec delta in this plan is implemented by the same function set in `adapter/mod.rs`, so splitting group A by file would make three agents rebuild one mental model. Parallelism is a side effect of coherent clustering, not the goal.
- **Promotes to ADR:** no

## Discovery Notes

### [D1] NOTE_NR_OF_CORES was never read back at pushdown, contradicting the original issue text

- **Finding:** The original ask framed the note as "captured at createVirtualSchema and replayed at pushdown". `find_referencing_symbols` on `NOTE_NR_OF_CORES` returns two references outside tests, both writes: the constant definition and the insert in `build_adapter_notes`. `handle_pushdown_request` never reads it. The entries that genuinely round-trip through `adapter_note(request, ...)` are the derived ones: `NOTE_PARALLELISM_FACTOR`, `NOTE_DF_TARGET_PARTITIONS`, `NOTE_DF_THREADS_PER_UDF`, and `NOTE_S3_MAX_CONNECTIONS`.
- **Effect on the plan:** Removing `NOTE_NR_OF_CORES` needs no change to `handle_pushdown_request`. The plan's task list contains no pushdown-side task, which is correct rather than an omission. The removal is also safer than the original framing implied: there is no read path to break.
- **Promotes to ADR:** no

### [D2] Scope items the planning brief did not list

- **Finding:** Four references outside the brief's inventory reach the removed property. `deploy/scripts/secrets.sh:49` writes `BENCH_NR_OF_CORES=8` into every generated `bench/.env`. `docs/tuning.md` documents `NR_OF_CORES` as an operator-facing property at line 19, uses it as a substitution placeholder at line 41, and states the removed unknown-core default of 16 at line 53. `docs/benchmark.md:54` lists `BENCH_NR_OF_CORES`. Two permanent specs the brief did not name carry `NR_OF_CORES` in a scenario clause: `vs-adapter/create-virtual-schema` line 124 and `vs-adapter/refresh-and-set-properties` line 33.
- **Effect on the plan:** The feature table carries seven spec deltas rather than five. Tasks 4.4, 5.1, and 5.3 cover the three additional non-spec files.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The Docker CPU-quota probe asserted a value with no discriminating power

- **Finding:** Round 1 flagged task 3.2's assertion `DF_THREADS_PER_UDF == min(LH_EXASOL_CPUS, host available_parallelism())` as unfalsifiable in both configurations this repo runs. `.github/workflows/ci.yml:537` pairs a 2-vCPU runner with `LH_EXASOL_CPUS: "2"`, and `docker-compose.yml:119` defaults to `4`, which a 4-core developer box matches. In each case the expected value equals what an adapter reading the unconstrained host would report, so the probe passes with quota-awareness broken. The expected value also came from the test process's environment, describing how the container was launched rather than what it received.
- **Direction change:** Task 3.2 now reads the effective quota from the running container through `docker exec` on `/sys/fs/cgroup/cpu.max`, mirroring `crates/lakehouse-engine/tests/common/stack.rs:136`, and asserts `DF_THREADS_PER_UDF` against that. A precondition assertion requires the quota to be strictly below the test host's `available_parallelism()` and fails naming the unmet precondition otherwise. The `vs-adapter/create-virtual-schema-adapter-notes` scenario carries both changes as normative steps. Decision [5] records that CI as configured does not satisfy the precondition and names what does, and new task 3.4 measures the runner and lowers `LH_EXASOL_CPUS` for the E2E job when needed.
- **Promotes to ADR:** no

### [plan-review] Three artifacts claimed DEFAULT_S3_MAX_CONNECTIONS would have one surviving consumer

- **Finding:** Round 1 flagged the claim that deleting the unknown-core branch leaves the constant as the serde default alone. Two live consumers survive: `crates/lakehouse-engine/src/adapter/mod.rs:404`, the pushdown fallback for an absent or invalid `S3_MAX_CONNECTIONS` adapterNote, and `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs:192`, which passes it to `build_table_root_store`. The falsehood appeared in the `datafusion-scan/scan-execution-connection-concurrency` delta, in plan.md § Consequences row 4, and in task 1.6, which would have written it into a doc comment. Task 1.5 also left the import at `adapter/mod.rs:23` as an open question that line 404 already settles.
- **Direction change:** The delta step now names both surviving roles and confines the removal to the create-time AUTO unknown-core branch. Consequences row 4 names the two consumers by file and line and reframes the decision as leaving the constant where it is. Task 1.6 deletes only the unknown-core clause and keeps the reverse-dependency rationale. Task 1.5 states as fact that the import stays. Decision [3] § Consequences names both roles.
- **Promotes to ADR:** no

### [plan-review] Two NR_OF_CORES doc comments were missing from the sweep task

- **Finding:** Round 1 flagged task 1.4 as covering four doc comments and omitting two. `crates/lakehouse-engine/src/adapter/mod.rs:48` reads `Default: max(NR_OF_CORES * 2, 8)` and line 52 reads `floor applied when NR_OF_CORES is 0 or very small`. Both name what this plan deletes, and the § Checklist residual-reference sweep asserts only four Exasol-parameter mentions remain, so the sweep would have failed on them.
- **Direction change:** Task 1.4 now covers both lines, restating line 48 as the hardware-aware default over the detected core count and line 52 as the floor applied to a small detected core count, and states that neither may reference `NR_OF_CORES` or `0`.
- **Promotes to ADR:** no

### [plan-review] The multi-node check was an agent-tickable task rather than a ship gate

- **Finding:** Round 1 flagged tasks 6.1 and 6.2 as a manual cluster check placed among implementation tasks and assigned to parallelization group B, where an agent can tick a checkbox for work the plan itself says no agent can run. They carried no ship-blocking condition, and § Manual Testing had no multi-node row. `specs/_decision/046-refactor-pushdown-node-count.md:20` records the opposite shape for the #184 plan this one follows: a mandatory pre-merge gate in § Manual Testing with an explicit MUST NOT ship condition.
- **Direction change:** Section 6 and tasks 6.1 and 6.2 are deleted, and group B now lists `4.1-4.4, 5.1-5.3`. § Manual Testing carries a mandatory multi-node gate row stating the command, the expected `DF_THREADS_PER_UDF` value against the cluster's reported per-node core count, a MUST NOT ship condition on a mismatch, the follow-up issue obligation, and the prohibition on reintroducing the VS property. The § Checklist row points at that gate instead of a deleted task.
- **Promotes to ADR:** no

### [plan-review] The inherited-NR_OF_CORES refresh clause had no implementing task

- **Finding:** Round 1 flagged the `vs-adapter/refresh-and-set-properties` delta clause stating that an `NR_OF_CORES` entry left by an earlier adapter version survives the rebuild unread and inert. No task implemented or tested it, and the § Implementation Tasks preamble claimed the opposite. The mapped test seeds only `OTHER_KEY` and `TABLE_MAP` at `crates/lakehouse-engine/src/adapter/adapter_tests.rs:396`. Once `build_adapter_notes` stops writing the key, nothing else pins merge-not-clobber for it, and § Impact's backward-compatibility claim rests on that clause alone.
- **Direction change:** New task 2.8 seeds `"NR_OF_CORES": "8"` into that request's `schemaMetadataInfo.adapterNotes` and asserts it survives the rebuild with its original value. The § Implementation Tasks preamble now separates the two deltas and states that the refresh delta needs task 2.8. Group A covers `2.1-2.8`. § Scenario Coverage already maps the refresh scenario to `refresh_rebuilds_table_map_preserves_notes` and is unchanged.
- **Promotes to ADR:** no
