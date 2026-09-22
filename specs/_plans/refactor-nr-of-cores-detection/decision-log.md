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

### [5] Docker E2E asserts the container's CPU set, and the multi-node check stays manual

- **Decision:** Add `adapter_detects_container_cpuset` to the Docker E2E suite. It creates a probe virtual schema with `PARALLELISM_FACTOR = '1'` so the AUTO derivation records the detected core count unchanged as `DF_THREADS_PER_UDF`, then asserts that entry equals the number of CPUs in the affinity set read back from the running container's own `/sys/fs/cgroup/cpuset.cpus.effective`. The stack pins that set through `LH_EXASOL_CPUSET`. Record the multi-node check as a mandatory pre-merge gate in plan.md § Manual Testing, not as an implementation task.
- **Alternatives:** Constrain the container with a CFS bandwidth quota (`cpus:`) and assert against `/sys/fs/cgroup/cpu.max`. Rejected: the quota is invisible inside the UDF sandbox, so the assertion can never pass. Entry [9] records the measurements. Read `DF_THREADS_PER_UDF` from the default virtual schema. Rejected: at the default parallelism factor of 8, the AUTO derivation floors to 1 for every core count from 1 to 8, so the note carries no signal.
- **Rationale:** `PARALLELISM_FACTOR = '1'` sets `udf_instances_per_node` to 1, which makes `DF_THREADS_PER_UDF` an exact readout of the detected count. This is the only observable channel: the adapter writes no core count of its own, and CLAUDE.md forbids trusting code inspection over a live run.
- **Consequences:** If the E2E test shows the host core count rather than the container's CPU set, the implementer escalates rather than weakening the assertion. Task 3.5 states that stop condition. Reading the set from the container rather than from `LH_EXASOL_CPUSET` covers a developer machine whose set differs from what the compose file requests. The assertion discriminates only when the container's set is strictly smaller than the test host's own core count, so the test fails naming that precondition when it is not met. Task 3.4 keeps the CI measurement step that prints `nproc` and narrows the set to stay below it.
- **Promotes to ADR:** no

### [6] The bench harness loses the override on both targets

- **Decision:** Remove `NR_OF_CORES` and `BENCH_NR_OF_CORES` from `bench/run.sh`, the three sweep scripts, `bench/.env.example`, `deploy/scripts/secrets.sh`, `bench/README.md`, and `docs/benchmark.md`.
- **Alternatives:** Keep the property for the bench path only. Rejected per decision [1].
- **Rationale:** `LH_EXASOL_CPUSET` pins the docker container's CPU set, so docker-mode sweeps keep an equivalent and more honest lever. Remote mode loses the lever outright, which is the accepted cost of removing the property.
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

### [9] [revision] Detection stays host-side, and the Docker E2E constrains CPU by affinity

Supersedes the verification mechanism in decision [5], and the mechanism claim in interview answer A1 that `LH_EXASOL_CPUS` gives docker bench an effective core-count lever through the CPU quota.

- **Decision:** `resolve_nr_of_cores()` and `core_count_or_default` ship exactly as decisions [1] and [4] describe, with no cgroup-reading code added. The Docker stack constrains the Exasol container by CPU affinity (`cpuset`) instead of by CFS bandwidth quota (`cpus:`), and `adapter_detects_container_cpuset` asserts against `cpuset.cpus.effective`. A CPU limit expressed only as a quota is not detectable, and issue #421 tracks that gap.
- **Trigger:** The task 3.2 gate fired. `adapter_detects_container_cpu_quota` asserted `DF_THREADS_PER_UDF == 2` against a container held at `cpus: "2"` on a four-CPU host, and read `4`. Task 3.3 required a stop and an escalation rather than a weakened assertion.
- **Falsified hypothesis:** The first explanation was that `available_parallelism()` reads only `sched_getaffinity`, which a CFS quota never changes, so the adapter needed its own `cpu.max` reader combined by a minimum. Reading the pinned 1.94 toolchain refuted this. `library/std/src/sys/thread/unix.rs:177` already computes `min(sched_getaffinity count, cgroup quota)`, and `mod cgroups` already parses `cpu.max` for v2 and `cpu.cfs_quota_us` with `cpu.cfs_period_us` for v1, floor-dividing and treating any miss as unlimited. The proposed reader would have duplicated that logic in a narrower form.
- **Confirmed mechanism:** Three live checks located the real cause. The container's ordinary namespace reads `/sys/fs/cgroup/cpu.max` as `200000 100000`, which is 2 cores, and the `exasql` daemon shares that mount and cgroup namespace. A `PYTHON3 SCALAR` probe run inside the UDF sandbox reads neither that file nor `cgroup.controllers`: `/sys` exists, nothing is mounted under it, and the probe reported `affinity=4`. Under `docker update --cpuset-cpus="0-1"` the same probe reported `affinity=2` with cgroupfs still absent. The sandbox mounts no cgroup filesystem, so no UDF reads a quota in any language, while affinity propagates into it.
- **Alternatives:** Add the `cgroup_quota_cores()` reader anyway. Rejected: it would read an `ENOENT` path on every call, return `None` every time, and leave the collapsed minimum equal to today's value, so the same test would fail identically. Support cgroup v1 as well. Rejected: neither version is readable from the sandbox, so the limitation is the missing mount rather than the parsing. Read the core count from UDF metadata. Rejected: plan.md states it as a Non-Goal, and the SDK is not confirmed to expose a core count at all. Keep the quota mechanism and relax the assertion. Rejected: task 3.3 prohibits weakening it, and a passing non-discriminating test records false evidence, which round 1 already caught once.
- **Rationale:** The detection code was never wrong. The test harness constrained CPU through a mechanism the execution substrate cannot observe. Fixing the harness restores the evidence the plan needs and leaves the shipped adapter untouched.
- **Consequences:** No adapter source file changes. `docker-compose.yml`, four `.github/workflows/ci.yml` sites, the E2E test and its helper, `bench/.env.example`, `bench/run.sh`, and `bench/README.md` move from `LH_EXASOL_CPUS` to `LH_EXASOL_CPUSET`, which names a CPU set rather than a count. A stale `LH_EXASOL_CPUS` becomes inert. A host of fewer than four CPUs must set the variable explicitly, because Docker rejects a cpuset naming an absent CPU. A quota-limited node receives budgets sized for its affinity count, which causes contention rather than wrong results, at an unmeasured magnitude.
- **Promotes to ADR:** yes

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

### [plan-review] The operator migration guidance named the one mechanism the adapter cannot see

- **Finding:** Round 1 of the cpuset revision flagged plan.md § Impact and § Migration as directing the operator to constrain the node's CPU quota. § CPU-limit detection boundary in the same file states that a quota is invisible to the adapter, and the `vs-adapter/create-virtual-schema-adapter-notes` Background states the correct lever is CPU affinity. Those two lines were the only operator-facing migration guidance the plan shipped. An operator who followed them would set a Kubernetes `limits.cpu`, get budgets sized for the affinity count, and hit exactly the failure issue #421 records. Both lines predate the revision, which rewrote the surrounding text and left them.
- **Direction change:** § Impact now states that the operator constrains the executing node's CPU affinity through a Docker `cpuset` or the Kubernetes static CPU manager policy, and that a CFS bandwidth quota alone does not change the detected count, citing § CPU-limit detection boundary and issue #421. The § Migration row for the removed property now reads "Constrain the node's CPU affinity to set the effective count. A CFS bandwidth quota alone has no effect (#421)."
- **Promotes to ADR:** no

### [plan-review] Task 3.3 ordered an assertion message kept that the plan's own checklist forbids

- **Finding:** Round 1 flagged task 3.3's instruction to keep the precondition assertion "exactly as it stands". That assertion's message at `crates/lakehouse-engine/tests/e2e_scan_test.rs:1370-1378` names `LH_EXASOL_CPUS` and calls a CPU-set cardinality a quota. The § Checklist quota-variable sweep expects no match for `LH_EXASOL_CPUS` outside `specs/_plans` and `specs/_decision`. An implementer following task 3.3 literally would either fail that sweep or weaken it. The task's trailing clause about only the mechanism changing did not resolve the conflict, because the message is part of the assertion.
- **Direction change:** Task 3.3 now requires the precondition assertion's structure, the fail-not-skip rule, and the probe schema to survive, and requires every assertion message naming `LH_EXASOL_CPUS` or calling the value a quota to be restated. The precondition message MUST name the container's CPU set, its cardinality, and `LH_EXASOL_CPUSET`, and MUST direct the reader to narrow the set rather than lower a count.
- **Promotes to ADR:** no

### [human-review] The spec deltas narrated the removal instead of stating the end state

- **Finding:** Human review of the draft PR flagged the surviving delta content, the `DELTA:CHANGED` and `DELTA:NEW` blocks that `speq record` merges into the permanent library, as a transition narrative. Specs record state. A permanent spec that says "there is no `NR_OF_CORES` property", "no longer changes any resolved budget", "the prior behaviour", "the deleted unknown-core branch", or "a behavior change from the prior fixed override" describes yesterday against today, and reads as a defect once the plan is archived. `/speq:writing-guardrails` states the same rule: state the current content as fact, do not narrate the revisions that produced it. The narrative belongs in plan.md and this decision log, which record the change itself.
- **Direction change:** Five delta files lost their transition framing. `vs-adapter/create-virtual-schema-adapter-notes` states the general property-name rule and the CPU-quota lever without naming the removed property, generalizes the migration bullet to any `adapterNotes` entry the adapter does not write, and drops the dedicated `NR_OF_CORES`-is-inert scenario, whose only subject was a mechanism that is gone. That scenario's normative content folds into the surviving `available_parallelism()` scenario as one general step: the adapter accepts an unrecognized property name and resolves every budget to the same value without it. Task 2.5 keeps `nr_of_cores_property_is_ignored` and now carries its own rationale, so the compatibility assertion stays under test with no permanent scenario naming the removed property. § Scenario Coverage maps that test to the surviving scenario. `vs-adapter/refresh-and-set-properties` generalizes its preservation clause to an entry the adapter does not write, with task 2.8 pinning `NR_OF_CORES` as the concrete seeded key. `datafusion-scan/scan-execution-threading` drops "No VS property overrides it", "(the prior behaviour)", "rather than a separate unknown-core branch", and "exactly as the pre-mode behaviour did". `datafusion-scan/scan-execution-connection-concurrency` states the AUTO derivation as uniform across every resolved core count, asserts the single-core result of 4 directly, and names the two surviving roles of `DEFAULT_S3_MAX_CONNECTIONS` as plain fact. The 16-to-4 change stays recorded in decision [3] § Consequences and plan.md § Impact. `e2e-harness/cloud-e2e-harness` keeps its MUST-NOT-pass constraint on `NR_OF_CORES` with a forward rationale and drops the remote-lever comparison, which plan.md § Impact already carries.
- **Boundary kept:** A forward guarantee that names a legacy key stays, per the recorded `CLUSTER_NODES` precedent in `specs/vs-adapter/create-virtual-schema-adapter-notes/spec.md`: a MUST-NOT-carry constraint on the response, and a statement that a foreign entry survives the merge unread and inert, both describe enduring adapter behavior rather than the removal. Every `DELTA:REMOVED` block is untouched, because that marker is the delta mechanism for showing what a scenario said before.
- **Promotes to ADR:** no
