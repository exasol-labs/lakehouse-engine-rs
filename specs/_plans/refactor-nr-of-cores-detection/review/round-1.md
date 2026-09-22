# Plan Review Findings: refactor-nr-of-cores-detection (round 1)

> This document reviews the cpuset revision. It replaces the earlier round-1 file written for the
> pre-revision plan. That file's findings survive in `decision-log.md` § Review Findings and in git
> at commit `e0eeb23`.

## Summary
- Axes checked: 6/6
- Total findings: 8 (Blockers: 2, Advisory: 6)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Three failure stories, each routed into the taxonomy below.

**An operator throttles a production node with a CFS quota and the engine oversizes its budgets.**
plan.md § Impact and § Migration both direct the operator to constrain the node's CPU quota.
§ CPU-limit detection boundary in the same file states that a quota is invisible to the adapter. The
operator sets a Kubernetes `limits.cpu`, the adapter reads the affinity count, and the thread,
connection, and parallelism budgets exceed the node's CPU allowance. Routed to
`[REQUIREMENT_CONFLICT]` BLOCKER 1.

**The E2E probe ships telling developers to change a variable that no longer exists, and the plan's
own checklist fails at the end of implementation.** Task 3.3 orders the precondition assertion kept
exactly as it stands. That assertion's message names `LH_EXASOL_CPUS`. The § Checklist
quota-variable sweep expects no match for that name. The implementer either fails the sweep or
relaxes it. Routed to `[REQUIREMENT_CONFLICT]` BLOCKER 2.

**A docker bench comparison across this change attributes a scheduling artefact to the engine.**
Decision [6] calls `LH_EXASOL_CPUSET` an equivalent lever. A pinned CPU set is not equivalent to a
floating bandwidth quota for throughput measurement. § Impact warns only about the remote target.
Routed to `[UNSTATED_ASSUMPTION]` ADVISORY.

## Intent Fidelity

[no objection — axis checked]

The pivot preserves the original goal. All three removals stay intact and untouched by this
revision. The `NR_OF_CORES` VS property removal holds in tasks 1.2, 2.2, and 2.5, and in the three
`DELTA:REMOVED` blocks at `vs-adapter/create-virtual-schema-adapter-notes/spec.md:91-119`. The
`NOTE_NR_OF_CORES` adapterNotes removal holds in task 1.3 and in that delta's scenario step at line
87. The `0 = unknown` sentinel removal holds in tasks 1.5, 2.6, and 2.7, and in the `DELTA:REMOVED`
blocks of `datafusion-scan/scan-execution-threading` and
`datafusion-scan/scan-execution-connection-concurrency`. `git diff` on plan.md confirms sections 1
and 2 changed only by the added § 1 preamble, so no adapter-code task moved.

The round-2 interview choice maps cleanly. The user chose "A+B: switch to cpuset, document the
CFS-quota blind spot". Tasks 3.2 through 3.5 and 4.5 carry the cpuset switch. Issue #421 exists, is
open, and is cited inline at `vs-adapter/create-virtual-schema-adapter-notes/spec.md:45`, matching
the `(#27)` convention CLAUDE.md names. Decision [9] states that no adapter source file changes,
which `git status` confirms for `crates/lakehouse-engine/src/adapter/mod.rs` against the
pre-revision commit.

The superseded round-1 interview direction is not re-litigated here. Decision [9] records it as
falsified by live measurement and entry [9] § Alternatives states why the proposed
`cgroup_quota_cores()` reader would be dead code.

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Impact third bullet (line 113); decision-log.md § Design Decisions [6] § Rationale (line 62); plan.md § Implementation Tasks task 5.2 (line 176)
- Issue: decision [6] claims "`LH_EXASOL_CPUSET` pins the docker container's CPU set, so docker-mode sweeps keep an equivalent and more honest lever". A pinned CPU set is not equivalent to a bandwidth quota for a throughput measurement. Under `cpus: "4"` the container floated across every host CPU with a four-core-equivalent budget. Under `cpuset: "0-3"` it is pinned to four named CPUs, so cache locality and contention with host processes on those same CPUs both change. `bench/` exists to produce comparable numbers. § Impact names the comparability break on the remote target only, at line 111, and says nothing about the docker target. A regression measured across this change on the docker target could be read as an engine regression when it is a scheduling artefact.
- Fix: Add one sentence to plan.md § Impact's `LH_EXASOL_CPUSET` bullet (line 113) stating that a docker-mode bench number taken under `cpus:` is not directly comparable to one taken under `cpuset:`, because pinning changes cache locality and host contention. Reword decision-log.md [6] § Rationale (line 62) to say the lever is equivalent in core count and not in scheduling behavior. Extend task 5.2 to require that sentence in the `bench/README.md` Parallelism section.

#### [EFFORT_MISESTIMATION] ADVISORY
- Location: plan.md § Implementation Tasks task 3.3 (line 161); `crates/lakehouse-engine/tests/e2e_scan_test.rs` fn `exasol_container_cpu_quota`
- Issue: task 3.3 says "Delete the cgroup-v1 and fractional-quota error branches, which have no cpuset equivalent." The helper carries four branches with no cpuset equivalent, not two. Task 3.3 names the cgroup-v1 `out.status.success()` assertion and the `quota_us % period_us` fractional assertion. It does not name the `assert_ne!(quota, "max", ...)` unconstrained-container assertion at line 1443, whose cpuset analogue is the test-body precondition rather than a helper branch, nor the `cores >= 1` sub-one-core assertion at line 1470, which is quota arithmetic with no cpuset counterpart. It also does not name the `period_us > 0` assertion or the "carries no period" panic, both of which read a field `cpuset.cpus.effective` does not have.
- Fix: Rewrite task 3.3's deletion clause in plan.md to name every branch the cpuset read drops: the cgroup-v1 `out.status.success()` message, the `assert_ne!(quota, "max", ...)` unconstrained assertion, the missing-period panic, the `period_us > 0` assertion, the fractional `quota_us % period_us` assertion, and the `cores >= 1` assertion. State that the cpuset helper keeps only the `docker exec` invocation, an empty-output panic, and the set parser.

#### [EFFORT_MISESTIMATION] ADVISORY
- Location: plan.md § Implementation Tasks task 4.2 (line 168)
- Issue: task 4.2 says "Update the five `build_vs_extra_props` calls in the `bench/run.sh selftest` block". `grep -n "build_vs_extra_props" bench/run.sh` returns four calls inside that block, at lines 147, 150, 156, and 161. The remaining two call sites at lines 371 and 405 belong to task 4.1, and line 41 is the definition. Round 1 of the earlier review raised this and the revision did not action it. An advisory never blocked, so leaving it was legitimate, but the count is still wrong and the revision renumbered nothing around it.
- Fix: Change "the five `build_vs_extra_props` calls" to "the four `build_vs_extra_props` calls at lines 147, 150, 156, and 161" in task 4.2 of plan.md.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Checklist quota-variable sweep row (line 262); plan.md § Terminology left unchanged (lines 89-91); `specs/_decision/067-add-unity-e2e-ci-job.md:20`
- Issue: the quota-variable sweep excludes `specs/_decision` by pathspec, and the plan never states why. `specs/_decision/067-add-unity-e2e-ci-job.md:20` records `LH_EXASOL_CPUS: "2"` as part of the `e2e-unity` job shape that ADR describes. Task 3.4 replaces that entry with `LH_EXASOL_CPUSET: "0-1"`, so the ADR will describe a job configuration that no longer exists. The exclusion is a defensible scope boundary, because an ADR is a dated record rather than live configuration, but the boundary lives only inside a grep pathspec. § Terminology left unchanged states the equivalent boundary for the four conceptual `NR_OF_CORES` mentions and states nothing about this one. `/speq:audit`'s ADR pass checks each ADR against the current code, so the next audit reads the mismatch as drift with no recorded answer.
- Fix: Add a paragraph to plan.md § Terminology left unchanged stating that `specs/_decision/067-add-unity-e2e-ci-job.md:20` records `LH_EXASOL_CPUS: "2"` as a point-in-time record of the job shape added by that decision, that an ADR is not rewritten when a later plan renames a variable it names, and that both § Checklist sweeps therefore exclude `specs/_decision` deliberately.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md § Impact first bullet (line 109); plan.md § Migration first row (line 123); against plan.md § CPU-limit detection boundary (line 81) and `specs/_plans/refactor-nr-of-cores-detection/vs-adapter/create-virtual-schema-adapter-notes/spec.md` § Background (lines 34-47)
- Issue: the plan gives the operator a migration instruction its own detection boundary declares ineffective. § Impact line 109 reads "An operator who relied on it to raise or lower the derived thread and connection budgets loses that lever and must constrain the node's CPU quota instead." § Migration line 123 reads "Drop the property. Constrain the node's CPU quota to set the effective count." § CPU-limit detection boundary line 81 reads "The adapter detects a CPU limit expressed as affinity. It does not detect a limit expressed only as a CFS bandwidth quota." The adapter-notes delta states the correct lever at lines 37-38: "An operator who needs a smaller effective core count constrains the executing node's CPU affinity, which `std::thread::available_parallelism()` honours." The two plan.md lines are the only operator-facing migration guidance this plan ships, and both name the one mechanism the revision established as invisible. An operator who follows them sets a limit the engine cannot see and gets budgets sized for more CPU than the node may use, which is exactly the failure issue #421 records. Both lines predate the revision and the revision rewrote the surrounding text without correcting them.
- Fix: In plan.md § Impact line 109, replace "must constrain the node's CPU quota instead" with a statement that the operator constrains the executing node's CPU affinity instead, for example a Docker `cpuset` or the Kubernetes static CPU manager policy, and that a CFS bandwidth quota alone does not change the detected count (§ CPU-limit detection boundary, issue #421). In plan.md § Migration line 123, replace "Constrain the node's CPU quota to set the effective count." with "Constrain the node's CPU affinity to set the effective count. A CFS bandwidth quota alone has no effect (#421)."
- Escalation: MECHANICAL. Three artifacts already in this plan settle which mechanism is correct, so no requester judgment is needed.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md § Implementation Tasks task 3.3 (line 161); against plan.md § Checklist quota-variable sweep row (line 262); `crates/lakehouse-engine/tests/e2e_scan_test.rs:1377`
- Issue: task 3.3 orders "Keep the precondition assertion, the fail-not-skip rule, and the `PARALLELISM_FACTOR = '1'` probe schema exactly as they stand". The precondition assertion's message at `e2e_scan_test.rs:1370-1378` reads "PRECONDITION UNMET: the Exasol container's CPU quota ({quota}) must be strictly less than this host's core count ({host_cores}) ... Lower LH_EXASOL_CPUS and recreate the container." Kept exactly as it stands, that message names the removed variable and calls a CPU-set cardinality a quota. § Checklist line 262 then fails: its quota-variable sweep runs `git grep -n "LH_EXASOL_CPUS\b" -- . ':!specs/_plans' ':!specs/_decision'` and expects "No match. Every caller names `LH_EXASOL_CPUSET`". The two instructions cannot both be satisfied. The trailing clause "only the constraint mechanism and the expected-value source change" does not resolve the conflict, because "exactly as they stand" is unconditional and the assertion's message is part of the assertion. An implementer who follows task 3.3 literally either fails the plan's own checklist or weakens the sweep to make it pass.
- Fix: In plan.md task 3.3, replace "Keep the precondition assertion, the fail-not-skip rule, and the `PARALLELISM_FACTOR = '1'` probe schema exactly as they stand" with "Keep the precondition assertion's structure, the fail-not-skip rule, and the `PARALLELISM_FACTOR = '1'` probe schema. Restate every assertion message that names `LH_EXASOL_CPUS` or calls the value a quota: the precondition message at `crates/lakehouse-engine/tests/e2e_scan_test.rs:1370-1378` MUST name the container's CPU set, its cardinality, and `LH_EXASOL_CPUSET`, and MUST direct the reader to narrow the set rather than to lower a count."
- Escalation: MECHANICAL. Task 3.3, the § Checklist row, and the test file settle the conflict between them.

### Implementation-leakage check

[no objection — sub-check certified]

The new `(#421)` Background bullet at `vs-adapter/create-virtual-schema-adapter-notes/spec.md:40-47`
is depended on by the Docker scenario. That scenario's `GIVEN` names a CPU affinity set rather than
a quota, and its `THEN` at line 149 asserts the recorded value equals the affinity-set cardinality.
The bullet's closing clause, "An affinity-based limit ... is detected exactly", is the fact that
`THEN` step rests on, and the quota half is what makes the `GIVEN`'s choice of mechanism a
requirement rather than an arbitrary test detail. The bullet is a recorded limitation with a tracked
issue, which is this repo's stated convention for a named deviation, and the user's round-2 answer
chose that recording explicitly. It is not leakage.

## Task Breakdown

[no objection — axis checked]

Every delta has an implementing task. `vs-adapter/create-virtual-schema-adapter-notes` maps to tasks
1.2 through 1.4, 2.3 through 2.5, and 3.1 through 3.3. `-resources` maps to tasks 1.4 and 2.6.
`vs-adapter/create-virtual-schema` carries no task beyond task 2.1's argument drop, which the
§ Implementation Tasks preamble states as fact at line 130. `vs-adapter/refresh-and-set-properties`
maps to task 2.8. `scan-execution-threading` maps to tasks 1.4 and 2.6.
`scan-execution-connection-concurrency` maps to tasks 1.5, 1.6, and 2.7. `cloud-e2e-harness` maps to
tasks 4.1 through 4.4. New task 3.2's `docker-compose.yml` change implements the adapter-notes
Docker scenario's `GIVEN`, so it implements something in scope.

Every line citation the revision introduced matches the current working tree. `docker-compose.yml`
line 119 carries `cpus: "${LH_EXASOL_CPUS:-4}"`. `.github/workflows/ci.yml` carries the measurement
step at lines 515-542 and the three static entries at 648, 753, and 858, which are the post-merge
numbers rather than the stale 537/647/750/853 the earlier review cited. `bench/.env.example` line 15
carries `LH_EXASOL_CPUS=4`. `bench/run.sh` line 368 carries the quota comment. `bench/README.md`
line 108 sits inside the Parallelism section task 5.2 rewrites. Both renamed test symbols exist.

Cluster coherence holds. Group A absorbs `docker-compose.yml` beside the E2E test and the CI
workflow, which is correct because the compose constraint and the assertion that reads it are one
mental model. Group B's new dependency cell names task 4.5 against the compose change group A lands,
which is the real ordering constraint: `bench/.env.example` must not name `LH_EXASOL_CPUSET` before
`docker-compose.yml` reads it.

## Design Depth

[no objection — axis checked]

The revision adds no module, interface, or boundary, so the quick-diagnostic table does not apply.
Decision [9] § Alternatives rejects a `cgroup_quota_cores()` reader on the ground that it would
duplicate logic the standard library already runs and would return `None` on every call inside the
sandbox. That is the correct call under the deep-module and information-leakage rules: the rejected
reader would have added a second owner for a decision `std::thread::available_parallelism()` already
owns, and its dead `Option` would have leaked a cgroup-layout assumption into the adapter.

`[ADR_OVERPROMOTION]` check on the one new `Promotes to ADR: yes` entry: decision [9] passes the
gate. Its substance is a design and behavior boundary, namely that the adapter detects a CPU limit
expressed as affinity and not one expressed only as a CFS bandwidth quota, with issue #421 as the
tracked exception. That is not a procedural or workflow decision, and it is not a corollary of
decision [1], which promotes the separate claim that the core count is a resolved input rather than
configuration or state. The harness half of [9] rides along with the boundary rather than carrying
the promotion on its own.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Impact (line 107)
- Issue: the section opens "Four user-visible changes ship with this plan. All four are breaking." Five bolded changes follow, at lines 109, 111, 113, 115, and 117. The revision added the `LH_EXASOL_CPUSET` change at line 113 and left the count. The second sentence is also false against the fifth change, whose own text at line 117 reads "No code reads it, so nothing breaks." The pre-revision text carried the same off-by-one, so the revision inherited one error and added another.
- Fix: In plan.md § Impact line 107, replace "Four user-visible changes ship with this plan. All four are breaking." with "Five user-visible changes ship with this plan. Four are breaking. The `adapterNotes` change is not."

#### [PROSE_UNCLEAR] ADVISORY
- Location: decision-log.md § Review Findings, first `[plan-review]` entry (lines 110-113); decision-log.md § Interview answer A4 (line 15); decision-log.md § Design Decisions [9] supersession line (line 83)
- Issue: entry [9]'s supersession line names two superseded records, "the verification mechanism in decision [5], and the mechanism claim in interview answer A1". Two further records carry the falsified quota mechanism and are named nowhere. Interview answer A4 at line 15 specifies an E2E check "under a known `LH_EXASOL_CPUS`-constrained container". The round-1 `[plan-review]` entry at line 113 states "Task 3.2 now reads the effective quota from the running container through `docker exec` on `/sys/fs/cgroup/cpu.max`" and "new task 3.4 measures the runner and lowers `LH_EXASOL_CPUS` for the E2E job when needed". Both sentences are now false against plan.md, where task 3.2 edits `docker-compose.yml`, task 3.3 reads `cpuset.cpus.effective`, and task 3.4 writes `LH_EXASOL_CPUSET`. Preserving the audit trail is the right call, and task 3.3 line 161 already depends on that entry keeping the old test name. The defect is that a reader cannot tell which parts are history without reading entry [9] and diffing plan.md.
- Fix: Extend entry [9]'s supersession line in decision-log.md (line 83) to name interview answer A4 and the round-1 `[plan-review]` entry "The Docker CPU-quota probe asserted a value with no discriminating power" alongside decision [5] and answer A1. Add one line directly under that entry's heading at line 110 reading "Superseded by decision [9]. The task numbers and the `cpu.max` mechanism below record the pre-revision plan and are kept as the audit trail for the finding task 3.3 cites by its old test name."

### Guardrail sweep on the revised prose

[no objection — sub-check certified]

The text the revision added carries no em dash, no semicolon, and no contraction. The em dashes
`git grep` returns in the artifacts are pre-existing recorded spec text, namely the feature title and
the memory-budget Background bullet carried over from
`specs/vs-adapter/create-virtual-schema-adapter-notes/spec.md`, plus one table cell where `—` means
"none". Tables and recorded spec text fall outside the governed set. § CPU-limit detection boundary
states scale with measurements rather than intensifiers, and it names the magnitude as unmeasured
rather than hedging it.
