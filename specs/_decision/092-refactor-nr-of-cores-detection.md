# Decisions: refactor-nr-of-cores-detection

## ADR: The core count is a resolved input, not configuration or state

**ID:** core-count-resolved-input-not-configuration
**Plan:** refactor-nr-of-cores-detection
**Status:** Accepted

### Context

The adapter resolved a per-node core count at `createVirtualSchema` time through an `NR_OF_CORES` VS property, a `NOTE_NR_OF_CORES` adapterNotes entry, and a `0 = unknown` sentinel. Only the bench harness used the override, and Docker already controls the core count through the CPU quota. No pushdown reads the persisted count back, and only the four derived values round-trip, as with the cluster node count in issue #184.

### Decision

The `NR_OF_CORES` property and the `NOTE_NR_OF_CORES` entry are removed. `handle_create_virtual_schema` reads the core count from `std::thread::available_parallelism()` at one call site and passes it as a plain `u32` to the four derivations: parallelism factor, DataFusion target partitions, DataFusion threads per UDF, and the S3 connection budget.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the property as a bench-only escape hatch | Rejected: a property the adapter reads is supported whatever the docs say, and no need exists |
| Keep the adapterNotes entry for future readers | Rejected: nothing reads it, and an unread persisted value drifts |

### Consequences

A `CREATE VIRTUAL SCHEMA` that still supplies `NR_OF_CORES` succeeds and the property has no effect. An existing schema keeps its persisted entry, unread and inert through refresh. The four derivations keep a `nr_of_cores` parameter, so each stays a pure, testable function.

## ADR: Detection stays host-side, and the Docker E2E constrains CPU by affinity

**ID:** cpu-detection-affinity-not-quota
**Plan:** refactor-nr-of-cores-detection
**Status:** Accepted

### Context

A Docker E2E test that limited the container by CFS quota (`cpus: "2"` on four CPUs) read four cores. The standard library already computes the minimum of the affinity count and the cgroup quota. Live checks showed that the UDF sandbox mounts no cgroup filesystem, so no UDF can read a quota in any language, while CPU affinity propagates into it. The detection code was correct, and the test constrained CPU through a mechanism the sandbox cannot observe.

### Decision

Detection ships unchanged, with no cgroup-reading code. The Docker stack limits the Exasol container by CPU affinity (`cpuset`), and `adapter_detects_container_cpuset` asserts against `cpuset.cpus.effective`. A CPU limit set only as a quota cannot be detected inside the UDF sandbox, which issue #421 tracks.

### Options Considered

| Option | Verdict |
|--------|---------|
| A cgroup quota reader taking the minimum with affinity | Rejected: the path does not exist in the sandbox, and the standard library already does this |
| Support cgroup v1 as well as v2 | Rejected: neither is readable from the sandbox |
| Read the core count from UDF metadata | Rejected: a plan Non-Goal, and the SDK may not expose a core count |
| Keep the quota and relax the assertion | Rejected: a non-discriminating test records false evidence |

### Consequences

No adapter source changes. The Docker compose file, CI sites, E2E test, and bench files move from `LH_EXASOL_CPUS` to `LH_EXASOL_CPUSET`, which names a CPU set, and a stale `LH_EXASOL_CPUS` becomes inert. A host with fewer than four CPUs must set the variable, because Docker rejects a cpuset naming an absent CPU. A quota-limited node gets budgets sized for its affinity count, which causes contention and not wrong results, at an unmeasured magnitude (issue #421).
