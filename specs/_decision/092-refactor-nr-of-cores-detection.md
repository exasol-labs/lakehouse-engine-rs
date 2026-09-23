# Decisions: refactor-nr-of-cores-detection

## ADR: The core count is a resolved input, not configuration or state

**ID:** core-count-resolved-input-not-configuration
**Plan:** refactor-nr-of-cores-detection
**Status:** Accepted

### Context

The adapter resolved a per-node core count at `createVirtualSchema` time through three
mechanisms: an `NR_OF_CORES` VS property override, a `NOTE_NR_OF_CORES` adapterNotes
entry, and a `0 = unknown` sentinel. No recorded need for the property override
exists; the bench harness was its only user, and the docker bench path already
controls the real core count through the container's CPU quota. `find_referencing_symbols`
on `NOTE_NR_OF_CORES` returns two writes and zero reads: no pushdown ever reads the
persisted count back. Only the four derived entries (parallelism factor, DataFusion
target-partition count, DataFusion threads-per-UDF count, and the S3 connection
budget) round-trip. This is the same shape issue #184 established for the cluster
node count, which is read once in `dispatch` and threaded onward as a plain `usize`
argument rather than persisted.

### Decision

Remove the `NR_OF_CORES` VS property and the `NOTE_NR_OF_CORES` adapterNotes entry.
Read the per-node core count from `std::thread::available_parallelism()` at one call
site in `handle_create_virtual_schema`, then pass it to the four derivations as a
plain `u32`.

### Options Considered

- Keep the property as an undocumented bench-only escape hatch. Rejected: a property
  the adapter reads is a supported property whatever the docs say, and no recorded
  need for the override exists.
- Keep the `NOTE_NR_OF_CORES` adapterNotes entry for future readers. Rejected:
  nothing reads it, and an unread persisted value drifts from the node that produced
  it.

### Consequences

A `CREATE VIRTUAL SCHEMA` statement that still supplies `NR_OF_CORES` succeeds and
the property has no effect, because the adapter reads properties by name and ignores
unknown names. A schema created under an earlier version keeps its persisted
`NR_OF_CORES` entry, which survives unread and inert through refresh. The four
derivations keep their `nr_of_cores` parameter rather than reading the host
themselves, so each stays a pure, independently testable function.

## ADR: Detection stays host-side, and the Docker E2E constrains CPU by affinity

**ID:** cpu-detection-affinity-not-quota
**Plan:** refactor-nr-of-cores-detection
**Status:** Accepted

### Context

Task 3.2's Docker E2E test constrained the Exasol container by a CFS bandwidth quota
(`cpus: "2"` on a four-CPU host) and asserted `DF_THREADS_PER_UDF == 2`. The test read
`4`. The first explanation was that `std::thread::available_parallelism()` reads only
`sched_getaffinity` and never a cgroup quota, requiring a `cpu.max` reader combined by
a minimum. Reading the pinned 1.94 toolchain refuted this: `library/std/src/sys/thread/unix.rs:177`
already computes `min(sched_getaffinity count, cgroup quota)`, parsing `cpu.max` for
cgroup v2 and `cpu.cfs_quota_us`/`cpu.cfs_period_us` for v1. A hand-written reader
would have duplicated that logic in a narrower form.

Three live checks located the real cause. The container's ordinary namespace reads
`/sys/fs/cgroup/cpu.max` as `200000 100000` (2 cores), and the `exasql` daemon shares
that mount and cgroup namespace. A `PYTHON3 SCALAR` probe run inside the UDF sandbox
reads neither that file nor `cgroup.controllers`: `/sys` exists, nothing is mounted
under it, and the probe reported `affinity=4`. Under `docker update --cpuset-cpus="0-1"`
the same probe reported `affinity=2` with cgroupfs still absent. The sandbox mounts no
cgroup filesystem, so no UDF reads a quota in any language, while CPU affinity
propagates into it. The detection code was never wrong; the test harness constrained
CPU through a mechanism the execution substrate cannot observe.

### Decision

`resolve_nr_of_cores()` and `core_count_or_default` ship exactly as designed, with no
cgroup-reading code added. The Docker stack constrains the Exasol container by CPU
affinity (`cpuset`) instead of by CFS bandwidth quota (`cpus:`), and
`adapter_detects_container_cpuset` asserts against `cpuset.cpus.effective`. A CPU
limit expressed only as a quota is not detectable from inside the UDF sandbox, and
issue #421 tracks that gap.

### Options Considered

- Add a `cgroup_quota_cores()` reader over `/sys/fs/cgroup/cpu.max` and take the
  minimum with the affinity count. Rejected: the path does not exist inside the UDF
  sandbox, so the reader would return `None` on every call and the minimum would
  collapse to the existing value, duplicating logic the Rust standard library already
  runs.
- Support cgroup v1 alongside v2 in an adapter-side reader. Rejected: neither cgroup
  version is readable from the sandbox, so the limitation is the missing mount, not
  the parsing.
- Read the core count from UDF metadata instead of the host. Rejected: plan.md states
  it as a Non-Goal, and the SDK is not confirmed to expose a core count at all.
- Keep the quota mechanism and relax the assertion to a non-discriminating check.
  Rejected: a passing non-discriminating test records false evidence, which an
  earlier plan-review round had already caught once.

### Consequences

No adapter source file changes. `docker-compose.yml`, four `.github/workflows/ci.yml`
sites, the Docker E2E test and its helper, `bench/.env.example`, `bench/run.sh`, and
`bench/README.md` move from `LH_EXASOL_CPUS` to `LH_EXASOL_CPUSET`, which names a CPU
set rather than a count. A stale `LH_EXASOL_CPUS` becomes inert. A host of fewer than
four CPUs must set the variable explicitly, because Docker rejects a cpuset naming an
absent CPU. A quota-limited node receives budgets sized for its affinity count, which
causes contention rather than wrong results, at an unmeasured magnitude, tracked as
issue #421.
