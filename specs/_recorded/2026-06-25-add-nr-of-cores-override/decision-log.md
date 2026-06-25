# Decision Log: add-nr-of-cores-override

Date: 2026-06-25

## Interview

**Q:** Does an explicit `NR_OF_CORES` property override the connect-back `SELECT PARAM_VALUE('NR_OF_CORES')` value?
**A:** Yes. Precedence: explicit `NR_OF_CORES` property (parses to integer ≥1) overrides; else the connect-back auto-detected value; else 0 (unknown). An absent/empty/non-positive property value is ignored (falls back to auto-detect).

**Q:** Should the (detected or overridden) core count change the DEFAULTS of `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF`?
**A:** Yes. When those properties are absent/empty/non-positive, default each to `max(nr_of_cores, 1)` (was a hard-coded 1). An explicitly supplied `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF` positive integer still wins over the cores-driven default.

**Q:** Behavior compatibility?
**A:** Backward compatible when cores are unknown (0 → defaults stay 1, i.e. identical single-threaded behavior). When cores are known or overridden, scans now auto-parallelize — this is an INTENTIONAL behavior change that must be documented in the spec delta / ADR.

**Q:** Is `PARALLELISM_FACTOR` affected?
**A:** No change to its resolution logic (already defaults to `max(nr_of_cores*2, 8)`); it simply now sees an overridable nr_of_cores.

**Q:** Any cap on the cores-driven default?
**A:** No additional cap; the value is the operator-supplied or detected core count. (The engine's existing 80% concurrency throttle and PARALLELISM_FACTOR floor of 8 remain.)

**Q:** Version bump?
**A:** crate `lakehouse-engine` 0.10.0 → 0.11.0 (new feature, backward-compatible default-when-unknown).

## Design Decisions

### [1] Property-override takes full precedence over connect-back detection

- **Decision:** When `NR_OF_CORES` VS property is present and parses to an integer ≥ 1, the adapter uses it directly and skips issuing `SELECT PARAM_VALUE('NR_OF_CORES')` over the connect-back session. The connect-back session for `SELECT NPROC()` (cluster node count) is still opened normally.
- **Alternatives:** (a) Always run connect-back detection and ignore the property; (b) Use the maximum of the two; (c) Use the property only when connect-back fails.
- **Rationale:** Full override is the simplest mental model for operators. When an operator explicitly supplies the core count, that is a deliberate act and they expect it to be used. Combining or averaging introduces surprising behavior. The property being a "fallback" (option c) would make it useless in the most common deployment scenario where connect-back works.
- **Promotes to ADR:** no

### [2] DataFusion threading defaults are now cores-driven, not hard-coded

- **Decision:** `resolve_df_target_partitions` and `resolve_df_threads_per_udf` now accept `nr_of_cores: u32` and default to `max(nr_of_cores, 1)` instead of the hard-coded constant `1`. The explicit-wins behavior is preserved. This is an intentional breaking behavior change for deployments where `NR_OF_CORES` resolves to a value > 1.
- **Alternatives:** (a) Keep hard-coded default `1`, require explicit property to enable threading (old behavior); (b) Apply a fraction of cores (e.g., `nr_of_cores / parallelism_factor`) as the default; (c) Cap the default at some fixed maximum.
- **Rationale:** Hard-coded `1` leaves DataFusion single-threaded on multi-core nodes by default — this defeats the purpose of detecting the core count. Using `max(nr_of_cores, 1)` makes the system behave "right by default" without operator tuning, while remaining backward-compatible when cores are unknown (0 → 1). A fraction-of-cores default (option b) adds complexity and requires the operator to understand the interaction with `PARALLELISM_FACTOR`; the simpler approach is to use all detected cores and let the operator reduce via explicit properties if needed. The engine's 80% concurrency throttle provides the actual safety net.
- **Promotes to ADR:** yes

### [3] Version bump to 0.11.0 (minor, not patch)

- **Decision:** Bump `lakehouse-engine` crate version from `0.10.0` to `0.11.0`.
- **Alternatives:** Patch bump to `0.10.1`.
- **Rationale:** The behavior change (DataFusion threading defaults become cores-driven when cores are known) is a new feature and an intentional behavior change for existing clusters where `NR_OF_CORES` resolves to > 1. A minor version bump correctly signals a backward-compatible feature addition per SemVer. A patch bump would understate the behavioral impact.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
