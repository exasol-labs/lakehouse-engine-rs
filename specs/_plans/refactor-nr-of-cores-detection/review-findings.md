# Code Review Findings: refactor-nr-of-cores-detection

## Summary
- Files reviewed: 16
- Total findings: 10 (standard: 8, expert: 2)

Verified clean before writing this file: `cargo clippy --all-targets` (0 warnings),
`cargo test -p lakehouse-engine --lib adapter` (912 passed, 0 failed),
`bash bench/run.sh selftest` (exit 0).

Two judgment calls the brief asked about came out in the implementation's favour and are
deliberately NOT findings:

- **`core_count_or_default`'s `.get() as u32` cast.** Rejecting
  `u32::try_from(..).unwrap_or(u32::MAX)` was correct. `available_parallelism()` is bounded by a
  cgroup quota or a CPU mask; a value above `u32::MAX` is not a reachable state, and the branch
  would be untestable. The prior `available_parallelism_or_0` used the same cast, so this is not a
  new exposure.
- **The deleted `nr_of_cores == 0` branch in `resolve_s3_max_connections` is genuinely
  unreachable.** `resolve_s3_max_connections` has exactly one production call site
  (`crates/lakehouse-engine/src/adapter/mod.rs:268`), fed by `resolve_nr_of_cores()`
  (`mod.rs:249`), which returns `NonZeroUsize::get()` on `Ok` and the literal `1` on `Err`. No path
  can deliver `0`.

The doc-comment rewrites were also checked specifically for transition narrative. None of the
rewritten comments in `adapter/mod.rs`, `scan/diagnostics.rs`, `scan/mod.rs`, or `scan/spec.rs`
uses "no longer", "used to be", or "the removed property"; each states present behaviour. Two of
them are nonetheless factually wrong about the current code, which is findings 1 and 2 below.

## Standard fixes

### crates/lakehouse-engine/src/adapter/mod.rs

#### [OUTDATED_COMMENT] `build_adapter_notes` doc claims every recorded entry is read back

- Location: lines 661-663
- Issue: the rewritten doc comment ends "Each entry is one a pushdown reads back". That is false
  for `DF_THREADING_MODE`, which the same rewrite added to the list two lines above.
  `NOTE_DF_THREADING_MODE` (declared `mod.rs:66`) is written at `mod.rs:687` and read by nothing:
  `handle_pushdown_request` reads `PARALLELISM_FACTOR`, `DF_TARGET_PARTITIONS`, `DF_BATCH_SIZE`,
  `DF_THREADS_PER_UDF`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `S3_MAX_CONNECTIONS`, and
  `JOIN_BROADCAST_MAX_BYTES` at `mod.rs:374-401`, plus `TABLE_MAP` at `mod.rs:524`. The comment
  contradicts its own list.
- Fix: In `crates/lakehouse-engine/src/adapter/mod.rs`, replace the sentence "Each entry is one a
  pushdown reads back; the per-node core count the resource budgets are derived from is not among
  them." in the `build_adapter_notes` doc comment with two accurate sentences: that every entry
  except `DF_THREADING_MODE` is read back by `handle_pushdown_request`, that `DF_THREADING_MODE` is
  recorded as an operator-visible record of which derivation ran, and that the per-node core count
  is a derivation input the adapter discards rather than a recorded entry.

### crates/lakehouse-engine/src/scan/spec.rs

#### [OUTDATED_COMMENT] `DEFAULT_S3_MAX_CONNECTIONS` doc undercounts its consumers

- Location: lines 1318-1324
- Issue: the rewritten comment asserts the constant serves "two roles" and names the serde default
  (`spec.rs:1330`) and the adapter's pushdown-side `S3_MAX_CONNECTIONS` adapterNote fallback
  (`mod.rs:400`). There is a third production consumer:
  `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs:192` passes the
  constant to `build_table_root_store` for the Delta plan-time table-root object store. plan.md's
  own Consequences table names that call site as a surviving consumer, so the omission is a
  regression against the plan's own analysis. Anyone changing the value now misses a live call
  site. The previous wording had the same gap, but stated it as an open list rather than an
  explicit count of two.
- Fix: In `crates/lakehouse-engine/src/scan/spec.rs`, change the `DEFAULT_S3_MAX_CONNECTIONS` doc
  comment from "in two roles" to "in three roles" and add the third: the connection budget
  `delta_format_reader::…` (`crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs:192`)
  gives `build_table_root_store` when reading a Delta table root at plan time. Keep the existing
  reverse-dependency rationale sentence unchanged.

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [MISSING_BOUNDARY_TEST] `core_count_or_default`'s success arm has no test

- Location: lines 552-578 and 585-592
- Issue: `core_count_defaults_to_one_when_detection_fails` pins the `Err` arm. Nothing pins the
  `Ok` arm. The only coverage is `assert!(nr_of_cores >= 1, …)` at line 553-556 in
  `core_count_from_available_parallelism_is_not_recorded`, which cannot fail: on `Ok` the value
  comes from `NonZeroUsize::get()` and on `Err` it is the literal `1`, so both arms satisfy it. An
  implementation of `core_count_or_default` that discarded `Ok(n)` and returned `1`
  unconditionally, collapsing every node in the cluster to a single-thread budget, would pass the
  entire unit suite. This is exactly the branch the plan introduced the pure seam to make testable.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, add a test
  `core_count_uses_the_detected_count_when_detection_succeeds` immediately after
  `core_count_defaults_to_one_when_detection_fails`, asserting
  `core_count_or_default(Ok(std::num::NonZeroUsize::new(12).unwrap())) == 12` with a message
  stating that a reported count must pass through unchanged rather than collapse to the
  detection-failure default.

#### [MAGIC_NUMBER] Rewritten S3 budget test hard-codes 4 instead of `S3_CONNECTIONS_PER_THREAD`

- Location: lines 1791, 1795-1798, 1802-1805
- Issue: `resolve_s3_max_connections_auto_one_core_yields_four` asserts the literal `4` twice and
  carries the literal in its own name. The immediately preceding test in the same file,
  `resolve_s3_max_connections_auto_scales_with_cores`, expresses every expectation as
  `S3_CONNECTIONS_PER_THREAD` or `8 * S3_CONNECTIONS_PER_THREAD` (lines 1745-1775). The constant is
  in scope through `use super::*`. Two adjacent tests of one formula now disagree on how the
  multiplier is written, and a future change to `S3_CONNECTIONS_PER_THREAD` breaks one of them with
  a bare-number diff.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, rename
  `resolve_s3_max_connections_auto_one_core_yields_four` to
  `resolve_s3_max_connections_auto_one_core_yields_one_threads_share`, and replace both expected
  literals `4` with `S3_CONNECTIONS_PER_THREAD`. Keep both assertion messages as they stand.

#### [DUPLICATE_TEST] `NR_OF_CORES` absence from fresh adapterNotes is asserted twice

- Location: lines 381-384 and 575-578
- Issue: `adapter_notes_omit_cluster_nodes` (line 361) and
  `core_count_from_available_parallelism_is_not_recorded` (line 552) both build notes from the same
  `{"type": "createVirtualSchema"}` request with the same argument list and both assert
  `parsed.get("NR_OF_CORES").is_none()`. The assertion added to `adapter_notes_omit_cluster_nodes`
  is the redundant one: that test owns the cluster-node scenario, while
  `core_count_from_available_parallelism_is_not_recorded` is the test plan.md's § Verification maps
  to the core-count scenario. Both were requested by task 2.3; the duplication is real regardless.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, delete the
  `assert!(parsed.get("NR_OF_CORES").is_none(), …)` block at lines 381-384 of
  `adapter_notes_omit_cluster_nodes`, leaving that test on its single `CLUSTER_NODES` concept. Do
  not touch the equivalent assertion in `core_count_from_available_parallelism_is_not_recorded`.

### crates/lakehouse-engine/tests/e2e_scan_test.rs

#### [SWALLOWED_ERROR] `exasol_container_cpu_quota` discards the `docker exec` exit status and stderr

- Location: the `exasol_container_cpu_quota` helper, the `.output()` call and the `raw` binding
  that follows it
- Issue: `Command::output()` returns `Ok` for a command that ran and failed. The helper reads only
  `out.stdout` and never inspects `out.status` or `out.stderr`. On a cgroup v1 host, or against a
  container where `/sys/fs/cgroup/cpu.max` is absent, `cat` exits non-zero, writes the real cause
  to stderr, and leaves stdout empty; the helper then panics with
  "/sys/fs/cgroup/cpu.max in <container> is empty: \"\"", which names neither the missing file nor
  cgroup v1. This test has never been executed against a live container, so a misdiagnosed first
  failure is the likely outcome rather than a hypothetical one.
- Fix: In `crates/lakehouse-engine/tests/e2e_scan_test.rs`, in `exasol_container_cpu_quota`, add a
  check immediately after the `.output()` call: when `!out.status.success()`, panic with a message
  carrying the container name, `out.status`, and
  `String::from_utf8_lossy(&out.stderr).trim()`, and stating that a missing `cpu.max` means the
  host runs cgroup v1, which this assertion does not support.

#### [MISSING_BOUNDARY_TEST] `exasol_container_cpu_quota` silently truncates a non-integral quota

- Location: the `exasol_container_cpu_quota` helper, the `let cores = quota_us / period_us;` line
  and the `cores >= 1` assertion below it
- Issue: `cpu.max` holds a quota and a period in microseconds and Docker's `--cpus` accepts a
  fractional value, so `LH_EXASOL_CPUS=2.5` yields `250000 100000`. Integer division truncates that
  to `2`. The helper guards only the below-one-core case; a fractional quota above one core passes
  the guard and hands the caller a number that need not match what `available_parallelism()`
  reports inside the container. `adapter_detects_container_cpu_quota` then fails with a message
  blaming the adapter for a harness configuration problem. The helper's own `cores >= 1` message
  already instructs the operator to "set LH_EXASOL_CPUS to a whole number of cores", so only half
  of the stated contract is enforced.
- Fix: In `crates/lakehouse-engine/tests/e2e_scan_test.rs`, in `exasol_container_cpu_quota`, add an
  assertion between the two `parse` calls and the `cores >= 1` check that
  `quota_us % period_us == 0`, with a message naming `quota_us`, `period_us`, and the truncated
  ratio, and stating that a fractional CPU quota makes the expected core count ambiguous so
  `LH_EXASOL_CPUS` must be a whole number.

### .github/workflows/ci.yml

#### [OUTDATED_COMMENT] Four CPU-quota comments still assert a fixed 2-vCPU runner

- Location: lines 508-512, 643, 746, 849
- Issue: the `e2e` job's rewritten comment says "Hosted runners are small" and points at the new
  measuring step, but still quotes the Docker error "range of CPUs is from 0.01 to 2.00, as there
  are only 2 CPUs available" as if the runner had 2 vCPUs, which the rewrite deliberately stopped
  claiming. Separately, `e2e-lakekeeper` (line 643), `e2e-unity` (line 746), and `e2e-azure`
  (line 849) each carry "See the `e2e` job's identical override — same 2-vCPU hosted-runner cap."
  The `e2e` job's override is no longer identical: it now has a runtime cap step that those three
  jobs do not have, and no longer asserts a 2-vCPU count. (Those three jobs run
  `e2e_lakekeeper_test`, `e2e_unity_test`, and `e2e_azure_test`, none of which contains
  `adapter_detects_container_cpu_quota`, so they are functionally unaffected and must NOT gain the
  cap step. Only their comments are wrong.) plan.md task 3.4 also asked for the vCPU comment to be
  corrected to the measured count, which no comment yet states.
- Fix: In `.github/workflows/ci.yml`, rewrite the four comments. At lines 508-512, drop the quoted
  "only 2 CPUs available" Docker error and state instead that `docker-compose.yml`'s exasol default
  of 4 can exceed a hosted runner's core count, that the cap step below measures the runner and
  lowers the value, and that the printed `nproc` in the job log is the authority on the runner's
  size. At lines 643, 746, and 849, replace "See the `e2e` job's identical override — same 2-vCPU
  hosted-runner cap." with a comment stating that this job caps the Exasol container the same way
  `e2e` does but needs no runtime measurement, because its test binary carries no container-quota
  assertion.

## Expert fixes

### .github/workflows/ci.yml

#### [INFORMATION_LEAKAGE] `LH_EXASOL_CPUS` has two owners and the winner is an unpinned platform rule

- Location: line 513 (job-level `env:`) and line 540 (the `$GITHUB_ENV` write in the "Cap the
  Exasol CPU quota below the runner core count" step)
- Issue: one value, the Exasol container's CPU quota, is now declared in two places in the same
  job. Which one `docker compose` sees in the later steps is decided entirely by GitHub Actions'
  precedence between a job-level `env:` entry and a same-named variable written to `$GITHUB_ENV`
  by an earlier step. That precedence is not pinned by anything in this repository and was not
  verified during implementation; the change assumes the `$GITHUB_ENV` write wins. If it does not,
  the step is a silent no-op, `LH_EXASOL_CPUS` stays `2`, and on a 2-vCPU runner
  `adapter_detects_container_cpu_quota` fails on its own `PRECONDITION UNMET` assertion, turning a
  required check red for a reason that points at the test rather than at the workflow. CLAUDE.md's
  Verification discipline forbids resting on an unchecked platform behaviour, and the ordering
  itself is correct (the step precedes "Pull stack images" and "Start stack"), so the only defect
  is the split ownership. The structural fix removes the question instead of answering it.
- Fix: In `.github/workflows/ci.yml`, make the cap step the single owner of `LH_EXASOL_CPUS` for
  the `e2e` job. Delete the `LH_EXASOL_CPUS: "2"` entry from the `e2e` job's `env:` block at line
  513, keeping `EXASOL_IMAGE`. In the "Cap the Exasol CPU quota below the runner core count" step,
  replace `quota="$LH_EXASOL_CPUS"` with a literal starting value `quota=2` and a comment naming it
  as the job's baseline quota, keep the existing `nproc` print, the `cores -lt 2` hard failure, and
  the `quota -ge cores` lowering to `cores - 1`, then keep the unconditional
  `echo "LH_EXASOL_CPUS=$quota" >> "$GITHUB_ENV"`. Add a final
  `echo "effective LH_EXASOL_CPUS: $quota"` so the resolved value is visible in the job log. Leave
  the `LH_EXASOL_CPUS: "2"` job-level entries in `e2e-lakekeeper`, `e2e-unity`, and `e2e-azure`
  untouched: those jobs have no cap step, so they have exactly one owner already.

### crates/lakehouse-engine/src/adapter/mod.rs

#### [UNREACHABLE_CODE] Delete the dead `.max(1)` floor in `resolve_s3_max_connections`

- Location: line 877
- Issue: routed to Expert because the fix's failure mode is a passing test over a wrong budget.
  `auto_threads_per_udf` floors at `1` (`mod.rs:813`) and
  `S3_CONNECTIONS_PER_THREAD` is `4` (`mod.rs:114`), so `per_instance_threads *
  S3_CONNECTIONS_PER_THREAD` is never below `4` and the trailing `.max(1)` is dead on every input.
  The only existing assertion over this floor is the weak `>= 1` at the end of
  `resolve_s3_max_connections_auto_scales_with_cores`, which would keep passing even if the
  reasoning above were wrong and the product could reach `0`. Confirm the argument before removing
  the guard: `resolve_s3_max_connections` has exactly one production call site (`mod.rs:268`) and
  its `nr_of_cores` comes from `resolve_nr_of_cores()` (`mod.rs:249`), which is positive by
  construction.
- Fix: In `crates/lakehouse-engine/src/adapter/mod.rs`, replace the final expression of
  `resolve_s3_max_connections`, `(per_instance_threads * S3_CONNECTIONS_PER_THREAD).max(1)`, with
  `per_instance_threads * S3_CONNECTIONS_PER_THREAD`. Then strengthen the floor assertion in
  `crates/lakehouse-engine/src/adapter/adapter_tests.rs`'s
  `resolve_s3_max_connections_auto_scales_with_cores` from
  `resolve_s3_max_connections(&absent, 2, 8) >= 1` to
  `assert_eq!(resolve_s3_max_connections(&absent, 2, 8), S3_CONNECTIONS_PER_THREAD, …)` so the
  removed guard's contract is pinned by an exact value rather than an inequality that a zero-budget
  regression could not break. Run `cargo test -p lakehouse-engine --lib adapter` and show 0
  failures.
