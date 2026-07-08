# Verification Report: add-delete-benchmark-flag

## Bottom Line Up Front

**PASS — fully green, including a live docker-mode run.** All automated gates pass on the
post-`main`-merge state (`cargo test`, `cargo clippy`, `cargo fmt --check`, `bench/run.sh selftest`,
full `make test-e2e`), a code review found and fixed two doc/error-message defects, and — closing the
gap the first pass of this report flagged — a full live `BENCH_WITH_DELETES=1 make bench` run against
the local Docker stack completed successfully, producing the real delete-count sanity-check evidence
this feature is meant to prove. The OFF path was also re-confirmed live.

This plan touches zero Rust/engine code of its own — it is pure benchmark-harness and deploy tooling
(shell/Python/SQL + docs) — so no spec delta applies. While syncing with `main` (which had advanced
during implementation), the merge surfaced one pre-existing regression, unrelated to this plan, which
was root-caused and fixed as its own commit (see "Incidental Fix" below).

## Live Docker-Mode Run (plan task D.1) — the evidence this report previously lacked

`BENCH_SLC_VERSION=0.20.3 BENCH_WITH_DELETES=1 bash bench/run.sh`, full run, exit 0:

- Delete-bearing namespace authored idempotently (`make_deletes_docker.sh` skipped re-authoring since
  `tpch_deletes` already had all 8 tables).
- Report header: `namespace=tpch_deletes` / `deletes=on ns=tpch_deletes` — annotation present as
  designed.
- **Delete-count sanity check: `OK delete-count LINEITEM: 1709938 (~95.0% of baseline 1800093)`** — the
  direct proof that Iceberg v2 merge-on-read position deletes are applied on read, landing exactly on
  the ~5%-deleted target.
- Full Q1–Q9b / NQ1–NQ5 query set ran to completion against the delete-bearing tables with correct,
  non-empty results.
- All 10 pushdown checks passed (`shard_key`, `limit`, `filter`, `aggregates`, `countdistinct`,
  `arg_expr`, `LIKE`, `order_by`, ...).
- A second, immediate OFF-path run (`bash bench/run.sh`, no flag) completed cleanly against the
  pristine baseline namespace, confirming the reversibility guarantee live, not just by code review.

### A second, independent bug found and fixed the same way

After the ON-path run succeeded (using an explicit `BENCH_SLC_VERSION=0.20.3` override), a follow-up
OFF-path confirmation run WITHOUT that override failed: `bench/run.sh`'s own hardcoded default
(`SLC_VERSION="${BENCH_SLC_VERSION:-0.16.0}"`, line 181) is stale — it installs an SLC built against a
different ABI fingerprint than the `.so` (built against `exasol-udf-sdk` 0.20.3, per `Cargo.toml`),
causing `F-UDF-CL-RUST-9001: ABI version mismatch: expected 4, found 6` on the very first VS operation.
The `Makefile`'s own `install-slc` target already correctly defaults to `SLC_VERSION ?= 0.20.3` — only
`run.sh`'s independent hardcoded default had drifted. Fixed by updating `run.sh`'s default to `0.20.3`
to match. Re-ran the OFF-path bench with no override afterward: clean pass, all pushdown checks OK,
`Done.` This is unrelated to the delete-flag feature (it affects the pre-existing, unmodified default
codepath) but was blocking any bench run in this environment, so fixed as its own small commit.

### Why this took several attempts (root cause, for the record)

Two earlier live-run attempts stalled for 15–40+ minutes and were killed. Root cause: a **stale
`bench/.env`**, left over in the working tree from earlier `test1` (remote AWS) benchmarking, set
`BENCH_TARGET=remote` plus a real (now-unreachable) `EXASOL_HOST`. Every `make bench` invocation in
this session was silently trying to reach that dead remote host instead of the local Docker stack.
`wait_exasol`'s TCP check has no per-attempt connect timeout, so the unreachable host didn't fail
fast — it hung for a long time per retry. Exporting `BENCH_TARGET=docker` alone was not sufficient to
fix it either: other vars the same `.env` set (`LH_BUCKETFS_PORT`, `EXASOL_SYS_PASSWORD`,
`BUCKETFS_WRITE_PASS`) still leaked into the "docker-mode" run, causing a second-order failure
(BucketFS upload against the wrong port/password). Moving `bench/.env` aside entirely resolved it.
A permanent note was added to `CLAUDE.md` ("Bench harness gotchas") so this doesn't recur, and
[issue #87](https://github.com/exasol-labs/lakehouse-engine-rs/issues/87) was filed separately for the
underlying usability gap (long-running bench steps produce no intermediate progress output, making a
slow run indistinguishable from a hung one).

## Incidental Fix (from syncing with `main`, not part of this plan's scope)

Merging `origin/main` (7 commits, including a join-pushdown rendering change, `e66b95a`) brought in two
new E2E tests that failed live: `e2e_scalar_over_aggregate_grouped_join_result_correct` and its
N-table variant. Root-caused (confirmed against the live stack, not theorized): **not** a
pushdown/adapter bug — the VS correctly advertised exactly the columns the long-lived Docker Iceberg
warehouse actually had. The seed helper (`tests/common/seed.rs`) reused an existing `fact_lineitem`
table without checking its schema; the persistent warehouse still had a pre-`e66b95a` 4-column version,
so the newly-added `L_RETURNFLAG`/`L_EXTENDEDPRICE` columns never materialized. Fixed by making the
seed's idempotency schema-aware (reuse only when the persisted field signature matches; otherwise drop
and recreate). Committed separately (`fix(e2e): reseed Iceberg tables whose persisted schema drifted
from the seed`) — not mixed into the bench-flag commit. No production/adapter code changed; a
version-bump that commit initially included was reverted since the fix is test-harness-only.

## Automated Checks (post-`main`-merge state)

| Check | Command | Result |
|---|---|---|
| Build | UDF `.so` release build | PASS |
| Test | `cargo test` | PASS — 551 passed, 2 ignored, 0 failures |
| Lint | `cargo clippy --all-targets` | PASS — no issues found |
| Format | `cargo fmt --check` | PASS — clean |
| Bench selftest | `./bench/run.sh selftest` | PASS — `selftest OK` |
| E2E | `make test-e2e` | PASS — all 5 binaries green (capability, count_distinct, join, positional_deletes, scan), 0 failed |
| Live bench (ON) | `BENCH_WITH_DELETES=1 bash bench/run.sh` | PASS — see "Live Docker-Mode Run" above |
| Live bench (OFF) | `bash bench/run.sh` | PASS — baseline run clean |

## Code Review

Dispatched to `code-reviewer` against all 12 changed/new files. Findings:

1. **[Fixed]** `bench/run.sh` and `bench/README.md` told the operator to invoke
   `deploy/scripts/make-deletes-remote.sh <env>` — that script is entirely env-var-configured and takes
   no positional argument. Both call sites now describe the correct invocation.
2. **[No action]** `bench/make_deletes_docker.sh` duplicates `run_fixtures.sh`'s `SPARK_CONF` array
   verbatim — a documented, deliberate duplication (no cross-`docker run`-boundary way to share a bash
   array).

Verified clean: the `BENCH_WITH_DELETES=0` OFF-path invariant, SQL↔Python delete-logic lockstep, both
delete-authoring callers' idempotency contracts, the Terraform S3-upload wiring, the `bench-remote.sh`
teardown-trap signal handling, and the Docker network name used by the new Spark-authoring helper.

## Scenario Coverage

| Assertion | Test Type | Location | Status |
|---|---|---|---|
| Namespace switch: OFF → baseline ns, ON → delete ns | Offline selftest + live | `bench/run.sh selftest` + live run | PASS |
| Report header annotated only when deletes ON | Offline selftest + live | `bench/run.sh selftest` + live run (`deletes=on ns=tpch_deletes`) | PASS |
| Delete-count ratio bounds accept 0.90–0.98, reject 0.80/1.00 | Offline selftest + live | `bench/run.sh selftest` + live run (95.0% observed) | PASS |
| Delete-bearing LINEITEM ≈ 95% of baseline (live) | Integration (docker, live) | `bench/run.sh` (`BENCH_WITH_DELETES=1`) | **PASS** — `1709938 (~95.0% of baseline 1800093)` |
| Engine reads MOR position deletes correctly (read-path backstop) | E2E (CI) | `make test-e2e` / `packaging/e2e-harness-positional-deletes` | PASS (unchanged by this plan, re-verified green post-merge) |

## Scope Confirmation

- No Rust/engine code changed by this plan itself (confirmed via `git status`/`git show --stat` on the
  bench-flag commit — all changes are in `bench/`, `deploy/`, `scripts/spark-fixtures/`, and docs). A
  separate, clearly-labeled commit fixes an unrelated pre-existing regression surfaced by merging
  `main` (see "Incidental Fix").
- No spec delta, per plan.md's documented convention (bench harness is not a spec feature).
- New files: `scripts/spark-fixtures/create_tpch_deletes.sql`, `bench/make_deletes_docker.sh`,
  `deploy/scripts/make_deletes_remote.py`, `deploy/scripts/make-deletes-remote.sh`,
  `deploy/scripts/bench-remote.sh`.
- Modified: `bench/run.sh`, `bench/.env.example`, `bench/README.md`, `deploy/README.md`,
  `deploy/data-stack/main.tf`, `deploy/data-stack/outputs.tf`, `CLAUDE.md` (bench gotcha note).

## Remaining Follow-up (not a completion gate)

Task D.2 — an actual `bench-remote.sh test1` run against the real AWS-deployed Exasol cluster — remains
an operational follow-up, as already scoped in the plan. `test1` is not currently deployed (checked
live: the leftover `EXASOL_HOST` from a prior session is unreachable); standing it back up costs real
AWS money and requires explicit user go-ahead before attempting.
