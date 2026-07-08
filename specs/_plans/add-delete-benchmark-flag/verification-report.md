# Verification Report: add-delete-benchmark-flag

## Bottom Line Up Front

**PASS with one caveat.** All automated gates required for this repo (`cargo test`, `cargo clippy`,
`cargo fmt --check`, `bench/run.sh selftest`) are green, and a code review found and the fixes for the
only two real defects (doc/error-message text naming a phantom CLI argument) are already applied. This
plan touches zero Rust/engine code — it is pure benchmark-harness and deploy tooling (shell/Python/SQL +
docs) — so no spec delta and no `make test-e2e` regression risk applies.

**Caveat:** the live docker-mode delete-bench run (plan task D.1: `BENCH_WITH_DELETES=1 make bench`
against the local stack) was attempted but not completed in this session — it was killed after stalling
with no new output for 15+ minutes past the UDF `.so` build, in an environment where a command-proxy
hook (`rtk`) and a cold host `cargo test` rebuild (triggered by an unrelated `cargo clean` earlier in the
session) both plausibly explain the silence without indicating an actual bug (a subsequent `cargo test`
run in the same environment showed identical multi-minute silent-but-progressing behavior — `target/`
grew from 5G to 11G with zero stdout — before completing successfully). The live run was not
re-attempted after the interruption. **A live `BENCH_WITH_DELETES=1 make bench` run is recommended
before merge** to close this gap with real evidence; everything else about the implementation has been
verified by code review + the offline `selftest` coverage that exercises the same string/threshold logic
the live run would exercise.

## Automated Checks

| Check | Command | Result |
|---|---|---|
| Build | UDF `.so` release build | PASS — completed during the (later-killed) D.1 attempt: `Finished 'release' profile [optimized] target(s) in 1m 19s`. No Rust source changed by this plan. |
| Test | `cargo test` | PASS — 539 passed, 2 ignored, 0 failures (21 suites, 15.18s) |
| Lint | `cargo clippy --all-targets` | PASS — no issues found |
| Format | `cargo fmt --check` | PASS — clean |
| Bench selftest | `./bench/run.sh selftest` | PASS — `selftest OK` (includes new `delete_header_suffix`, `delete_ratio_ok`, `resolve_delete_ns` cases) |
| Shell/Python syntax | `bash -n` (4 new/changed scripts) + `python3 -m py_compile make_deletes_remote.py` | PASS — all clean |

## Code Review

Dispatched to `code-reviewer` against all 12 changed/new files. Findings:

1. **[Fixed]** `bench/run.sh:395` and `bench/README.md:44` told the operator to invoke
   `deploy/scripts/make-deletes-remote.sh <env>` — that script is entirely env-var-configured and takes
   no positional argument. Impact was bounded (the ignored arg would just hit the script's `:?` guards
   and fail with the correct message), but both call sites now describe the correct invocation.
2. **[No action]** `bench/make_deletes_docker.sh` duplicates `run_fixtures.sh`'s `SPARK_CONF` array
   verbatim — flagged as a documented, deliberate duplication (no cross-`docker run`-boundary way to
   share a bash array); already carries an explicit "keep in lockstep" comment.

Verified clean (no findings): the `BENCH_WITH_DELETES=0` OFF-path byte-for-byte invariant (every new
code path is gated behind the flag or is provably inert when off), SQL↔Python delete-logic lockstep
between `create_tpch_deletes.sql` and `make_deletes_remote.py` (same 8 tables, same surrogate keys, same
`% 20 = 0` predicate, same TBLPROPERTIES), the idempotency contracts in both delete-authoring callers,
the Terraform S3-upload wiring, the `bench-remote.sh` teardown-trap signal handling, and the Docker
network name used by the new Spark-authoring helper.

## Scenario Coverage

| Assertion | Test Type | Location | Status |
|---|---|---|---|
| Namespace switch: OFF → baseline ns, ON → delete ns | Offline selftest | `bench/run.sh selftest` (`resolve_delete_ns` case) | PASS |
| Report header annotated only when deletes ON | Offline selftest | `bench/run.sh selftest` (`delete_header_suffix` case) | PASS |
| Delete-count ratio bounds accept 0.90–0.98, reject 0.80/1.00 | Offline selftest | `bench/run.sh selftest` (`delete_ratio_ok` case) | PASS |
| Delete-bearing LINEITEM ≈ 95% of baseline (live) | Integration (docker, live) | `bench/run.sh` (`BENCH_WITH_DELETES=1`) | **NOT RUN** — see caveat above |
| Engine reads MOR position deletes correctly (read-path backstop) | E2E (CI) | `make test-e2e` / `packaging/e2e-harness-positional-deletes` | Unchanged by this plan; not re-run this pass (no engine code touched) |

## Scope Confirmation

- No Rust/engine code changed (confirmed via `git status` — all changes are in `bench/`, `deploy/`,
  `scripts/spark-fixtures/`, and docs).
- No spec delta, per plan.md's documented convention (bench harness is not a spec feature).
- New files: `scripts/spark-fixtures/create_tpch_deletes.sql`, `bench/make_deletes_docker.sh`,
  `deploy/scripts/make_deletes_remote.py`, `deploy/scripts/make-deletes-remote.sh`,
  `deploy/scripts/bench-remote.sh`.
- Modified: `bench/run.sh`, `bench/.env.example`, `bench/README.md`, `deploy/README.md`,
  `deploy/data-stack/main.tf`, `deploy/data-stack/outputs.tf`.

## Recommendation

Ready for `/speq:record add-delete-benchmark-flag` from a code/spec-hygiene standpoint. Before or
shortly after merge, run `BENCH_WITH_DELETES=1 make bench` live (outside a command-proxy-hooked shell,
or with patience through the multi-minute silent-but-progressing build phase) to get the one piece of
direct evidence this pass couldn't produce: the actual `OK delete-count LINEITEM: <n> (~95% of baseline
<m>)` sanity-check line from a real run. Task D.2 (the `test1` remote run) remains, as already scoped in
the plan, an operational follow-up rather than a completion gate.
