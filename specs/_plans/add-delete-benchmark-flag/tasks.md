# Tasks: add-delete-benchmark-flag

## Group A — Delete-authoring mechanism (Spark, one-time data prep)
- [x] A.1 Add parameterized `scripts/spark-fixtures/create_tpch_deletes.sql` (source/target ns): CTAS each of the 8 TPC-H tables into the target ns with `format-version=2` + `write.{delete,update,merge}.mode=merge-on-read`, then `DELETE FROM <t> WHERE <surrogate_key> % 20 = 0` (deterministic 5%). Header: MOR/position-delete rationale + `#340` drop condition + 5% contract. [expert]
- [x] A.2 Docker authoring helper (`bench/make_deletes_docker.sh` or `run.sh` fn): run `apache/spark:3.5.7` with `run_fixtures.sh`'s SPARK_CONF against `create_tpch_deletes.sql`, source=baseline ns → target=delete ns; idempotent (skip if target ns already populated).
- [x] A.3 Remote authoring `deploy/scripts/make_deletes_remote.py` (Spark SQL, EMR Serverless, mirror `spark_compare.sh` plumbing): author `tpch_deletes` Glue db from `tpch` Glue tables; one-time data prep, documented as a remote prerequisite. [expert]

## Group B — Benchmark flag wiring (bench/run.sh + .env)
- [x] B.1 Add `BENCH_WITH_DELETES` (default 0) + `BENCH_DELETE_NAMESPACE` (default `${ICEBERG_NAMESPACE}_deletes` docker / `tpch_deletes` remote) to `bench/.env.example` with BENCH_* doc-comments + one-time-authoring note.
- [x] B.2 Wire flag into `run.sh`: ON → `NAMESPACE`=delete ns + report-header annotation; docker calls A.2 idempotently after `tpch_loader`; remote asserts delete ns resolves (clear error → A.3); flag-gated sanity check: delete LINEITEM count ∈ [90%,98%] of baseline. OFF path byte-for-byte unchanged.
- [x] B.3 Extend `run.sh selftest` (offline): namespace-switch (OFF→baseline / ON→delete), header annotation only when ON, ratio bounds accept 0.95 reject 0.80/1.00.

## Group C — test1 remote wrapper + docs
- [x] C.1 Add `deploy/scripts/bench-remote.sh <env>`: `trap 'cluster-down.sh <env>' EXIT` set FIRST, then cluster-up → secrets → make bench (forward BENCH_* env); teardown guaranteed on success/failure/interrupt; final line confirms teardown + reminds to verify termination. [expert]
- [x] C.2 Docs: `deploy/README.md` (wrapper, one-time `make_deletes_remote.py`, `tpch_deletes` Glue db) + `bench/README.md` (`BENCH_WITH_DELETES`/`BENCH_DELETE_NAMESPACE`, docker auto-authoring, MOR-position-delete semantics, OFF=identical guarantee).

## Group D — Validation
- [x] D.1 Docker: `BENCH_WITH_DELETES=1 make bench` authors `tpch_deletes`, runs full suite, delete-count sanity passes; confirm OFF-path output unchanged. COMPLETED live: `OK delete-count LINEITEM: 1709938 (~95.0% of baseline 1800093)`, full Q1-Q9b/NQ1-NQ5 suite + all pushdown checks passed; OFF-path re-run confirmed clean against the pristine baseline. Root cause of two earlier stalls (stale `bench/.env` pointing at a dead remote `test1` host) and a second bug found along the way (stale `SLC_VERSION` default in `run.sh`, ABI mismatch) are both fixed. See verification-report.md.
- [ ] D.2 (Operational follow-up) `bench-remote.sh test1` with deletes after one-time `make_deletes_remote.py`; record timings in `docs/performance.md`; backlog item if a regression surfaces.

## Phase E: Code Review
- [x] E.1 Review changed files (shell/python/sql + docs) with code-reviewer agent. Found + fixed 2 doc/error-message defects (phantom `<env>` arg for `make-deletes-remote.sh`); all other checks clean (OFF-path invariant, SQL/Python lockstep, idempotency contracts, Terraform wiring, trap logic).

## Phase F: Verification
- [x] F.1 Build: UDF `.so` release build completed successfully (observed during the killed D.1 run, "Finished release profile [optimized] target(s)"); no Rust source touched by this plan so it is unaffected.
- [x] F.2 Test: `cargo test` — 539 passed, 2 ignored, 0 failures.
- [x] F.3 Lint: `cargo clippy --all-targets` — no issues found.
- [x] F.4 Format: `cargo fmt --check` — clean.
- [x] F.5 Bench selftest: `./bench/run.sh selftest` — `selftest OK`.
- [x] F.6 Verification report.
