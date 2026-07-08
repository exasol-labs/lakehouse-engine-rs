# Plan: add-delete-benchmark-flag

## Summary

Add an opt-in `BENCH_WITH_DELETES` flag to the manually-invoked TPC-H benchmark suite that runs the
existing query set against Iceberg v2 **merge-on-read position-delete** copies of the tables (5% of
rows deleted per table, authored by Apache Spark), and wrap the manual `test1` remote-bench sequence
(`cluster-up → secrets → make bench → cluster-down`) into a single failure-safe scripted command.

## Design

### Context

The benchmark today (`bench/run.sh`, docker + remote modes) only ever reads clean, delete-free
tables: docker mode loads TPC-H via the Rust `tpch_loader` test, remote mode reads pre-loaded Glue
tables from `gen_load.py` — neither writes deletes (grep of `bench/` + `deploy/` for
"position delete"/"merge-on-read": zero hits outside engine read-path code). We want a perf signal
for the engine's merge-on-read **read cost**: reconstructing the live row set by applying Parquet
position-delete files during a scan. The engine's positional-delete read path is already fully
specified and CI-proven (`datafusion-scan/scan-execution-positional-deletes`,
`packaging/e2e-harness-positional-deletes`, `packaging/positional-delete-fixtures`); what is missing
is a *scale* measurement of it inside the competitive bench suite, plus a scripted, cost-safe way to
run the whole suite against the real `test1` cluster.

Two mechanism facts constrain the design:

1. **PyIceberg (used by `gen_load.py`) is copy-on-write only** — its `table.delete()` rewrites data
   files, it does not author merge-on-read position-delete files. The required v2 merge-on-read
   position deletes can only be authored here by **Apache Spark's Iceberg runtime**: a plain
   `DELETE FROM` against a `write.delete.mode=merge-on-read` (format-version=2) table commits Parquet
   position deletes. This is the *exact* precedent already in the repo for E2E fixtures
   (`scripts/spark-fixtures/`, `apache/iceberg-rust#340` tracking comment). Format-version=3 Puffin
   deletion vectors and equality deletes are deliberately NOT used — the engine rejects the former at
   plan time and the latter is a Flink upsert artifact, neither is this feature's target.
2. **A Spark runtime already exists in both bench modes** — docker mode has the one-shot
   `spark-iceberg-fixtures` compose service (`apache/spark:3.5.7` + Iceberg Spark runtime, joined to
   the same local REST catalog + MinIO); remote mode has EMR Serverless (`deploy/data-stack`
   `enable_emr_serverless`, driven by `bench/spark_compare.sh` + `deploy/scripts/spark_queries.py`).
   So NO new infrastructure is stood up — both delete-authoring paths reuse Spark tooling that is
   already wired.

- **Goals** — an opt-in flag whose OFF path is byte-for-byte today's behavior and whose ON path runs
  the identical query set against 5%-position-deleted MOR copies; deterministic/reproducible deletes;
  both docker and remote modes; a single failure-safe `bench-remote.sh <env>` that always tears the
  cluster down; docs.
- **Non-Goals** — no engine code change (the read path is already spec'd and CI-proven); no new spec
  feature (bench harness is not a spec feature — repo convention); no copy-on-write / equality /
  deletion-vector deletes; no mutation of the baseline `tpch` tables (OFF path must stay pristine);
  no new AWS infrastructure; the operational `test1` run + `docs/performance.md` edit is a follow-up
  operational step, not a plan-completion gate.

### Decision

Author a **separate** delete-bearing namespace (`tpch_deletes`, override `BENCH_DELETE_NAMESPACE`)
of merge-on-read copies via Spark, and flip which namespace the VS is built against with
`BENCH_WITH_DELETES`. Deletes are a deterministic 5% modulo predicate. The remote-bench sequence
becomes one script with a teardown trap.

#### Architecture

```
                       ┌─────────────────────────────────────────────┐
                       │ delete-authoring (Spark, ONE-TIME per catalog)│
 baseline tpch ns ────▶│ CTAS → tpch_deletes ns  (format-version=2,    │──▶ tpch_deletes ns
 (untouched, OFF path) │   write.delete.mode=merge-on-read)            │    (MOR, 5% pos-deleted)
                       │ DELETE FROM t WHERE <surrogate_key> % 20 = 0  │
                       └─────────────────────────────────────────────┘
   docker: docker run apache/spark  (reuse run_fixtures.sh SPARK_CONF) ▲
   remote: EMR Serverless job       (reuse spark_compare.sh plumbing)  ┘

 bench/run.sh:  NAMESPACE = BENCH_WITH_DELETES ? <delete ns> : <baseline ns>
   docker  →  authors <delete ns> idempotently after tpch_loader, then runs suite
   remote  →  asserts <delete ns> exists (else clear error), then runs suite
   + delete-aware sanity: COUNT(LINEITEM) ≈ 0.95 × baseline when flag ON

 deploy/scripts/bench-remote.sh <env>:
   trap 'cluster-down.sh <env>' EXIT  →  cluster-up → secrets → make bench → (down via trap)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Separate delete-bearing namespace, baseline untouched | delete-authoring + `run.sh` namespace switch | OFF path stays provably byte-for-byte identical to today; ON path is a pure namespace swap; fully reversible (drop the namespace) |
| Spark `DELETE FROM` on v2 MOR table = position deletes | `create_tpch_deletes.sql` | The only in-repo mechanism that authors merge-on-read position deletes (PyIceberg is CoW-only); reuses the established `scripts/spark-fixtures/` precedent |
| Deterministic modulo delete (`key % 20 = 0`) | `create_tpch_deletes.sql` | ≈5% for uniform surrogate keys, no random-seed state → the same deleted set every run → a stable, reproducible benchmark; keying LINEITEM on `L_ORDERKEY` spreads position deletes across all data files |
| Docker authors, remote pre-authors | `run.sh` (docker) vs one-time job (remote) | Mirrors the existing asymmetry: docker mode loads its own data, remote mode consumes pre-loaded Glue data — an sf=30 EMR job is a one-time data-prep step, not a per-run cost |
| Teardown trap in the wrapper | `bench-remote.sh` | A live `r8i.2xlarge`×N cluster bills continuously; `trap … EXIT` guarantees `cluster-down.sh` runs even if the bench aborts mid-run (cost safety, same discipline as `deploy/README.md`'s Trino teardown warnings) |
| Env-only knobs, offline selftest coverage | `.env.example` + `run.sh selftest` | Matches every other bench knob (`BENCH_PARALLELISM_FACTOR`, `BENCH_DF_*`): default-off, caller-overridable, string logic self-checked with no DB |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| No spec feature delta | A `packaging` delta for the bench delete path | The bench harness is explicitly NOT a spec feature (repo convention; the direct precedent `add-arithmetic-aggregate-pushdown-and-benchmark-suite` treated all bench work as non-spec). This plan adds zero engine behavior — it exercises the already-spec'd `scan-execution-positional-deletes` read path at scale. Inventing a delta would violate the repo's own convention. |
| Separate `tpch_deletes` namespace | Apply deletes in-place + a "regenerate to reset" step | In-place mutation makes the OFF path non-pristine and un-reproducible without a reload; a parallel namespace keeps OFF byte-for-byte identical and ON a pure swap. Storage cost (~a second TPC-H set) is acceptable and documented. |
| Deterministic `key % 20 = 0` (not random sampling) | `TABLESAMPLE` / random-seeded delete | Reproducibility: identical deleted set on every re-run → benchmark numbers are comparable across runs and machines. Random sampling would make two runs incomparable. |
| 5% applied to all 8 tables; small dims imprecise | Skip REGION/NATION; per-table exact 5% | `key % 20` is uniform and trivial; REGION (5 rows) / NATION (25 rows) get 0–1 imprecise deletes but contribute negligibly to scan/merge cost, so exactness there is pointless. LINEITEM/ORDERS/PARTSUPP (the cost-dominant tables) get an accurate ≈5%. |
| Spark for authoring (docker compose image + remote EMR) | PyIceberg in `gen_load.py`; iceberg-rust writer | PyIceberg cannot author MOR position deletes (CoW-only); iceberg-rust 0.10 has no position-delete writer (`#340`). Spark is the established repo mechanism and is already available in both modes — no new dependency or infra. |
| Single `bench-remote.sh` wrapper with teardown trap | A Makefile `bench-test1` target | A script can install a robust `trap … EXIT` for guaranteed, cost-safe teardown and take an `<env>` arg; a Make target cannot cleanly guarantee teardown on mid-recipe failure. Placed in `deploy/scripts/` next to the up/down/secrets scripts it chains. |

## Features

**No spec feature deltas.** This plan is benchmark-harness + deploy tooling that exercises the
already-specified, CI-proven positional-delete read path — it adds no new or changed engine
behavior. Per the repo convention (the bench harness is explicitly not a spec feature; see the
direct precedent `specs/_recorded/2026-07-04-add-arithmetic-aggregate-pushdown-and-benchmark-suite`,
which treated its new bench queries and sweep as non-spec work), the change appears only as
implementation tasks and manual/verification rows below.

Correctness of the engine's merge-on-read read path is the existing backstop
(`crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs` +
`packaging/e2e-harness-positional-deletes`, in `make test-e2e`); this plan does not re-prove it.

## Dependencies

- **Docker mode**: the existing `spark-iceberg-fixtures` compose service image (`apache/spark:3.5.7`
  + Iceberg Spark runtime 1.10.1) and the local REST catalog + MinIO — all already in
  `docker-compose.yml`.
- **Remote mode**: `deploy/data-stack` applied with `-var enable_emr_serverless=true` (the same
  toggle `bench/spark_compare.sh` already requires), Glue `tpch` tables pre-loaded by `gen_load.py`,
  and a Glue database for the delete namespace.
- **`bench-remote.sh`**: the existing `cluster-up.sh` / `secrets.sh` / `cluster-down.sh` and their
  `AWS_PROFILE` + OpenTofu `test1` workspace (already present at
  `deploy/cluster-stack/terraform.tfstate.d/test1/`).

## Implementation Tasks

### Group A — Delete-authoring mechanism (Spark, one-time data prep)

A.1 Add a parameterized Spark SQL script `scripts/spark-fixtures/create_tpch_deletes.sql` (source
namespace + target namespace substituted by the caller) that, for each of the 8 TPC-H tables:
`CREATE TABLE <target>.<t> USING iceberg TBLPROPERTIES ('format-version'='2',
'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read',
'write.merge.mode'='merge-on-read') AS SELECT * FROM <source>.<t>`, then
`DELETE FROM <target>.<t> WHERE <surrogate_key> % 20 = 0` (per-table surrogate: R_REGIONKEY,
N_NATIONKEY, S_SUPPKEY, C_CUSTKEY, P_PARTKEY, PS_PARTKEY, O_ORDERKEY, L_ORDERKEY). Header comment must
state the MOR/position-delete rationale + `#340` drop condition + the deterministic-5% contract, in
lockstep with the existing fixture files. [expert]

A.2 Docker authoring path: a helper (`bench/make_deletes_docker.sh`, or a `run.sh` function) that
runs the `apache/spark:3.5.7` image with the SAME `SPARK_CONF` as
`scripts/spark-fixtures/run_fixtures.sh` (local REST catalog + MinIO), substitutes source/target
namespaces into `create_tpch_deletes.sql`, and executes it. Idempotent: skip if the target namespace
already has the 8 tables populated (mirror `tpch_loader`'s "skip if already present").

A.3 Remote authoring path: `deploy/scripts/make_deletes_remote.py` (Spark SQL entrypoint, sibling of
`spark_queries.py`) run as an EMR Serverless job via a small runner (mirror `bench/spark_compare.sh`'s
`start-job-run` + poll + log-scrape plumbing) that authors the `tpch_deletes` Glue database from the
`tpch` Glue tables. One-time data-prep, analogous to `gen_load.py`; documented as a prerequisite for
remote delete-bench, never run per bench invocation. [expert]

### Group B — Benchmark flag wiring (`bench/run.sh` + `.env`)

B.1 Add `BENCH_WITH_DELETES` (default `0`) and `BENCH_DELETE_NAMESPACE` (default
`${ICEBERG_NAMESPACE}_deletes` docker / `tpch_deletes` remote) to `bench/.env.example` with the
`BENCH_*` doc-comment convention; note the OFF path is unchanged and the ON path's one-time
authoring prerequisite (docker: automatic; remote: run `make_deletes_remote.py` first).

B.2 Wire the flag into `bench/run.sh`: when ON, set `NAMESPACE` to the delete namespace and annotate
the report header (`deletes=on ns=<...>`); docker mode calls A.2 (idempotent) after `tpch_loader`;
remote mode asserts the delete namespace resolves at least one table via the VS and errors clearly if
not (pointing at A.3). Add a flag-gated correctness sanity check: `COUNT(*)` of the delete-bearing
LINEITEM is between 90% and 98% of the baseline count (proves position deletes are being applied on
read, not ignored and not over-deleting). Keep the entire OFF path byte-for-byte unchanged.

B.3 Extend `bench/run.sh selftest` (offline, no DB) to cover the new string logic: the
`BENCH_WITH_DELETES` namespace switch resolves to the baseline ns when OFF and the delete ns when ON;
the report-header annotation is present only when ON; the 90–98% ratio bounds check accepts a 0.95
ratio and rejects 0.80 / 1.00.

### Group C — `test1` remote wrapper + docs

C.1 Add `deploy/scripts/bench-remote.sh <env>` that installs `trap 'cluster-down.sh <env>' EXIT`
BEFORE bringing anything up, then runs `cluster-up.sh <env>` → `secrets.sh <env>` → `make bench`
(forwarding `BENCH_WITH_DELETES` and any `BENCH_*` env through to `run.sh`), so the cluster is torn
down on success, failure, or interrupt (cost safety). Print a clear final line stating whether
teardown ran and remind the operator to verify termination (mirror `deploy/README.md`'s existing
teardown-verification warnings). [expert]

C.2 Docs: `deploy/README.md` — document `bench-remote.sh <env>`, the one-time
`make_deletes_remote.py` delete-prep step, and the `tpch_deletes` Glue database; `bench/README.md` —
document `BENCH_WITH_DELETES` / `BENCH_DELETE_NAMESPACE`, the docker auto-authoring, and the
merge-on-read-position-delete semantics (link the `scripts/spark-fixtures/` precedent). Keep the
"OFF = identical to today" guarantee explicit in both.

### Group D — Validation

D.1 Docker-mode validation: `BENCH_WITH_DELETES=1 make bench` on the local stack — authors
`tpch_deletes`, runs the full Q1–Q9b/NQ1–NQ5 set, and the delete-count sanity check passes
(LINEITEM ≈ 95% of baseline). Confirm OFF-path `make bench` output is unchanged from a pre-change run.

D.2 (Operational follow-up, not a completion gate) `deploy/scripts/bench-remote.sh test1` with
`BENCH_WITH_DELETES=1` after a one-time `make_deletes_remote.py`; record delete-vs-baseline timings in
`docs/performance.md`, and open/close a `specs/backlog.md` item if a merge-on-read read-cost
regression surfaces.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | A.1 first; then A.2 and A.3 concurrent |
| Group B | B.1 any time; B.2 after A.2 (docker path needs the authoring helper); B.3 after B.2 |
| Group C | C.1 after B.2 (wrapper forwards the flag `run.sh` consumes); C.2 any time after B.1/C.1 land |
| Group D | D.1 after B.2 + A.2; D.2 after C.1 + A.3 |

Sequential dependencies:
- A.1 → A.2, A.3
- A.2 → B.2 → B.3; B.2 → C.1
- B.2 + A.2 → D.1; C.1 + A.3 → D.2

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none expected) | — | Purely additive: new scripts + new env knobs + a new namespace switch. The OFF path is unchanged, so nothing is replaced or removed. |

## Verification

The benchmark harness is not CI (not part of `make test-e2e`); its automated proof is the offline
`selftest` plus the docker-mode run, exactly as the direct precedent verified its bench work. The
engine read-path correctness backstop is the existing E2E test, unchanged.

### Scenario Coverage

<!-- No spec scenarios (no spec feature). These map the harness's own internal assertions. -->

| Assertion | Test Type | Test Location | Test Name / Check |
|-----------|-----------|---------------|-------------------|
| Namespace switch: OFF → baseline ns, ON → delete ns | Integration (offline string logic) | `bench/run.sh` (`selftest`) | delete-namespace-switch case |
| Report header annotated only when deletes ON | Integration (offline string logic) | `bench/run.sh` (`selftest`) | header-annotation case |
| Delete-count ratio bounds accept 0.95, reject 0.80/1.00 | Integration (offline string logic) | `bench/run.sh` (`selftest`) | ratio-bounds case |
| Delete-bearing LINEITEM ≈ 95% of baseline (position deletes applied on read) | Integration (live, docker) | `bench/run.sh` (`BENCH_WITH_DELETES=1`, docker) | delete-count sanity check |
| Engine reads MOR position deletes correctly (read-path backstop, unchanged) | Integration (E2E, CI) | `crates/lakehouse-engine/tests/*` (`e2e_positional_deletes`) | existing `packaging/e2e-harness-positional-deletes` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| OFF path unchanged | `make bench` (docker, no flag) | Identical query set + pushdown checks as today; report header shows no `deletes=` annotation |
| Docker delete-bench | `BENCH_WITH_DELETES=1 make bench` | `tpch_deletes` authored; full suite runs; `OK  delete-count LINEITEM: <n> (~95% of baseline)` |
| Selftest (offline) | `./bench/run.sh selftest` | `selftest OK` (new namespace-switch / header / ratio cases pass) |
| Remote delete-prep (one-time) | `python deploy/scripts/make_deletes_remote.py` via its EMR runner | `tpch_deletes` Glue database populated with 8 MOR tables |
| test1 wrapper, cost-safe | `AWS_PROFILE=spot-strata-deployer BENCH_WITH_DELETES=1 deploy/scripts/bench-remote.sh test1` | cluster up → bench → cluster down (down runs even on failure); final line confirms teardown |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 (unchanged — no Rust edits) |
| Test (host) | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
| Bench selftest | `./bench/run.sh selftest` | `selftest OK` |
