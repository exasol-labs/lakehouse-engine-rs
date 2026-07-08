# Decision Log: add-delete-benchmark-flag

Date: 2026-07-08

## Interview

Planned in **headless** mode — no live interview. The User Intent was treated as the full request;
conventional defaults were assumed and recorded below. The escalation bar (irreducible decisions
only) was applied; nothing met it (see "Escalation assessment").

**Q (implied):** Which delete semantics — copy-on-write, equality, or merge-on-read position deletes?
**A:** Explicitly stated: Iceberg v2 **merge-on-read position deletes**, NOT copy-on-write / equality
/ deletion vectors.

**Q (implied):** OFF vs ON semantics of the flag?
**A:** Stated: keep OFF byte-for-byte identical to today; ON runs the same query set against
delete-bearing tables.

**Q (implied):** How much deletion, and on which tables?
**A:** 5% per table; "applies to every TPC-H table... or clarify if some are too small — use
judgment, document." Judgment applied (see Decision [3]).

**Q (implied):** Must remote (`test1`) be scripted end-to-end?
**A:** Stated: yes — scripted start + stop of `test1` around the run, not just docker mode.

## Design Decisions

### [1] No spec feature delta — benchmark/deploy tooling only
- **Decision:** Ship zero spec deltas. This plan adds benchmark scripts, a Spark delete-authoring
  step, a deploy wrapper, and docs — no engine behavior.
- **Alternatives:** A `packaging` spec delta for the bench delete path.
- **Rationale:** The bench harness is explicitly not a spec feature (repo convention; the direct
  precedent `add-arithmetic-aggregate-pushdown-and-benchmark-suite` treated new bench queries + the
  sweep as non-spec). The engine's merge-on-read read path is already spec'd and CI-proven
  (`datafusion-scan/scan-execution-positional-deletes`, `packaging/e2e-harness-positional-deletes`,
  `packaging/positional-delete-fixtures`); this plan only measures it at scale. Inventing a delta
  would contradict the repo's own convention.
- **Promotes to ADR:** no

### [2] Delete authoring via Apache Spark `DELETE FROM` on a v2 merge-on-read table
- **Decision:** Author position deletes with Spark's Iceberg runtime — a plain `DELETE FROM` against
  a `write.delete.mode=merge-on-read` (format-version=2) table. Docker reuses the `apache/spark:3.5.7`
  compose fixture image; remote reuses EMR Serverless.
- **Alternatives:** PyIceberg `table.delete()` in `gen_load.py` (rejected: copy-on-write only, does
  not author MOR position-delete files); iceberg-rust writer (rejected: 0.10 has no position-delete
  writer, `apache/iceberg-rust#340`).
- **Rationale:** Spark is the only in-repo mechanism that authors MOR position deletes and is the
  established precedent (`scripts/spark-fixtures/`); it is already available in both bench modes, so
  no new dependency or infrastructure is stood up.
- **Promotes to ADR:** yes

### [3] 5% deterministic modulo delete (`key % 20 = 0`), applied to all 8 tables
- **Decision:** `DELETE FROM t WHERE <surrogate_key> % 20 = 0` (≈5% for uniform keys), keyed per
  table (R_REGIONKEY … L_ORDERKEY). Applied to all 8 tables; small dims (REGION, NATION) are
  imprecise (0–1 rows) and that is accepted.
- **Alternatives:** Random/`TABLESAMPLE` sampling (rejected: non-reproducible — two runs would be
  incomparable); skip REGION/NATION (unnecessary — the modulo is uniform and free, and small dims
  don't affect cost either way); per-table exact 5% (rejected: needless complexity for dims that
  contribute negligibly to scan/merge cost).
- **Rationale:** Determinism gives a stable, reproducible benchmark (same deleted set every run);
  keying LINEITEM on `L_ORDERKEY` spreads position deletes across all data files, exercising the
  read-path merge broadly; the cost-dominant tables get an accurate ≈5%.
- **Promotes to ADR:** no

### [4] Separate delete-bearing namespace (`tpch_deletes`), flag flips the VS namespace
- **Decision:** Author a parallel namespace of MOR copies; `BENCH_WITH_DELETES=1` sets the bench's
  `NAMESPACE` to `BENCH_DELETE_NAMESPACE` (default `${ICEBERG_NAMESPACE}_deletes` docker /
  `tpch_deletes` remote). Baseline `tpch` is never mutated.
- **Alternatives:** In-place deletes on `tpch` + a "regenerate to reset" path.
- **Rationale:** Keeps OFF provably byte-for-byte identical and reversible; ON is a pure namespace
  swap over the already-parameterized `ICEBERG_NAMESPACE`. Extra storage (a second TPC-H set) is
  acceptable and documented.
- **Promotes to ADR:** no

### [5] Flag naming + default-off + selftest coverage
- **Decision:** `BENCH_WITH_DELETES` (default `0`) and `BENCH_DELETE_NAMESPACE`, matching the
  `BENCH_*` env convention; new string logic covered by the offline `run.sh selftest`.
- **Alternatives:** A positional `run.sh` arg; a Makefile variable.
- **Rationale:** Consistency with every other bench knob (`BENCH_PARALLELISM_FACTOR`, `BENCH_DF_*`) —
  default-off, caller-overridable, self-checked offline; the OFF default guarantees today's behavior.
- **Promotes to ADR:** no

### [6] Docker auto-authors; remote pre-authors as a one-time job
- **Decision:** Docker mode authors `tpch_deletes` inside `run.sh` (idempotent, after `tpch_loader`);
  remote mode requires a one-time `deploy/scripts/make_deletes_remote.py` EMR job and `run.sh` only
  asserts the namespace exists.
- **Alternatives:** Author the remote delete namespace on every bench run.
- **Rationale:** Mirrors the existing asymmetry (docker loads its own data; remote consumes
  pre-loaded Glue data); an sf=30 EMR delete job is a one-time data-prep cost, not a per-run cost.
- **Promotes to ADR:** no

### [7] `bench-remote.sh <env>` wrapper with a teardown trap (cost safety)
- **Decision:** A single script chains `cluster-up → secrets → make bench → cluster-down`, with
  `trap 'cluster-down.sh <env>' EXIT` installed before bring-up so teardown runs on success, failure,
  or interrupt; forwards `BENCH_*` env.
- **Alternatives:** A Makefile `bench-test1` target.
- **Rationale:** A live cluster bills continuously; a script can guarantee cost-safe teardown via a
  trap (a Make recipe cannot cleanly guarantee teardown on mid-recipe failure) and takes an `<env>`
  arg. Placed beside the up/down/secrets scripts it wraps.
- **Promotes to ADR:** yes

## Escalation assessment (headless mode)

No decision met the irreducible bar (irreversible, changes user-facing behavior, two genuinely
incompatible architectures, or security/compliance). In particular, the one flagged risk — a Spark
runtime dependency that isn't in the repo — does **not** materialize: Spark is already wired in both
modes (docker `spark-iceberg-fixtures` compose service; remote EMR Serverless via `spark_compare.sh`
+ `enable_emr_serverless`). No new infrastructure is required, so no `OPEN QUESTIONS` escalation.

## Review Findings

<!-- Populated by speq-implement after code review. -->
