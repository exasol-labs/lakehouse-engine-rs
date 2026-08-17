# Feature: Unity Catalog E2E Harness — Delta Query Result Coverage

End-to-end coverage of the actual rows a query returns over the seeded Delta fixtures — delete-free,
deletion-vector, column-mapped, partitioned, join/aggregate, and unplannable-type tables — run through
the same `unity-e2e` stack and virtual schema as `e2e-harness/unity-catalog-e2e-harness`. Split out of
that feature once its scenario count crossed this library's per-spec organization threshold.

## Background

<!-- DELTA:NEW -->
* **This delta is issue #321.** It adds the row-level half of Delta plan-time file pruning: proof that
  a query whose files were pruned returns the SAME rows it returned before pruning existed. The
  plan-side half — which files survive, asserted on the resolved file list and on the generated
  pushdown SQL — belongs to `vs-adapter/delta-file-pruning`, because it asserts a planning outcome
  rather than a returned row. This feature keeps its recorded charter: every scenario that asserts the
  ROWS a query returns over a seeded Delta table.
* **No new fixture, Makefile target, or test tier is added, for this delta either.** The two fixtures
  involved are already seeded and already under assertion: `basic_partitioned` (6 files, 6 rows,
  partitioned by `letter`, one file under the Hive default-partition directory) and `multi_part_stats`
  (5 files, 5 rows, delete-free, unpartitioned, disjoint per-file statistics). These scenarios extend
  the existing `make test-e2e-unity` suite in the same `e2e_unity_test.rs` binary.
* **The shipped partitioned scenario is the anchor this delta builds on, and it stays unedited.** Its
  clause requiring `SELECT * FROM ... WHERE letter = 'a'` to return exactly the rows whose logged
  partition value is `a` was written when the filter narrowed rows only. Pruning does not change the
  rows it demands, so it holds verbatim and becomes the regression that catches an unsound prune.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A query whose files were pruned returns the same rows as before pruning

* *GIVEN* the seeded fixtures `unity.delta_e2e.basic_partitioned` and `unity.delta_e2e.multi_part_stats`
* *WHEN* the suite issues a partition-column predicate against the first, a statistics-excluded range
  predicate and an equality against the second, and a predicate matching no file at all
* *THEN* every query SHALL return exactly the rows the same query returns against the same data with no
  pruning predicate applied, so pruning is invisible in every result
* *AND* a predicate matching NO file SHALL return zero rows as a normal empty result, and MUST NOT
  fail, hang, or return a row
* *AND* a query mixing a prunable predicate with an unprunable one — for example an equality alongside
  a `LIKE` — SHALL return the rows BOTH predicates select, proving the unprunable half is still
  evaluated above the scan rather than dropped with the pruning it could not drive
* *AND* the suite SHALL capture the generated pushdown SQL for at least one pruning query and assert it
  drives the scan UDF, so a silent fallback to an unaccelerated wrapper fails the suite rather than
  passing on correct rows
* *AND* the suite MUST fail (not skip) when the Unity Catalog server, MinIO, or Exasol is unreachable
<!-- /DELTA:NEW -->
