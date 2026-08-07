# Injection surface measurements — fix-e2e-harness-undeclared-limit

## Rows-per-fetch-response measurement (task 1.6)

**Measured against the live Docker stack** (`docker compose up -d --wait exasol minio
iceberg-rest`, `.so` built via `make cross-musl-udf-build`), never assumed or computed.

Method: an UNCAPPED (`resultSetMaxRows: 0`) WebSocket session issued a bare raw scan,
`SELECT token FROM MY_LAKEHOUSE.HIGH_CARD_PROBE`, against the `high_card_probe` fixture
(`HIGH_CARD_ROWS = 30_000` rows of ~100-byte `token` values,
`crates/lakehouse-engine/tests/common/seed.rs:2267`, `:2378`). The result came back as a
`resultSetHandle` (not inlined), so a single `fetch` command was issued at the harness's
present `numBytes: 67108864` (64 MiB) budget (`crates/lakehouse-engine/tests/common/exasol_ws.rs:212`),
and the returned row count was read directly off the response — not inferred from byte-size
arithmetic.

Result:

| Metric | Value |
|---|---|
| Total rows advertised by `execute()` (`resultSet.numRows`) | 30,000 |
| Rows returned in fetch response #1 | 30,000 |
| Total fetch responses needed to read the full result set | 1 |

**Client-side truncation is NOT reachable at this budget with the fixtures that exist
today.** The entire 30,000-row `high_card_probe` raw scan (~3 MB of `token` data) fits in
one 64 MiB fetch response, so `ExaConn::fetch_result_columns`'s current single-fetch-and-return
behavior (`crates/lakehouse-engine/tests/common/exasol_ws.rs:190-228`) already reads the
whole result set for this fixture — it never needs the second `fetch` call the code has no
loop to make.

Implication for task ordering: Phase 2 (`harness_reads_high_cardinality_result_set_to_completion`,
task 2.1) cannot rely on `high_card_probe` at its current 30,000-row scale to observe
truncation — the failing test must either seed a larger/wider fixture that exceeds 64 MiB in
one fetch response, or otherwise force a second `fetch` round-trip, before task 2.2's fix to
the read loop has any observable effect. The Phase-2-before-Phase-3 ordering in this plan is
justified by the *general* correctness gap in `fetch_result_columns` (no loop over
`startPosition` at all, regardless of whether any current fixture happens to trigger it) —
not by an assumption that `high_card_probe` itself demonstrates truncation, which this
measurement disproves.

Probe used: a throwaway test (`tmp_row_fetch_probe_test.rs`, deleted after this measurement
was recorded) that opened its own uncapped WebSocket session and looped raw `fetch` calls
(same wire protocol as `ExaConn::fetch_result_columns`) until all advertised rows were read,
logging the row count of each response. Production `ExaConn`/`fetch_result_columns` code was
not modified to take this measurement.

## Capped-versus-uncapped pushdown shape matrix (task 1.2)

**Headline: a declared `resultSetMaxRows` cap changed NOTHING in the adapter exchange for any
of the seven statement shapes.** The `pushdownRequest`, the full echoed adapter exchange
(`getCapabilities` + `pushdown`), and the generated scan SQL — including the literal scan-spec
JSON handed to `LAKEHOUSE_SCAN` — were byte-identical between the capped and uncapped capture
of every shape. No shape gained a `limit`. The cap truncated the **delivered result set** at
the statement level and never reached the adapter.

This contradicts the premise stated in `plan.md` § Context ("Exasol converts that cap into a
`pushdownRequest` `limit`") and the mechanism asserted in the comment at
`crates/lakehouse-engine/tests/e2e_join_test.rs:113-117`. See § Consequences for downstream
tasks below.

### Provenance

| Item | Value |
|---|---|
| Stack | local Docker compose: `exasol`, `minio`, `iceberg-rest`, all healthy |
| Exasol image | `docker.io/exasol/docker-db:2025.2.1`, WebSocket protocol v3 |
| Iceberg catalog | `apache/iceberg-rest-fixture:1.10.1` |
| `.so` | built with `make cross-musl-udf-build` before the first capture |
| Tool | `scripts/capture-pushdown-payload.sh '<SQL>'`, unchanged, once per variant |
| Capped variant | `CAPTURE_RESULT_SET_MAX_ROWS=37` (task 1.1's knob → `capped_result_sets(37)`) |
| Uncapped variant | `CAPTURE_RESULT_SET_MAX_ROWS` unset → `unbounded_result_sets()` → `resultSetMaxRows: 0` |
| Fixture | `typed_distinct_probe` (12 rows, 2 data files) in VS `MY_LAKEHOUSE`, plus `fact_orders` / `dim_customer` for the join shape (seeded by running `e2e_join_test::e2e_broadcast_join_pushdown_shape` first, since the capture binary seeds only `typed_distinct_probe`) |
| Runs | 7 shapes × 2 variants, plus one uncapped replicate and six controls — 20 captures total |

`37` was chosen because it is neither `0` nor `10000`, and is larger than every row count these
statements return (max 12), so a `limit` appearing in a capture could only have come from the
declared cap.

### Diff method and its noise floor

Each capture's `EXPLAIN VIRTUAL` blob was split into (a) the adapter-generated SQL and (b) the
echoed adapter-exchange JSON array, and the pair was diffed field by field: the
`pushdownRequest` object alone, the whole exchange array, the generated SQL, and the
real-execution response.

A third **uncapped replicate** of shape 1 establishes the noise floor. Its `pushdownRequest`,
exchange, and generated SQL (including Parquet file names and shard assignment) were identical
to the first uncapped run — seeding short-circuits on an already-populated table
(`create_and_append_files`, `crates/lakehouse-engine/tests/common/seed.rs:394`), so file paths
are stable across runs. The only thing that varied between *any* two runs, capped or not, was
**result row order**, which also varied between the two uncapped runs: shard-interleaving
nondeterminism, not an effect of the cap.

### Per-shape result

`limit` location legend: **common scan spec** = the `"limit":n` key in the single scan-spec JSON
literal passed to `LAKEHOUSE_SCAN`; **per-shard spec** = a limit in an individual shard's file
list; **outer SQL** = a rendered `LIMIT` in the adapter-generated wrapper SQL.

| # | Shape | Exact statement (`{table}` = `MY_LAKEHOUSE.TYPED_DISTINCT_PROBE`) | Rows | Declared cap produced a `limit`? | Where it landed | Other differing field |
|---|---|---|---|---|---|---|
| 1 | bare projection | `SELECT C_VARCHAR FROM {table}` | 12 | **No** | — | none — captures IDENTICAL |
| 2 | projection + filter | `SELECT C_VARCHAR FROM {table} WHERE C_DECIMAL_A > 15` | 7 | **No** | — | none — captures IDENTICAL |
| 3 | single-group aggregate | `SELECT COUNT(*), SUM(C_DECIMAL_A) FROM {table}` | 1 | **No** | — | none — captures IDENTICAL |
| 4 | `GROUP BY` aggregate | `SELECT C_VARCHAR, COUNT(*) FROM {table} GROUP BY C_VARCHAR` | 9 | **No** | — | none — captures IDENTICAL |
| 5 | `COUNT(DISTINCT)` | `SELECT COUNT(DISTINCT C_VARCHAR) FROM {table}` | 1 | **No** | — | none — captures IDENTICAL |
| 6 | `ORDER BY … LIMIT` | `SELECT C_VARCHAR FROM {table} ORDER BY C_VARCHAR LIMIT 5` | 5 | **No** — the only `limit` present is the statement's own `LIMIT 5`, identical in both variants | `pushdownRequest.limit = {"numElements": 5}` and `"limit":5` in the common scan spec, plus `ORDER BY "C_VARCHAR" ASC NULLS LAST LIMIT 5` in the generated SQL | none — captures IDENTICAL; the declared `37` neither replaced nor tightened the SQL's `5` |
| 7 | broadcast-eligible inner equi-join | `SELECT c.C_NAME, o.O_ORDERDATE FROM MY_LAKEHOUSE.FACT_ORDERS o JOIN MY_LAKEHOUSE.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY WHERE o.O_ORDERDATE >= DATE '2024-01-05'` | 6 | **No** | — | none — captures IDENTICAL; both carry the broadcast `"join":{` common-blob block and neither carries the `LHS_T0` two-scan wrapper |

**Every one of the seven pairs was identical** in `pushdownRequest`, full adapter exchange, and
generated scan SQL. Result row *values* were also identical per shape (shape 3 → `12` /
`282.99`; shape 4 → the same 9 groups with the same counts; shape 5 → `8`), differing only in
row order as described above.

Shape 6 doubles as the **detector's positive control**: the diff method does find a `limit` when
one exists, and shows exactly where it lands — so the seven "No" verdicts are a real absence,
not a blind grep.

### Controls

Six further captures rule out the two ways the primary result could be an artifact — the cap
never reaching the server, and `37` being an unlucky value.

| Control | Statement | Cap | `limit` in `pushdownRequest` / scan spec | Real-execution result |
|---|---|---|---|---|
| c1 | bare projection | 5 | none | `numRows` **5** (uncapped: 12) |
| c2 | bare projection | 10000 (the harness's historical default) | none | `numRows` 12 |
| c3 | single-group aggregate | 5 | none | `COUNT(*)` = **12**, `SUM` = `282.99` — the correct uncapped values |
| c4 | broadcast join | 10000 | none | 6 rows; broadcast `"join":{` block present, no `LHS_T0` |
| c5 | broadcast join | 5 | none | `numRows` **5** (truncated from 6); broadcast `"join":{` block still present, no `LHS_T0` |
| c6 | `COUNT(DISTINCT)` | 5 | none | `8` — the correct uncapped value |

What each control establishes:

- **c1 is the cap-delivery positive control.** A declared cap of `5` against a 12-row table
  returned 5 rows. The `resultSetMaxRows` attribute is delivered and honored by this server; it
  truncates the result set the statement delivers. So the seven "No" verdicts are not a broken
  test harness.
- **c2 covers the value this plan is actually flipping.** The historical `10000` default injects
  no `limit` either — the flip to `0` therefore changes no adapter request for these shapes.
- **c3 and c6 probe the real-execution path, where the `EXPLAIN VIRTUAL` echo cannot see.** A
  `limit` landing in the common scan spec is applied per shard, so it would have corrupted both
  values: shape 3's `COUNT(*)` would have come back as at most 10 (5 per shard over two
  6-row files) rather than 12, and shape 5's per-shard `DISTINCT` row scans would have
  under-counted rather than returning 8. Both returned the correct uncapped value under a cap of
  5, so no `limit` reached the scan on the directly-executed path either.
- **c4 and c5 test the mechanism `e2e_join_test.rs:113-117` asserts.** At the historical `10000`
  default — and at `5` — the broadcast join block is still emitted and the `LHS_T0` two-scan
  fallback is not. The cap does not disqualify broadcast-join pushdown on this stack.

### Observation boundary

The capture tool reads the adapter exchange out of `EXPLAIN VIRTUAL`, and `resultSetMaxRows` is
an attribute of the statement being executed — the `EXPLAIN VIRTUAL` wrapper, not the inner
`SELECT`. A `limit` injected only into a *directly executed* statement's pushdown request would
therefore be invisible to the echo, and this measurement cannot exclude that on shape 7 alone,
because a broadcast join and the two-scan fallback return the same six rows.

Controls c3 and c6 are the evidence that bounds it for the row-scan and aggregate shapes: a
scan-spec `limit` is observable in the *result values* there, and it was absent. For shape 7,
c4/c5 show the `EXPLAIN VIRTUAL` plan unchanged under a cap, which is the same observation
`e2e_broadcast_join_pushdown_shape` makes — so whatever a directly-executed join request
carries, the shape assertion that the opt-out was added to protect is not affected by the cap.

### Consequences for downstream tasks

1. **Task 1.4's affected-assertion list is expected to be empty.** No shape gained a `limit`, so
   no E2E assertion about pushdown shape or scan-spec content changes when the default flips from
   `10000` to `0`. Task 1.4 should verify that against the binaries rather than inherit it.
2. **Phase 4's predicted blast radius collapses to the truncation axis.** The only behavior the
   flip changes on this stack is that a statement returning more than 10000 rows is no longer
   truncated to 10000 (c1/c5 show truncation is the cap's real effect). Any assertion that passed
   because of a *pushed limit* has no basis in this measurement; assertions that passed because of
   result-set truncation are the ones to watch, and only `high_card_probe` (30,000 rows) exceeds
   10000 today.
3. **Task 1.5's shape matrix in `docs/debugging-pushdown.md` must record the measured answer —
   "no shape converts a declared cap into a pushdown `limit` on Exasol 2025.2.1" — not the
   premise.** The `capped_result_sets` doc comment
   (`crates/lakehouse-engine/tests/common/exasol_ws.rs:148-155`) currently states the unmeasured
   claim ("Declares a row cap that reaches the adapter as a pushdown `limit`") and needs the same
   correction; so does `unbounded_result_sets`' "forcing every join onto the unaccelerated
   fallback via a pushdown `limit`" (`:139`).
4. **Task 5.3 (`declared_cap_reaches_adapter_as_pushdown_limit`) cannot pass as specified**, and
   neither can the spec-delta scenario "A declared row cap reaches the adapter as a pushdown
   limit" (`specs/_plans/fix-e2e-harness-undeclared-limit/e2e-harness/e2e-harness/spec.md`), which
   requires the capped scan spec to carry `limit` `n`. The measurement says the two scan specs are
   identical. The scenario and its test need re-authoring against what was measured — a declared
   cap truncates the delivered result set and leaves the adapter request untouched. Task 5.2
   (`undeclared_cap_pushes_no_limit`) is unaffected and is confirmed by shape 1.
5. **Task 3.3's comment deletion is now better supported, not weaker.** The premise of
   `e2e_join_test.rs:113-117` is not merely stale, it does not reproduce: c4 shows the broadcast
   block is emitted at the historical `10000` default. The two tests it guards
   (`e2e_broadcast_join_pushdown_shape`, `e2e_broadcast_join_result_correct`) pin the same plan
   with or without the opt-out.

## Task 1.4: affected-assertion list

**Confirmed empty**, verified against the 11 E2E binaries rather than inherited from § Consequences
above.

Method: `find_referencing_symbols` on `capped_result_sets` / `unbounded_result_sets` located every
call site that declares a row cap (6 sites, all `.unbounded_result_sets()`: `e2e_join_test.rs` 118,
139, 193, 1174, 1362 and `e2e_lakekeeper_test.rs:884`; `e2e_count_distinct_test.rs`'s task-2.1 test
also calls it, pending task 3.4's re-point). A keyword sweep for `resultSetMaxRows`, `"limit"`,
`LHS_T0`, and `10000`/`10_000` across all 11 binaries then found every assertion that mentions a
`limit` key or a broadcast-vs-fallback plan shape, to check each one's premise against § Per-shape
result above:

- `e2e_join_test.rs:113-129` (`e2e_broadcast_join_pushdown_shape`) — the one known example, already
  covered by Consequence 5: c4/c5 show its premise does not reproduce, and it is deleted by task 3.3
  rather than fixed in Phase 4.
- `e2e_capability_test.rs:3369-3394` (`e2e_order_by_aggregate_with_limit_zero_returns_no_rows`) and
  `e2e_capability_test.rs:1047` (`e2e_count_star_over_limited_subselect_pushdown`) both assert on a
  `limit` key or row count produced by a SQL-level `LIMIT` clause in the statement itself, on a plain
  `exa_conn()` — not on `resultSetMaxRows`. Unaffected by the flip: the shape-1/2/3/4/5/7 rows above
  show the cap never contributes a `limit` key regardless of its value, so these assertions hold
  identically at `10000` and at `0`.
- Every other `LHS_T0` / broadcast-shape assertion across `e2e_scan_test.rs`, `e2e_capability_test.rs`,
  `e2e_count_distinct_test.rs`, and the rest of `e2e_join_test.rs` runs on a plain `exa_conn()` and
  pins a plan shape driven by the statement's own predicates/joins/aggregation, not by a row cap; none
  references `resultSetMaxRows` or a cap-declaring call.
- No E2E test raw-scans a fixture larger than 10,000 rows on a plain (uncapped-after-flip)
  `exa_conn()`. `high_card_probe` (30,000 rows) is the only fixture that exceeds the old default, and
  the only tests touching it (`high_cardinality_count_distinct_completes`,
  `harness_reads_high_cardinality_result_set_to_completion`) reach it through `COUNT(DISTINCT)` or an
  explicit uncapped connection, never a capped raw scan whose result count would shift at the flip.

**Verdict: the predicted scope of Phase 4 is the truncation axis only** — a statement returning more
than 10,000 rows on a connection that used to inherit the invented cap. Today that is only
`high_card_probe`, and Phase 2's fetch-completeness fix and Phase 3's default flip together mean even
that fixture is read to completion rather than truncated. No pushdown-shape, scan-spec, or plan-
selection assertion in any of the 11 E2E binaries depends on the declared cap producing an
adapter-visible `limit`.
