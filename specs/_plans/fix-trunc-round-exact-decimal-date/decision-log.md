# Decision Log: fix-trunc-round-exact-decimal-date

## Interview

Headless planning run. The orchestrator passed these answers from the clarifying interview with the user. Each pair paraphrases one answer.

**Q:** What is the scope?
**A:** Issues #431 and #201 together. The issue bodies are the source of truth. #431: `TRUNC` and `ROUND` on DECIMAL run as float in DataFusion, `TRUNC` returns wrong values, and `ROUND` announces a wrong result type and fails comparisons. #201: `TRUNC`, `ROUND`, and `DATE_TRUNC` on DATE and TIMESTAMP fail at plan time or return wrong values.

**Q:** What should the plan emphasise?
**A:** In the user's words: "The plan should focus on e2e testing a wide range of values to ensure correctness with a wide set of values and edge cases, while maintaining the tests compact". One compact, data-driven E2E over one purpose-built fixture of edge values per type. Each case compares the VS with a native Exasol copy, including equal error and NULL outcomes. The E2E covers every pushdown position and the decline fallbacks without one test per case. Host unit tests add exhaustive or property-style kernel checks against independent oracles. The plan justifies the matrix size and keeps the runtime small.

**Q:** Which approach?
**A:** The scan session registers DataFusion scalar functions `exa_trunc` and `exa_round`, following the `FLOAT_DIV` precedent (`crates/lakehouse-engine/src/scan/checked_div.rs`, #370). #201 adds `Date32` and `Timestamp` branches and render-time declines.

**Q:** Which result types?
**A:** `exa_trunc(Decimal128(p,s))` returns `Decimal128(p,s)`, and `exa_round` returns `Decimal128(min(p+1,38),s)`. The declared return type must equal the type of the returned array. The root cause of the `ROUND` error is that DataFusion 54.1 `round` announces `Decimal128(19,2)` and returns `Decimal128(18,2)`. The adapter already declares the Exasol result type of each projection.

**Q:** How does the scale argument behave?
**A:** A non-integer `n` rounds half away from zero: `TRUNC(12.345, 1.7) = 12.34`, `ROUND(12.345, 1.5) = 12.35`, `ROUND(12.345E0, -1.5) = 0`. `n` may arrive as `Int64`, `Float64`, or a decimal (#475). A NULL `n` gives NULL. An `n` at or above the scale leaves the value unchanged. `TRUNC(12.345, -40) = 0`. Exasol pushes a column `n` as `trunc("S_ACCTBAL", CAST("S_NATIONKEY" AS DECIMAL(9,0)))`, so a per-row `n` must work.

**Q:** What happens on `ROUND` overflow?
**A:** Native `ROUND` past 36 digits fails with SQL state `22003`, so `exa_round` must fail too. A UDF error surfaces as `F-UDF-CL-RUST-9001` with SQL state `22002`. This difference goes into `docs/semantic-differences.md`.

**Q:** How do DOUBLE arguments behave?
**A:** DOUBLE `TRUNC` is `trunc(x * 10^n) / 10^n`, which stays. For DOUBLE `ROUND`, the `n = 0` step behaves like `floor(y + 0.5)`, and `ROUND(0.49999999999999994E0) = 1`. A non-finite intermediate is an error (`22003`) in Exasol, for example `TRUNC(x, 400)` and `TRUNC(1.5E300, 10)`, and both functions must raise. Exasol folds literal-only calls before pushdown, so DOUBLE parity is tested with column values.

**Q:** How large is the defect, and what speed must the fix keep?
**A:** Float `trunc` gives wrong values for 5,685 of 50,000 rows of `DBXA.SUPPLIER` (11.4%). The exact kernel must not be slower than float `trunc`. Measured: `SUM(TRUNC(x,1))` 57.5 ms float, 16.2 ms `i64`, 43.4 ms `i128`. `WHERE TRUNC(x,1) <> x` 143.4 ms, 22.2 ms, 50.4 ms. Use `i64` for `p` at most 18 with one power of ten per literal `n`, and `i128` above.

**Q:** Is a benchmark query part of this plan?
**A:** A `bench/` `TRUNC` query is an open item. Benchmark requirements live only in `bench/requirements.md`, never in `specs/`.

**Q:** Which constants do unit tests use?
**A:** Bare numeric literals parse as float in the DataFusion dialect (#475), so unit tests use typed or string-cast constants.

**Q:** Which environments?
**A:** `DBXA` and `DBXA_COPY` on staging are for manual verification only. The committed E2E runs against the local Docker Exasol.

**Q:** Which date forms must be covered?
**A:** `TRUNC(date)`, `TRUNC(date, 'MM'|'Q'|'IW'|'WW'|'W'|'CC'|'YYYY')`, `TRUNC(ts, 'HH')`, `ROUND(date, 'MM'|'YYYY')` including `05-15` against `05-16`, `DATE_TRUNC('quarter'|'day'|'month'|'year')`, and the range edges `0001-01-01` and `9999-12-31`, never through a nanosecond timestamp.

**Q:** Which forms decline?
**A:** `D`, `DAY`, `DY`, `DATE_TRUNC('week')`, and any non-literal format or unit. Each is confirmed on the `WHERE`, select-list, `GROUP BY`, and aggregate-argument paths.

**Q:** What happens to a non-numeric first argument?
**A:** Until the date branches exist, a non-numeric first argument keeps failing at plan time.

**Q:** Is withdrawing `FN_TRUNC` in scope?
**A:** No.

## Design Decisions

### [1] Reproduce an advertised Exasol function with a scan-session function when no rendered SQL can

- **Decision:** When no DataFusion SQL text the type-blind translator can render reproduces an advertised Exasol function, the DataFusion dialect renders a call to a function that the scan session registers. That function owns Exasol's semantics and selects its computation from the Arrow argument types when DataFusion plans the query. A divergence that rendered SQL can reproduce keeps a SQL rendering.
- **Alternatives:** Rewrite the expression in the adapter by argument type: rejected, because the adapter knows column types but not the type of a nested expression, so it cannot choose for every argument. Withdraw `FN_TRUNC`, `FN_ROUND`, and `FN_DATE_TRUNC`: rejected, because it loses the pushdown and the interview put the withdrawal out of scope. A SQL wrapper around DataFusion's built-ins: rejected, because no wrapper turns a float `trunc` into exact decimal arithmetic or teaches `trunc` an Exasol date format.
- **Rationale:** `/speq:adr-rules` criterion 2 (a long-lived constraint that binds how every future divergent function is fixed) and criterion 4 (it rejects the plausible withdrawal and wrapper alternatives). `speq decision-log show` lists `checked-float-division-rendering-layer-fix`, which records the same mechanism for `FLOAT_DIV` only, `checked-float-division-name-owned-by-vs-expression`, which this plan conforms to for name ownership, and `advertised-capability-null-contract-is-adapters-to-reproduce`, which prefers a SQL wrapper where one reproduces Exasol. This decision is consistent with that ADR and states when a session function is the right tool instead. No accepted ADR states the general rule.
- **Consequences:** The sibling project that consumes `crates/vs-expression` must register each such function or stop advertising the capability, and a missing registration fails loudly at first use. Each session function adds a plan-time type dispatch that a test must pin, because a declared type that differs from the returned type is the defect class of #431.
- **Architecture:** no change: the functions sit inside the `datafusion-scan` session builder, which already registers `lakehouse_render_nested_json` and `vs_checked_float_div`, and `datafusion-scan` already depends on `crates/vs-expression`.
- **Promotes to ADR:** yes

### [2] The translator classifies the second argument by node shape

- **Decision:** In the DataFusion dialect, `TRUNC` and `ROUND` accept one argument, or a second argument that is a numeric literal (`literal_exactnumeric`, `literal_double`), the `NEG` of a numeric literal, a `CAST` to a numeric type, or a `literal_string` in the format vocabulary. A format literal renders as its group's canonical token. Every other second argument declines: a column, a `literal_null`, a `CAST` to a string type, any other function, `D`, `DAY`, `DY`, a string outside the vocabulary, and more than two arguments. The Exasol dialect renders verbatim and is not edited. `LOG` keeps its rendering.
- **Alternatives:** Render any second argument and let the scan function dispatch on its Arrow type: rejected, because a column holding format strings could carry `D`, which the scan cannot evaluate without `NLS_FIRST_DAY_OF_WEEK`, and the scan cannot decline per row. Decline every non-literal scale: rejected, because Exasol wraps a column scale in `CAST(<column> AS DECIMAL(9,0))`, the interview requires per-row scales, and a numeric `CAST` cannot carry a format.
- **Rationale:** The node shape is the only information the translator has, and it separates the two argument kinds exactly. A bare numeric column as the scale declines and costs only the pushdown of that expression. Task 1.1 confirms the shapes Exasol sends.
- **Promotes to ADR:** no

### [3] `crates/vs-expression` owns the function names and the format vocabulary

- **Decision:** `crates/vs-expression` exports `EXA_TRUNC_FN = "exa_trunc"`, `EXA_ROUND_FN = "exa_round"`, `EXA_DATE_TRUNC_FN = "exa_date_trunc"`, and two plain types, `ExaDatetimeFormat` and `DateTruncUnit`. Each type parses an Exasol spelling, returns a canonical token, and parses a canonical token. The scan functions read the canonical token through the same types and never parse an Exasol spelling.
- **Alternatives:** Pass Exasol's spelling through and parse it in the scan: rejected, because the vocabulary would then live in two crates that must agree (back-door leakage). Keep a literal name on each side: rejected by the accepted ADR `checked-float-division-name-owned-by-vs-expression`.
- **Rationale:** One module owns the vocabulary decision, and the canonical token is the only interface between the translator and the scan. The types depend on nothing, so `crates/vs-expression` keeps its dependency set.
- **Promotes to ADR:** no

### [4] `DATE_TRUNC` gets its own function and unit list

- **Decision:** `DATE_TRUNC` renders `exa_date_trunc('<unit>', <source>)`. The accepted units are `microseconds`, `milliseconds`, `second`, `minute`, `hour`, `day`, `month`, `quarter`, `year`, `decade`, `century`, and `millennium`, case-insensitively. `week`, every other spelling (including Exasol's `mon` and `y`), and a non-literal unit decline.
- **Alternatives:** Map `DATE_TRUNC` units onto the `TRUNC` formats: rejected, because native `DATE_TRUNC('hour', <date>)` returns the date while native `TRUNC(<date>, 'HH')` fails, and `decade` and `millennium` have no format. Keep DataFusion's `date_trunc`: rejected, because DataFusion 54.1 has no `decade`, `century`, or `millennium`, accepts only the singular sub-second spellings, and converts a DATE to a nanosecond timestamp.
- **Rationale:** Measured on native Exasol: `DATE_TRUNC('decade'|'century'|'millennium', DATE '0005-05-05')` returns `0001-01-01`, `millennium` returns `9001-01-01` for `9999-12-31`, `century` returns `1901-01-01` for `2000-12-31`, and `week` returns `2024-05-12` under `NLS_FIRST_DAY_OF_WEEK = 7`. Declining the rare spellings keeps the vocabulary to one name per unit, and native Exasol still evaluates them.
- **Promotes to ADR:** no

### [5] Exact decimal kernel with an `i64` fast path

- **Decision:** `exa_trunc` and `exa_round` compute `Decimal128(p,s)` with `p` at most 36 exactly, in `i64` when `p` is at most 18 and the result fits, otherwise in `i128`. A scalar scale gets one precomputed power of ten per call. An integer type is treated as `DECIMAL(P,0)` with `P` from `scan-types/type-mapping` (3, 5, 10, or 20). `TRUNC` returns `Decimal128(p,s)`, and `ROUND` returns `Decimal128(min(p+1,38),s)`. A scale arrives as an integer, float, or decimal scalar or array and is rounded half away from zero when not integral. A `Decimal128` wider than 36 digits converts to `Float64` and takes the DOUBLE path of entry [7]. `Decimal256`, strings, and Booleans fail at planning.
- **Alternatives:** Always `i128`: rejected, because it is 2.7 times slower than `i64` on the measured `SUM` and 2.3 times on the measured filter. Return the narrower type native Exasol declares for a literal scale: rejected, because `return_type` sees only argument types, and the strict emit cast already converts `Decimal128(p,s)` to the declared type without loss. Fail for a wider-than-36-digit decimal: rejected, because DataFusion's float `trunc` and `round` return values for it today, and native Exasol, which sees a `VARCHAR`, also converts it to DOUBLE (measured: `TRUNC(<varchar>, 1)` and `ROUND(<varchar>, 1)` declare `DOUBLE`, the one-argument forms declare `DECIMAL(18,0)`).
- **Rationale:** The measured speeds (16.2 ms `i64` and 43.4 ms `i128` against 57.5 ms float) meet the requirement on both paths. Measured native values: `ROUND(99.99, 1) = 100.0` and `ROUND(99.99) = 100` on `DECIMAL(4,2)`, `TRUNC(99.99, 1) = 99.9`, a NULL scale gives NULL, `ROUND(x, 40)` returns `x`, and a `DECIMAL(5,2)` scale of `1.70` acts as 2 and of `-1.50` as -2. Arrow's decimal rescale rounds half away from zero, so DataFusion's `CAST(n AS DECIMAL(9,0))` matches Exasol's.
- **Promotes to ADR:** no

### [6] `ROUND` overflow is judged at the result scale

- **Decision:** `exa_round` fails when the rounded value needs more than 36 digits at the result scale `r`. `r` is the clamp of `n` to 0 to `s` for a scalar `n`, and `s` for an array `n`. `exa_trunc` never fails on a DECIMAL.
- **Alternatives:** Fail only when the value overflows `Decimal128(38)`: rejected, because native Exasol fails earlier. Judge every case at scale `s`: rejected, because native `ROUND(9999999999999999999999999999999999.99, 1)` on `DECIMAL(36,2)` succeeds with a literal `n`.
- **Rationale:** Native Exasol checks against its declared result type, which carries scale `r` for a literal `n` and scale `s` for a column `n` (measured: `ROUND(D18_2, 1)` declares `DECIMAL(18,1)`, `ROUND(D18_2, N)` declares `DECIMAL(19,2)`). Measured: 36 nines with `n = -1` fail with `22003`. The 34-digit value above succeeds with literal `n = 1`, and the same value with a column `n` of 1 or with `CAST(N AS DECIMAL(9,0))` fails.
- **Promotes to ADR:** no

### [7] DOUBLE steps reproduce native Exasol as measured

- **Decision:** `p = 10^|n|` comes from a table of correctly rounded powers from `1E0` to `1E308`, and an `|n|` above 308 is a non-finite `p`. For `n >= 0`, `y = x * p` and the result is `step(y) / p`. For `n < 0`, `y = x / p` and the result is `step(y) * p`. The `TRUNC` step is `trunc(y)`. The `ROUND` step is `sign(y) * floor(|y| + 0.5 + c)` in DOUBLE arithmetic, with `c` of at least `5.0E-14` and below `5.1E-14`. A non-finite `p`, `y`, or result fails with `numeric value out of range`. A stored non-finite value also fails, because Exasol admits no non-finite DOUBLE.
- **Alternatives:** `floor(y + 0.5)` as the interview described it: rejected, because native `ROUND(2.49999999999999E0)` returns 3. Rust's `f64::round`: rejected for the same reason. Pre-round `y` to 13 decimals: rejected, because native `ROUND(100.5 - 5.68E-14)` rounds up, which the pre-rounding model does not predict. Compute `10^|n|` by repeated multiplication: rejected, because the product accumulates rounding error from `1E23` on.
- **Rationale:** The measured threshold refines the interview's description. At `y = 0.5` and `y = 2.5`, a value `5.0E-14` below the half rounds up and `5.1E-14` below rounds down. Measured native values on column data: `ROUND` returns 3 for `2.4999999999999996` and `2.49999999999999`, 2 for `2.4999999999999`, 1 for `0.49999999999999994` and `0.499999999999999`, -1 for `-0.49999999999999994`, 1234567 for `1234567.4999999998` and `1234567.49999999`, 1000 for `1000.4999999999999`, 4503599627370496 for `4503599627370495.5`, and -3 for `-2.5`. `ROUND(x, 2)` returns `2.68` for `2.675`, `1.01` for `1.005`, `0.29` for `0.285`, and `2.5` for `2.4999999999999996`. `TRUNC(x, 2)` returns `2.49`, `1.0`, `0.28`, and `2.67` for the same four values. `TRUNC(0.49999999999999994, 1) = 0.4`, `ROUND(0.49999999999999994, 1) = 0.5`. For `x = 2.675`, `n = 308`, `400`, `-320`, and `-400` fail, and `n = -308` returns 0. `TRUNC(1.5E300, 10)` fails, `ROUND(1.5E300, -300) = 2E300`, and `ROUND(1.5E300, -301) = 0`.
- **Consequences:** `c` is an empirical constant. The unit tests pin it with the measured table, and the E2E compares near-half values with native Exasol on each CI image, so a version that rounds differently fails a test instead of drifting silently.
- **Promotes to ADR:** no

### [8] Day-count formats follow Exasol's hybrid calendar

- **Decision:** The scan computes `WW`, `IW`, `W`, and the ISO year in Exasol's calendar: Julian before 1582-10-15, Gregorian from it, with 1582-10-04 followed by 1582-10-15. It reads the year-month-day label of the stored `Date32` or timestamp, converts the label to a day number in that calendar, computes the period start, and converts the result label back to a proleptic Gregorian `Date32`. A result label that proleptic Gregorian lacks (February 29 of 100, 200, 300, 500, and later Julian-only leap years before 1582) fails. The label formats (century, year, quarter, month, day, time) and every `DATE_TRUNC` unit compute on the label alone.
- **Alternatives:** Compute in proleptic Gregorian: rejected, because the weekday differs before 1582 (native `DATE '0001-01-01'` is a Saturday, proleptic Gregorian says Monday), so `IW` and `WW` would return wrong labels with no error. Fail every day-count format before 1582: rejected, because sentinel dates such as `0001-01-01` are common, and the measured native values are reproducible.
- **Rationale:** The scan emits the label, and native Exasol stores the same label, so the label is what both sides share. Measured native values: `TO_CHAR(DATE '0001-01-01', 'DY') = SAT`, `1582-10-04` is a Thursday and `1582-10-15` a Friday, `ADD_DAYS(DATE '1582-10-04', 1) = 1582-10-15`, `DATE '0100-02-29'` is valid, and `DATE '1582-10-10'` normalizes to `1582-10-15`. `TRUNC` from `0100-03-05`: `WW` `0100-03-04`, `IW` `0100-03-02`, `W` `0100-03-01`, quarter `0100-01-01`, `ROUND` `IW` `0100-03-02`. From `1500-06-15` (a Monday): `WW` `1500-06-10`, `IW` and `W` `1500-06-15`. From `1582-10-04`: every week format returns `1582-10-01`. From `1582-10-15`: `WW`, `IW`, `W` `1582-10-01`, `ROUND` `IW` `1582-10-18`. From `1582-10-20`: `WW`, `IW`, `ROUND` `IW`, `W` `1582-10-18`. From `1600-03-05`: `WW` `03-04`, `IW` `02-28`, `ROUND` `IW` `03-06`, `W` `03-01`. From `0001-01-08`: `WW` `01-08`, `IW` `01-03`, `ROUND` `IW` `01-10`, `W` `01-08`.
- **Consequences:** The scoped exception for Julian-only February 29 labels is recorded in the spec delta and in `docs/semantic-differences.md`. The E2E fixture check `e2e_trunc_round_fixture_reads_back_like_native` establishes that the scan emits pre-1582 labels unchanged. If it fails, the label shift is a separate defect, which the implementer reports to the user without weakening the test.
- **Promotes to ADR:** no

### [9] Failures surface through one typed error and a per-session record

- **Decision:** The three functions raise a typed `ExasolFnError` whose message names `TRUNC` or `ROUND` and contains Exasol's phrase. They record their first failure in one `Arc<OnceLock<ExasolFnError>>` that `register_exasol_fns` creates for each session and hands to all three. `classify_scan_error` recognises the type after the checked division. The dispatcher function, renamed `reframe_session_function_failure`, reads the checked-division record first and the new record second, and applies the existing replace and compose rules.
- **Alternatives:** One record per function: rejected, because the dispatcher would read three records for one rule. Move the checked division onto the shared record: rejected, because it changes `checked_div.rs` and its tests for no behavior gain. A process-global record: rejected by the accepted ADR `checked-float-division-session-scoped-failure-recording`.
- **Rationale:** The Parquet row filter flattens a predicate error to text, the same route #370 measured, so the typed record is needed. Conforms to `checked-float-division-session-scoped-failure-recording`: the record is scoped to the session that registered it and never survives the scan, the one dispatcher reframes a failed scan, and the replace and compose rules stay as that ADR states them. That ADR names the dispatcher function `reframe_checked_division`. The rename to `reframe_session_function_failure` is an implementation detail that leaves the ADR's decision unchanged, so no superseding ADR is needed.
- **Promotes to ADR:** no

### [10] One differential E2E binary over one edge-value fixture

- **Decision:** `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs` seeds the Iceberg fact table `trunc_round.trunc_round` and the 3-row Iceberg dimension table `trunc_round.trunc_round_dim`, and the native tables `TRUNC_ROUND_NATIVE.TRUNC_ROUND` and `TRUNC_ROUND_NATIVE.TRUNC_ROUND_DIM`, all from the same Rust constants. It reads the Iceberg tables through its own virtual schema `TRUNC_ROUND_VS` and runs six table-driven tests. A case passes when the VS and the native tables give equal declared types and equal rows, or both fail. The fact table holds about 28 rows. Each value column covers one domain and is padded with NULL, so NULL input is itself a case:
  - `D4_2` `DECIMAL(4,2)`: carry into a new digit (`±99.99`), half ties at `n = 1` (`±12.35`, `±0.05`), and zero.
  - `D18_2` `DECIMAL(18,2)`: the `i64` limit (`±9999999999999999.99`), half ties, and values float `trunc` gets wrong (for example `8.20`, `0.29`, `1.15`, `4.35`), chosen by a host check.
  - `D19_4` `DECIMAL(19,4)`: the first `i128` precision, with half ties.
  - `D36_2` (magnitudes below `9E33`) and `D36_0` (magnitudes below `5E35`): 34- to 36-digit values with half ties at `n = 1` and `n = -1`.
  - `D38_2` `DECIMAL(38,2)`, which Exasol sees as `VARCHAR`: values within 15 significant digits, plus `123456789012345678.91`, whose unscaled value exceeds `2^53`.
  - `I32` and `I64`: the type limits and ties at `n = -1`.
  - `DBL` and `FLT`: the measured near-half values, the measured `n = 2` values, `-0.0`, one subnormal, and `±1E250`.
  - `N_INT` (cycling `-40`, `-2`, `-1`, `0`, `1`, `2`, `3`, `40`) and `N_DEC` `DECIMAL(5,2)` (cycling `1.70`, `-1.50`, `1.50`, `0.49`, `-0.49`): per-row scales, non-NULL on every row that holds a numeric value.
  - `DT`: `2024-01-15` and `2024-01-16`, `2024-05-15` and `2024-05-16`, `2024-05-31`, `2024-06-30` and `2024-07-01`, `2024-08-15` and `2024-08-16`, `2024-02-29`, ISO-year edges (`1999-12-31`, `2020-12-31`, `2021-01-03`, `2024-12-30`), century edges (`2000-12-31`, `2050-12-31`, `2051-01-01`), `1900-02-28`, and `1900-03-01`.
  - `DT_OLD`: `0001-01-08`, `0005-05-05`, `0010-06-15`, `0100-03-05`, `1500-06-15`, `1582-10-04`, `1582-10-15`, `1582-10-20`, `1600-03-05`. No value yields a Julian-only label, which the unit tests cover.
  - `TS` (millisecond-exact, on 2024-05-15 unless a date is given): `2024-02-29 23:59:59.999`, and one pair per time and week threshold of the spec's format table, one millisecond before and at the threshold: `SS` (`10:37:42.499`, `10:37:42.500`), `MI` (`10:37:29.999`, `10:37:30.000`), `HH` (`10:29:59.999`, `10:30:00.000`), `DD` (`11:59:59.999`, `12:00:00.000`), `IW` and, in 2024, `WW` (`2024-05-16 11:59:59.999`, `2024-05-16 12:00:00.000`), `W` (`2024-05-18 11:59:59.999`, `2024-05-18 12:00:00.000`), and `WW` in a year that starts on a Wednesday (`2025-05-17 11:59:59.999`, `2025-05-17 12:00:00.000`).
  - `TSTZ`: instants whose UTC wall clock sits on a day or hour boundary.
  - `FMT` (`'MM'`) and `UNIT` (`'month'`): string columns for the non-literal decline cases.
  - Probe columns, NULL except on one row: `P36_0_MAX` (36 nines), `P36_2_MAX` (`9999999999999999999999999999999999.99`, on a row where `N_INT = 1`), `P_DT_MIN` (`0001-01-01`), `P_DT_MAX` (`9999-12-31`), `P_TS_MAX` (`9999-12-31 23:59:59.999`), and `P_DBL_HUGE` (`1.5E300`). They keep each failing case out of the select-list statements.
  - The dimension table holds `DIM_ID` 1 to 3 and `DIM_MONTH` `2024-02-01`, `2024-05-01`, and `2024-08-01`. Its column names do not occur in the fact table, so `disjoint_schema_guard` admits the broadcast join.
- **Case matrix:** one select-list statement per domain row applies every listed argument to its column. "Groups" means absent plus every format group of the spec's format table that applies to the type, each in its canonical spelling, and one other spelling per group in lower case.

  | Domain | Functions | Literal arguments | Per-row scale | Positions beyond the select list |
  |--------|-----------|-------------------|---------------|----------------------------------|
  | `D4_2` (scale 2, 2 integer digits) | `TRUNC`, `ROUND` | absent, `-40`, `-2`, `-1`, `0`, `1`, `2`, `3`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT`, `N_DEC` | none |
  | `D18_2` (2, 16) | `TRUNC`, `ROUND` | absent, `-40`, `-16`, `-1`, `0`, `1`, `2`, `3`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT`, `N_DEC` | `WHERE TRUNC(D18_2, 1) <> D18_2`; `GROUP BY ROUND(D18_2)`; `SUM(TRUNC(D18_2, 1))`; join projection |
  | `D19_4` (4, 15) | `TRUNC`, `ROUND` | absent, `-40`, `-15`, `-1`, `0`, `1`, `4`, `5`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT`, `N_DEC` | none |
  | `D36_2` (2, 34) | `TRUNC`, `ROUND` | absent, `-40`, `-34`, `-1`, `0`, `1`, `2`, `3`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT`, `N_DEC` | `WHERE ROUND(D36_2, N_INT) <> D36_2` |
  | `D36_0` (0, 36) | `TRUNC`, `ROUND` | absent, `-40`, `-36`, `-1`, `0`, `1`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT`, `N_DEC` | `SUM(ROUND(D36_0, -1))` |
  | `D38_2` (DOUBLE path) | `TRUNC`, `ROUND` | absent, `-40`, `-2`, `-1`, `0`, `1`, `2`, `3`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT` | none |
  | `I32` (0, 10) | `TRUNC`, `ROUND` | absent, `-40`, `-10`, `-1`, `0`, `1`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT` | none |
  | `I64` (0, 19) | `TRUNC`, `ROUND` | absent, `-40`, `-19`, `-1`, `0`, `1`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT` | none |
  | `DBL` | `TRUNC`, `ROUND` | absent, `-40`, `-2`, `-1`, `0`, `1`, `2`, `15`, `40`, `1.5`, `-1.5`, `1.7` | `N_INT`, `N_DEC` | `WHERE ROUND(DBL, 2) <> DBL`; `MIN` and `MAX` of `ROUND(DBL, 2)`; join leg filter `ROUND(DBL, 1) > 0` |
  | `FLT` | `TRUNC`, `ROUND` | as `DBL` | `N_INT` | none |
  | `DT` | `TRUNC`, `ROUND`, `DATE_TRUNC` | groups; every `DATE_TRUNC` unit | none | `WHERE TRUNC(DT, 'MM') = DATE '2024-05-01'`; `WHERE DATE_TRUNC('quarter', DT) = DATE '2024-04-01'`; `GROUP BY TRUNC(DT, 'Q')`; `MIN(TRUNC(DT, 'MM'))`; join condition `TRUNC(DT, 'MM') = DIM_MONTH` |
  | `DT_OLD` | `TRUNC`, `ROUND`, `DATE_TRUNC` | groups; every `DATE_TRUNC` unit | none | none |
  | `TS` | `TRUNC`, `ROUND`, `DATE_TRUNC` | groups; every `DATE_TRUNC` unit | none | `WHERE ROUND(TS, 'HH') = TIMESTAMP '2024-05-15 11:00:00'`; `GROUP BY DATE_TRUNC('month', TS)`; `MAX(DATE_TRUNC('month', TS))` |
  | `TSTZ` | `TRUNC`, `ROUND`, `DATE_TRUNC` | groups; every `DATE_TRUNC` unit | none | none |

  The aggregate arguments run in one multi-aggregate statement. The `WHERE`, `GROUP BY`, and aggregate statements each assert an `exa_` call in the pushdown SQL.
- **Error and edge cases:** each case runs on both sides, and the runner requires both to fail or both to return equal rows:
  - `ROUND(P36_0_MAX, -1)` fails. `ROUND(P36_2_MAX, 1)` returns. `ROUND(P36_2_MAX, N_INT)` fails.
  - `TRUNC(P_DBL_HUGE, 10)` and `ROUND(P_DBL_HUGE, 10)` fail. `ROUND(P_DBL_HUGE, -300)` and `ROUND(P_DBL_HUGE, -301)` return. `TRUNC(DBL, 308)`, `TRUNC(DBL, 400)`, and `TRUNC(DBL, -320)` fail. `TRUNC(DBL, -308)` returns.
  - `TRUNC(P_DT_MAX, <each date group>)` returns, in one statement. `ROUND(P_DT_MAX, ...)` fails for `CC`, `YYYY`, `IYYY`, `Q`, `MM`, and `IW`, one case each, and returns for `WW`, `W`, and `DD` in one statement.
  - `ROUND(P_TS_MAX, ...)` fails for `DD`, `HH`, `MI`, and `SS`, one case each.
  - `TRUNC(P_DT_MIN, 'IYYY')` and `TRUNC(P_DT_MIN, 'IW')` fail. `TRUNC(P_DT_MIN, <the other date groups>)` returns in one statement, and `ROUND(P_DT_MIN, ...)` for `IYYY` and `IW` returns in one statement.
  - `TRUNC(DT, 'HH')` and `ROUND(DT, 'MI')` fail.
  - `WHERE` position, the Parquet row-filter route: `WHERE ROUND(P36_0_MAX, -1) > 0`, `WHERE TRUNC(P_DBL_HUGE, 10) > 0`, and `WHERE ROUND(P_DT_MAX, 'MM') > DATE '0001-01-01'` fail.
  - Every VS failure message names `TRUNC` or `ROUND`, contains the case's Exasol phrase, and does not contain `assigned data could not be read`.
- **Decline cases**, with `NLS_FIRST_DAY_OF_WEEK = 7`, each compared with native and checked for the absence of `exa_` calls: in `WHERE`, `TRUNC(DT, 'D')`, `TRUNC(DT, 'DAY')`, `TRUNC(DT, 'dy')`, `ROUND(DT, 'D')`, `TRUNC(DT, ' MM')` (both sides fail), `TRUNC(DT, FMT)`, `DATE_TRUNC('week', TS)`, `DATE_TRUNC('mon', TS)`, and `DATE_TRUNC(UNIT, TS)`. In the select list, `TRUNC(DT, 'D')`. As a `GROUP BY` key, `DATE_TRUNC('week', TS)`. As an aggregate argument, `MIN(TRUNC(DT, 'DY'))`. Around an aggregate, `DATE_TRUNC('week', MAX(DT))`.
- **Join cases**, each compared with native and each joining on `TRUNC(DT, 'MM') = DIM_MONTH`. The accepted case adds `TRUNC(D18_2, 1)` to the projection and `ROUND(DBL, 1) > 0` as the fact-leg filter, and asserts `has_broadcast_join_block` and an `exa_` call for each of the three forms. The declined case projects plain columns and uses `TRUNC(DT, 'D') = DATE '2024-05-12'` as the fact-leg filter, and asserts no broadcast block and no `exa_` call, because the N-scan wrapper evaluates the declined filter and the cross-leg condition in Exasol SQL. The rows that hold `DT` values `2024-02-29`, `2024-05-15`, `2024-05-16`, and `2024-08-16` also hold `D18_2` values and `DBL` values, and one of them holds a negative `DBL`, so each case returns rows and the accepted filter removes one.
- **Folded forms:** task 1.1 records here every form Exasol folds before pushdown, and task 4.2 leaves it out of the case tables.
- **Fixture rules:** a select-list expression that native Exasol rejects for a fixture value (for example a declared type too narrow for a rounded value) moves to the error cases. A fixture value that does not read back like native in `e2e_trunc_round_fixture_reads_back_like_native` is a separate defect: the implementer reports it to the user, and the user decides whether the value leaves the fixture.
- **Alternatives:** One test per case: rejected, because the user asked for compact tests, and each extra test would repeat the setup and the comparison. Hard-coded expected values in the E2E: rejected, because native Exasol on the same image is the stronger oracle. A fixture per pushdown position: rejected, because one fixture serves every position. A self-join of the fact table for the join cases: rejected, because `disjoint_schema_guard` sends a join with shared column names to the fallback.
- **Rationale:** The matrix gives every numeric domain the scale set the DECIMAL and DOUBLE scenarios name (absent, very negative, minus the integer digits, -1, 0, 1, the scale, the scale plus 1, 40, and non-integer values), every date domain every format group and unit, and every pushdown position at least one case. The value bounds (`DBL` at most `1E250`, `D36_2` below `9E33`, `D36_0` below `5E35`) keep every select-list statement free of overflow for every literal and per-row scale. The statement count is about 185: 14 select-list statements (VS, native, and `EXPLAIN VIRTUAL` each, 42), 10 position statements (30), 2 join cases (6), 32 error and edge cases on both sides (64), 13 decline cases (39), and 2 readback statements. About 72 of them are scan queries through the VS. At an estimated 0.3 to 0.6 seconds per scan query and 0.2 seconds per `EXPLAIN VIRTUAL`, the suite runs in about one minute, under the 2-minute budget, which the first E2E run confirms. The select-list statements contain only forms that render, because one declined select-list item routes the whole request to the qualified wrapper (ADR `cast-timestamp-precision-decline-not-approximate`). Aggregate cases apply `SUM` only to DECIMAL results and only `MIN` and `MAX` to DOUBLE results, because partial aggregation sums floats in another order than native Exasol. Unit tests carry what a bounded fixture cannot: every day of six windows, every precision and scale, every `TimeUnit`, and the `i64` and `i128` agreement.
- **Promotes to ADR:** no

### [11] Scope boundaries

- **Decision:** A string argument keeps failing at plan time, as the interview requires, and the gap is an open question `(#TBD)`. Withdrawing `FN_TRUNC` stays out of scope. A `bench/` `TRUNC` query is a follow-up `(#TBD)` whose requirement belongs in `bench/requirements.md`. Labels inside the 1582 gap (`1582-10-05` to `1582-10-14`) stay out of the fixture, because native Exasol cannot store them.
- **Alternatives:** Decline a bare VARCHAR column argument in the adapter, as the adapter declines a non-string `LIKE` subject (ADR `like-cast-date-decline-other-nonstring`): deferred, because the interview fixed the string behavior for this plan, and the decline covers only a bare column.
- **Rationale:** Each boundary keeps the plan to the two issues. The string-argument failure is a scan-side gap that exists today and that this plan does not widen. For every form the adapter renders, the plan conforms to ADR `advertised-capability-must-render-purge-the-native-retry-fiction`: the adapter renders an `exa_` call or hands the node to native Exasol through a wrapper path.
- **Promotes to ADR:** no

### [12] Native values are re-checked on the pinned image before the kernels are built

- **Decision:** Task 1.2 re-runs the native probes behind entries [5] to [8] on `exasol/docker-db:2025.1.16`, the image `docker-compose.yml` pins, and on `exasol/docker-db:8.29.13`, the image of the `E2E (8.29.x)` CI job. A differing value stops the implementation until the user decides. The E2E then compares with native Exasol on each CI image.
- **Alternatives:** Trust the 2026.2.0 measurements: rejected, because AGENTS.md requires SQL claims verified against a live Exasol, and the committed suite runs on another version.
- **Rationale:** The measurements came from the `local` exapump profile (Exasol 2026.2.0) because the project container was unhealthy during planning.
- **Promotes to ADR:** no

## Review Findings

### [13] [plan-review] Date ROUND rule was ambiguous and conflicted with its own examples

- **Finding:** The trunc-round spec said `ROUND` returns the nearest period start and picks the later one at the midpoint. Its own `Q` and `YYYY` examples and native Exasol's `MM`, `WW`, `W`, and `IYYY` results contradict that rule, and the `IYYY`, `WW`, and `W` rules were not stated.
- **Direction change:** The spec's format table gains a column that names, per format group, the field and threshold where `ROUND` moves to the next period start, plus a bullet stating that the last `WW` or `W` period holds at most 3 days. The old scenario is split into one scenario for the period rules, with an example table that holds the reviewer's probe values, and one for type, time-format, and zone handling. Task 1.2 re-measures every example value and each timestamp threshold on both pinned images. Tasks 3.2 and 3.5 implement and test the table's rules. Decision-log [10] adds `2024-01-15`, `2024-01-16`, and `2024-05-31` to `DT` and a threshold pair per time and week format to `TS`.
- **Promotes to ADR:** no

### [14] [plan-review] Trunc-round spec Background carried plan-time facts

- **Finding:** The Background held a staging statistic (5,685 of 50,000 values), a DataFusion `round` type mismatch that only the datafusion-scan delta depends on, and a measurement-version bullet that goes stale once the plan is recorded.
- **Direction change:** All three are deleted from the trunc-round spec Background. Decision-log [12] keeps the measurement version, and the datafusion-scan delta keeps the declared-type statement.
- **Promotes to ADR:** no

### [15] [plan-review] E2E case matrix was not enumerated

- **Finding:** Decision-log [10] listed fixture values but not the arguments each statement applies, so the width of the E2E could not be checked. Per-row scale columns were not aligned with the value rows, and the statement count did not match the domains.
- **Direction change:** Decision-log [10] now holds a case matrix, one row per domain, with the functions, the literal scale set or format and unit set, the per-row scale columns, and the positions beyond the select list. Every numeric domain covers absent, `-40`, minus the integer digits, `-1`, `0`, `1`, the scale, the scale plus 1, `40`, `1.5`, `-1.5`, and `1.7`. Every date domain covers every format group, no format, and every `DATE_TRUNC` unit. `N_INT` and `N_DEC` are non-NULL on every row that holds a numeric value. `DBL` stops at `1E250` and the 36-digit columns stay below `9E33` and `5E35`, so no select-list statement overflows. The entry lists the error, decline, and join cases, and derives about 185 statements and about 72 VS scan queries, which plan.md's E2E runtime row repeats.
- **Promotes to ADR:** no

### [16] [plan-review] Error E2E could not catch the storage-read framing

- **Finding:** The E2E runner only checked that a VS failure contains Exasol's phrase, which the storage-framed message also contains, so the E2E passed with task 2.4 broken. No error case sat in a `WHERE` predicate, the Parquet row-filter route where the message is flattened.
- **Direction change:** Task 4.2's runner asserts that every VS failure names `TRUNC` or `ROUND` and does not contain `assigned data could not be read`, as the checked-division test does. Decision-log [10] adds three `WHERE`-position error cases: `ROUND(P36_0_MAX, -1) > 0`, `TRUNC(P_DBL_HUGE, 10) > 0`, and `ROUND(P_DT_MAX, 'MM') > DATE '0001-01-01'`.
- **Promotes to ADR:** no

### [17] [plan-review] Broadcast-join run path had no E2E proof

- **Finding:** The datafusion-scan delta claims registration on the broadcast-join run path, but the trunc-round spec defined only four single-table pushdown positions and the E2E ran no join. A self-join of the fact table cannot fill the gap, because `disjoint_schema_guard` sends shared column names to the fallback.
- **Direction change:** The trunc-round spec's pushdown positions add a broadcast join's projection, leg filter, and join condition, and the decline scenario names the N-scan wrapper route. Task 4.1 seeds the dimension table `trunc_round_dim` (`DIM_ID`, `DIM_MONTH`) with a native copy. Task 4.2 adds an accepted and a declined join case that assert the broadcast shape or its absence, and task 4.3 adds `e2e_trunc_round_broadcast_join_matches_native` to the Scenario Coverage rows it proves.
- **Promotes to ADR:** no
