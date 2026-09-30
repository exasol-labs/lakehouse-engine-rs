# Decision Log: fix-trunc-round-date-trunc-date-args

## Interview

**Q:** #431 (the `exa_trunc`/`exa_round` UDFs) is still OPEN. Nothing exists in the tree (a grep for `exa_trunc`/`exa_round` finds nothing), and no plan for it exists in `specs/_plans`. How to sequence?
**A:** "Build on #431 (Recommended)." The spec deltas assume #431's UDFs and renderer arm exist. This plan only adds the Date32/Timestamp branches, the format decline, and the DATE_TRUNC routing. Task 1 is a gate: implementation is blocked until #431 is merged. Mark every #431-dependent statement explicitly and invent nothing about #431 beyond what the issue says.

**Q:** How to handle DATE_TRUNC units with no TRUNC-format equivalent, or session-dependent ones?
**A:** "Map known, decline rest (Recommended)." Map year/quarter/month/day/hour/minute/second to TRUNC formats via `exa_trunc`. Decline `'week'`, any unit outside that set, and non-literal units, so Exasol evaluates them (render error, then the existing fallback paths). The called function is set by the third answer (decision [2]).

**Q:** Measured on `exasol/docker-db:2025.1.16`: `TRUNC(<date>, 'HH')` fails natively with `22769`, while `DATE_TRUNC('hour', <date>)` returns the date unchanged. Keep a separate date-trunc UDF over the shared TRUNC core, or render DATE_TRUNC through `exa_trunc`?
**A:** "Keep separate UDF."

**Q:** Exasol uses the Julian calendar before 1582-10-15, and the scan reads `Date32` as proleptic Gregorian. Emulate the Julian calendar in the UDFs, or keep a tracked exception?
**A:** "Tracked exception (#TBD)." No Julian arithmetic. A human decides on the issue.

## Design Decisions

### [1] Extend #431's UDFs and gate implementation on #431

- **Decision:** The DataFusion dialect keeps rendering `TRUNC`/`ROUND` as #431's truncation and rounding UDFs. This plan adds their `Date32` and `Timestamp` branches. Task 1.1 blocks all implementation until #431 is merged and replaces the `<trunc-fn>`/`<round-fn>` placeholders with the landed names.
- **Alternatives:** Absorb #431 into this plan (rejected: #431 is a separate tracked issue with its own numeric scope). Build independent date-only UDFs (rejected: two UDFs per Exasol function, and the renderer cannot choose between them without argument types).
- **Rationale:** Interview answer 1. The UDF sees its argument's Arrow type at planning time on every render path, so one UDF per Exasol function serves numbers, dates, and timestamps.
- **Promotes to ADR:** no

### [2] DATE_TRUNC renders as a separate date-trunc UDF over the shared TRUNC core

- **Decision:** `DATE_TRUNC(unit, x)` renders as `<date-trunc-fn>(x, '<token>')`, where `<token>` is the TRUNC format of the same unit. `<date-trunc-fn>` computes with the TRUNC core, except that a DATE keeps its value for an hour, minute, or second token.
- **Alternatives:** (a) Render through `<trunc-fn>` and let it return a DATE unchanged for an hour token (rejected: a pushed `TRUNC(<date>, 'HH')` would return rows where native Exasol fails with `22769`). (b) Render through `<trunc-fn>` and let it raise (rejected: `DATE_TRUNC('hour', <date>)` returns the date natively and pushes correctly today). (c) Decline DATE_TRUNC's hour, minute, and second units (rejected: loses the pushdown of `DATE_TRUNC('hour', <timestamp>)`).
- **Rationale:** The user confirmed this design (interview answer 3) after seeing the measured evidence. On `exasol/docker-db:2025.1.16`, `TRUNC(<date>, 'HH')` fails with `22769` at compile time on a native table, yet Exasol delegates it for a virtual table (`EXPLAIN VIRTUAL`). `DATE_TRUNC('hour', <date>)` returns the date unchanged. The two calls differ only in that case, so one flag on a shared core covers it. The unit mapping and decline set are interview answer 2's.
- **Consequences:** The sweep's banned-token list gains `<date-trunc-fn>`. The name follows #431's naming convention.
- **Promotes to ADR:** no

### [3] The format vocabulary is declared once in `crates/vs-expression`

- **Decision:** One public declaration in `crates/vs-expression` lists the TRUNC/ROUND tokens, the unit each names, the session-week set, and the DATE_TRUNC unit mapping. The translator's decline and the scan UDFs' parser both read it.
- **Alternatives:** A renderer allowlist plus a separate UDF parser (rejected: back-door agreement on one vocabulary in two crates). Forward every token and let the UDF alone decide (rejected: the UDF cannot decline, it can only fail).
- **Rationale:** A corollary of ADR 084 (`specs/_decision/084-fix-float-div-predicate-divzero.md`, "The function name is owned by `crates/vs-expression`; the implementation is owned by `crates/lakehouse-engine`"). A contract test (task 3.6) proves that every forwarded token parses.
- **Promotes to ADR:** no

### [4] Decline every non-literal second argument, the numeric form included

- **Decision:** In the DataFusion dialect, `TRUNC`/`ROUND` render only with no second argument, a numeric literal, or a supported string token. Any other node declines, and `literal_null` and column references are included.
- **Alternatives:** Decline only non-literal string arguments (rejected: the translator has no column types, so it cannot tell a digits column from a format column).
- **Rationale:** A per-row format cannot be checked for `D`/`DAY`/`DY` at render time. Exasol constant-folds foldable arguments before pushdown (measured: `'M' || 'M'` arrives as `'MM'`, `1+1` as `2`, `-1` as `-1`), so only row-dependent arguments decline.
- **Consequences:** Numeric `TRUNC(x, n_col)` loses pushdown and stays correct. This narrows #431's surface. The `vs-expression-translator-scalar-fns` CHANGED delta removes `ROUND` and `TRUNC` from the generic math rule and points both names to `vs-expression-translator-datetime-trunc`, which owns the decline.
- **Promotes to ADR:** no

### [5] Decline a string token outside the vocabulary

- **Decision:** A `literal_string` second argument that the vocabulary does not accept declines, for example `'XX'`, `' MM'`, or `'2'`.
- **Alternatives:** Forward it and let the UDF raise (rejected for `'2'`: Exasol casts it to a number on numeric `TRUNC`, measured `TRUNC(1.2345, '2')` is `1.23`).
- **Rationale:** Exasol evaluates the declined call with its own semantics: a number for a numeric string, and its own `22769` error for an invalid date format.
- **Promotes to ADR:** no

### [6] The UDFs raise Exasol's errors through ADR 084's typed session record

- **Decision:** An hour, minute, or second token on a DATE raises in `<trunc-fn>`/`<round-fn>` with Exasol's message text. A TRUNC, ROUND, or DATE_TRUNC result past `9999-12-31`, or outside the time unit's `i64` range, raises `datetime field overflow`. Each error is typed on the DataFusion error chain. The three UDFs record their first failure on one session-scoped record, and the scan dispatcher reframes from it as it does for a checked division (task 3.4). The DATE plus hour, minute, or second check raises at planning time in `return_field_from_args`, which receives the literal format. Every other check raises per batch.
- **Alternatives:** Return the date unchanged, or clamp (rejected: a filter consumes the value inside DataFusion and changes row counts without an error). Raise `DataFusionError::Execution` and accept the storage-read framing as a limitation (rejected: the recorded checked-division scenario names that framing a defect for a user error). Raise the DATE plus hour check per row (rejected: a shard with no rows returns no error, while native Exasol fails at compile time).
- **Rationale:** Without a record, the projection and filter routes surface `scan failed: assigned data could not be read: …` (`classify_scan_error` to `redact_storage_error`, `scan/emit.rs`). The filter route also flattens the error to `Error evaluating filter predicate: …` text. A planning-time raise surfaces as `DataFusion SQL error: …` or `partial aggregate SQL error: …`. ADR 084's record removes the storage-read framing for the checked division. Every run path's error funnels through `run_scan_dispatch` (`scan/mod.rs:209`), so the same reframe also removes the planning prefix. The decision applies ADR 084 to a second error family.
- **Consequences:** A scan the adapter prunes to zero files runs no UDF (`empty_result_sql`, `adapter/pushdown/mod.rs:205`). It returns no rows for `TRUNC(<date>, 'HH')`, where native Exasol fails. The scan delta names that divergence.
- **Promotes to ADR:** no

### [7] Dates before 1582-10-15 stay proleptic Gregorian as a tracked exception

- **Decision:** The UDFs compute in the proleptic Gregorian calendar that the scan reads `Date32` in. The divergence from Exasol's Julian calendar before the Gregorian reform is an explicit exception in the scan delta (`#TBD`, open question for a human to file). The user confirmed the tracked exception (interview answer 4).
- **Alternatives:** Emulate Exasol's hybrid calendar inside the UDF (rejected: every `Date32` in the engine is read proleptically, so a UDF-only fix leaves the engine inconsistent. Also, a Julian-only result such as `0100-02-29` is not representable as `Date32`).
- **Rationale:** Measured: `DATE '0100-02-29'` is valid, `DATE '1582-10-10'` normalizes to `1582-10-15`, and `DAYS_BETWEEN(DATE '1970-01-01', DATE '0001-01-01')` is `719164` against a proleptic 719162. The calendars agree from `1582-10-15` on. Label-only TRUNC formats still match on every date.
- **Consequences:** Inferred, not measured through the virtual schema: the pushed `DAYS_BETWEEN` family may share this pre-reform divergence, because it also computes on proleptic day counts. The open question covers that inference.
- **Promotes to ADR:** no

### [8] Planning-time measurements used the container's bundled `exaplus`

- **Decision:** The Exasol values in the spec deltas were captured with `/opt/exasol/db-2025.1.16/bin/Console/exaplus` inside the Docker Exasol container, because `exapump` is not installed on the planning host. Task 1.2 re-runs every cited expression through `exapump`.
- **Alternatives:** Leave the spec values unmeasured until implementation (rejected: CLAUDE.md requires measured, not recalled, Exasol behavior before it enters a spec).
- **Rationale:** The measurements were read-only literal queries plus two scratch schemas, dropped afterwards. The re-run restores the `exapump` tooling rule before any code depends on the values.
- **Promotes to ADR:** no

### [9] E2E tests live in a new file with a native-copy oracle

- **Decision:** `crates/lakehouse-engine/tests/e2e_datetime_trunc_test.rs` creates its own virtual schema over the seeded `events` table, copies the rows into a native table once, and compares each pushed query against the same expression over the copy. The file joins the `test-e2e` target in `Makefile`, which CI runs.
- **Alternatives:** Extend `e2e_capability_test.rs` (rejected: one knowledge cluster per file, and that file carries unrelated capability groups). Inline-literal oracles (rejected: twenty dates per shape make the literals unwieldy).
- **Rationale:** A native copy evaluates exactly the rows the scan reads, under the session's own NLS settings.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] UDF errors reached the user under the storage-read framing

- **Finding:** Decision [6] claimed that `DataFusionError::Execution` needs no typed recovery. Every non-division scan error goes through `redact_storage_error`, so the user saw `scan failed: assigned data could not be read: …`. The recorded checked-division scenario names that framing a defect. The planning-time route was unstated.
- **Direction change:** Option (a). The UDFs carry a typed error and record it on the session, and `run_scan_dispatch` reframes from the record (new task 3.4, new scan-delta scenario "A date/time UDF failure reaches the user with Exasol's message on every route"). The DATE plus hour, minute, or second check raises at planning time. Decision [6], plan.md § Impact, tasks 3.2, 3.5, and 4.4, and the manual test now match.
- **Promotes to ADR:** no

### [plan-review] The numeric non-literal decline contradicted the recorded math-function scenario

- **Finding:** The translator delta declined `TRUNC(x, n_col)`, while the recorded `vs-expression-translator-scalar-fns` scenario "Math scalar functions translate to DataFusion math calls" renders every listed name over its arguments.
- **Direction change:** A new `sql-comprehension/vs-expression-translator-scalar-fns` CHANGED delta removes `ROUND` and `TRUNC` from the generic rule and points both names to `vs-expression-translator-datetime-trunc`. That feature now renders a numeric literal second argument too. Task 1.1 reconciles the block's wording with #431's recorded scenario. plan.md § Features, task 2.2, and Scenario Coverage list the delta.
- **Promotes to ADR:** no

### [plan-review] Nanosecond results before the unit's range were unspecified

- **Finding:** The TRUNC scenario required a result for every nanosecond input, but `Timestamp(Nanosecond)` starts at `1677-09-21 00:12:43.145224192`. `TRUNC` with `CC` of a `1690` value is `1601-01-01` natively and cannot be represented.
- **Direction change:** The range scenario now covers all three UDFs and both range ends, and names the nanosecond case an explicit exception against native Exasol. The TRUNC scenario's GIVEN requires a representable result. The compliance bullet quotes Iceberg § Primitive Types and § Parquet for `timestamp_ns`. Task 3.1, plan.md § Impact, and Scenario Coverage (`trunc_below_nanosecond_range_raises`) match.
- **Promotes to ADR:** no

### [plan-review] Two Background facts had no dependent scenario step

- **Finding:** The translator Background cited `TRUNC(1.2345, 'MM')` failing with `22018`, and the scan Background named the registration site "next to the checked-division UDF". No scenario step used either fact.
- **Direction change:** Both clauses are deleted. The scan delta's numeric-argument scenario keeps the `22018` fact, and task 3.3 keeps the registration site.
- **Promotes to ADR:** no
