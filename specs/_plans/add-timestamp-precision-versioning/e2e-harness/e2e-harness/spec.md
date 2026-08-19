# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying correctness of projection, filter, and Iceberg file-pruning pushdown against a local Exasol Docker container.

## Background

* **This delta is issue #359.** It adds THREE scenarios and amends no recorded clause. The first is a
  timestamp round-trip that asserts VALUE fidelity at the declared precision; the second gates the
  suite on both supported Exasol major versions; the third repairs the one existing assertion that
  compares a VS timestamp's RENDERED STRING against a native oracle. Every recorded scenario, seed
  fixture, and provisioning helper otherwise stays as recorded.
* **Every timestamp-adjacent assertion in the suite today is blind to precision loss, which is why
  microsecond truncation shipped untested on every Exasol version.**
  `e2e_projection_filter_limit_returns_correct_rows` asserts only that `event_ts` is non-null;
  `e2e_int96_far_future_timestamp_scans_without_overflow` prefix-matches
  `"9999-12-31 23:59:59"` at seconds resolution;
  `count_distinct_bare_column_type_matrix_matches_single_node` counts distinct `c_ts` values whose
  `typed_probe()` offsets are whole milliseconds (`BASE_TS_MICROS + ms * 1_000`), so no sub-millisecond
  content exists to lose; and `create_vs_maps_iceberg_schema` matches the declared type by PREFIX, so
  `TIMESTAMP` and `TIMESTAMP(6)` are indistinguishable to it. None of these is wrong — together they
  simply cannot fail on a truncating engine.
* **The new fixture needs its OWN namespace and virtual schema, per the recorded precedent.**
  `vs-adapter/create-virtual-schema` records that a fixture added to `e2e_lakehouse` enters every
  existing suite's `createVirtualSchema` enumeration and can churn assertions a plan promises to leave
  untouched; `e2e_non_ascii_identifier_test` is the working precedent for a standalone binary that
  seeds its own namespace, creates its own VS, and is invisible to the rest of the suite.
* **The new E2E binary MUST be added to the `test-e2e` make target.** That target enumerates its test
  binaries explicitly, so a new binary that is not listed never runs in the suite gate.
* **The expected precision MUST be derived from the LIVE session, not from an environment variable or
  a Docker image tag.** `cargo test --features exasol-e2e` runs against whatever stack is up, so an
  `EXASOL_IMAGE`-derived expectation silently picks the wrong arm whenever the variable is absent or
  stale — the same class of failure a stray `bench/.env` produces. Reading the running engine's own
  version makes the expectation correct however the stack was started.
* **The expectation MUST be an INDEPENDENT oracle, not a call into the production version parser.** A
  test that computes its expected declaration by calling the very rule under test cannot fail when that
  rule is wrong. The helper therefore carries its own explicit version-to-precision table, and the
  production rule's own inputs are covered separately by a unit matrix over concrete version strings.
* **Two whole-millisecond-agreeing value families are chosen deliberately, so the assertion cannot
  depend on whether Exasol truncates or rounds to the declared precision.** Every seeded fractional
  part has a fourth digit below 5 (`.000001`, `.000002`, `.123456`, `.123457`), so truncation and
  round-to-nearest both produce the same millisecond value on the 8.x arm. Asserting the millisecond
  PREFIX rather than a rounding mode keeps the 8.x expectation honest without pinning behavior the
  scenario has not captured.
* **`E2E` is a required status check on `main`'s ruleset, so the matrix MUST NOT rename it.** A
  matrixed job whose legs both carry new names leaves the ruleset waiting on a check that never
  reports; PRs then block until an admin edits the ruleset. Keeping one leg's name exactly `E2E`
  preserves the existing requirement, and the second leg's name is a NEW check an admin adds — the same
  operator step issue #336 already tracks for `e2e-azure`.
* **`upload-artifact@v7` rejects a name already used by another upload in the same run**, which the
  workflow already records for `e2e-azure`'s `exa-logs-azure`. Two matrix legs both uploading
  `exa-logs` on failure would fail the upload rather than the test, hiding the diagnostic the step
  exists to produce.
* **PR #358 already proved the whole suite passes on `8.29.13`** across the E2E, Lakekeeper, Unity, and
  Azure gates, so the 8.x leg is expected green from the start; the version gate is what keeps it green
  once the 2025.x declaration changes.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Microsecond-distinct Iceberg timestamps round-trip at the declared precision

* *GIVEN* a live Exasol instance, MinIO, and an Iceberg REST catalog
* *AND* an Iceberg table seeded into its OWN namespace — invisible to every other suite's `createVirtualSchema` enumeration — carrying an `id` column, a `timestamp` column, and a `timestamptz` column, each timestamp column holding FOUR values that differ ONLY below millisecond resolution: `2024-01-01 00:00:00.000001`, `.000002`, `.123456`, and `.123457`, every one of whose fourth fractional digit is below 5 so truncation and round-to-nearest agree at millisecond resolution
* *AND* a virtual schema created over that namespace through a real `createVirtualSchema`
* *AND* the running engine's own version, read from the LIVE session and mapped to an expected precision by a test-owned table that MUST NOT call the production version rule it exists to check
* *WHEN* an Exasol user projects both timestamp columns and counts their distinct values through the virtual schema
* *THEN* `SYS.EXA_ALL_COLUMNS` SHALL report BOTH columns' `COLUMN_TYPE` as EXACTLY `TIMESTAMP(6)` when the expected precision is 6 and EXACTLY `TIMESTAMP` when it is 3, matched in full rather than by prefix — the prefix tolerance the recorded assertions use is precisely what made the truncation invisible
* *AND* at the microsecond precision the projected values SHALL render all SIX seeded fractional digits for every row of BOTH columns, and `COUNT(DISTINCT)` over each column SHALL return 4
* *AND* at the millisecond precision the projected values SHALL render the seeded millisecond prefix (`.000` for the first pair, `.123` for the second) for every row of BOTH columns, and `COUNT(DISTINCT)` over each column SHALL return 2 — the two microsecond-distinct values of each pair collapsing into one, which this scenario records as a named Exasol 8.x version limitation rather than a defect
* *AND* the `timestamptz` column's values SHALL be the same UTC instants as the `timestamp` column's, so the two columns' assertions differ ONLY in the Iceberg source type and never in an expected value — the zone-awareness trade-off `datafusion-scan/type-mapping` records is out of this scenario's scope
* *AND* the query SHALL be proven to reach the scan UDF rather than an unaccelerated fallback, so the asserted values are the ones the scan emitted rather than ones Exasol computed for itself
* *AND* the scenario SHALL FAIL, not skip, when no live Exasol instance is available, per this repo's E2E contract
* *AND* the new test binary SHALL be listed in the `test-e2e` make target, because that target enumerates its binaries explicitly and an unlisted binary never runs in the suite gate
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The E2E suite gates on both supported Exasol major versions

* *GIVEN* the core `e2e` CI job, whose stack images are selected entirely by the `EXASOL_IMAGE` variable that the Makefile and `docker-compose.yml` both already read
* *WHEN* CI runs that job
* *THEN* it SHALL run the whole existing E2E suite TWICE — once against the current default `2025.x` image and once against an `8.29.x` image — as two legs of ONE matrixed job with identical steps, and MUST NOT duplicate the job body or swap the default image
* *AND* each leg SHALL pass its image through `EXASOL_IMAGE` so the image reaches both the `docker compose` steps and `make test-e2e`, requiring NO change to `docker-compose.yml` or the Makefile
* *AND* exactly ONE leg's status-check name SHALL be EXACTLY `E2E`, so `main`'s existing required-check requirement keeps being satisfied by a reporting check; the other leg SHALL carry a distinct name that names its Exasol version, and adding it to the ruleset SHALL be recorded as an operator action rather than assumed
* *AND* each leg's failure-log artifact SHALL carry a name unique to that leg, because `upload-artifact@v7` rejects a name already used by another upload in the same run — two legs both uploading `exa-logs` would fail the upload instead of surfacing the diagnostic
* *AND* the `release` job SHALL keep depending on `e2e` and therefore SHALL wait for BOTH legs, so neither version can be skipped on the way to a release
* *AND* `e2e-lakekeeper`, `e2e-unity`, and `e2e-azure` SHALL stay single-version, because they gate catalog integrations orthogonal to the Exasol engine version and a second leg of each would triple the stack cost for no new coverage
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A VS timestamp compared as a rendered string uses a precision-matched oracle

* *GIVEN* the recorded assertion that `UPPER(c_ts)` over the virtual table declines pushdown and matches an in-session native oracle — today `UPPER(CAST(TIMESTAMP '2024-01-01 00:00:00.100' AS TIMESTAMP))`, whose CAST target is the bare `TIMESTAMP` the VS column used to be declared as
* *WHEN* the virtual column is declared `TIMESTAMP(6)` and Exasol renders it with six fractional digits
* *THEN* the oracle's CAST target SHALL carry the SAME declared type the virtual column carries on the running engine, so the two sides of the comparison are rendered at one precision and the assertion keeps testing the declined-pushdown behavior it was written for rather than failing on a digit count
* *AND* the expected declared type SHALL come from the ONE shared helper this delta's round-trip scenario introduces, and MUST NOT be a second copy of the version-to-precision table
* *AND* every OTHER recorded timestamp assertion SHALL keep passing unchanged on both version arms, and the reason SHALL be that each is precision-insensitive rather than precision-correct: the declared-type checks in `create_vs_maps_iceberg_schema` and the Delta/Unity suites match by PREFIX, the INT96 far-future check prefix-matches at seconds resolution, and every `HOURS_BETWEEN`/`YEAR`/`SECOND(c_ts, 3)`/`COUNT(DISTINCT)` assertion reads a derived value no sub-millisecond digit reaches
* *AND* no recorded assertion SHALL be loosened to accommodate the new declaration — the one assertion that moves is the oracle's CAST target, and it moves to become MORE specific, not less
<!-- /DELTA:NEW -->
</content>
