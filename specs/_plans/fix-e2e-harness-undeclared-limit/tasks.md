# Tasks: fix-e2e-harness-undeclared-limit

> Issue [#312](https://github.com/exasol-labs/lakehouse-engine-rs/issues/312). Reference `#312` in
> every implementing commit.

## Rules for every implementer agent working these tasks

- **Use Serena's MCP symbolic tools for all code navigation and editing.** Call
  `initial_instructions` first if the tools are not loaded. Use `get_symbols_overview` and
  `find_symbol` for discovery, `find_referencing_symbols` for usages, and
  `replace_symbol_body` / `insert_after_symbol` / `insert_before_symbol` / `rename_symbol` /
  `safe_delete_symbol` / `replace_content` for edits. Never a raw `Edit` on a symbol reached
  through Serena. `grep` and `Glob` are for discovery only. `Read` and `Edit` are fine for
  non-code files — specs, `docs/debugging-pushdown.md`, `Makefile`, shell scripts.
- **The E2E stack does not start itself.** `make test-e2e` never runs `docker compose`. Bring the
  stack up first: `docker compose up -d --wait exasol minio iceberg-rest`. Without it every
  DB-backed test fails and looks exactly like a real regression.
- **Read the exit code, never the tail.** `make test-e2e | tail` masks a non-zero exit.
- **Unit tests live in a sibling `*_tests.rs` file.** No `#[cfg(test)]` test code in a production
  source file. See `CLAUDE.md`.
- **Verify against the live stack, never from documentation or code inspection.** This plan's
  central claim is a measurement. Do not substitute reasoning for a run.
- **Each E2E binary is gated by its own crate-root `#![cfg(feature = "…")]`**, and
  `crates/lakehouse-engine/Cargo.toml` wires no `required-features` for any of them:
  `exasol-e2e` gates `e2e_scan_test`, `e2e_capability_test`, `e2e_count_distinct_test`,
  `e2e_join_test`, `e2e_positional_deletes_test`, `e2e_int96_timestamp_test`, `e2e_refresh_test`,
  `e2e_non_ascii_identifier_test` and `e2e_capture_pushdown`; `lakekeeper-e2e` gates
  `e2e_lakekeeper_test`; `azure-e2e` gates `e2e_azure_test`; `cloud-e2e` gates `cloud_e2e_test`.
  Compiling a binary under the wrong feature flag produces an EMPTY binary and a meaningless
  exit 0 — nothing in the file is type-checked. Name the binary's own feature, always.

## Phase 1: Measure the injection surface (Group A — gates Group C)

- [x] 1.0 Add `ExaConn::capped_result_sets(max_rows: u32)` to
      `crates/lakehouse-engine/tests/common/exasol_ws.rs`: set `self.result_set_max_rows = max_rows`
      and return `Self`. This change is **purely additive** — leave `connect_inner`'s `10000` default
      and `unbounded_result_sets` exactly as they are; task 3.1 changes those. The three knobs
      coexist for the duration of Phase 1 and Phase 2.
      The task exists because `result_set_max_rows` is a private field
      (`crates/lakehouse-engine/tests/common/exasol_ws.rs:21`) whose only present-day setter is
      `unbounded_result_sets`, which can express `0` and nothing else. Tasks 1.1, 1.2 and 1.6 must
      declare a small cap distinguishable from both `0` and `10000`, and cannot do so without this
      method.
      The doc comment MUST state the design intent, not just the mechanism: a declared cap reaches
      the adapter as a pushdown `limit`, so a capped session exercises a different adapter plan than
      an uncapped one; declare a cap only when the test's assertion is about that capped plan. This
      doc comment is the single owner of a fact currently re-derived in scattered test comments.
      Task 1.5 adds its cross-reference to the measured shape matrix.
      **Blocks tasks 1.1, 1.2 and 1.6.**
- [x] 1.1 Add an optional `CAPTURE_RESULT_SET_MAX_ROWS` env var to
      `crates/lakehouse-engine/tests/e2e_capture_pushdown.rs`: unset means no declared cap; set to
      `n` means the capture connection calls `capped_result_sets(n)` (available from task 1.0). Keep
      the binary driven entirely by env vars — `scripts/capture-pushdown-payload.sh` must need no
      change, since it inherits the environment. Depends on task 1.0.
- [x] 1.2 Run the capped-versus-uncapped capture against the live Docker stack for all seven
      statement shapes, twice each — once with `CAPTURE_RESULT_SET_MAX_ROWS` unset, once set to a
      small value distinguishable from any SQL `LIMIT` in the statement. Shapes: bare projection;
      projection + filter; single-group aggregate; `GROUP BY` aggregate; `COUNT(DISTINCT)`;
      `ORDER BY … LIMIT`; broadcast-eligible inner equi-join. Pick the small value from neither `0`
      nor `10000`, so a `limit` in the capture is attributable to the declared cap and to nothing
      else. Diff the `pushdownRequest` and the generated scan-spec JSON for each pair. Depends on
      task 1.0. `[expert]`
- [x] 1.3 Record the measurement in
      `specs/_plans/fix-e2e-harness-undeclared-limit/injection-surface.md`: one row per shape, the
      exact statement run, whether the declared cap produced a `limit`, where that `limit` landed
      (common scan spec, per-shard spec, generated outer SQL), and any other field that differed.
      Note explicitly any shape where the two captures were identical — that is the evidence a
      shape is unaffected.
- [x] 1.4 Derive the affected-assertion list from 1.3: for each shape that gained a `limit`, name
      the E2E tests using that shape whose assertion prose does not describe a limit. This is the
      predicted scope of Phase 4. Use Serena's `find_referencing_symbols` and search tools over the
      11 E2E binaries; do not trust the census counts in `plan.md` without re-verifying them.
- [x] 1.5 Add the measured shape matrix to `docs/debugging-pushdown.md`, in the capture-tool
      section: which statement shapes convert a declared row cap into a pushdown `limit`, and the
      `CAPTURE_RESULT_SET_MAX_ROWS` knob that reproduces the comparison. This is the permanent home
      for the finding — the plan directory is archived by `/speq:record`. Add the cross-reference
      from task 1.0's `capped_result_sets` doc comment to this section, now that the section exists.
- [x] 1.6 Measure how many rows one `fetch` response actually returns. Run an UNCAPPED raw scan of
      `high_card_probe` (`HIGH_CARD_ROWS = 30_000` rows of ~100-byte `token` values —
      `crates/lakehouse-engine/tests/common/seed.rs:2267`, `:2378`) against the live Docker stack and
      read it at the harness's present `numBytes: 67108864` (64 MiB) budget. Report the rows one
      response returned and how many responses the full result set took. **Do not compute this
      figure — run it.** Record it in `injection-surface.md` alongside the shape matrix, and state
      plainly whether client-side truncation is reachable at that budget with the fixtures that exist
      today. No artifact in this plan may assert a value for this number before the run: 30,000 rows
      at ~100 bytes is roughly 3 MB against a 64 MiB budget, so one response plausibly returns the
      whole result set, and the Phase-2-before-Phase-3 ordering must be justified by what this
      measurement says rather than by an assumed truncation.
      **Blocks task 2.1.** Depends on task 1.0.

## Phase 2: Make the result reader complete (Group B — depends on task 1.6, gates Group C)

- [x] 2.1 Write the failing test first: `harness_reads_high_cardinality_result_set_to_completion`
      in `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs`. **Do not rely on the fixture
      happening to exceed one response** — task 1.6 measures whether it does, and at ~100 bytes per
      row 30,000 rows is roughly 3 MB against the harness's 64 MiB budget. Force the chunking
      instead: select the `token` column from `high_card_probe` through the VS on a connection
      declaring no cap (use `unbounded_result_sets()` until task 3.4 re-points it at the new
      default), read the result set through task 2.2's `numBytes`-parameterized entry point with
      `num_bytes = 65_536`, and assert both (a) the returned row count equals `HIGH_CARD_ROWS`
      (30,000) and (b) the read consumed more than one `fetch` response. At 64 KiB against ~100-byte
      rows a response holds at most a few hundred rows, so 30,000 rows span tens of responses;
      assert `responses >= 2` as the invariant and put the observed count in the assertion's failure
      message. Do not assert an exact response count — server-side row packing is what task 1.6
      measures, not something this plan may fix in advance. Cite task 1.6's recorded
      rows-per-response figure in the test's doc comment as the basis for the 64 KiB choice. Against
      the unfixed single-fetch reader this test MUST fail on assertion (a). The binary already seeds
      this table.
- [x] 2.2 Fix `ExaConn::fetch_result_columns` in
      `crates/lakehouse-engine/tests/common/exasol_ws.rs` to read the result set to completion.
      Today it issues one `fetch` at `startPosition: 0` with `numBytes: 67108864`, ignores how many
      rows the response returned, and closes the handle. Loop instead: accumulate per column,
      advance `startPosition` by the rows each response actually returned, and stop when the
      accumulated count reaches the `numRows` the metadata reported. Panic with the outstanding
      count if a response returns zero rows while rows remain — a silent short read is the failure
      mode this task exists to remove. Close the handle exactly once, on every exit path.
      Put the loop behind a `numBytes`-parameterized entry point so task 2.1 can force chunking:
      implement `fetch_result_columns_with_num_bytes(&mut self, result_set: &Value, num_bytes: u64)
      -> (Vec<Vec<Value>>, usize)`, whose second element is the number of `fetch` responses consumed,
      and keep `fetch_result_columns(&mut self, result_set: &Value) -> Vec<Vec<Value>>` as a thin
      delegate passing the present `67_108_864` and discarding the count. Twelve call sites outside
      `exasol_ws.rs` use the two-argument form (`e2e_scan_test.rs` 271/826/1632/1960/2215/2484,
      `e2e_capability_test.rs` 2946/2973, `e2e_int96_timestamp_test.rs:268`,
      `e2e_count_distinct_test.rs` 127/173, `common/e2e_harness.rs:288`), and the delegate keeps every
      one of them unchanged. Re-derive that list with Serena rather than trusting these line
      numbers. `[expert]`
- [x] 2.3 Confirm 2.1 now passes and that no existing `e2e_count_distinct_test` assertion regressed.
      Run the whole binary, not just the new test.

## Phase 3: Flip the default and replace the knob (Group C — depends on Groups A and B)

- [x] 3.1 In `crates/lakehouse-engine/tests/common/exasol_ws.rs`, make the two remaining harness
      changes: change `connect_inner` to initialize `result_set_max_rows: 0` (Exasol's own documented
      "no limit" default), and delete `unbounded_result_sets`. `capped_result_sets` already exists
      from task 1.0 and needs no change here. Use Serena's `safe_delete_symbol` for the removal so
      the six existing call sites surface as references rather than as later compile errors.
- [x] 3.2 Delete the six `.unbounded_result_sets()` calls: `e2e_join_test.rs` lines 118, 139, 193,
      1174, 1362 and `e2e_lakekeeper_test.rs:884`. Each becomes a plain `exa_conn()`. Re-derive the
      line numbers with Serena rather than trusting them — earlier tasks may have shifted them.
- [x] 3.3 Delete the comment at `crates/lakehouse-engine/tests/e2e_join_test.rs:113-117`. Its
      premise no longer holds. Do not reword it into a claim about the new default; the
      `capped_result_sets` doc comment owns that fact now. Check whether the two tests it links
      (`e2e_broadcast_join_pushdown_shape` and `e2e_broadcast_join_result_correct`) still pin one
      plan without it, and say so in the commit body.
- [x] 3.4 Re-point task 2.1's uncapped connection at the new default — drop its now-deleted
      `.unbounded_result_sets()` call, since a plain `exa_conn()` is uncapped. Task 1.1 already uses
      `capped_result_sets` and needs no change. Then verify the workspace compiles clean before Phase
      4 begins. Host `cargo test` compiles none of the E2E binaries, and no single feature flag
      compiles all of them, so run all four gates explicitly:
      `cargo clippy --all-targets --features exasol-e2e`,
      `cargo clippy --all-targets --features lakekeeper-e2e`,
      `cargo clippy --all-targets --features azure-e2e`,
      `cargo clippy --all-targets --features cloud-e2e`.
      A stale reference to the deleted `unbounded_result_sets` in `e2e_lakekeeper_test.rs` surfaces
      only under `--features lakekeeper-e2e`, and one in `e2e_join_test.rs` only under
      `--features exasol-e2e`. Neither gate catches the other's file: see § Rules on crate-root
      feature gating.
- [x] 3.5 Land the `e2e-harness/lakekeeper-e2e-harness` spec delta together with task 3.2's deletion
      of the `e2e_lakekeeper_test.rs:884` call site. The delta is authored at
      `specs/_plans/fix-e2e-harness-undeclared-limit/e2e-harness/lakekeeper-e2e-harness/spec.md` and
      rewrites the Background bullet at `specs/e2e-harness/lakekeeper-e2e-harness/spec.md:61`, which
      records the hardcoded 10000-row default and the `unbounded_result_sets()` opt-out as the
      suite's present behavior and is load-bearing for the recorded scenario "A two-table broadcast
      join over a vended-credential warehouse returns correct rows" (`:119`). The delta also restates
      that scenario: its GIVEN gains "a harness connection that declares no row cap … requires no
      opt-out call", and its THEN gains the row-fetch-time broadcast clause. The scenario's test is
      `lakekeeper_vended_broadcast_join_result_correct` (`crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`),
      whose `.unbounded_result_sets()` call task 3.2 deletes — so run it under
      `make test-e2e-lakekeeper` and confirm the broadcast assertion still holds on a plain
      `exa_conn()`. Verify the delta still describes the code after 3.2 lands, and correct the delta if
      it diverged. Without this delta `/speq:record` leaves the permanent library asserting behavior
      that no longer exists and naming a symbol that no longer compiles.

## Phase 4: Fix every unmasked failure (Group D — depends on Group C)

**This phase is open-ended by construction.** The number of failing assertions is unknown until
Phase 3 lands; task 1.4 gives a prediction, not a bound. Every newly-red test is closed here — none
is left red, and none is left for a later plan to rediscover.

**One rule closes every failure, applied uniformly to every test in every binary.** For each task
below, the loop is the same:

1. Bring the stack up. Run the binary under the new default.
2. For each newly-failing test, file a GitHub issue (`gh issue create`) describing what the flip
   broke, referencing #312.
3. Add an explicit `capped_result_sets(n)` call to that one test's connection setup, with a comment
   citing the issue number, so the test passes again and #312 can land. Use `n = 10000` to reproduce
   the pre-flip behavior unless the test's own row counts require a different value, and say why if
   they do. Then move to the next failing test.
4. Report the per-binary outcome: tests run, tests newly failing, and the issue number and cap value
   for each one.

**No production-code fix is made in this phase.** The filed issue owns that work. There is no test
classification, no membership test, and no judgment call about which failures qualify — every test
the flip turns red takes steps 2 and 3, including `e2e_broadcast_join_pushdown_shape` and
`e2e_broadcast_join_result_correct` (`crates/lakehouse-engine/tests/e2e_join_test.rs`), which carry
no carve-out. Task 1.4's affected-assertion list predicts which shapes the measurement expects to be
affected; it gates and classifies nothing here.

**The broadcast-join pair is expected to need no change at all.** Both tests already call
`exa_conn().unbounded_result_sets()` (`e2e_join_test.rs:118`, `:139`), so both already send
`resultSetMaxRows: 0`. Task 3.1 makes a plain `exa_conn()` send that same `0`, and task 3.2 deletes
the two now-redundant calls as dead code, so the flip changes nothing about the request either test
issues. Confirm that against the run rather than assuming it; if either does turn red, it takes
steps 2 and 3 like any other test.

- [x] 4.1 `e2e_scan_test` (54 `exa_conn()` sites). `[expert]`
- [x] 4.2 `e2e_capability_test` (69 `exa_conn()` sites — the largest blast radius). `[expert]`
- [x] 4.3 `e2e_join_test` (19 sites; the binary whose comment documented the injection, and the one
      most entangled with #307's limit-disqualifies-broadcast behavior). `[expert]`
- [x] 4.4 `e2e_count_distinct_test` (12 sites).
- [x] 4.5 `e2e_positional_deletes_test` (9 sites).
- [x] 4.6 `e2e_refresh_test` (7 sites).
- [x] 4.7 `e2e_int96_timestamp_test` (2 sites).
- [x] 4.8 `e2e_non_ascii_identifier_test` (1 site).
- [x] 4.9 `e2e_lakekeeper_test` (8 `exa_conn()` sites plus one direct `connect_redacting` at
      `:571`). Needs the Lakekeeper overlay stack; run `make test-e2e-lakekeeper`.
- [x] 4.10 `e2e_azure_test` (3 `exa_conn()` sites plus one direct `connect_redacting` at `:649`).
      Real Azure Blob Storage credentials are required to execute this binary. If they are
      unavailable, compile it under `--features azure-e2e` — the binary's own gate is
      `#![cfg(feature = "azure-e2e")]` (`crates/lakehouse-engine/tests/e2e_azure_test.rs:37`), so
      `--features exasol-e2e` would compile an empty binary and exit 0 without type-checking a line
      of it. Then review every assertion against the Phase 1 evidence by inspection, and state plainly
      in the verification report that CI is the only execution proof.
- [x] 4.11 `cloud_e2e_test` (5 direct `ExaConn::connect_redacting` sites, `cloud-e2e` feature). Runs
      against SaaS staging and is in no CI job. Compile it under `--features cloud-e2e`, review its
      assertions by inspection, and state in the verification report that it was not executed.
- [x] 4.12 Re-run the two locally executable suites end to end after every per-binary fix has
      landed: `make test-e2e` and `make test-e2e-lakekeeper`. A fix in one binary can change the
      shared VS or fixture state another binary depends on, so a green per-binary run is not
      sufficient. Check exit codes.

## Phase 5: Pin the new behavior (Group E — depends on Group C)

- [x] 5.1 Add `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs` and register it in the
      `make test-e2e` target's binary list in the `Makefile`. Seed the `typed_distinct_probe`
      fixture (12 rows, cheap) through the shared `common::e2e_harness` provisioning helpers — do
      not re-declare provisioning logic, per
      `e2e-harness/e2e-harness/Every E2E binary provisions the scan path from one shared harness definition`.
- [x] 5.2 Write `undeclared_cap_pushes_no_limit`: issue a bare projection carrying no SQL `LIMIT`
      through a connection that declares no cap, assert via `explain_virtual_sql` that the generated
      scan spec carries no `limit`, and assert the statement returns every seeded row rather than a
      prefix. Must fail, not skip, when the stack is unavailable.
- [x] 5.3 Write `declared_cap_truncates_delivered_result_set_not_pushdown_request` (**RENAMED and
      REASSERTED** — the task originally read "Write `declared_cap_reaches_adapter_as_pushdown_limit`
      … assert the capped scan spec carries `limit` `n`", which task 1.2's measurement disproves:
      no statement shape converts a declared cap into a pushdown `limit` on Exasol 2025.2.1, so the
      original assertion asserted a falsehood. Verified again here before rewriting — the literal
      assertion was run against the live stack and failed, with the capped connection's scan spec
      carrying no `limit`. See `injection-surface.md` § Consequences item 4 and `decision-log.md`;
      the same correction was already applied to the `exasol_ws.rs` doc comments (task 1.5) and the
      lakekeeper spec delta (task 3.5)). Run the identical statement through a
      `capped_result_sets(n)` connection and a no-cap connection, assert the two pushed plans are
      identical and neither carries a `limit`, and assert the capped connection delivers exactly `n`
      rows against the fixture's full count — the cap's measured effect is result-set truncation, not
      pushdown. The `e2e-harness/e2e-harness` spec delta's Background bullet and this scenario were
      corrected to match. This test is what keeps the Phase 1 measurement from decaying into a
      comment. `[expert]`
- [x] 5.4 Run the full checklist from `plan.md`: `make cross-musl-udf-build`, `cargo test`,
      `cargo clippy --all-targets`, `cargo fmt`, `make test-e2e`, `make test-e2e-lakekeeper`. Report
      exit codes, not output tails.

## Phase 6: Review Fixes

> Numbered `6.x` rather than `4.x`: `## Phase 4` and the indices `4.1`-`4.12` are already taken by
> the unmasked-failure phase above, so a second `Phase 4` group would collide on both.

- [x] 6.1 Restructure `ExaConn::fetch_result_columns_with_num_bytes` in
      `crates/lakehouse-engine/tests/common/exasol_ws.rs` so no path can return a prefix. Read
      `let advertised = result_set["numRows"].as_u64();` before branching. When
      `result_set["resultSetHandle"]` is present, stop returning early on inline `data`: seed `cols`
      and `rows_read` from the inline array when one is present and then enter the existing loop, so
      a partial inline chunk alongside a handle is the first chunk rather than the whole answer. When
      no handle is present, keep returning the inline columns but first assert every column's length
      equals `advertised`, in a message naming both the advertised and the accumulated count —
      guarded on `advertised.is_some()` so DDL and other responses that omit `numRows` keep working
      across the twelve existing `fetch_result_columns` call sites. A failing-test-first cycle is not
      available for the inline-plus-handle shape (this server never produces it), so the added
      assertion is the guard: verify no call site regressed by running `make test-e2e` and
      `make test-e2e-lakekeeper` to a zero exit code, reading exit codes rather than output tails.
      Leave the method's doc comment's completeness claim as written. `[expert]`
- [x] 6.2 In `crates/lakehouse-engine/tests/common/exasol_ws.rs`, extend `capped_result_sets`'s doc
      comment with one sentence stating when to declare a cap: for a test whose assertion is about
      result-set truncation at row-delivery time, and for `e2e_capture_pushdown`'s
      `CAPTURE_RESULT_SET_MAX_ROWS` capped-versus-uncapped comparison — and that a test asserting
      pushdown or plan shape needs no cap. Keep the existing measured-behavior sentences and the
      `docs/debugging-pushdown.md` cross-reference.
- [x] 6.3 In `crates/lakehouse-engine/tests/common/exasol_ws.rs`, re-verify via Serena whether
      `fetch_result_columns_with_num_bytes`'s zero-rows panic path still closes the handle
      (`self.close_result_set(handle);`) while the two sibling panic paths (missing
      `responseData.numRows`, missing `responseData.data`) do not. If the asymmetry still exists,
      delete that one `close_result_set` call so all three panic paths behave alike, leaving the
      successful-completion close after the loop in place. If task 6.1 already resolved it, make no
      change and report that.
- [x] 6.4 In `crates/lakehouse-engine/tests/e2e_capture_pushdown.rs`, update the module doc's "Driven
      entirely by the `CAPTURE_SQL` env var" sentence to name both env vars — required `CAPTURE_SQL`,
      optional `CAPTURE_RESULT_SET_MAX_ROWS` (unset means no declared row cap) — keeping the existing
      reuse-without-editing rationale.
- [~] 6.5 In `crates/lakehouse-engine/tests/e2e_capture_pushdown.rs`, change the
      `match std::env::var("CAPTURE_RESULT_SET_MAX_ROWS")` so only `Err(std::env::VarError::NotPresent)`
      means "no cap"; any other `Err` panics naming the variable and error. Change the parse-failure
      panic message to include the received value, e.g.
      `panic!("CAPTURE_RESULT_SET_MAX_ROWS must be a u32, got {n:?}")`.
- [x] 6.6 In `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs`, delete the
      `capped_plan_unescaped` binding in
      `declared_cap_truncates_delivered_result_set_not_pushdown_request` and assert
      `!capped_plan.contains("\"limit\"")` directly on the raw plan, matching
      `undeclared_cap_pushes_no_limit`'s convention. Keep the assertion's failure message.
- [x] 6.7 In `docs/debugging-pushdown.md`, reword the `broadcast-eligible inner equi-join` table row
      to the hedged form already used in the lakekeeper spec delta ("was never shown to reach the
      adapter... or to suppress broadcast eligibility", broadcast block still emitted, no `LHS_T0`
      wrapper, at caps of 5 and 10000). Add one sentence after the controls paragraph stating the
      observation boundary: the capture observes the adapter exchange through `EXPLAIN VIRTUAL`,
      where `resultSetMaxRows` applies to the wrapper statement, so result-value controls bound the
      row-scan/aggregate shapes but not the join shape via the echo alone.
