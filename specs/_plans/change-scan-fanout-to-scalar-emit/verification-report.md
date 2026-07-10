# Verification Report: change-scan-fanout-to-scalar-emit

## Verdict: PASS (all gates green)

Implementation of Groups A–F complete; code review passed (correctness verdict PASS, 3 cleanups
applied, 1 deferred); one E2E-only blocker found and fixed; all automated gates green.

| Gate | Command | Result |
|------|---------|--------|
| Build (`.so`) | `make cross-musl-udf-build` | ✅ exit 0 (rust:1.94-bookworm) |
| Host tests | `cargo test` | ✅ 583 passed, 2 ignored, 0 failed (workspace) |
| Lint | `cargo clippy --all-targets` | ✅ no issues |
| Format | `cargo fmt --check` | ✅ clean |
| E2E | `make test-e2e` | ✅ 80 passed, 0 failed, exit 0 (8+6+10+11+45) |

## E2E blocker found and fixed (recorded in decision-log)

First E2E run failed 7/8 capability tests with `The script has a static return argument definition.
Dynamic return arguments are not supported in this case` (SQL state 04000/42000). Root cause,
verified by a minimal live repro and cross-checked against the deployed staging spike (`DBX` VS):
`build_fan_out_inner` rendered the distributor call WITH a query-side `EMITS (files VARCHAR(2000000))`,
but `LAKEHOUSE_DISTRIBUTE_FILES` is a LUA SET script with a STATIC `EMITS` definition — Exasol rejects
a query-side EMITS on a statically-defined script. The scalar scan (dynamic/null output schema) was
never the problem; staging drives the identical scalar-over-distributor shape successfully.

Fix: drop the query-side `EMITS(...)` from the distributor call (the scan keeps its own). Affected
unit tests updated; a negative assertion pins the distributor call carries no query-side EMITS. The
plan's design decisions [1]/[2] stand — this was a one-line call-site bug, not a design flaw.

## Scenario coverage

All Verification-table scenarios are covered by passing tests: nested-distributor over scalar scan,
single-shard short-circuit, projection/limit/aggregate/grouped/top-n/broadcast/N-scan-join shapes,
the LUA distributor as a non-`.so` script, and the batched scalar scan loop. E2E parity tests
(80 total across scan/capability/count-distinct/join/positional-delete suites) all return correct
result multisets.

## Manual testing

`EXPLAIN VIRTUAL` on the live local stack confirms the pushdown shape (nested
`LAKEHOUSE_DISTRIBUTE_FILES … GROUP BY shard_key` inside an outer ungrouped `LAKEHOUSE_SCAN(...)`
scalar select, no query-side EMITS on the distributor). Verified byte-for-byte against the working
staging `DBX` pushdown shape.
