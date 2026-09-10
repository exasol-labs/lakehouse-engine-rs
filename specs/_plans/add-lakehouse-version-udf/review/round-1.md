# Plan Review Findings: add-lakehouse-version-udf (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 0 (Blockers: 0, Advisory: 0)
- Intent Fidelity blockers: 0

## Premortem

Six months from now this plan failed catastrophically:

1. The `extract_version_value` extractor mishandles exapump output (a row-count footer like
   `1 row` starts with a digit and looks like a version), so the smoke test compares a footer
   against the expected version, always fails, and blocks every install. **Routed to:**
   Requirement Quality. Checked: the plan's Requirements section specifies the extractor MUST
   yield the value whether or not a header line appears and MUST yield an empty result (not a
   footer) when no value line is present. Task 3.3 adds unit tests covering a digit-leading
   version value and an output with no value line. The aliased column header
   (`AS LAKEHOUSE_ENGINE_VERSION`) ensures the header is recognizable. Satisfied.

2. The `install-script-e2e` CI job calls `LAKEHOUSE_VERSION()` against a released `.so` that
   does not export the symbol, fails the release gate, and deadlocks the first release. **Routed
   to:** Feasibility. Checked: the plan gates both the DDL and the smoke-test call on a
   hardcoded floor version (`VERSION_UDF_MIN_ENGINE_VERSION`). Below the floor the installer
   creates no version script and runs the existing fingerprint smoke test. The release bootstrap
   deadlock is explicitly addressed in plan.md Design > Release bootstrap. Satisfied.

3. The `engine_version_at_least` comparator uses a different algorithm from CI's `is_greater`
   and disagrees on an edge case, so the floor gate fires incorrectly. **Routed to:**
   Feasibility. Checked: the plan's Requirements section constrains the comparator to plain
   `X.Y.Z` only (the same domain CI's `is_greater` documents). Task 3.3 adds unit tests for
   `engine_version_at_least`. Both are simple dotted-numeric comparisons over a small domain.
   Satisfied.

## Intent Fidelity

No objection -- axis checked. The user asked for a `LAKEHOUSE_VERSION()` RETURNS-shape RUST
SCALAR SCRIPT UDF reporting `env!("CARGO_PKG_VERSION")`, plus an installer change replacing the
error-interpretation smoke test with an exact-match version check. The plan delivers both. The
interview decided: new sibling feature (`packaging/version-udf`), exact-match check. The plan
operationalizes both decisions without substitution, addition, or reduction. The floor gate
(`VERSION_UDF_MIN_ENGINE_VERSION`) is not scope creep: it is required by `--lakehouse-version`
(which pins older releases that predate the entry point) and by the `install-script-e2e` release
bootstrap, both documented in plan.md Design > Release bootstrap.

## Feasibility

No objection -- axis checked. Key claims verified against the codebase:

- `exasol-udf-macros` 0.24.0 selects RETURNS from a `Result<Option<T>, UdfError>` signature
  (planning notes confirm verification at the macro source).
- `extract_query_value` (install.sh lines 1175-1193) skips `[0-9]*` lines, confirmed to
  discard a version value such as `0.45.0`.
- `smoke_test_sql` (line 1260) builds the placeholder scan call; `classify_fingerprint_response`
  (line 1393) classifies the response. Both are reachable for the below-floor fallback path.
- `create_engine_scripts` (line 1326) is called from `install_engine` (line 1362) and
  `deploy_personal_local` (line 1388). Both callers execute after `RESOLVED_ENGINE_VERSION` is
  set (line 865). The floor-gated version DDL (task 2.2) can read the resolved version from
  the global.
- The `release` job in `ci.yml` (lines 987-1013) derives `tag=v$VER` from
  `crates/lakehouse-engine/Cargo.toml`, and its `needs` includes `install-script-e2e`
  (confirmed in planning notes). The release bootstrap analysis is sound.
- `print_next_step_template` (line 1425) grants CONNECTION access for `LAKEHOUSE_ADAPTER` and
  `LAKEHOUSE_SCAN` only (lines 1449-1450). The version script is not mentioned, satisfying
  the no-grant requirement by default. Task 2.5 is a verification step, not a code change.
- No `lib_tests.rs` exists yet in `crates/lakehouse-engine/src/`. The sibling test file is new.

## Requirement Quality

No objection -- axis checked. The spec delta has 8 scenarios covering: version reporting (2),
DDL creation (1), smoke-test pass (1), version mismatch (1), fingerprint mismatch (1), other
error (1), and below-floor fallback (1). Each scenario has concrete GIVEN/WHEN/THEN steps with
named values (`RESOLVED_ENGINE_VERSION`, floor reference, `X.Y.Z` format constraint). The plan's
Requirements section adds five implementation-level constraints (version extraction, version
comparison domain, floor constant identity, test layout, E2E fail-not-skip contract). No
requirement conflicts with any recorded spec: `packaging/single-so-two-entry-points` describes
the adapter-and-scan pairing by dispatch shape and does not claim the `.so` exports only two
symbols (confirmed via `speq feature get`). No ambiguous or untestable requirement found. Spec
delta validated against the speq search results: no existing feature covers version reporting or
installer smoke-test verification.

## Task Breakdown

No objection -- axis checked. All 17 tasks (4 groups: Rust entry point, installer script,
installer tests, documentation) trace to the single `packaging/version-udf` spec delta. Every
scenario in the delta maps to at least one test in the Verification > Scenario Coverage table.
The plan uses one parallelization group, justified: every task implements the same spec delta,
and the floor constant in `install.sh` is meaningless without the entry point in `lib.rs`.
Task granularity is appropriate: each task targets one function or one file, and each is
independently verifiable by the test named in the Scenario Coverage table. No traceability gap:
no task implements something outside the delta, and no delta scenario lacks an implementing task.

## Design Depth

No objection -- axis checked. The design adds one crate constant (`ENGINE_VERSION`), one entry
point function, and four shell helpers (`engine_version_at_least`, `ddl_version`,
`version_smoke_test_sql`, `extract_version_value`). No new module boundary or abstraction is
introduced. The version value has a single owner (the constant, fed by `env!("CARGO_PKG_VERSION")`
at compile time). The chain of trust (Cargo.toml -> constant -> UDF return value; Cargo.toml ->
CI tag -> release -> installer `RESOLVED_ENGINE_VERSION`) is documented in the architecture
diagram and has no back-door leakage: both sides derive from the same manifest field, and the
installer's `normalize_version` strips the leading `v`. The `extract_version_value` helper is
justified by a concrete code incompatibility (`extract_query_value` discards digit-leading
lines). The floor gate serves double duty (release bootstrap and older-release support) without
adding code that a sequenced-release alternative would avoid. No tactical shortcut present.

## Prose Quality

No objection -- axis checked. Scanned plan.md, decision-log.md, and the spec delta for
writing-guardrails violations. Active voice throughout. No em dashes, no semicolons, no
contractions. No hedging or filler words. BLUF structure in plan.md Design > Context (problem
statement first, goals second). Normative keywords (`MUST`, `SHALL`, `MUST NOT`) used per
RFC 2119 in the spec delta scenarios. Technical identifiers (`extract_query_value`,
`RESOLVED_ENGINE_VERSION`, `CARGO_PKG_VERSION`) preserved verbatim. Sentence lengths within
the 25-word descriptive cap. No process narration: the plan states decisions as facts, not as
steps the planner took.
