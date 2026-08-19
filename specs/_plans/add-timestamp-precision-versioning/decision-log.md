# Decision Log: add-timestamp-precision-versioning

## Interview

**Q:** The TIMESTAMP(6) upgrade touches two independent hardcoded-TIMESTAMP call paths:
`iceberg_primitive_to_exasol` (Iceberg catalog metadata) and `compatible_exasol_type` (generic
Arrow→Exasol, also used for Delta table schema declarations). Should the version-gated TIMESTAMP(6)
precision apply to Delta-sourced timestamp columns too, or only to Iceberg as the issue title implies?
**A:** Iceberg + Delta. Fix both call paths so any timestamp column — Iceberg or Delta — gets
microsecond precision on 2025.x+. Do not leave a known-identical bug unfixed in the Delta path. This
widens issue #359's literal Iceberg-only wording — treat it as an intentional, user-approved scope
widening, not scope creep to flag.

**Q:** `database_version()` returns a raw String (e.g. `"8.29.13"`, `"2025.2.1"`) with no existing
parsing convention in this codebase, and defaults to `""` if the SDK/engine doesn't populate it. What
should the gate do when the version string is empty or unparseable?
**A:** Default to TIMESTAMP(6) when the version string is empty/unparseable (assume modern engine).
This is the user's explicit, deliberate choice — opposite of the conservative "default to bare
TIMESTAMP" option that was offered as the recommended default. Implement and spec this exactly as
chosen; do not silently substitute the conservative default.

**Q:** Which CI jobs in `.github/workflows/ci.yml` should get the 8.29.x + 2025.x matrix? There are 4
E2E jobs today (core `e2e`, `e2e-lakekeeper`, `e2e-unity`, `e2e-azure`), none matrixed currently.
**A:** Core `e2e` job only. Leave `e2e-lakekeeper`, `e2e-unity`, `e2e-azure` single-version (they test
catalog integrations orthogonal to Exasol version).

## Design Decisions

### [1] The Delta declaration path is `unity_type_name_to_exasol`, not `compatible_exasol_type`

- **Decision:** Honor the interview's Iceberg+Delta widening by version-gating
  `iceberg_primitive_to_exasol` (Iceberg) and `unity_type_name_to_exasol` (Delta). Leave
  `arrow_to_exasol_type`/`compatible_exasol_type` — the functions issue #359's scope text and the
  interview question both name — untouched, and record that exclusion as a scenario.
- **Alternatives:** Thread the gate through `compatible_exasol_type` as the interview question assumed;
  thread it through both and accept a redundant parameter.
- **Rationale:** A census of the whole workspace found `arrow_to_exasol_type` has zero production call
  sites — a fact `datafusion-scan/type-mapping-module-structure` already records — and
  `compatible_exasol_type`'s only production consumer is the `needs_json_fallback` boolean, whose answer
  for `Timestamp(_, _)` is `false` at every precision. A Delta table reaches `createVirtualSchema` only
  through the Unity Catalog kind, where `unity_type_name_to_exasol` maps `TIMESTAMP`/`TIMESTAMP_NTZ`.
  So the user's intent — Delta timestamps get microsecond precision — is fully served, by the function
  that actually declares them. The exclusion is recorded rather than silent because the issue names
  `arrow_to_exasol_type` and an unexplained omission is indistinguishable from an oversight.
- **Promotes to ADR:** yes

### [2] `TimestampPrecision` owns the version rule and both declaration strings

- **Decision:** A two-variant `Copy` enum in `crates/lakehouse-engine/src/types/mapping.rs`,
  `TimestampPrecision::{Millisecond, Microsecond}`, owning `from_database_version(&str)` and both
  declaration strings. Both producers read it; neither keeps a `"TIMESTAMP"` literal.
- **Alternatives:** A `bool supports_parameterized_timestamp` parameter; a free function returning
  `&'static str`; a broader `EngineFeatures` struct that could carry future version-gated decisions.
- **Rationale:** Two producers already stated the same declaration independently — the exact shape that
  let the catalog-decimal guard reach four copies before issue #329 consolidated it. A `bool` threaded
  through five signatures inverts silently at any call site; the variant names carry the meaning where
  they are read. A broader feature struct generalizes for a second use case that does not exist, which
  the design guardrails treat as a decision the module declined to make. The module is the right home
  because it already owns Exasol's own type domain (`exasol_representable_catalog_decimal`).
- **Promotes to ADR:** yes

### [3] The version STRING crosses into `types/mapping.rs`; the `UdfContext` does not

- **Decision:** `handle_create_virtual_schema` reads `ctx.database_version()` exactly once, inline, and
  threads the resolved `TimestampPrecision` as a plain parameter through `build_listing_virtual_tables`
  → `column_source_type_to_exasol` → both producers. `from_database_version` takes `&str`.
- **Alternatives:** Thread `&dyn UdfContext` into `types/mapping.rs`; add a
  `timestamp_precision_from_context(ctx)` wrapper mirroring `cluster_nodes_from_context`; read the
  version again in the scan UDF entry point.
- **Rationale:** Keeping `types/mapping.rs` free of `UdfContext` keeps it performing no I/O and reading
  no ambient state, so the dependency still points from the adapter inward. `cluster_nodes_from_context`
  is the recorded precedent for the shape, but not for the wrapper: it earns its name by normalizing
  `0` → `1`, whereas a wrapper here would forward one call and add nothing — the shallow-function smell
  the design guardrails name. A second read in the scan UDF would give one decision two owners; the
  scan's `EMITS` types already arrive in the pushdown request's `dataType` JSON.
- **Promotes to ADR:** yes

### [4] Parse the leading dot-separated component and gate on `>= 2025`

- **Decision:** Take the substring before the first `.`, parse it as an integer, and treat `>= 2025` as
  microsecond-capable.
- **Alternatives:** Full semver comparison; matching an `8.` prefix explicitly; a regex.
- **Rationale:** Exasol moved from `8.x` to calendar versioning, so one integer threshold separates the
  two lines with no comparison machinery. The observed strings are exactly the Docker image tags already
  in use (`8.29.13`, `2025.2.1`), and task 1 captures what a live engine actually reports before the
  parse is relied on. A future `9.x` is the only ambiguity, and Exasol's calendar versioning makes it
  unlikely; if it appeared it would take the unparseable-input default arm's answer anyway.
- **Promotes to ADR:** no

### [5] Empty and unparseable versions both take the microsecond default

- **Decision:** `""` and any string whose leading component does not parse both yield
  `TIMESTAMP(6)` — one arm, not two.
- **Alternatives:** Default to bare `TIMESTAMP` (the conservative option, which was recommended and
  which the user explicitly rejected); distinguish empty from unparseable.
- **Rationale:** The user's explicit choice, recorded here so a later reader does not "fix" it back. The
  trade-off is real and asymmetric: the conservative default silently truncates every timestamp value on
  any engine it misjudges, while this default makes an engine that cannot accept `TIMESTAMP(6)` fail
  visibly at `createVirtualSchema`. Two arms for two inputs that carry no information would drift.
- **Promotes to ADR:** yes

### [6] `exasol_type_to_json` needs a `TIMESTAMP(p)` arm, or the column silently becomes VARCHAR

- **Decision:** Add a `TIMESTAMP(p)` arm emitting `{"type":"timestamp","fractionalSecondsPrecision":p}`,
  matched after the exact `TIMESTAMP WITH LOCAL TIME ZONE` and bare `TIMESTAMP` arms.
- **Alternatives:** None viable — this was a gap found while reading the code, not a choice.
- **Rationale:** The function's timestamp branch matches the string `TIMESTAMP` exactly, so a
  `TIMESTAMP(6)` string would fall through every arm to the catch-all and declare the column
  `{"type":"varchar","size":2000000}` — a silently wrong type rather than a rejected request. The field
  name is `fractionalSecondsPrecision`, pinned by the existing ADR
  `timestamp-precision-field-is-fractional-seconds-precision`; Exasol uses `precision` only for
  `DECIMAL` and `INTERVAL`. Its inverse `exasol_type_from_json` already reads that field, so this
  completes a matched pair rather than inventing a convention.
- **Promotes to ADR:** no

### [7] `TIMESTAMP(6)` is the target, not `TIMESTAMP(9)`, and the ceiling is iceberg-rust

- **Decision:** Declare microsecond precision. Iceberg's `timestamp_ns`/`timestamptz_ns` still declare
  `TIMESTAMP(6)`.
- **Alternatives:** `TIMESTAMP(9)` for the nanosecond Iceberg variants.
- **Rationale:** The Iceberg spec's Primitive Types table defines `timestamp` as *"Timestamp,
  microsecond precision, without timezone"* and `timestamptz` as *"Timestamp, microsecond precision,
  with timezone"*, and Appendix A pins Parquet `TIMESTAMP_MICROS`, *"Stores microseconds from 1970-01-01
  00:00:00.000000."* — so `TIMESTAMP(3)` is a spec deviation this plan fixes rather than a trade-off to
  record. Exasol accepts `TIMESTAMP(p)` for `p` in 0-9 and the UDF output path already carries nine
  fractional digits, so the ceiling is entirely upstream: iceberg-rust's `TimestampNs` handling calls
  `timestamp_to_micros`, truncating before any value reaches DataFusion. Declaring 9 would advertise a
  precision the read path cannot deliver.
- **Promotes to ADR:** yes

### [8] Precision and zone-awareness stay two independent decisions

- **Decision:** Amend the recorded "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario with
  the precision qualifier only, and state explicitly that the zone-flattening trade-off is untouched.
- **Alternatives:** Fold both into one statement about the timestamp declaration.
- **Rationale:** The `timestamptz` → plain `TIMESTAMP` mapping is a separate, already-recorded Exasol
  target-type limitation (Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type,
  `sqlCode 22002`). Conflating it with precision would make a later reader believe this plan revisited
  the zone question, or that fixing the zone question would also move the precision.
- **Promotes to ADR:** no

### [9] The E2E precision expectation is an independent oracle read from the live session

- **Decision:** One shared helper in `crates/lakehouse-engine/tests/common/` reads the running engine's
  version from the live session and maps it to an expected precision with its OWN explicit table. It
  must not call `TimestampPrecision::from_database_version`, and must not read `EXASOL_IMAGE`.
- **Alternatives:** Call the production parser (one owner, no duplication); read `EXASOL_IMAGE`, which
  the CI matrix leg already sets.
- **Rationale:** A test that computes its expectation by calling the rule under test cannot fail when
  that rule is wrong — the duplication here is a deliberate oracle, and the production rule's own inputs
  are covered by a separate unit matrix over concrete version strings. `EXASOL_IMAGE` is rejected
  because `cargo test --features exasol-e2e` runs against whatever stack is up: an absent or stale
  variable silently selects the wrong assertion arm, the same class of failure a stray `bench/.env`
  produces.
- **Promotes to ADR:** yes

### [10] One CI matrix leg keeps the status-check name `E2E`

- **Decision:** Matrix entries carry the image, the status-check name, and the failure-log artifact
  name. The `2025.x` leg's check name stays exactly `E2E`; the `8.29.x` leg gets a new name that an
  admin adds to `main`'s ruleset.
- **Alternatives:** Rename both legs and update the ruleset; add an aggregate gate job named `E2E` that
  `needs: [e2e]`; duplicate the job body into a second sibling job.
- **Rationale:** `E2E` is a required check on `main`'s ruleset. Renaming both legs leaves the ruleset
  waiting on a check that never reports, blocking every PR until an admin intervenes. An aggregate gate
  job is worse than it looks: without `if: always()` it is *skipped* when its dependency fails, and
  GitHub counts a skipped required check as satisfied — silently unblocking a red E2E; with
  `if: always()` it breaks the existing cascade-skip behavior when `build-so` fails. Duplicating the job
  body copies ~60 lines with no YAML-anchor support to keep them in step. The per-leg artifact name is
  not optional: `upload-artifact@v7` rejects a duplicate name in one run, which the workflow already
  records for `e2e-azure`.
- **Promotes to ADR:** yes

### [11] Live verification precedes the declaration change

- **Decision:** Task 1 captures, against both Docker images, that Exasol accepts
  `fractionalSecondsPrecision` in a `createVirtualSchema` column `dataType`, declares the column
  `TIMESTAMP(6)`, echoes the field back on the pushdown request, preserves microseconds across
  `emit_batch`, and which live-session source carries the engine version.
- **Alternatives:** Trust issue #359's findings, the Exasol VS data-type documentation, and PR #226's
  existing `TIMESTAMP(p)` CAST coverage.
- **Rationale:** CLAUDE.md § Verification discipline forbids assuming a SQL capability from
  documentation, memory, or a capability registry. PR #226 proved `TIMESTAMP(p)` works as a *CAST target
  and EMITS type*, which is a different exchange from the `createVirtualSchema` response's `dataType`
  object — nothing captured to date shows Exasol accepting the field on that path. The version source
  matters too: `ctx.database_version()` has zero call sites in this repo, so what a live engine reports
  through it is unobserved.
- **Promotes to ADR:** no

### [12] Existing virtual schemas keep the old declaration until refreshed

- **Decision:** Treat the persisted-declaration lag as an operator-facing consequence to document, not a
  migration to automate.
- **Alternatives:** Force a refresh; version the adapter notes so a stale declaration is detected.
- **Rationale:** Exasol persists the column types a `createVirtualSchema` response declared, and the
  adapter is stateless by mission constraint — it has no hook to rewrite an existing schema and must not
  gain one. `ALTER VIRTUAL SCHEMA <name> REFRESH` is the existing, documented mechanism, and the
  precision is re-derived from the handshake on every request rather than recorded in `adapterNotes`, so
  a refresh after an engine upgrade picks up the new precision with no further action.
- **Promotes to ADR:** no

### [13] The 8.x millisecond truncation is a named version limitation, not a tracked exception

- **Decision:** Record in the spec that bare `TIMESTAMP` on 8.x truncates sub-millisecond Iceberg data,
  as a version limitation. No GitHub issue is filed for it.
- **Alternatives:** File a tracked-exception issue cited inline in the spec, per CLAUDE.md's
  Iceberg-compliance rule for known deviations.
- **Rationale:** The rule requires a deviation to be fixed in the same plan or recorded as an accurately
  scoped tracked exception. The deviation *is* fixed in this plan on every engine that can express the
  fix; what remains on 8.x is an Exasol target-type limitation with no available remedy — the same class
  as the struct/list/map JSON-`VARCHAR` and the `timestamptz` zone-flattening trade-offs, both of which
  the library records as named trade-offs rather than tracked exceptions. A tracked issue would imply
  future work that cannot exist while 8.x lacks parameterized `TIMESTAMP`.
- **Promotes to ADR:** no

### [14] `CLAUDE.md` gets a one-line pointer, not a second copy of the version rule

- **Decision:** Task 12 rewrites the single `Timestamp(_, _)` row of `CLAUDE.md`'s Data types table to
  name both declarations and point at the `datafusion-scan/type-mapping` spec for the exact rule.
  `CLAUDE.md` keeps the invariant facts it already carries (no arrays, lists, structs, or maps) and does
  not restate the version parse, the `>= 2025` threshold, or the empty/unparseable fallback.
- **Alternatives:** Restate the full version-gated rule in `CLAUDE.md`; leave the row untouched and let
  the spec alone carry the change.
- **Rationale:** The stale `| Timestamp(_, _) | TIMESTAMP |` row is the same failure issue #359 reports —
  a statement nobody re-checked after the code moved. Restating the rule in a second document recreates
  that failure mode, because only one of the two copies is test-backed:
  `TimestampPrecision::from_database_version`'s unit matrix pins the spec's version, and nothing pins
  `CLAUDE.md`'s. So the rule keeps exactly one owner, and `CLAUDE.md` carries only what an agent needs to
  know before reading further — that the declaration is version-gated, and where the rule lives. Leaving
  the row untouched is not an option: an auto-loaded project instruction that contradicts the shipped
  code misleads every future session.
- **Promotes to ADR:** yes

## Task 1 Live Captures

Captured 2026-08-19 against the project Docker Compose stack (`docker-compose.yml`: MinIO +
`apache/iceberg-rest-fixture:1.10.1` + Exasol), SLC/SDK `exasol-udf-sdk` 0.22.1, before any
declaration-code change. No `bench/.env` existed, so nothing could redirect the run at a remote
target. Probe apparatus (all removed afterwards, working tree left clean):

- A throwaway `tests/e2e_tsprecision_probe.rs` seeding its own `e2e_tsprobe` Iceberg namespace with
  `ts_precision_probe(id long, ts timestamp, tstz timestamptz)` holding
  `2024-01-01 00:00:00.000001 / .000002 / .123456 / .123457` as `TimestampMicrosecondArray` values.
- Two throwaway **LUA ADAPTER SCRIPT**s returning a canned `createVirtualSchema` response — the only
  way to put `fractionalSecondsPrecision` on that exchange before the Rust adapter emits it — and
  re-raising the pushdown request verbatim as a Lua `error(...)` so the echoed JSON is observable.
- A temporary third `.so` entry point `LAKEHOUSE_VERSION_PROBE` emitting `ctx.database_version()`,
  `ctx.database_name()`, `ctx.script_name()`. `database_version()` has no other observable surface in
  this repo.

### [C1] Exasol accepts `fractionalSecondsPrecision` in a `createVirtualSchema` column `dataType`

**2025.2.1 — accepted and honored.** `CREATE VIRTUAL SCHEMA` over the Lua adapter returned `ok` and
`SYS.EXA_ALL_COLUMNS` reported:

```
"TS6"  (declared {"type":"timestamp","fractionalSecondsPrecision":6}) -> TIMESTAMP(6)
"TS9"  (declared {"type":"timestamp","fractionalSecondsPrecision":9}) -> TIMESTAMP(9)
"TSD"  (declared {"type":"timestamp"})                                -> TIMESTAMP(3)
```

**8.29.13 — accepted and SILENTLY DOWNGRADED.** Identical DDL returned `ok`; both `TS6` and `TS9`
reported `TIMESTAMP(3)`. 8.x neither rejects the field nor honors it.

**Two consequences the plan text did not anticipate:**

1. `SYS.EXA_ALL_COLUMNS` never reports a bare `TIMESTAMP`. A column declared `{"type":"timestamp"}`
   reports `TIMESTAMP(3)` on **both** engines. Task 5's oracle must therefore expose
   `TIMESTAMP(3)` — not the string `TIMESTAMP` — as the millisecond arm's expected `COLUMN_TYPE`,
   and tasks 7/9 must assert against that.
2. The plan's stated risk that an engine "rejects `TIMESTAMP(6)` and fails visibly at
   `createVirtualSchema`" (decision [5]) does **not** materialize on 8.29.13: it accepts and clamps.
   The unparseable-version default therefore fails silently, not loudly, on an 8.x-like engine. The
   user's recorded choice stands — this only corrects the rationale's claimed failure mode.

### [C2] The pushdown request echoes `fractionalSecondsPrecision` — on 2025.x only

**2025.2.1** — the field is echoed in **both** `involvedTables[].columns[].dataType` and
`pushdownRequest.selectListDataTypes`:

```json
{ "dataType" : { "fractionalSecondsPrecision" : 6, "type" : "TIMESTAMP" }, "name" : "TS6" }
{ "dataType" : { "fractionalSecondsPrecision" : 3, "type" : "TIMESTAMP" }, "name" : "TSD" }
```

**8.29.13** — the key is **absent entirely**, even for the column declared with precision 6:

```json
{ "dataType" : { "type" : "TIMESTAMP" }, "name" : "TS6" }
```

This is the mechanism that makes 8.x byte-identical for free: `exasol_type_from_json` reads no
`fractionalSecondsPrecision`, returns bare `"TIMESTAMP"`, and the generated `emit_exa_types` and
`EMITS` clause are unchanged — verified end to end on the real Rust adapter, whose 8.29.13 pushdown
SQL carried `"emit_exa_types":["TIMESTAMP"]` and `EMITS ("TS" TIMESTAMP)` while the 2025.2.1 run of
the identical query carried `["TIMESTAMP(3)"]` and `EMITS ("TS" TIMESTAMP(3))`.

### [C3] `TIMESTAMP(6)` is accepted as the scan script's `EMITS` output type on both engines

The real `LHVS.LAKEHOUSE_SCAN` RUST SCALAR script was invoked directly with the pushdown SQL
`EXPLAIN VIRTUAL` produced, rewritten only in the `EMITS` clause and the spec's `emit_exa_types`.
Both engines returned `status: "ok"` — no DDL, parse, or `emit_batch` type error.

Exasol 8.29.13 accepts `TIMESTAMP(p)` **only** for `p` in `{3, 6}`; every other precision is
rejected as `0A000 Feature not supported: TIMESTAMP(p) - timestamp with custom precision` (probed
`p = 0..9` as a `CAST` target). `TIMESTAMP(6)` is accepted syntactically and clamped to milliseconds
semantically. Confirms decision [7]'s `TIMESTAMP(6)` target is the only parameterized precision that
is even expressible on both engine lines.

### [C4] Microsecond values survive the `emit_batch` round trip with all six digits

Same invocation, reading the seeded microsecond fixture through DataFusion → `emit_batch`:

| Engine | `EMITS`/`emit_exa_types` | Rendered values |
|---|---|---|
| 2025.2.1 | `TIMESTAMP(6)` | `.000001`, `.000002`, `.123456`, `.123457` — **4 distinct, all six digits** |
| 2025.2.1 | `TIMESTAMP(3)` (today's declaration) | `.000000`, `.000000`, `.123000`, `.123000` — `COUNT(DISTINCT) = 2` |
| 8.29.13 | `TIMESTAMP(6)` | `.000000`, `.000000`, `.123000`, `.123000` — clamped |

The bug and the fix are both reproduced: nothing about the emitted VALUE changes, only whether
Exasol keeps the digits.

**Correction for tasks 7 and 8 — the rendered digit COUNT is always six.** The WebSocket protocol
renders every `TIMESTAMP` value with six fractional digits regardless of the declared precision:
a `TIMESTAMP(3)` column renders `2024-01-01 00:00:00.123000`, not `...123`. An assertion phrased as
"the full rendered fractional digits" cannot discriminate the two arms by digit count — it must
compare the **value** (`.123456` versus `.123000`), and `COUNT(DISTINCT)` (4 versus 2) is the
sharper discriminator.

Native-Exasol renderings captured for task 8's oracle:

| SQL | 2025.2.1 | 8.29.13 |
|---|---|---|
| `SELECT TIMESTAMP '2024-01-01 00:00:00.123456'` | `...123456` | `...123000` |
| `CAST(TIMESTAMP '…123456' AS TIMESTAMP)` | `...123000` | `...123000` |
| `CAST(TIMESTAMP '…123456' AS TIMESTAMP(6))` | `...123456` | `...123000` |
| `UPPER(CAST(TIMESTAMP '…100' AS TIMESTAMP))` | `VARCHAR(26)` `...100000` | `VARCHAR(26)` `...100000` |
| `UPPER(CAST(TIMESTAMP '…100' AS TIMESTAMP(6)))` | `VARCHAR(26)` `...100000` | `VARCHAR(26)` `...100000` |

An Exasol `TIMESTAMP` literal is itself precision-gated on 2025.x, so the task-8 oracle's CAST target
does need to carry the declared type — but only for a fixture with sub-millisecond digits. The
existing `.100` fixture renders identically under both CAST targets on both engines.

### [C5] The engine version source: `ctx.database_version()` returns the bare version string

| Source | 2025.2.1 | 8.29.13 |
|---|---|---|
| `ctx.database_version()` (`exasol-udf-sdk` 0.22.1, live SLC handshake) | `2025.2.1` | `8.29.13` |
| WebSocket login `responseData.releaseVersion` | `2025.2.1` | `8.29.13` |
| `SYS.EXA_METADATA` `databaseProductVersion` | `2025.2.1` | `8.29.13` |
| `SYS.EXA_METADATA` `databaseMajorVersion` | `2025` | `8` |
| `SYS.EXA_METADATA` `databaseVersion` | **no such row** | **no such row** |

`ctx.database_version()` reports exactly the Docker image tag — no product prefix, no build suffix,
no `v`. Decision [4]'s "parse the leading dot-separated component, gate on `>= 2025`" is confirmed
against real strings, and the `8.`-leading requirement holds.

**`SYS.EXA_METADATA` has no `PARAM_NAME='databaseVersion'` row** — the name the plan's task-1 text
and task-5 sketch assumed returns zero rows on both engines. Task 5's oracle must read one of:
`SYS.EXA_METADATA` `PARAM_NAME='databaseProductVersion'` (recommended: same string as
`ctx.database_version()`, one plain SQL query through the existing `ExaConn`), or
`databaseMajorVersion` (already the parsed integer, but a second representation the production rule
never sees). The WebSocket login `releaseVersion` carries the same string but `ExaConn` currently
discards the login response, so using it would require widening that shared helper.

### [C6] The version gate is a deliberate choice, not a correctness requirement

[C1] + [C2] together mean an ungated `TIMESTAMP(6)` declaration would also be harmless on 8.29.13:
the engine clamps the declaration to `TIMESTAMP(3)` and strips the field from the pushdown echo, so
the `EMITS` clause and every emitted value would be unchanged. The plan's version gate is still
worth keeping — it makes the 8.x behavior an explicit, tested decision rather than a dependency on
one engine's silent clamping, and it is what makes `SYS.EXA_ALL_COLUMNS` honest about what the
adapter asked for. But the recorded rationale should not claim the gate prevents a failure on 8.x;
it prevents a *silent misdeclaration*.

## Review Findings

<!-- Populated by speq-implement after code review. -->
</content>
