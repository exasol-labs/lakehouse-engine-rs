# Decision Log: refactor-e2e-harness

## Interview

No live interview — headless plan from GitHub issue #168. The issue body is the full statement of
intent. Conventional defaults were assumed and recorded below.

**Q (implied by the issue's proposed change):** Where is the shared-helper module boundary, and what
stays file-local?
**A (assumed):** A single `common/e2e_harness` module holds the byte-identical constants and helpers
plus a parameterized `create_virtual_schema`; per-binary VS names, namespaces, seeding, `OnceLock`
guards, and file-specific assertions stay local. See Decision [1].

**Q (implied):** How does `CloudExaConn`/`encrypt_password` fold into `common/exasol_ws.rs` without
changing either suite's observable behaviour?
**A (assumed):** Add an opt-in `redact_sql` flag to `ExaConn`; cloud connects in redacting mode. See
Decision [2].

## Design Decisions

### [1] Shared harness module boundary: `common/e2e_harness` + file-local orchestration

- **Decision:** Create `common/e2e_harness.rs` (gated `exasol-e2e`) holding the 10 byte-identical
  constants, `install_slc`, `exa_conn`, `upload_so`, `create_schema_and_scripts`, `VsProps` +
  `create_virtual_schema`, `explain_virtual_sql`, `parse_int`/`parse_numeric`, and the
  `local_stack_creds`/`local_stack_storage`/`local_stack_catalog`/`resolve_fixture_files`
  catalog-inspection helpers (shared by scan/int96/positional_deletes). Each binary keeps its own
  `OnceLock`, thin `setup_e2e()`/`setup_full_stack()`, `VS_NAME` (and extra VS-name) constants,
  file-specific seeding, and assertions.
- **Alternatives:** (a) Put everything including per-binary setup into one shared function — rejected:
  setup orchestration genuinely differs (int96's two-tier wait, refresh creates no VS in setup,
  differing seed calls). (b) A `setup.rs` per test type — rejected: more files, no gain.
- **Rationale:** `install_slc`/`exa_conn`/`create_schema_and_scripts` are byte-identical and merge
  cleanly; the divergent parts (VS properties, seeding, waits) are parameterized or stay local. This
  eliminates the ~1,000 duplicated lines while preserving every per-binary difference.
- **Promotes to ADR:** yes

### [2] Fold `CloudExaConn` into `ExaConn` via an opt-in `redact_sql` flag

- **Decision:** Delete `CloudExaConn` and cloud's `encrypt_password`. Add a `redact_sql` bool to
  `common/exasol_ws::ExaConn`: `connect(...)` stays (redact_sql=false, unchanged for the 7 local
  binaries); a redacting constructor sets it true. When true, `execute` failure output omits the SQL
  statement and the Exasol response. Widen the `exasol_ws` gate in `common/mod.rs` to
  `any(exasol-e2e, cloud-e2e)`.
- **Alternatives:** (a) Keep a separate cloud client — rejected: ~150 duplicated lines, the whole
  point of the issue. (b) Always redact — rejected: the local suite's SQL-in-failure output is a
  debugging aid and the docker stack carries no secrets.
- **Rationale:** The only load-bearing difference between the two clients is credential redaction; an
  opt-in flag is the minimal fold that preserves the local suite's debuggability and the cloud
  suite's no-credential-leak guarantee (`packaging/cloud-e2e-harness` Background).
- **Promotes to ADR:** yes

### [3] `create_virtual_schema` takes a `VsProps` struct

- **Decision:** Collapse the five divergent `create_virtual_schema` signatures into
  `create_virtual_schema(conn, &VsProps)`, where `VsProps` carries `vs_name`, `namespace`,
  `catalog_conn_name` (default `LAKEHOUSE_CATALOG_CREDS`), `parallelism_factor: Option<usize>`, and
  `join_broadcast_max_bytes: Option<&str>`, built via `VsProps::new(..)` + builder setters. The helper
  emits base VS properties plus the optional `PARALLELISM_FACTOR` / `JOIN_BROADCAST_MAX_BYTES` clauses.
- **Alternatives:** Five wrapper fns; a builder-only API; a macro — rejected as heavier for no gain.
- **Rationale:** A struct with defaults expresses every per-binary property set without dropping any
  property (the divergence the issue's "duplication" claim hides).
- **Promotes to ADR:** no

### [4] Behaviour-neutral normalizations recorded as deliberate, not silent

- **Decision:** (a) The shared `create_virtual_schema` always re-issues the idempotent
  `CREATE OR REPLACE CONNECTION` (folding join's separate `create_connection`, which called it once
  before creating two VS). (b) The WebSocket login `clientName`/`driverName` label is standardized to
  one value across suites.
- **Alternatives:** Preserve join's create-connection-once ordering via a `VsProps` flag; parameterize
  the login label — both rejected as complexity for no observable benefit.
- **Rationale:** `CREATE OR REPLACE CONNECTION` is idempotent, so re-issuing it produces no observable
  difference; the login label is cosmetic and asserted nowhere. Both are named here so the "no
  behaviour change" acceptance criterion is honoured with the trade-offs explicit, not hidden.
- **Promotes to ADR:** no

### [5] Two additive spec scenarios lock the refactor's invariants; no delta for behaviour-preserved features

- **Decision:** Add one NEW scenario to `packaging/e2e-harness` (single shared harness definition →
  identical script DDL across binaries, per-binary properties as explicit parameters) and one NEW
  scenario to `packaging/cloud-e2e-harness` (cloud suite drives Exasol through the shared redacting
  `ExaConn`; no credential leak on failure). No delta for `e2e-harness-grouped-agg`,
  `e2e-harness-grouped-order`, `e2e-harness-positional-deletes`, `int96-timestamp-fixture`,
  `positional-delete-fixtures` — their behaviour is unchanged and they gain no invariant.
- **Alternatives:** No spec delta at all (pure refactor) — rejected: the two invariants (single-source
  provisioning; relocated credential-redaction guarantee) are regression risks worth a spec guard.
  Deltas on every touched packaging feature — rejected: over-speccing internal structure where
  behaviour and invariants are unchanged.
- **Rationale:** Specs are behavioural; add scenarios only where the refactor introduces or relocates a
  testable invariant. The redaction contract is runtime-observable (credential values absent from
  output); the single-source invariant is observable as byte-identical provisioning DDL across binaries
  plus all-binaries-pass in `make test-e2e`.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] `resolve_fixture_files` async/sync divergence

- **Finding (BLOCKER 1):** The plan classified `resolve_fixture_files` as a byte-identical clean move
  alongside `install_slc`/`exa_conn`/`create_schema_and_scripts`. Verified false against the code:
  `e2e_int96_timestamp_test.rs:309` defines it `async` (driven by `rt.block_on` at line 383), while
  `e2e_positional_deletes_test.rs:294` defines it synchronous with its own internal current-thread
  runtime (called directly at lines 359/420/621). Each also closes over a different fixture-module
  `NAMESPACE` symbol (`int96_fixtures::NAMESPACE` vs `pos_delete_fixtures::NAMESPACE` — equal string
  values, distinct symbols). Tasks 3.4/3.5 named no call-site rework, so a shared helper would compile-
  break or panic ("runtime from within a runtime") in one binary — a behaviour change under the plan's
  no-behaviour-change bar. The reviewer also flagged that `resolve_fixture_files` is absent from
  `e2e_scan_test.rs` (which shares only `local_stack_*`).
- **Direction change:** Specified the shared helper as `async fn resolve_fixture_files(namespace: &str,
  table: &str)` taking the namespace explicitly (no hardcoded module constant). int96 keeps its
  `rt.block_on` wrapper unchanged and gains the `NAMESPACE` argument (task 3.4); added sub-task 3.5.1 to
  convert positional_deletes' three sync call sites to `rt.block_on(resolve_fixture_files(NAMESPACE,
  table))` and give each `#[test]` fn a current-thread runtime. Corrected the Design/Context prose,
  Migration table, and Dead-Code table to state the divergence and the int96/positional_deletes-only
  scope (scan shares only `local_stack_*`).
- **Promotes to ADR:** no

### [plan-review] Cloud redaction scenario scope — narrowed to the `execute()` path (Option A)

- **Finding (BLOCKER 2):** The cloud spec's NEW scenario made a blanket "no credential value SHALL appear
  in test output" claim, but task 1.1 redacts only the `execute()` failure path. `ExaConn` has three
  other credential-printing failure paths left uncovered — `query_scalar_i64` (line 120),
  `query_row_count` (line 128), and the `connect()` auth-failure assert (line 72). The scenario was also
  mapped to `cloud_scan_reads_with_vended_credentials`, a success-path read test that never triggers a
  failure and so cannot verify a redaction-on-failure guarantee. The security-relevant MUST was thus
  neither fully implemented nor tested.
- **Direction change:** Chose **Option A** (narrow the spec to match the implementation) over Option B
  (widen redaction to all four paths). Rationale: no current cloud test passes credential-bearing SQL
  through `query_scalar_i64`/`query_row_count`, the login response echoes no credential, and Decision [2]
  already scoped redaction to `execute()` — so narrowing is the honest, minimal fix. Reworded the
  scenario THEN/AND to scope redaction to the `execute()` DDL-failure path and to name the out-of-scope
  methods with rationale; tightened task 1.1's scope statement likewise. Added negative test
  `cloud_redacting_conn_omits_credentials_on_failure` (task 2.2, Group D): opens a redacting `ExaConn`,
  issues a failing credential-bearing DDL embedding dummy sentinel credentials, captures the `execute()`
  panic via `catch_unwind`, and asserts neither the SQL nor the sentinels appear — mirroring
  `e2e_refresh_test.rs:628-632`. Re-mapped the scenario's coverage row from the success-path test to this
  negative test. The residual question of covering the other three paths is left as the reviewer's
  standing ADVISORY, carried to the PR body rather than fixed here.
- **Promotes to ADR:** no
