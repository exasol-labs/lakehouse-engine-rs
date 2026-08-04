# Plan: fix-absent-table-location-error-consistency

## Summary

Hoist the absent-table-location error above the vended/static storage split so every file-resolution path rejects a `loadTable` response carrying no table `location`, and purge the remaining text that describes the REST `warehouse` as a storage location. Tracked as GitHub issue #296; most of the work landed already in commit `6d08c8a`, so this plan closes a narrow code gap plus three wording defects.

## Design

### Context

Commit `6d08c8a` removed the `warehouse`-as-storage-anchor fallback on the vended path, but placed the absent-location check INSIDE the `if creds.use_vended_credentials` arm of `resolve_file_list` (`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`). The non-vended path therefore still tolerates an absent location silently: it produces an empty `table_root` and carries on. The Apache Iceberg table spec makes `location` `_required_` in v1, v2, and v3, so an absent location is a malformed catalog response on every path, not a vended-path concern.

- **Goals** — one path-independent rejection of an absent table `location`; one spec owner for that rule; host-level test coverage that cannot silently regress; no remaining text anywhere in the repo that reads `warehouse == storage location`.
- **Non-Goals** — changing `relativize_path_to_root`'s empty-root handling; changing `resolve_vended_storage`'s signature or behaviour; changing the `createVirtualSchema` path; widening the join backend-divergence guard; touching fixture `warehouse` values that happen to be URIs on the local MinIO stack.

### Decision

Add no module, no interface, and no boundary. The change moves six existing lines up past one `if` and rewords one error string. Per `/speq:design-philosophy`, the Quick Diagnostic table applies to a change that introduces a new module, interface, or boundary; this change introduces none, so the table is deliberately skipped rather than silently passed. The two design questions the plan does answer are WHERE the guard lives and HOW it is tested.

#### Architecture

```
resolve_file_list(session, catalog_props, storage, creds, allow_http, filter_json)
  │
  ├─ load_table_any_auth ─────────────► LoadTableResult          (unchanged)
  │
  ├─ REJECT empty metadata.location ──► UdfError::User           ◄── MOVED HERE
  │        (one check, before the split; join sides inherit it)
  │
  ├─ if use_vended_credentials → resolve_vended_storage(..)      (unchanged)
  │  else                      → storage.clone()                 (unchanged)
  │
  └─ build Iceberg table → plan files                            (unchanged)

resolve_table_schema (createVirtualSchema) reads current_schema() only — no location, no guard.
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Guard at the resolve-once seam | `resolve_file_list`, above the vended/static split | The narrowest site every location-dependent path passes exactly once; `resolve_one_join_side` delegates here, so join sides are covered without a second check |
| Loopback HTTP catalog fake | new host unit test in `file_resolution.rs`'s `mod tests` | Drives the real production function with a synthetic malformed response; the pattern is already established in `crates/lakehouse-catalog/src/session.rs` tests (`session.rs:407`, `:464`, `:521`) with no test dependency beyond `tokio` |
| Reference, do not restate | `pushdown-planning-cloud-credentials` cites `pushdown-planning` | One normative clause, one owner — a path-independent rule stated inside a vended-only scenario is what produced the original defect |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Guard in `resolve_file_list`, above the split | Guard inside `load_table_any_auth` (`lakehouse-catalog`), which would make the rule path-independent by construction and reuse that crate's existing loopback-fake tests | Rejected: it would also reject the response on the `createVirtualSchema` path, which reads no location — failing a whole virtual-schema creation over a field it never uses. The interview also fixed this placement (A1). |
| No testability extraction | Extract a pure `require_table_location(&str)` helper; or a `resolve_effective_storage(result, static, use_vended, allow_http)` wrapper in `lakehouse-catalog` | Both rejected. A one-line `is_empty` guard behind a function is classitis, and it cannot test "both paths" — after the hoist there is one path through it. The wrapper is worse: it reintroduces exactly the storage-backend parameter and the do-the-work/return-the-input boolean that `pushdown-planning-cloud-credentials` forbids on the vended entry point. |
| Test drives `resolve_file_list` over a loopback HTTP fake | Live-cloud E2E assertion (already exists, gated on cloud env vars); Exasol E2E | Runs on every `cargo test` with no DB, no cloud, and no new dependency. The existing coverage (`cloud_e2e_test.rs:688`) asserts the location is PRESENT and only runs with live AWS credentials, so it cannot gate this rule. |
| One spec owner: `vs-adapter/pushdown-planning` | Leave the rule in `pushdown-planning-cloud-credentials`; state it in both | The rule holds with vending disabled, so a vended feature cannot own it. Stating it twice invites the two copies to drift. |
| No ADR edits | Amend ADR 053 / 006 / 001 | Audited clean — see § Audit Findings. No ADR ratifies the misconception, so there is nothing to correct. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning | CHANGED | `specs/_plans/fix-absent-table-location-error-consistency/vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/pushdown-planning-cloud-credentials | CHANGED | `specs/_plans/fix-absent-table-location-error-consistency/vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| datafusion-scan/scan-execution-file-metadata | CHANGED | `specs/_plans/fix-absent-table-location-error-consistency/datafusion-scan/scan-execution-file-metadata/spec.md` |

The `datafusion-scan/scan-execution-file-metadata` delta adds exactly one Background bullet and changes no scenario wording. That bullet records that the adapter can no longer emit an empty table root. The feature's three empty-table-root clauses are therefore retained as a wire-format totality property rather than a reachable path. Those three are two normative `SHALL` clauses and one descriptive Background bullet. The delta reproduces exactly two scenarios byte-identically to name the `SHALL` clauses, and reproduces no recorded Background bullet.

## Impact

No behaviour changes for a spec-conformant catalog: every Iceberg v1/v2/v3 `loadTable` response carries a `location`, so no existing query reaches the new error. A malformed response that omits `location` now fails at plan time on the non-vended path too, instead of resolving an empty table root and emitting every file path as an absolute URI. The error text becomes path-independent — it names the absent `location` rather than a vended storage-backend failure. Not a breaking change: no CONNECTION field, virtual-schema property, SQL shape, or wire format moves.

## Audit Findings

Repo-wide audit of the `warehouse == storage location` misconception. Recorded so a later reader does not redo it. **Verified clean, no change needed:**

| Surface | Finding |
|---------|---------|
| `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs` `resolve_one_join_side` | Builds the per-side `CatalogProps` by overriding only `table`, then delegates to `resolve_file_list`. Never reads `.warehouse`. Inherits the hoisted guard for free. |
| `crates/lakehouse-catalog/src/vended.rs` | `resolve_vended_storage(result, anchor, allow_http)` takes no `ConnectionCreds` and no `StorageBackend`, so no warehouse can reach it. Its unit test `vended_storage_anchor_is_the_s3_table_location` already pins "must be an S3 URI, not the HTTPS catalog URI". |
| `crates/lakehouse-catalog/src/namespace.rs` | `warehouse` appears only as a REST routing prefix (`glue_catalog_prefix`, `build_list_*_url` path segment, omitted when empty). Reads no `location`. |
| `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` `resolve_table_schema` | Reads `result.metadata.current_schema()` only. `createVirtualSchema` resolves no storage anchor and no table root. |
| `crates/lakehouse-catalog/src/session.rs` | `build_load_table_url` / `resolve_load_table_prefix` are routing-prefix only; the non-SigV4 no-override fallback is the EMPTY prefix, and `session.rs:507-520` already documents that it is explicitly not the warehouse. |
| `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md` L63, L66, L147-152 | Already correct. L66 already states the `warehouse` is a routing identifier carrying no storage location; the closed-scheme bullet and the scheme-selection scenario name only the table `location`. Only L66's "the vended resolution therefore reports the missing location directly" and L153's placement need amending, both covered by the delta. |
| `specs/_decision/**`, `specs/_recorded/**` | ADR 053 has zero `warehouse` occurrences and pins the no-CONNECTION-value signature. ADR 006 treats `warehouse` as a bare account id / routing prefix. ADR 001 L1218 correctly says the fallback is an EMPTY prefix "rather than the warehouse"; L1492 correctly calls `table_root` "the vended-credential anchor". `_recorded/` has zero hits. No ADR ratifies the misconception. |
| Other `specs/**` `warehouse` mentions | Routing-prefix and accurate, e.g. `rest-catalog-oauth-auth/spec.md` L47-48 ("a catalog-assigned warehouse NAME rather than an S3 URI") and `e2e-harness/lakekeeper-e2e-harness/spec.md` L25-26 ("warehouse-name (not an S3 URI)"). |
| Fixture values `"warehouse": "s3://warehouse/"` | Benign: on the local `iceberg-rest` + MinIO stack the warehouse value coincidentally IS a URI. `tests/tpch_loader.rs:447`, `e2e_scan_test.rs:427`, `e2e_count_distinct_test.rs:71-80`, `crates/lakehouse-catalog/src/test_support.rs:15`/`:84`, `docs/catalogs.md:68`, `docs/install.md:151`. Left alone. |
| `docs/benchmark.md` L257, L268 | Already correct. |

**Defects found — the four deliverables below:** the vended-only siting of the absent-location check, the absence of any host test for it, two `cloud_e2e_test.rs` env-var doc strings, and one `docs/catalogs.md` field-table row.

## Requirements

| Requirement | Details |
|-------------|---------|
| Iceberg spec conformance | `location` is `_required_` in the v1, v2, and v3 columns of `apache/iceberg` `format/spec.md`'s Table Metadata field table. Quoted in the `pushdown-planning` delta Background; verified against the fetched file, not from memory. |
| Two wire shapes, two owners | "Absent `location`" is two distinct wire shapes with two distinct error paths. **Key present but empty** (`"location": ""`) → the hoisted guard in `resolve_file_list`; this is the shape task 1 tests and task 2 fixes. **Key omitted** → rejected earlier, at deserialization in `authed_get_json` (`crates/lakehouse-catalog/src/iceberg_io.rs:89-94`), because `iceberg-0.10.0` declares `location: String` non-`Option` with no `#[serde(default)]` on all three metadata variants (`src/spec/table_metadata.rs:810`, `:855`) and `TableMetadata` deserializes via `#[serde(try_from = "TableMetadataEnum")]` over `#[serde(untagged)] enum TableMetadataEnum { V3, V2, V1 }` (`:783-788`), so an omitted key matches no variant. Both shapes yield a `UdfError::User` and neither substitutes the `warehouse`; they differ only in message specificity. Scope decision recorded in decision-log.md [8]. |
| No panic | The error MUST be `UdfError::User`. A panic inside a UDF is an abnormal VM exit and the engine SIGKILLs every sibling VM of the statement part. |
| Reproduction gate | CLAUDE.md requires a reported bug be reproduced against the Docker Exasol container before it is fixed. This defect is UNREACHABLE through a live catalog — a spec-conformant catalog always sends `location`, and no supported catalog can be configured to omit it. The reproduction is therefore the host unit test, which constructs the malformed response directly; `make test-e2e` serves as the no-regression gate, not as the repro. |

## Implementation Tasks

1. **Host unit test for the absent-location rejection, both paths.** Add `absent_table_location_errors_on_both_vended_and_static_paths` to `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`'s existing `#[cfg(test)] mod tests` (`file_resolution.rs:822-823` — the attribute at `:822`, `mod tests {` at `:823`). Bind a `tokio::net::TcpListener` on `127.0.0.1:0`, serve one HTTP/1.1 JSON `loadTable` response whose `metadata` is the minimal valid v2 table metadata with `"location": ""`, then call `resolve_file_list` once with `use_vended_credentials = true` and once with `false`, asserting `UdfError::User` AND that both messages are identical and name the absent `location`. Assert the message content, not only the variant — a bare `matches!(UdfError::User(_))` would pass pre-fix if the non-vended arm happened to fail downstream for another reason, which would silently void the failing-test-first gate. This task MUST fail on the non-vended arm before task 2. [expert]

   Harness notes, verified during planning:
   - Set `creds.use_sigv4 = true` (with dummy `region`/`access_key`/`secret_key`) so each `resolve_file_list` call issues EXACTLY ONE HTTP request. `resolve_catalog_auth` returns `CatalogAuth::Sigv4` without any request (`auth.rs:215`), and `resolve_load_table_prefix` short-circuits the `/v1/config` lookup on that path (`if let CatalogAuth::Sigv4 = auth { return glue_catalog_prefix(warehouse); }`, `session.rs:148-151`, proved by the paired test at `session.rs:407`), so a one-shot listener per call is sufficient. `use_sigv4` is orthogonal to `use_vended_credentials`, which is what the test varies. Without it, `CatalogSession::resolve` issues a `/v1/config` GET first and reqwest may keep-alive the connection for the `loadTable` GET, so the fake would have to loop over both `accept()` and per-stream requests.
   - `CatalogSession::resolve` builds its own `reqwest::Client` per call (`session.rs:204`), so the two calls cannot share a pooled connection; give each its own listener.
   - Serve the JSON over HTTP rather than constructing a `LoadTableResult` value: `iceberg-catalog-rest` is only a dev-dependency of `lakehouse-engine`, and naming the type in production code would pull the REST crate across the crate boundary the `lakehouse-catalog` split exists to hold.
   - `iceberg::spec::TableMetadata` deserializes `location` as a plain `String` with no non-empty validation (`iceberg-0.10.0/src/spec/table_metadata.rs:75`, `:810`, `:855`), so `"location": ""` round-trips and reaches the guard. Copy the minimal valid v2 metadata JSON from `crates/lakehouse-catalog/src/vended.rs:303-317`. Use the KEY-PRESENT-BUT-EMPTY shape, not an omitted key: an omitted key fails deserialization before the guard (see § Requirements, "Two wire shapes, two owners"), so a test that omits it would assert the wrong error and would pass before task 2, voiding the failing-test-first gate.
   - Extend `crates/lakehouse-engine/Cargo.toml`'s `[dev-dependencies] tokio` features with `"net"` and `"io-util"` (currently `["rt-multi-thread", "macros", "time"]`, line 71). `tokio::net::TcpListener` needs `net` and `tokio::io::{AsyncReadExt, AsyncWriteExt}` need `io-util`; workspace tokio declares only `["rt", "macros"]` (`Cargo.toml:65`). Today both features arrive solely through Cargo feature unification via `reqwest`/`hyper-util`, so the harness would compile by accident and break the moment that transitive choice changes. Declaring them directly matches the discipline the same `[dev-dependencies]` block already applies in its `iceberg-storage-opendal` comment.
2. **Hoist the check above the vended/static split, and rewrite the doc comment that frames it as vended-only.** In `resolve_file_list` (`file_resolution.rs:243-262`), move the `table_location.is_empty()` guard out of the `if creds.use_vended_credentials` arm to immediately after `let table_location = result.metadata.location();`, and reword the message path-independently — it must name the absent table `location` and the `warehouse` as a non-substitute, and must NOT say "the storage backend cannot be resolved" (on the non-vended path the static backend resolves fine). Leave `relativize_path_to_root` and `resolve_vended_storage` untouched.

   Then rewrite the comment at `file_resolution.rs:243-247`, whose final clause currently reads "so an absent location is its own error **on the vended branch below**" (`:247`). After the hoist that clause is false, and it restates the exact vended-only framing decision-log.md [5] blames for the original gap surviving commit `6d08c8a`. Neither task-5 regex matches it, so it ships unless this task removes it. State instead that the guard runs above the vended/static split on every path. The comment's surviving substance — that the anchor is the table's own location, that `storage_credentials[*].prefix` is matched against it, and that neither the catalog REST URI nor the REST `warehouse` can stand in — is correct and MUST be kept.
3. **Correct two `cloud_e2e_test.rs` env-var doc strings.** `crates/lakehouse-engine/tests/cloud_e2e_test.rs:10` (`GLUE_WAREHOUSE — S3 URI of the Iceberg warehouse (e.g. s3://my-bucket/path/)`) and `:794` (`CATALOG_AUTH_WAREHOUSE — S3 URI of the Iceberg warehouse`). Both values are fed to `CatalogSession::resolve` as the routing prefix (`:713`, `:723`), matching `docs/benchmark.md:257`. Describe them as routing identifiers — a bare AWS account id under Glue.
4. **Tighten the `docs/catalogs.md` field-table row.** Line 36 reads "Iceberg warehouse location: an `s3://…` path normally, an AWS account id under Glue, or a warehouse **name** under Lakekeeper", contradicting the same file's L87 ("`warehouse` is the AWS **account id**, not an `s3://` path") and L156 ("**Warehouse is a name, not a path.** … It is not an `s3://` location."). Restate it as the catalog routing identifier, and keep the local `iceberg-rest`/MinIO stack's URI-shaped value as the coincidence it is rather than the norm.
5. **Verification sweep.** Run the issue's audit regex and the wording sweep it does not catch, and confirm both return only correct text. Both sweeps MUST exclude `target/`, `.git/`, AND `specs/_plans/` — this plan's own artifacts quote the offending strings in order to describe them, so they match every regex here and are not defects:

   ```bash
   grep -rniE "fall.?back to the warehouse|warehouse.*(also an|is an?) .*location|warehouse.*when.*(absent|empty)" . \
     | grep -vE '/target/|/\.git/|specs/_plans/'
   grep -rn "S3 URI of the Iceberg warehouse" . \
     | grep -vE '/target/|/\.git/|specs/_plans/'
   ```

   The first sweep MUST return EXACTLY these four hits, all of which are correct text, and no others:
   - `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:66` — the recorded Background bullet stating the rule (superseded by this plan's delta, so it still reads this way until `/speq:record` merges).
   - `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:153` — the recorded scenario clause stating the rule (likewise pre-merge).
   - `docs/catalogs.md:156` — "It is not an `s3://` location", which states the correct principle.
   - `crates/lakehouse-catalog/src/session.rs:279` — "omits the warehouse prefix when empty", an unrelated URL-building doc comment about the prefix, not a location.

   The second sweep MUST return nothing after task 3.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1 → 2 (sequential: failing test, then fix) |
| Group B | 3, 4 |
| Group C | 5 |

Sequential dependencies:
- Group A task 1 → Group A task 2 (TDD: the test must fail first)
- Group A, Group B → Group C (the sweep verifies both)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Error string | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:255-260` | The vended-framed message ("so the storage backend cannot be resolved") is replaced by the path-independent one; the old wording must not survive alongside it |
| Comment clause | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:247` | The clause "so an absent location is its own error on the vended branch below" becomes false once the guard is hoisted, and it is the vended-only framing decision-log.md [5] blames for the original gap. No task-5 regex matches it, so task 2 must remove it explicitly |

Explicitly NOT dead code, and MUST NOT be removed: `relativize_path_to_root`'s empty-`table_root` branch (`file_resolution.rs:35`). After task 2 an empty root is unreachable from a `loadTable` response, which makes the branch unreachable — not dead. It is the total-function property of the wire encoding (empty root ⇒ every path absolute), rejoined by the scan UDF in `reconstruct_abs_uri` (`crates/lakehouse-engine/src/scan/object_store.rs:250`), and never a storage anchor. The VS-side call at `pushdown/mod.rs:250` is `relativize_shards_to_root`, which strips the root rather than rejoining it. The scan-side half of this property is owned by `datafusion-scan/scan-execution-file-metadata`; see that feature's delta. Its three empty-table-root clauses — two normative `SHALL` clauses and one descriptive Background bullet — are retained for the same reason.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| File resolution rejects a loadTable response that carries no table location | Unit | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` (`mod tests`) | `absent_table_location_errors_on_both_vended_and_static_paths` |
| The storage backend under vending is selected from the table location's URI scheme (CHANGED clause only) | Unit | `crates/lakehouse-catalog/src/vended.rs` (`mod tests`) | existing `vended_storage_anchor_is_the_s3_table_location` and the empty-anchor unsupported-scheme assertions — unchanged, and MUST still pass, because the amended clause removes a requirement rather than adding behaviour |
| Relative paths resolve against the table root and absolute paths pass through (retained empty-root clause) | Unit | `crates/lakehouse-engine/src/scan/object_store.rs` (`mod tests`) | existing `reconstruct_absolute_entry_passes_through` (`:1020` asserts an empty root) and `reconstruct_relative_entry_normalizes_single_separator` — unchanged, and MUST still pass; the delta adds a Background bullet and changes no scenario text |
| Delete-file relative and absolute paths resolve like data-file paths (retained empty-root clause) | Unit | `crates/lakehouse-engine/src/scan/spec.rs` (`mod tests`) | existing `legacy_empty_root_treats_paths_as_absolute` — unchanged, and MUST still pass, for the same reason |

The absent-location scenario is a UNIT test rather than the default integration test because its input is a malformed catalog response no live stack can produce: no supported Iceberg catalog can be configured to omit `location`, so there is no integration fixture that reaches the branch. The test still exercises the real production entry point (`resolve_file_list`) over real HTTP against a loopback fake, so only the catalog server is synthetic.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning | `cargo test -p lakehouse-engine absent_table_location` | 1 passed; both `use_vended_credentials` arms report the same `UdfError::User` naming the absent `location` |
| vs-adapter/pushdown-planning | `grep -rn "S3 URI of the Iceberg warehouse" . \| grep -vE '/target/\|/\.git/\|specs/_plans/'` | no output — the `specs/_plans/` exclusion is required because task 3 and task 5 quote the string they remove |
| datafusion-scan/scan-execution-file-metadata | `cargo test -p lakehouse-engine reconstruct_` then `cargo test -p lakehouse-engine legacy_empty_root_treats_paths_as_absolute` | all pass unchanged (`reconstruct_absolute_entry_passes_through` asserts passthrough against an empty root at `object_store.rs:1020`) — the empty-root rejoin branch is retained, so its behaviour MUST NOT change |
| vs-adapter/pushdown-planning-cloud-credentials | `cargo test -p lakehouse-catalog vended` | all pass unchanged — the vended selector's behaviour and signature are untouched |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures — regression gate only; requires the Docker Exasol + MinIO + REST-catalog stack to be brought up manually first, or every DB-backed test FAILS and mimics a real regression |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
