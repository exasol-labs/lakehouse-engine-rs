# Decision Log: fix-namespace-noniceberg-table-skip

## Interview

Headless mode — no live interview. The orchestrator supplied issue #138 and directed: bias toward the simplest fix (skip-and-warn) over a new table-filter property, and document the scope trade-off explicitly.

**Q (headless):** Skip-and-warn, an explicit include/exclude table-filter property, or both?
**A (assumed):** Skip-and-warn only. It fully resolves the reported failure with no new configuration surface. See Decision 2.

**Q (headless):** How is a non-Iceberg table distinguished from a genuine catalog failure?
**A (assumed):** HTTP 404 on the per-table `loadTable`. Every other error aborts. See Decision 1.

## Design Decisions

### [1] Skip a per-table load only on HTTP 404; abort on every other error

- **Decision:** During `createVirtualSchema` enumeration, a per-table `loadTable` returning HTTP 404 causes that table to be skipped with a warning; any other failure (transport error, non-404 HTTP status) aborts the whole request. The 404 is detected by a classifier (`is_table_not_found`) that matches the code-authored `catalog returned HTTP 404` prefix in the `UdfError::User` message.
- **Alternatives:** Skip on any per-table error. Rejected: an any-error skip masks auth, throttling, and outage faults behind a silent partial schema, turning a catalog-wide misconfiguration into a quietly incomplete virtual schema.
- **Rationale:** 404 is the Iceberg REST OpenAPI `NoSuchTableException` signal ("table to load does not exist") and the exact status AWS Glue returns for a non-Iceberg table (#138 body: `NoSuchIcebergTableException`). Keying on it skips exactly the reported case and nothing else. `UdfError` is SDK-owned and carries only opaque `String` variants (no status field), and the single catalog error site already flattens the status into the message — so the discriminator keys on the code-controlled status *prefix* rather than a structured status. See `[plan-review] classifier cannot read a structured status` below.
- **Promotes to ADR:** yes

### [2] No table-filter property in this bug fix

- **Decision:** Do not add an include/exclude table-filter property. Skip-and-warn is the whole fix.
- **Alternatives:** Add `TABLE_INCLUDE` / `TABLE_EXCLUDE` properties, or both filter + skip.
- **Rationale:** Skip-and-warn resolves the reported failure with zero new configuration surface. A filter property is a separable enhancement that also touches property persistence and adapterNotes; adding it here would widen a bug fix into a feature. Headless default: do not add configuration surface unless required to fix the reported failure. A filter remains available as a follow-up issue if operators later want to curate which Iceberg tables are exposed.
- **Promotes to ADR:** yes

### [3] Build TABLE_MAP from surviving tables, not the raw listing

- **Decision:** Move the per-table loop and `TABLE_MAP` construction behind one `build_virtual_tables` seam so the returned `tables` list and `TABLE_MAP` are built from the same surviving (loadable-Iceberg) set.
- **Alternatives:** Keep building `TABLE_MAP` over all listed idents (current pre-loop call), then filter the tables list only.
- **Rationale:** A `TABLE_MAP` entry for a skipped table would advertise an unqueryable virtual table whose later pushdown would 404 again. The map and the table list must share one surviving set. This also gives the skip/abort control flow a single dependency-injectable test seam.
- **Promotes to ADR:** no

### [4] Surface skipped tables via udf_log! warn, not a hard signal

- **Decision:** Emit one `udf_log!(ctx, warn, …)` line per skipped identifier to script output, routed through the existing redaction path.
- **Alternatives:** Return skip counts in adapterNotes; fail; stay silent.
- **Rationale:** The warning is operational visibility, not part of the persisted contract. `udf_log!` is the scan path's existing diagnostics channel. The macro's own level check (`warn <= debug_level`) does pass at the default `info` level, so a `warn` line is emitted; however, whether the ADAPTER SCRIPT VM's stderr reaches SCRIPT_OUTPUT the same way the scan-UDF VM's does is assumed by analogy, not verified — every existing `udf_log!` call site is on the scan path. This is acceptable because the warning is best-effort: if the adapter-script stderr is not captured, the skip still succeeds and the contract (empty/partial schema, no bad `TABLE_MAP` entry) is unaffected. Redaction reuse guarantees no credential leak in the warning text.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] classifier cannot read a structured status (BLOCKER)

- **Finding:** The plan's core discriminator rested on carrying a structured HTTP status to `is_table_not_found(err: &UdfError)`, but `UdfError` (SDK-owned, `exasol-udf-sdk-0.20.3`) has only opaque `String` variants — no status field. The single 404 site (`credentials.rs:425`) already flattens the status into `UdfError::User("catalog returned HTTP 404: …")`. "Keys on status, not free-text body" against `&UdfError` was therefore unachievable; the stated mechanism and the stated prohibition against string matching contradicted each other.
- **Direction change:** Adopted fix (a) — controlled-prefix match. `is_table_not_found` matches the code-authored, deterministic `catalog returned HTTP 404` prefix via `starts_with` (not `contains`, so a "404" inside a redacted non-404 body cannot false-match). Verified `redact_error` preserves that prefix (it strips only secret/credential substrings). No `credentials.rs` change is required. Reworded Decision [1], Consequences row 3, the Patterns table, Task 1, and the architecture diagram accordingly. The `catalog returned HTTP 404: …` format is now a load-bearing contract pinned by a new unit test (`catalog_error_message_uses_http_status_prefix`). Rejected fix (b) (structured `u16` error type threaded through `authed_get_json`/`resolve_table_schema`) as disproportionate for a bug fix — it would also perturb the `/v1/config` caller and the credential-redaction tests.
- **Promotes to ADR:** yes

### [plan-review] localize edits to mod.rs; no credentials.rs change (ADVISORY)

- **Finding:** Task 1 and the dead-code rows named only `mod.rs`, yet the original Task 1 also said to "surface the status from `authed_get_json`", implying a `credentials.rs` edit.
- **Direction change:** Removed the "surface the status" language. Added an explicit statement to Dead Code Removal that all edits are confined to `mod.rs` and no `credentials.rs` change is needed (the status is already in the message).
- **Promotes to ADR:** no

### [plan-review] all-non-Iceberg namespace coverage gap (ADVISORY)

- **Finding:** No scenario covered a namespace in which every listed table 404s.
- **Direction change:** Added a Background sentence and a new spec scenario ("A namespace whose every table is non-Iceberg yields an empty virtual schema") stating the intended behavior — success with an empty table set / empty `TABLE_MAP` plus one warning per skipped table — with a matching integration-test row and Task 4 coverage.
- **Promotes to ADR:** no

### [plan-review] single owner for the warning emission (ADVISORY)

- **Finding:** The architecture diagram and Task 3 disagreed on whether `build_virtual_tables` or the handler emits the `udf_log!` warning.
- **Direction change:** `build_virtual_tables` stays pure and testable — no `ctx`, emits nothing, returns `skipped_idents`; the handler owns warning emission. Updated the diagram, the `build_virtual_tables` signature (dropped `ctx`), the Patterns table, and Tasks 2/3.
- **Promotes to ADR:** no

### [plan-review] udf_log! adapter-path visibility claim and import (ADVISORY)

- **Finding:** Decision [4] asserted `udf_log!` is "visible at the default level" during CREATE VIRTUAL SCHEMA without evidence; all existing call sites are on the scan-UDF path. The `udf_log!` import was also missing from the adapter path.
- **Direction change:** Downgraded the certainty in Decision [4] — the macro's level check does pass at the default `info` level, but adapter-script stderr capture is assumed by analogy, not verified; noted the warning is best-effort so the contract holds regardless. Added `use exasol_udf_sdk::udf_log;` to Task 3's edit list.
- **Promotes to ADR:** no
