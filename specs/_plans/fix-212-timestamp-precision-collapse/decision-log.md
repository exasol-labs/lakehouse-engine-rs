# Decision Log: fix-212-timestamp-precision-collapse

## Interview

Planned headless from the orchestrator brief (no live interview). The brief carried the empirical context and the pre-decided design; it is paraphrased here as Q/A.

**Q:** Where does the precision collapse happen, and what must the fix do?
**A:** Two points, both fixed together: `exasol_type_from_json` (adapter EMITS declaration) and `render_cast_target` (vs-expression CAST rendering). Each must read the TIMESTAMP precision and render `TIMESTAMP(p)`, falling back to bare `TIMESTAMP` when absent.

**Q:** How should each dialect render the precision?
**A:** Exasol dialect and the EMITS clause render the precision verbatim (Exasol accepts 0-9). The DataFusion dialect snaps an unsupported precision to the nearest supported unit before rendering, because DataFusion 54 parses `TIMESTAMP(p)` only for `{0,3,6,9}`.

**Q:** What is the precision field name in the dataType JSON?
**A:** The brief stated `precision` (u64), "already empirically verified — do not re-derive." Verification during planning contradicted this; see decision [1].

## Design Decisions

### [1] TIMESTAMP precision field is `fractionalSecondsPrecision`, not `precision`

- **Decision:** Both collapse points read `fractionalSecondsPrecision` for a TIMESTAMP's fractional-seconds precision. `precision` is used by Exasol only for `DECIMAL` (with `scale`) and `INTERVAL` (with `fraction`).
- **Alternatives:** Read `precision` as the brief instructed — rejected. Read `fractionalSecondsPrecision` with a `precision` fallback — rejected as over-engineering; Exasol never sends `precision` on a TIMESTAMP.
- **Rationale:** The brief's `precision` claim was uncaptured — `scripts/capture-pushdown-payload.sh` echoes only the adapter's OUTPUT scan-spec JSON, never the input dataType descriptor, so the input field name was never actually observed. The authoritative Exasol `virtual-schema-common-java` data-type API doc documents `fractionalSecondsPrecision` (optional, default 3); the reference fixture `pushdown_request_alltypes.json` shows `C_TIMESTAMP_4` = `{"type":"TIMESTAMP","fractionalSecondsPrecision":7}`; and the repo's own committed fixtures (`crates/vs-expression/src/lib.rs:1683,1708`) already use `fractionalSecondsPrecision`. Planning for `precision` would make the fix a silent no-op that only fails at E2E. The default of 3 also explains the observed symptom (`got TIMESTAMP(3)`).
- **Promotes to ADR:** yes

### [2] DataFusion dialect snaps precision to the NEAREST supported unit

- **Decision:** For `Dialect::DataFusion`, snap `p` to the nearest of `{0,3,6,9}` (`0→0,1→0,2→3,4→3,5→6,7→6,8→9`; identity on `0/3/6/9`; clamp `>9` to 9). Exasol dialect and the EMITS clause use `p` verbatim.
- **Alternatives:** Ceil to the next supported unit (always keep ≥ requested precision, making every snap lossless before EMITS truncation) — rejected to honor the recorded "nearest" design (STATUS.md, brief).
- **Rationale:** DataFusion 54's SQL frontend parses `TIMESTAMP(p)` only for `{0,3,6,9}`. The gaps have non-integer midpoints (1.5/4.5/7.5), so "nearest" is unambiguous. Up-snaps produce a finer value the EMITS-declared `TIMESTAMP(p)` truncates back, staying faithful. The sole down-snap `1→0` drops the tenths digit for the exotic `TIMESTAMP(1)` cast — named as an accurately-scoped trade-off in the spec (the Iceberg source is microsecond-precision, and DataFusion cannot parse `TIMESTAMP(1)`).
- **Promotes to ADR:** yes

### [3] Absent precision renders bare TIMESTAMP; WLTZ still declines first

- **Decision:** When `fractionalSecondsPrecision` is absent, both functions render bare `TIMESTAMP`. `withLocalTimeZone: true` short-circuits (EMITS → `TIMESTAMP WITH LOCAL TIME ZONE`; vs-expression → decline) before any precision logic.
- **Alternatives:** Render explicit `TIMESTAMP(3)` on absence — rejected; bare `TIMESTAMP` equals Exasol's default `TIMESTAMP(3)` and preserves the existing `exasol_type_from_json_reads_with_local_time_zone_flag` assertion.
- **Rationale:** Least blast radius; the WLTZ precedence and its semantics are unchanged by this fix.
- **Promotes to ADR:** no

### [4] EMITS derivation home is `vs-adapter/pushdown-planning`

- **Decision:** The EMITS-precision behavior is specced as a new scenario under `vs-adapter/pushdown-planning` (which governs "the EMITS column list SHALL match the projected columns in order and type"), kept distinct from `datafusion-scan/type-mapping`'s raw-column timestamptz scenario.
- **Alternatives:** Extend `datafusion-scan/type-mapping` — rejected; that feature governs the createVirtualSchema raw-column schema (always bare TIMESTAMP), a different concern the brief warned against conflating.
- **Rationale:** `exasol_type_from_json` is named by no existing scenario; the projection EMITS scenario is its behavioral home.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Missing third collapse point: `exasol_type_to_arrow` emit-boundary coercion

- **Finding:** The plan fixed only the two producing points (`exasol_type_from_json` EMITS, `render_cast_target` CAST). It missed the consuming point: `exasol_type_to_arrow` (`crates/lakehouse-engine/src/types/mapping.rs`) matches TIMESTAMP by exact string compare (`upper == "TIMESTAMP"`) with no `TIMESTAMP(p)` parse arm. Once the fix ships, `exasol_type_to_arrow("TIMESTAMP(6)")` falls through to `None`; the scan's `target_arrow_type` (`emit.rs`) then routes the column through the `Utf8`/VARCHAR string path, stringifying the `Timestamp(Microsecond)` result against a `TIMESTAMP(6)` EMITS declaration. The `EXPLAIN VIRTUAL` 04000 check would pass but the actual `SELECT` would fail or return wrong values — defeating the plan's own manual-repro expectation. Blast radius: every emit path (plain row scan, grouped-aggregate fan-out `grouped_agg.rs`, broadcast-join legs `joins/planning.rs`).
- **Direction change:** Added a third collapse point across plan.md (Summary/Context/Design/Features/Tasks/Parallelization/Verification), a new spec delta (`datafusion-scan/type-mapping`, the home of `exasol_type_to_arrow`'s existing DECIMAL-binning and emit-boundary scenarios), and task 3 (parse `TIMESTAMP(p)` → `Timestamp(Microsecond, None)`, `p` ignored; unit test at `TIMESTAMP(0)`/`(6)`/`(9)` that fails on current code). The "ship together" invariant now names three points. The Arrow target drops `p`: the project already collapses every TIMESTAMP precision to one Arrow representation on the way IN (mission type table), so the reverse mapping matches. WLTZ needs no `exasol_type_to_arrow` change — its producer (`exasol_type_from_json`, decision [3]) short-circuits before precision logic and emits bare `TIMESTAMP WITH LOCAL TIME ZONE` with no `(p)` suffix, so the existing exact-match WLTZ arm stays correct; stated explicitly in the new spec and Design table.
- **Promotes to ADR:** no

### [plan-review] Advisory: non-identity-snap manual repro added; Summary sentence split

- **Finding:** Two advisories — (a) the manual-testing checklist had only an identity-precision repro (`TIMESTAMP(6)`), exercising neither the DataFusion nearest-snap nor the up-snap-then-truncate round-trip; (b) plan.md's Summary opening sentence exceeded the 25-word cap.
- **Direction change:** Accepted advisory (a): added a second manual repro `CAST(c_ts AS TIMESTAMP(5))` (DataFusion renders `TIMESTAMP(6)` after snapping, EMITS declares `TIMESTAMP(5)`, rows return truncated to `p=5`), covering the non-identity path end to end. Fixed (b): split the Summary into an outcome-first sentence and a "three points ship together" reason sentence, both under the cap.
- **Promotes to ADR:** no
