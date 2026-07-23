# Decisions: refactor-scan-spec-dispatch-dedup

## ADR: Flatten-Embed `CommonScanSpec` Into `ScanSpec`

**ID:** flatten-embed-common-scan-spec-into-scan-spec
**Plan:** `refactor-scan-spec-dispatch-dedup`
**Status:** Accepted

### Context

`ScanSpec` duplicated `CommonScanSpec`'s ~17 shard-invariant fields as its own field
declarations, keeping `to_common` and `from_parts` as field-by-field copy bodies that drift
silently on any tuning-field addition. The two-argument UDF wire (a shard-invariant common
blob as argument 0, a per-shard file list as argument 1) and the existing `from_parts_json`
contract had to stay byte-identical across the change.

### Decision

`ScanSpec` becomes `{ #[serde(flatten)] common: CommonScanSpec, files: Vec<FileEntry> }`;
`to_common` collapses to `self.common.clone()` and `from_parts` to `Self { common, files }`.
Every shard-invariant field read migrates from `spec.<field>` to `spec.common.<field>`, and
every `ScanSpec { .. }` struct literal moves to the nested `common: CommonScanSpec { .. }`
form, compiler-guided across the whole call-site graph in one atomic change.

### Options Considered

| Option | Verdict |
|--------|---------|
| `#[serde(flatten)] common: CommonScanSpec` alongside `files` | ✓ Chosen — keeps the common-blob and file-list wire byte-identical and leaves `from_parts_json` untouched |
| `Deref<Target=CommonScanSpec>` to preserve `spec.<field>` reads | ✗ Rejected — a discouraged pattern that still does not spare the compiler-forced construction-site edits |
| A declarative macro generating both structs | ✗ Rejected — cleverer than the codebase's "prefer simple" bar |
| A nested, non-flattened `common` field | ✗ Rejected in interview — changes the wire to nested JSON |

### Consequences

One declaration of the shard-invariant fields; the compiler forces every read and
construction site to migrate, so no field can silently revert to its serde default. The
migration touches ~100 read sites and 85 construction sites across the scan and adapter
modules in a single non-decomposable compile unit, which is the documented context-blowup
trigger for a graph-wide struct-field change — mitigated by routing the task to the expert
executor with a per-file call-site census and by the golden dispatch-SQL baseline (see the
companion ADR) as the byte-drift detector.

## ADR: Capture a Golden Dispatch-SQL Baseline Before a Byte-Identical Refactor

**ID:** golden-dispatch-sql-baseline-before-byte-identical-refactor
**Plan:** `refactor-scan-spec-dispatch-dedup`
**Status:** Accepted

### Context

The plan asserted the scan-driving SQL and empty-result output stay byte-identical across
the four-part dedup, but the only pre-existing regression guard was `.contains(...)` and
`.matches(...).count()` substring tests plus fragment-level `assert_eq!` calls. A regression
that keeps every checked substring but changes byte layout — a clause reordered by the
task-5 classifier rewrite, or a field reverting to its serde default in the task-2 flatten —
would pass every cited test, defeating the plan's own anti-silent-drift premise, concentrated
in exactly the highest-risk tasks.

### Decision

Before any dedup work, extract the post-resolution dispatch body of `handle_pushdown` into a
behavior-preserving `pub(crate) fn build_dispatch_sql(..)` offline seam, then capture nine
committed golden fixtures — five non-empty dispatch shapes through `build_dispatch_sql` and
four empty shapes through `empty_result_sql` — each asserted with a full-string
`assert_eq!` against `include_str!`-loaded fixtures. Every subsequent task's byte-identity
check, and the plan's Verification table, point at this golden comparison instead of at
substring tests.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extract an offline dispatch seam and assert nine golden fixtures full-string `assert_eq!` | ✓ Chosen — `handle_pushdown` needs live catalog I/O and the pre-existing leaf-builder tests hand-assemble the spec, bypassing the dispatcher's own construction, so neither discharges the byte-identical bar |
| Continue relying on `.contains(...)`/`.matches(...).count()` substring tests | ✗ Rejected — provably blind to a moved clause or a reverted serde default, the exact drift this refactor risks |

### Consequences

Every task from the struct flatten through the classifier extraction is checked against a
byte-for-byte pre-refactor baseline, not convention. This verification discipline — capture a
golden output baseline from pre-refactor code, then assert full-string byte-equality after
each change — generalizes to any future byte-identical / pure-refactor plan in this project;
substring checks cannot discharge a byte-identical acceptance bar.

## ADR: Shared `RequestShape` Classifier Consumed By Both the Dispatch and Empty-Result Paths

**ID:** shared-request-shape-classifier-dispatch-and-empty-result
**Plan:** `refactor-scan-spec-dispatch-dedup`
**Status:** Accepted

### Context

The request-routing decision — grouped aggregate first, then single-group aggregate, then
row scan, gated by the same aggregate-column-type validation and the same HAVING-present
hard-error decline — was encoded twice: once in the non-empty dispatcher, once in
`empty_result_sql`. The two trees had to be kept in lockstep by convention, with no compiler
or test enforcing agreement.

### Decision

Add `adapter/pushdown/request_shape.rs` exposing `classify_request_shape(req, col_types) ->
Result<RequestShape, UdfError>`, where `RequestShape = Grouped { detection, having } |
GroupByWrapper | SingleGroupAgg { items } | RowScan`. The classifier owns the 3-tier
priority, the `validate_agg_col_types` gates, and the HAVING-present hard-error decline. Both
`mod.rs`'s `build_dispatch_sql` and `file_resolution.rs`'s `empty_result_sql` consume it and
render only their own shape from the shared decision. The non-empty path keeps its
`is_lone_count_distinct`/`has_distinct`/ordinary sub-split as a rendering concern inside the
`SingleGroupAgg` arm, because the empty path collapses those three sub-cases into one
aggregate shape.

### Options Considered

| Option | Verdict |
|--------|---------|
| One shared classifier stopping at the 3-tier priority, sub-split kept in each renderer | ✓ Chosen — matches both consumers' actual contract; the empty path's aggregate shape does not distinguish the sub-cases |
| Push the Case-1/Case-2-3/ordinary single-group split into the enum too | ✗ Rejected — the empty path collapses those three into one shape, so the split belongs to non-empty rendering, not routing |
| Keep two hand-synced trees | ✗ Rejected — the whole point of issue #175 |
| Name the enum `PlanShape` | ✗ Rejected — collides with the unrelated `datafusion-scan/scan-execution-plan-shape` (DataFusion physical-plan shape) concept |

### Consequences

The routing decision is shared by construction: a future rule change (a new aggregate gate,
a new HAVING case) lands once and both paths pick it up identically, closing the exact drift
class issue #175 named. `RequestShape` is scoped to the pushdown-request routing decision and
does not overlap the DataFusion physical-plan-shape concept.
