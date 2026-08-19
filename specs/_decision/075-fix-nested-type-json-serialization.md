# Decisions: fix-nested-type-json-serialization

## ADR: The logical Arrow type stays `Utf8`; the nested type never enters the tag vocabulary

**ID:** nested-logical-type-stays-utf8
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

`iceberg_type_to_arrow` keeps mapping `list`/`struct`/`map` to `DataType::Utf8`, and
`arrow_type_to_tag`/`arrow_type_from_tag` gain no nested grammar. A code trace of the adapter found
that a recursive nested Arrow tag — the direction issue #350 and its research pass proposed — would
make the column a genuine `Struct`/`Map` during DataFusion execution, where DataFusion has no
comparison, ordering, hashing, or aggregation operator for it. That design would oblige the plan to
newly DECLINE five pushdown shapes at five separate decision sites — WHERE filters
(`type_accepted_rewrite`), N-scan per-leg conjuncts (`type_screened_leg_filter`), GROUP BY keys
(`classify_request_shape`), aggregate arguments including `COUNT(DISTINCT)`
(`validate_agg_col_types`), and the broadcast join condition (`render_broadcast_join`) — and to
re-sequence `handle_pushdown`, because `classify_where_filter` runs 37 lines BEFORE
`resolver.resolve` produces the logical schema the gates would need. It would also widen the
`col_types` parameter shape every guard, classifier, and builder in the pushdown layer shares.

### Decision

The JSON rendering is injected at the scan's physical-expression adapter and in the legacy path's
generated SQL, so the column's logical type is the rendered JSON string everywhere it is read: in
the registered DataFusion table schema, the compact `ScanSpec::logical_schema` tag vocabulary, the
pushdown planner's `needs_json_fallback` decisions, and Exasol's own `VARCHAR(2000000)` declaration.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the logical type `Utf8`; render JSON at the scan's physical-expression adapter | ✓ Chosen — leaves all five pushdown decision sites, the global capability constant, and the `ScanSpec` wire tag untouched |
| Make `iceberg_type_to_arrow` recursive and give `arrow_type_to_tag`/`arrow_type_from_tag` a recursive nested grammar | ✗ Rejected — makes the column a genuine nested type during DataFusion execution, forcing five new decline gates and a re-sequenced `handle_pushdown` |

### Consequences

interview A4's conclusion — that no new gate or error path is needed and that no expression
referencing such a column may reach DataFusion as a nested type — is preserved and reached more
cheaply: under this decision no such expression ever sees a nested type, because none exists in the
logical plan. A4's requirement to verify the pushdown shapes live is retained as plan task 16.

## ADR: The nested field descriptor is carried as data, not as a type

**ID:** nested-descriptor-carried-as-data-not-type
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

The vendored `scripts/unity/fixtures/stats-all-types` fixture — the only Delta fixture carrying a
struct — declares `delta.columnMapping.mode = name` and gives its three inner fields physical names
`col-7f2f94cf-…`, `col-26fcfd6b-…`, `col-92dcf16d-…`. Rendering physical names would emit those
opaque identifiers as JSON object names for the common Unity/Databricks column-mapped table shape.

### Decision

`LogicalField` gains an optional, format-neutral nested descriptor: each nested field's LOGICAL name
plus the ONE binding key its format's column-mapping selects (`field_id` XOR `physical_name` XOR
neither) — the same three-way choice `LogicalField` already makes per top-level column, recursed. It
is consumed only by the JSON renderer's name resolution.

### Options Considered

| Option | Verdict |
|--------|---------|
| Carry the nested descriptor as a separate, format-neutral field on `LogicalField` | ✓ Chosen — the column's TYPE is the JSON string; the descriptor is naming information only, and it makes nested rename/reorder/add/drop work rather than becoming a tracked exception |
| Render the file's PHYSICAL nested names and refuse column-mapped Delta tables | ✗ Rejected — would leave this plan with no working Delta struct coverage at all, since the only Delta struct fixture is column-mapped |
| Fold the nested structure into the `arrow_type` tag | ✗ Rejected — this is decision [1]'s rejected alternative; it makes the column a genuine nested type |

### Consequences

Nested rename, reorder, add, and drop all work through the same binding-key mechanism top-level
columns already use, rather than becoming a tracked exception.

## ADR: The JSON shape diverges from the Iceberg spec's Appendix D, deliberately and on the record

**ID:** json-shape-diverges-from-appendix-d
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

The Apache Iceberg table spec's § JSON single-value serialization prescribes *"JSON object by field
ID"* for a struct (`{"1": 1, "2": "bar"}`) and *"JSON object of key and value arrays"* for a map
(`{"keys": ["a","b"], "values": [1,2]}`). Appendix D is scoped to metadata single values — default
values and manifest bounds — not to query results, and the spec defines no JSON encoding for scan
output rows at all.

### Decision

A struct renders as a JSON object keyed by FIELD NAME and a map as a single JSON object keyed by its
stringified key, diverging from Appendix D's shapes.

### Options Considered

| Option | Verdict |
|--------|---------|
| Key a struct by field name and a map by its stringified key | ✓ Chosen — both are readable from Exasol SQL via an ordinary JSON path expression |
| Adopt Appendix D's shapes verbatim for spec conformance | ✗ Rejected — a field-ID-keyed object has no readable path expression, and parallel key/value arrays cannot be read by key at all, from Exasol SQL |

### Consequences

The divergence is recorded in the feature's Background with the scoping sentences quoted, per
CLAUDE.md's rule that a deviation is never a silent gap.

## ADR: Statistics pruning over a rendered nested column requires positive proof, not absence of failure

**ID:** nested-pruning-requires-positive-proof
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

A spike `EXPLAIN ANALYZE` of `WHERE tags = '["hello","world"]'` showed DataFusion DOES construct a
`pruning_predicate` and a bloom-filter stage over the JSON-rendered column. It pruned nothing in that
run, but the fixture had ONE row group, which cannot distinguish "statistics unavailable" from
"statistics available and happened to match". Parquet keeps statistics for a nested column's LEAF
values, so a min/max of `"hello"`/`"world"` compared against the document `["hello","world"]`
evaluates `"hello" <= '["hello","world"]'` as FALSE — `[` sorts below `h` — and would prune a row
group that does contain the match.

### Decision

The plan carries a dedicated task and spec clause requiring a MULTI-row-group Parquet fixture whose
per-group leaf statistics would falsely exclude the rendered document, and requiring the offending
pruning stage to be disabled for the column if any stage evaluates it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Require a multi-row-group fixture that positively proves no row is falsely pruned | ✓ Chosen — row loss from pruning is silent, so absence-of-failure on a single-row-group fixture proves nothing |
| Accept the spike observation that nothing was pruned and no error occurred | ✗ Rejected — a single-row-group fixture cannot discriminate "statistics unavailable" from "statistics available and happened to match" |

### Consequences

This is the one claim in the plan that a passing observation does not settle by itself — it is the
one silent-wrong-rows failure mode this design admits, so it gets a dedicated positive-proof
requirement rather than being inferred from the absence of an observed failure.

## ADR: Disable Parquet row-filter pushdown rather than decline the predicate to Exasol

**ID:** disable-parquet-row-filter-pushdown-for-nested-column
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

A spike measured a silent wrong-rows bug: DataFusion approves filter pushdown against the TABLE
schema (where the column is `Utf8`) and removes the `FilterExec`; at file-open time `build_row_filter`
re-checks against the PHYSICAL schema (nested), sets `non_primitive_columns = true`, and drops the
conjunct. `WHERE tags = '["hello","world"]'` returned BOTH rows and
`WHERE id = 2 AND tags = '["hello","world"]'` returned row 2 instead of nothing. A live run through
Exasol confirmed the bug end to end across `=`, `<>`, `>`, `IN`, `LIKE`, `UPPER(col) =`, and
`LENGTH(col) =`, and on both Iceberg and Delta. This is already true TODAY for a `list` column — a
pre-existing silent wrong-rows bug — and for `struct`/`map` the fix would otherwise convert today's
hard error into a silent wrong answer.

### Decision

When a table's registered schema carries a JSON-rendered nested column, the scan disables Parquet
row-filter pushdown for that table, so the optimizer keeps a `FilterExec` that evaluates the
predicate over the rendered `Utf8` column.

### Options Considered

| Option | Verdict |
|--------|---------|
| Disable Parquet row-filter pushdown for the whole table when it carries a nested column | ✓ Chosen — `pushdown_filters = false` made every measured query correct; transfers fewer rows across the `.so` boundary than declining to Exasol; needs no re-sequencing of `handle_pushdown`; fixes the pre-existing `list` bug in the same stroke |
| Decline the predicate in the VS adapter and self-apply it in the Exasol wrapper (`type_accepted_rewrite`) | ✗ Rejected — would need five separate decision sites, the same blast radius decision [1] rejected |
| Accept the current behavior | ✗ Rejected — ships a known silent wrong-rows bug beside its own fix |

### Consequences

The accepted cost is named: a query over a table carrying a nested column loses Parquet row-level
filter pushdown for all its columns, so late materialization no longer skips rows within a row
group. Row-group and page pruning from statistics is a separate stage, covered by the pruning ADR
above.

## ADR: The declared nested descriptor is the single signal for the diversion AND the pushdown withdrawal

**ID:** nested-descriptor-single-signal-diversion-and-withdrawal
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

Code review found that keying the cast diversion on the PHYSICAL Arrow type (so a spec authored
before the descriptor existed still renders) reintroduces the very bug this plan closes: a physical
type is unavailable before file open, so the pushdown withdrawal can only read the descriptor. Keying
the diversion on the physical type instead means a descriptor-less spec over a physically nested
column is rendered while `pushdown_filters` stays `true`, DataFusion approves the pushdown against
the `Utf8` logical schema, drops the conjunct against the physical nested schema, and returns EVERY
row.

### Decision

`ColumnBinding::nested_columns` keys the cast diversion on the logical field's declared nested member
descriptor — the same signal `raw_scan::renders_nested_json` reads to withhold Parquet row-filter
pushdown — additionally requiring the resolved column's type to be one of the five nested variants. A
physically nested column declaring no descriptor is left to the delegate, which fails loudly.

### Options Considered

| Option | Verdict |
|--------|---------|
| Key both the diversion and the pushdown withdrawal on the declared nested descriptor; fail loudly for a descriptor-less physically nested column | ✓ Chosen — failing the cast loudly is the only outcome that cannot silently lose a predicate |
| Key the diversion on the physical Arrow type, so a spec authored before the descriptor existed still renders | ✗ Rejected — shipped and code review found it reintroduces the silent wrong-rows bug this plan closes |

### Consequences

The serde migration promise is untouched: a legacy spec still deserializes, it just no longer
renders. The extra type check narrows the descriptor-keyed set so a descriptor the file's own type
contradicts is not fed to an encoder that would quote it.
