# Decision Log: fix-nested-type-json-serialization

## Interview

**Q1 — Binary scope:** The issue's own type-mapping table lumps Binary in with List/Struct/Map as an
"incompatible Arrow type serialized to JSON." Research confirmed Binary today falls through the same
Display-text fallback (raw hex/Display text, not a quoted JSON string) — same defect, just not called
out in the issue title. Should this plan fix Binary too, or stay scoped to List/Struct/Map?

**A1:** Binary has its own GitHub issue, #351. This plan is List/Struct/Map only — do not touch
Binary's JSON-serialization behavior at all (its current CAST-to-VARCHAR/Display-text path is out of
scope and must be left exactly as-is).

**Q2 — Map key handling:** Iceberg/Arrow Map keys aren't required to be strings (could be Int32,
Int64, decimal, date, uuid, etc.), but a JSON object requires string keys per RFC 8259. How should a
non-string-keyed Map be represented?

**A2:** Stringify the keys. Convert every non-string map key to its canonical string representation
before emitting the JSON object — `42` becomes `"42"`, `true` becomes `"true"`, a date becomes
`"2026-08-18"`, etc. This is the standard cross-ecosystem answer (serde_json, Python `json.dumps`,
JavaScript `JSON.stringify`, Java Jackson all do this), and it's lossless for every primitive type
Iceberg permits as a map key. The user explicitly rejected an array-of-pairs alternative
(`[{"key":42,"value":"v"},...]`) because it would preserve key types losslessly but break normal
JSON-object ergonomics (e.g. make `JSON_EXTRACT` on the emitted VARCHAR unusable) — an array-of-pairs
shape must NOT be built. The user acknowledged the tradeoff (key's original type is lost on read-back)
is acceptable because the schema already carries the true Iceberg/Delta type via
`createVirtualSchema`, so a consumer that cares can cast back explicitly.

**Q3 — The #357 nested `delta.typeChanges` gap:** `build_delta_table_schema` today validates
`delta.typeChanges` only on top-level columns; once Struct/Map/Array become scannable, an inner
field's own type-change annotation could go unvalidated. Fix it in this plan, demonstrate it's a
non-issue, or defer to a new tracked issue?

**A3:** Fix it in this plan. Specifically:
- Extend `build_delta_table_schema` to recurse into Struct/Map/Array fields and validate
  `delta.typeChanges` on nested `StructField`s, reading each entry's `fieldPath`.
- For a supported nested type change: no special action needed — the Parquet reader resolves it, and
  the JSON renderer serializes whatever Arrow hands back.
- For an unsupported nested type change: refuse the table at plan time, using the SAME refused-column
  mechanism the top-level check already uses (`ClassifiedDeltaColumn::Refused` / `RefusedColumn`),
  naming the offending nested field's path and both types.

**Q4 — Pushdown reachability of List/Struct/Map columns in WHERE/GROUP BY/ORDER BY/aggregate/join
expressions:** Initial framing assumed these columns stay `Utf8` all the way through DataFusion
execution, making comparisons "safe" as ordinary string operations. That assumption was corrected
mid-interview on the premise that the fix requires the scan's LOGICAL schema to carry the NATIVE
nested Arrow type, because that was believed to be the only way to avoid the plan-time cast failure —
which would make such a column a genuine nested type during DataFusion execution and break any
DataFusion-side comparison, grouping, or join on it.

**A4 (final, after correction):** No new gate or error path is needed. The VS adapter already owns
the pushdown SQL text it hands to Exasol/DataFusion — it does not have to push every expression into
the DataFusion-native scan. The resolution is architectural, not a new check:
1. DataFusion's raw scan projects the column as its real native type (Struct/Map/List) and
   JSON-serializes it only at the emit boundary, per Q4's corrected premise.
2. Any expression that REFERENCES a column whose underlying Arrow type is Struct/Map/List (a WHERE
   predicate, a GROUP BY/ORDER BY key, an aggregate argument, a join condition) is NOT pushed into the
   DataFusion-native scan SQL at all — it stays in the Exasol-dialect pushdown-wrapper SQL, where it
   operates on the ALREADY-serialized JSON VARCHAR value (the exact type Exasol itself declared for
   that column via `createVirtualSchema`). This is the existing "adapter declines a pushdown
   capability it cannot faithfully push into DataFusion, and generates the equivalent SQL itself"
   pattern CLAUDE.md's "Virtual Schema pushdown delegation" section already describes, and matches the
   `vs-adapter/pushdown-declined-filter-self-apply` spec area found during discovery — this plan
   should verify/extend that existing decline mechanism to cover List/Struct/Map columns rather than
   invent a new one. No type mismatch, no wrong results, no error — the cost is only that DataFusion
   scans more rows than a pushed filter would (a performance tradeoff, not a correctness gap), which
   must be named explicitly in the plan as an accepted tradeoff. The plan MUST verify live (per
   CLAUDE.md's verification-discipline rule) that the capability/decline logic actually already treats
   List/Struct/Map-typed expressions this way (or make it do so if it doesn't), rather than assuming
   it from code inspection alone.

## Design Decisions

### [1] The logical Arrow type stays `Utf8`; the nested type never enters the tag vocabulary

- **Decision:** `iceberg_type_to_arrow` keeps mapping `list`/`struct`/`map` to `DataType::Utf8`, and
  `arrow_type_to_tag`/`arrow_type_from_tag` gain no nested grammar. The JSON rendering is injected at
  the scan's physical-expression adapter and in the legacy path's generated SQL, so the column's
  logical type is the rendered JSON string everywhere it is read.
- **Alternatives:** Make `iceberg_type_to_arrow` recursive and carry a recursive nested Arrow tag on
  the wire — the direction issue #350 and its research pass proposed, and the premise interview A4 was
  answered against.
- **Rationale:** A nested logical type makes the column a genuine `Struct`/`Map` during DataFusion
  execution, where DataFusion has no comparison, ordering, hashing, or aggregation operator for it. A
  code trace of the adapter found that the recursive-tag design would then oblige the plan to newly
  DECLINE five pushdown shapes at five separate decision sites — WHERE filters
  (`type_accepted_rewrite`), N-scan per-leg conjuncts (`type_screened_leg_filter`), GROUP BY keys
  (`classify_request_shape`), aggregate arguments including `COUNT(DISTINCT)`
  (`validate_agg_col_types`), and the broadcast join condition (`render_broadcast_join`) — and to
  re-sequence `handle_pushdown`, because `classify_where_filter` runs 37 lines BEFORE
  `resolver.resolve` produces the logical schema the gates would need. It would also widen the
  `col_types` parameter shape that every guard, classifier, and builder in the pushdown layer shares.
  Keeping the logical type `Utf8` leaves all five sites, the global capability constant, and the
  `ScanSpec` tag vocabulary untouched, and reduces the blast radius to the scan side plus the Delta
  schema build.
- **Supersedes:** the "native nested Arrow type in the logical schema" premise of interview A4. A4's
  CONCLUSION — that no new gate or error path is needed and that no expression referencing such a
  column may reach DataFusion as a nested type — is preserved and reached more cheaply: under this
  decision no such expression ever sees a nested type, because none exists in the logical plan. A4's
  requirement to verify the pushdown shapes live is retained as plan task 16.
- **Promotes to ADR:** yes

### [2] The nested field descriptor is carried as data, not as a type

- **Decision:** `LogicalField` gains an optional, format-neutral nested descriptor: each nested
  field's LOGICAL name plus the ONE binding key its format's column-mapping selects (`field_id` XOR
  `physical_name` XOR neither) — the same three-way choice `LogicalField` already makes per top-level
  column, recursed. It is consumed only by the JSON renderer's name resolution.
- **Alternatives:** (a) render the file's PHYSICAL nested names and refuse column-mapped Delta tables;
  (b) fold the nested structure into the `arrow_type` tag, i.e. decision [1]'s rejected alternative.
- **Rationale:** The vendored `scripts/unity/fixtures/stats-all-types` fixture — the only Delta fixture
  carrying a struct — declares `delta.columnMapping.mode = name` and gives its three inner fields
  physical names `col-7f2f94cf-…`, `col-26fcfd6b-…`, `col-92dcf16d-…`. Rendering physical names would
  emit those opaque identifiers as JSON object names for the common Unity/Databricks column-mapped
  table shape, and refusing such tables would leave this plan with no working Delta struct coverage at
  all, so alternative (a) fails the issue's stated outcome. Carrying the descriptor separately from the
  type is the honest model: the column's TYPE is the JSON string; the descriptor is the naming
  information the renderer needs. It also makes nested rename, reorder, add, and drop work rather than
  becoming a tracked exception.
- **Promotes to ADR:** yes

### [3] Map keys are stringified through the JSON encoder, and non-string keys are handled by us

- **Decision:** Before encoding, a `Map` whose key child is not `Utf8`-family has that child replaced
  by a `Utf8` array: a nested key type is stringified as its own JSON rendering, every other type
  through the Arrow-to-`Utf8` cast, and a key type the cast rejects surfaces a clean error.
- **Alternatives:** Rely on `arrow-json`'s own map encoder; or emit an array of `{"key","value"}` pairs.
- **Rationale:** `arrow-json` 58.3.0's `MapEncoder::try_new` REFUSES every non-`Utf8` key with
  `"Only UTF8 keys supported by JSON MapArray Writer"`, verified live against `Int32`, `Date32`,
  `Boolean`, and `Decimal128` keys — so the stringification is not an ergonomic preference but the only
  way to render a map the Iceberg spec permits (§ Nested Types: *"Both map keys and map values may be
  any type, including nested types"*). The array-of-pairs shape was explicitly rejected in interview A2.
- **Promotes to ADR:** no

### [4] `explicit_nulls(true)`, and a null cell is SQL NULL rather than the text `null`

- **Decision:** The encoder runs with `explicit_nulls(true)`, so a null struct field and a null map
  value render as `"name":null`. A null top-level cell is guarded before encoding and emits an Arrow
  null, which becomes `Value::Null`.
- **Alternatives:** `arrow-json`'s default `explicit_nulls = false`, which OMITS null struct fields and
  null map values.
- **Rationale:** Omitting a null field makes one column's object shape vary row by row, so an Exasol
  `JSON_VALUE` path silently disappears on some rows — the JSON becomes unreliable for exactly the SQL
  access the JSON-`VARCHAR` contract exists to enable. The null guard is mandatory, not defensive:
  `Encoder::encode`'s contract states the behaviour is unspecified at a null index, and unguarded it
  renders a null struct as `{}` and a null list as `[]` — both valid JSON and both wrong — while a
  `DataType::Null` child panics through an `unreachable!()`.
- **Promotes to ADR:** no

### [5] Struct key order is the physical file's order, deliberately not canonicalized

- **Decision:** The rendered JSON object names appear in the physical file's field order. No
  lexicographic or schema-order re-sort is applied.
- **Alternatives:** Sort object names lexicographically for a file-independent canonical rendering.
- **Rationale:** The Iceberg spec makes struct field order non-semantic (§ Column Projection: *"The
  table schema's column names and order may change after a data file is written"*; § Schema Evolution
  permits *"reordering existing fields"*), so two files of one table may legally differ and a
  logically-equal value can render as two distinct strings, which `GROUP BY` and `DISTINCT` would
  separate. That is a semantic oddity, NOT an engine disagreement: the DataFusion-side and Exasol-side
  views read the same rendered string. Canonicalizing would change the rendering for every
  single-layout table — the overwhelming majority — to satisfy a rare one, and would require recursive
  `StructArray` reconstruction.
- **Promotes to ADR:** no

### [6] The JSON shape diverges from the Iceberg spec's Appendix D, deliberately and on the record

- **Decision:** A struct renders as a JSON object keyed by FIELD NAME and a map as a single JSON object
  keyed by its stringified key — not Appendix D's *"JSON object by field ID"* (`{"1": 1, "2": "bar"}`)
  and *"JSON object of key and value arrays"* (`{"keys":["a","b"],"values":[1,2]}`).
- **Alternatives:** Adopt Appendix D's shapes verbatim for spec conformance.
- **Rationale:** Appendix D is scoped to metadata single values — § Schemas applies it to default
  values, § Bound serialization to manifest bounds — and the Iceberg spec defines no JSON encoding for
  scan output rows at all, so this is a divergence from an out-of-scope section rather than a spec
  violation. Both Appendix D shapes are unusable from Exasol SQL: a field-ID-keyed object has no
  readable path expression, and parallel key/value arrays cannot be read by key — which is precisely
  the ergonomic argument interview A2 used to reject array-of-pairs, applied consistently. Recorded in
  the feature's Background with the scoping sentences quoted, per CLAUDE.md's rule that a deviation is
  never a silent gap.
- **Promotes to ADR:** yes

### [7] `needs_json_fallback` is left untouched and a narrower nested predicate is added beside it

- **Decision:** A new single-owner predicate covers exactly `List`, `LargeList`, `FixedSizeList`,
  `Struct`, and `Map`. `needs_json_fallback` keeps its signature and its answer for every input.
- **Alternatives:** Widen `needs_json_fallback` to mean "JSON-rendered" and let the nested set be its
  members.
- **Rationale:** `needs_json_fallback` is also true for `Binary` and an out-of-range `Decimal128`, both
  of which must keep the `CAST(col AS VARCHAR)` path this plan leaves byte-identical (interview A1,
  issue #351). It also gates the recorded TopN decline, which must keep declining an out-of-range
  decimal sort key and must keep NOT declining a nested one. The two predicates answer different
  questions: "does this type need serializing at all" versus "is this type rendered by the JSON
  encoder".
- **Promotes to ADR:** no

### [8] `binary` stays refused at every nesting depth on the Delta path

- **Decision:** A Delta `array`, `struct`, or `map` containing a `binary` member stays refused, joining
  the already-recorded `array<binary>` refusal. `binary_refusal`'s cited issue moves from #350 to #351.
- **Alternatives:** Admit nested `binary`, which the encoder renders as faithful lowercase hexadecimal
  — the same convention Iceberg's Appendix D gives `binary` and `fixed`.
- **Rationale:** Interview A1 scopes Binary out entirely. Admitting it nested would widen Binary's
  reach, which is issue #351's decision to make. The citation move is required by this feature's own
  recorded rule that *"a closed issue cited in a shipped error text reads as an unfixed gap with no
  owner"*, and is a message edit, not a behavior change. The resulting asymmetry — an ICEBERG nested
  `binary` IS rendered as hexadecimal, because the Iceberg reader refuses no type at all — is
  pre-existing and named in the spec rather than hidden.
- **Promotes to ADR:** no

### [9] `arrow_value_at` gains no nested arm

- **Decision:** The per-value converter in `scan/convert.rs` is not changed. Its wildcard
  display-string fallback stays byte-identical.
- **Alternatives:** Add a nested arm calling the shared encoder as a defensive backstop, which would
  also make the recorded `scan-execution` scenario literally true at the unit level.
- **Rationale:** The rendering now happens at the Arrow column level upstream, and `arrow_value_at`'s
  only callers are the partial-aggregate paths, which carry group keys and aggregate results — never a
  nested column. An arm there would be unreachable code, and
  `datafusion-scan/type-mapping-module-structure` records that this wildcard arm stays a wildcard,
  unrouted through the classifier. The `scan-execution` scenario is amended to state where the
  rendering happens instead.
- **Promotes to ADR:** no

### [10] One recursive Delta walk produces three answers

- **Decision:** `delta_schema.rs` gets ONE recursion over a column's nested tree that simultaneously
  classifies renderability, validates nested `delta.typeChanges`, and reads nested column-mapping
  annotations into the descriptor.
- **Alternatives:** Three independent walks, one per concern.
- **Rationale:** Three walks could disagree about which nested fields exist — a classifier that
  refuses a subtree the validator still visits, or a descriptor naming a field the classifier dropped.
  One visit per field makes that disagreement unrepresentable, and the three answers are all functions
  of the same node.
- **Promotes to ADR:** no

### [11] Nested-level type widening is rendered at the file's physical type, and named as a limitation

- **Decision:** No cast is applied to a nested member whose physical type differs from the current
  logical one. A Delta `decimal(10,1)` → `decimal(12,3)` widening renders `1.5` in the old file and
  `1.500` in the new one.
- **Alternatives:** Cast nested members to the current logical nested type before rendering.
- **Rationale:** Interview A3 already settled this: *"For a supported nested type change: no special
  action needed — the Parquet reader resolves it, and the JSON renderer serializes whatever Arrow
  hands back."* It is also forced by this repo's recorded contract that `delta.typeChanges` is a
  validation input and never a cast input, since a conforming writer may legally remove the annotation.
  The consequence is a rendering difference, never a wrong value, and it is stated in the feature's
  Background rather than left for a reader to discover.
- **Promotes to ADR:** no

### [12] Statistics pruning over a rendered nested column requires positive proof, not absence of failure

- **Decision:** The plan carries a dedicated task and spec clause requiring a MULTI-row-group Parquet
  fixture whose per-group leaf statistics would falsely exclude the rendered document, and requiring
  the offending pruning stage to be disabled for the column if any stage evaluates it.
- **Alternatives:** Accept the spike observation that nothing was pruned and no error occurred.
- **Rationale:** A spike `EXPLAIN ANALYZE` of `WHERE tags = '["hello","world"]'` showed DataFusion
  DOES construct `pruning_predicate=tags_null_count@2 != row_count@3 AND tags_min@0 <=
  ["hello","world"] AND ["hello","world"] <= tags_max@1` plus a bloom-filter stage over the
  JSON-rendered column. It pruned nothing (`row_groups_pruned_statistics=1 total → 1 matched`,
  `statistics_eval_time=2ns`), but the fixture had ONE row group, which cannot distinguish "statistics
  unavailable" from "statistics available and happened to match". Parquet keeps statistics for a
  nested column's LEAF values, so a min/max of `"hello"`/`"world"` compared against the document
  `["hello","world"]` evaluates `"hello" <= '["hello","world"]'` as FALSE — `[` sorts below `h` — and
  would prune a row group that does contain the match. Row loss from pruning returns fewer rows with
  no error, so it is the one silent-wrong-rows failure mode this design admits and the only claim in
  the plan that a passing observation does not settle.
- **Promotes to ADR:** yes

### [13] Disable Parquet row-filter pushdown rather than decline the predicate to Exasol

- **Decision:** When a table's registered schema carries a JSON-rendered nested column, the scan
  disables Parquet row-filter pushdown for that table, so the optimizer keeps a `FilterExec` that
  evaluates the predicate over the rendered `Utf8` column.
- **Alternatives:** Decline the predicate in the VS adapter and self-apply it in the Exasol wrapper via
  `type_accepted_rewrite` — one decision site, not the five a nested logical type would need; or accept
  the measured behavior.
- **Rationale:** A spike measured a silent wrong-rows bug. DataFusion 54.1 approves the filter pushdown
  against the TABLE schema, where the column is `Utf8` and therefore primitive, and the optimizer
  removes the `FilterExec`; at file-open time `build_row_filter` re-checks against the PHYSICAL schema,
  finds a nested column, sets `non_primitive_columns = true`, returns `None`, and the conjunct is
  dropped and applied nowhere. `WHERE tags = '["hello","world"]'` returned BOTH rows and
  `WHERE id = 2 AND tags = '…'` returned row 2 instead of nothing, with
  `pushdown_rows_matched=0, pushdown_rows_pruned=0, predicate_evaluation_errors=0`. A separate live run
  through Exasol against a freshly built pristine `9b39cbf` `.so` then confirmed the same bug end to
  end and showed it is broader than one operator: `=`, `<>`, `>`, `IN`, `LIKE`, `UPPER(col) =`, and
  `LENGTH(col) =` each matched all 4 rows of a seeded Iceberg table, `COUNT(*)` under such a predicate
  returned 4, `WHERE ID > 2 AND TAGS = 'zzz'` returned rows 3 and 4 — primitive conjunct applied,
  nested one dropped — and a Delta `array<integer>` reproduced it identically, while plain Iceberg
  `VARCHAR` and Delta `STRING` control columns correctly returned 0 rows. `IS NULL`, `IS NOT NULL`, and
  select-list expressions over the column were already correct and must stay correct. `EXPLAIN VIRTUAL`
  showed the predicate inside the scan spec with no compensating outer WHERE — exactly the delegation
  hazard CLAUDE.md warns about. This is already
  true TODAY for a `list` column — a pre-existing silent wrong-rows bug of the same root cause — and
  for `struct`/`map` the fix would otherwise convert today's hard error into a silent wrong answer,
  which is a regression in kind. `pushdown_filters = false` made every measured query correct, proving
  the rendering expression evaluates fine inside a `FilterExec`. Keeping the predicate in DataFusion
  transfers fewer rows across the `.so` boundary than declining it to Exasol, needs no re-sequencing of
  `handle_pushdown`, and fixes the pre-existing `list` bug in the same stroke.
- **Promotes to ADR:** yes

### [14] The delegated adapter receives the whole physical FieldRef, not just its data type

- **Decision:** The logical schema handed to `DefaultPhysicalExprAdapter` substitutes the bound
  physical FIELD entire — name, type, nullability, and metadata — for each nested column, and the
  substituting tree rewrite stops at the node it replaces instead of descending into it.
- **Alternatives:** Substitute only the data type, keeping the logical field's metadata and
  nullability; let the rewrite recurse normally.
- **Rationale:** Both were measured, and both fail. `DefaultPhysicalExprAdapter` emits a bare `Column`
  only on FULL field equality and otherwise emits a cast, so a data-type-only substitution leaves a
  metadata difference for any file whose nested column carries no `PARQUET:field_id` — an Iceberg
  name-mapping file, a Delta `none`-mode table, or plain Hive Parquet — and the scan dies with
  `Cast error: Casting from Utf8 to Struct(...) not supported`, with the JSON wrap trapped inside the
  cast. And a rewrite that re-enters its own output wraps the same column endlessly. Recording both is
  what keeps a later refactor from reintroducing either.
- **Promotes to ADR:** no

### [15] The Iceberg path has no refused-column mechanism, and this plan does not add one

- **Decision:** The plan removes the CAUSE of both formats' nested-column failure and leaves the
  Iceberg format reader's empty refused-column list exactly as it is.
- **Alternatives:** Give the Iceberg reader a refused-column mechanism mirroring Delta's, so both
  formats fail at plan time with the same message shape.
- **Rationale:** Live runs showed the same defect surfacing at two different layers.
  `adapter/pushdown/format/iceberg.rs` hardcodes `refused_columns: Vec::new()`, so an Iceberg nested
  column is never refused and the query dies at SCAN time inside the UDF —
  `scan failed: assigned data could not be read: Execution error: Cannot cast column 'addr' …`. Delta
  refuses the same column at PLAN time in the adapter with a readable per-column reason. Once nested
  columns render correctly neither surface is reachable, so building an Iceberg refusal mechanism would
  add a code path with no remaining trigger. The asymmetry is recorded because it explains why the two
  formats' recorded scenarios differ in shape, and `vs-adapter/delta-type-mapping` already notes that
  the Iceberg reader refuses no type at all.
- **Promotes to ADR:** no

### [16] The declared nested descriptor is the single signal for the diversion AND the pushdown withdrawal

- **Decision:** `ColumnBinding::nested_columns` keys the cast diversion on the logical field's declared
  nested member descriptor — the same signal `raw_scan::renders_nested_json` reads to withhold Parquet
  row-filter pushdown — and additionally requires the resolved column's type to be one of the five
  nested variants. A physically nested column declaring no descriptor is left to the delegate, which
  has no struct-to-text kernel and fails loudly.
- **Alternatives:** Key the diversion on the physical Arrow type, so a spec authored before the
  descriptor existed still renders.
- **Rationale:** That alternative shipped, and code review found it reintroduces the very bug this plan
  closes. A physical type is unavailable before file open, so the pushdown withdrawal can only read the
  descriptor; keying the diversion on the physical type instead means a descriptor-less spec over a
  physically nested column is rendered while `pushdown_filters` stays `true`, DataFusion approves the
  pushdown against the `Utf8` logical schema, drops the conjunct against the physical nested schema,
  and returns EVERY row. Failing the cast loudly is the only outcome that cannot silently lose a
  predicate. The serde migration promise is untouched: a legacy spec still deserializes, it just no
  longer renders. The extra type check is not a second owner — it narrows the descriptor-keyed set so a
  descriptor the file's own type contradicts (resolved verbatim to a primitive) is not fed to an
  encoder that would quote it.
- **Promotes to ADR:** yes

### [17] The join-condition shape is verified against a second table, not a self-join

- **Decision:** `nested_columns_push_down_as_the_declared_varchar_in_every_shape` verifies the
  join-condition shape by joining `complex_probe` to a purpose-seeded second Iceberg table,
  `complex_join_probe`, whose plain `string` column holds the documents `tags` renders to.
- **Alternatives:** Alias `complex_probe` to itself and join on the rendered column; leave the shape
  unverified and record a tracked exception.
- **Rationale:** The self-join shape hits a pre-existing adapter aliasing defect that reproduces with a
  plain primitive column and has nothing to do with nested rendering, so fixing it is out of scope and
  working around it would mask it. A second, distinct table is not a workaround but the shape the spec
  scenario actually names, and it discriminates: the partner carries an orphan document no probe row
  renders, so a cross-join would fail the assertion. Verified live against the Docker Exasol stack —
  the join returns exactly the two paired rows and `EXPLAIN VIRTUAL` shows it driving the scan UDF —
  so no tracked exception is owed for this shape.
- **Promotes to ADR:** no

## Review Findings
