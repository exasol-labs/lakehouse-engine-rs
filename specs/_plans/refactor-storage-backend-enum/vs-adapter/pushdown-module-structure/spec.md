# Feature: Pushdown Module Structure

Organizes the pushdown planning layer into concern-owned submodules behind a frozen public façade, so each pushdown decision has one home and the generated SQL is pinned by golden assertions rather than by convention.

## Background

<!-- DELTA:NEW -->
* This delta carves the scan spec's `storage` value out of SEVEN behavior-preservation gates of this feature: one Background bullet — the generated-SQL gate bullet — and one clause in each of six scenarios: "Behavior is unchanged across the refactor", "One blind traversal primitive backs every column-collecting walk", "One ordered pipeline function owns the type-rewrite pass order", "The dispatcher builds each fan-out spec from one shared shard-invariant base", "Both qualified single-table fallback guards call one shared helper", and "One classifier decides the request shape for both the dispatch and empty-result paths". It supersedes no other Background bullet and changes no structural rule.
* `vs-adapter/storage-backend-enum` (issue #274) wraps the scan spec's `storage` value in an externally-tagged backend variant. That value is embedded in the scan-driving SQL, so the two `dispatch_golden` decline-wrapper goldens (`group_by_fallback.sql`, `multi_count_distinct_decline.sql`) and three of the four join golden-SQL full-string assertions change by exactly one substring each. Those two goldens are the very output the "Both qualified single-table fallback guards call one shared helper" scenario gates, and `storage` is one of the shard-invariant fields the "one shared shard-invariant base" scenario's GIVEN enumerates, which is why both need the carve-out.
* The carve-out is narrow and directional: it permits an edit to the `storage` value ALONE. Every other byte of every golden, and every non-golden assertion, stays unedited — that unchanged remainder is what keeps this feature's gate falsifiable rather than retiring it.
* No column-collecting walk, no type-rewrite pass, no pipeline order, and no visibility rule changes. `vs-adapter/storage-backend-enum` edits no file under `pushdown/` except the golden fixtures and the golden-string literals.
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
* The generated-SQL gate of the "One blind traversal primitive backs every column-collecting walk" scenario stays the proof for this delta, narrowed to permit the `storage` value alone: the four join golden-SQL full-string assertions, the two `dispatch_golden` decline-wrapper assertions, and the declined-`ORDER BY` hidden-column assertions MUST all pass with no edit to any assertion or expected value outside that value, which `vs-adapter/storage-backend-enum` re-encodes as an externally-tagged backend variant. The collected table set drives side-local versus cross-side conjunct partitioning and is therefore visible in every other byte of the generated SQL.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Behavior is unchanged across the refactor

* *GIVEN* the pre-refactor unit and integration test suites for the pushdown planning layer
* *WHEN* the suites run against the refactored code
* *THEN* every test MUST pass with no change to any test assertion or expected value, EXCEPT the scan spec's `storage` value wherever an assertion embeds one
* *AND* the scan-driving SQL generated for a given pushdown request MUST be byte-identical to the pre-refactor output EXCEPT for that `storage` value's variant tag, whose tagged payload `vs-adapter/storage-backend-enum` requires to be byte-identical to the untagged encoding
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: One blind traversal primitive backs every column-collecting walk

* *GIVEN* the three read-only column-collecting walks that each hand-roll the same recursion — match a JSON object, act when its `type` is `column`, then recurse over every field value; match a JSON array and recurse over every element; stop at any other node — namely `collect_all_column_names` in `pushdown/support.rs`, plus `column_tables` (`collect_column_tables` before this delta) and `collect_side_column_names` in `pushdown/joins/rendering.rs`
* *WHEN* the pushdown layer collects column references from a `selectList`, a `filter` or `having` expression tree, a `groupBy` or `orderBy` array, or a join condition
* *THEN* exactly one traversal primitive in the shared `support` submodule SHALL own both the recursion and the `type == "column"` test, invoking a caller-supplied callback once per `column` object node and passing that node's own field map
* *AND* the primitive SHALL traverse blindly — every field of every object and every element of every array — because a collect rebuilds nothing, and a column reference can sit arbitrarily deep inside a function call, a `CASE`, or a comparison predicate
* *AND* the primitive SHALL be declared `pub(super)` in `support`, which already reaches `pushdown` and its `joins::rendering` descendant, so NO item's visibility widens and no join-module `use` path changes
* *AND* each of the three walks SHALL reduce to one call to that primitive with a closure over the `column` node's field map, and SHALL NOT retain a JSON-object or JSON-array recursion arm of its own, so the pushdown module tree holds ONE blind column-collecting traversal instead of three
* *AND* each closure MUST keep its predecessor's case-folding call verbatim — the Unicode `to_uppercase` for `collect_all_column_names`, the ASCII-only `to_ascii_uppercase` for both joins walks — because the two disagree for non-ASCII column and table names, so unifying them SHALL NOT happen under this scenario
* *AND* the column-tables walk MUST keep its `pub(super)` visibility, but SHALL carry its three outputs as RETURN VALUES rather than as `&mut` accumulator out-parameters — `column_tables(expr: &Json) -> (HashSet<String>, bool, bool)`, returning the folded `tableName` set, the untagged-column flag, and the any-column flag — because both call sites want three fresh values per use and one of them is inside a loop where per-iteration freshness is required; the walk's per-node decision, its `tableName` attribution, and its ASCII-only fold are unchanged, and only the transport of its results changes
* *AND* `collect_side_column_names` MUST keep its private visibility and its signature; that "compile unedited" guarantee now scopes to `collect_side_column_names` ALONE, and this scenario no longer asserts that `conjunct_single_side`, `referenced_side_columns`, or the N-scan side-attribution caller in `joins/sql_builders.rs` compile unedited — issue #181 edits all three, the two column-tables callers to destructure the returned tuple and `referenced_side_columns` to walk the shared clause set of `vs-adapter/pushdown-joins-module-structure`'s "One clause walk feeds both wrapper column-narrowing routines"
* *AND* every existing pushdown, joins, and top-N test MUST pass with no change to any test assertion or expected value EXCEPT the scan spec's `storage` value, including the four join golden-SQL full-string assertions, the two `dispatch_golden` decline-wrapper assertions — the declined `GROUP BY` fallback and the multi/mixed `COUNT(DISTINCT)` decline, whose committed goldens both carry a narrowed inner-scan `projection` — and the declined-`ORDER BY` hidden-column assertions; this is the gate that makes "no behavior change" falsifiable here, because the collected sets drive side-local versus cross-side conjunct partitioning, per-side column narrowing, the qualified-wrapper inner-scan projection, and the hidden-column append order, every one of which is visible in the generated SQL OUTSIDE its `storage` value
* *AND* the permitted `storage` edit SHALL be exactly the externally-tagged re-encoding `vs-adapter/storage-backend-enum` specifies, and no assertion SHALL be weakened, disabled, or deleted to accommodate it, because a golden whose every other byte is unchanged is what distinguishes an accepted wire re-encoding from a pushdown regression
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: One ordered pipeline function owns the type-rewrite pass order

* *GIVEN* the ordered type-rewrite pass sequences previously written out at their call sites as an explicit `.and_then` / `.map` chain — the three-pass filter sequence (LIKE-subject guard, then string-function-argument guard, then decimal-stringification rewriter) and the two-pass select-list sequence (string-function-argument guard, then decimal-stringification rewriter) — with the order's load-bearing rationale recorded only as a prose comment beside one of those chains
* *WHEN* the adapter applies type rewrites to a single-table WHERE-clause filter tree, or to a select-list expression item in `project_columns`, including the select-list item reached through the broadcast join's shared use of `project_columns`
* *THEN* exactly ONE function in the shared `support` submodule SHALL own the pass sequence for BOTH render surfaces, and every caller SHALL invoke that function instead of sequencing the passes itself, so adding or reordering a pass is an edit inside one function body rather than an edit at every caller
* *AND* that function's doc comment SHALL state its pass order AND why that order is load-bearing, so the rationale lives with the code that enforces it rather than beside one caller
* *AND* both render surfaces SHALL carry the SAME three-pass list — LIKE-subject guard, then string-function-argument guard, then decimal-stringification rewriter — so ONE ordering rule governs both, and neither the function's doc comment nor any test SHALL record a pass omission or cite `(#219)`, because the select-list surface no longer omits the LIKE-subject pass
* *AND* there SHALL be exactly one such function, named for the transformation rather than for either caller, with NO second per-surface entry point and NO per-surface alias delegating to it, because once the pass lists are equal a second name would be a duplicate body with nothing enforcing its agreement with the first — the same back-door the extraction of these passes exists to close
* *AND* that function SHALL take the expression tree and the column-type list and SHALL absorb the passes' fallibility disagreement behind one `Option`-returning signature — `None` meaning the tree declined, propagated unchanged from whichever pass declined, preserving the filter path's whole-filter decline and the select-list path's full-row fallback — so no caller composes a fallible pass against the infallible rewriter, and SHALL NOT absorb its caller's "is there a tree at all" question, keeping an absent filter distinguishable from a declined one
* *AND* all three passes SHALL narrow to private once the relocation removes their callers outside `support`, so no module outside `support` can sequence them by hand and the pipeline function becomes the only reachable entry point while each pass stays directly callable from `support`'s own tests, and the pipeline function SHALL itself be declared at the narrowest visibility that compiles — `pub(super)`, because `pushdown/mod.rs` calls it
* *AND* the pipeline function SHALL NOT name what any caller does with the result — not the Iceberg file-pruning call, not the full-row fallback flag, and not the SQL renderer — so the pipeline owns the pass order and nothing else; this is also the property that lets ONE function serve two callers whose decline meanings differ, and the raw pushdown filter tree forwarded to Iceberg-level file pruning MUST remain unmodified because the pipeline feeds only the DataFusion-bound scan filter
* *AND* the scan-driving SQL generated for every pushdown request MUST be byte-identical to its pre-extraction output EXCEPT for the scan spec's `storage` value, and every existing pushdown unit test, join golden-SQL assertion, and `dispatch_golden` fixture MUST pass with no change to any test assertion or expected value outside that `storage` value — the filter-chain tests in `pushdown/mod.rs` proving this by calling the extracted pipeline while keeping their rendered-SQL assertions unedited — with the ONE deliberate exception being the select-list LIKE-subject wiring of `vs-adapter/pushdown-planning-like-type-coercion`, which adds a pass to the select-list surface rather than relocating a sequence, is covered by that feature's own scenarios, and flips exactly one existing assertion: the test that pinned the two-pass select-list list, whose `(#219)` citation exists so that its failure reads as "the tracked gap is closed". Collapsing the two equalized functions into one is a rename over that already-corrected behavior and MUST flip no further assertion — the only permitted edit is dropping that same test's now-duplicate second assertion, whose two calls resolve to the one surviving function
* *AND* the `storage` exception SHALL be exactly the externally-tagged re-encoding `vs-adapter/storage-backend-enum` specifies, whose tagged payload is byte-identical to the untagged one, so it touches no pass, no pass order, and no decline path
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The dispatcher builds each fan-out spec from one shared shard-invariant base

* *GIVEN* the pushdown dispatcher's fan-out construction sites, each previously repeating the same shard-invariant tail verbatim — logical schema, name mapping, absent join, storage, the four DataFusion tuning fields, the memory-pool fields, and the S3 connection budget — plus an empty files list
* *WHEN* the dispatcher constructs the scan spec for the grouped-aggregate, group-by fallback, lone-`COUNT(DISTINCT)`, multi/mixed-`COUNT(DISTINCT)` decline, and single-group/row-scan dispatch shapes
* *THEN* every site SHALL derive its shard-invariant fields from one shared base value and set only the fields that differ at that site
* *AND* the shared-base rule SHALL hold for `storage` as a `StorageBackend` exactly as it held for a bare `StorageProps`: the base still carries the backend, no construction site re-derives or re-wraps it, and the wrapper adds NO per-site field
* *AND* the scan-driving SQL generated for each dispatch shape MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` value's variant tag, which `vs-adapter/storage-backend-enum` re-encodes over a byte-identical payload
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Both qualified single-table fallback guards call one shared helper

* *GIVEN* the two near-identical dispatch guards that route to the qualified single-table wrapper — a `GROUP BY` request that declined grouped decomposition, and a multi or mixed `COUNT(DISTINCT)` single-group request
* *WHEN* each guard builds its referenced-column projection, its fan-out spec, and its wrapper SQL
* *THEN* both guards SHALL call one shared helper that performs the referenced-column-projection, fan-out-spec, and wrapper-SQL sequence
* *AND* the wrapper SQL each guard produces MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` value's variant tag in its committed golden — `group_by_fallback.sql` and `multi_count_distinct_decline.sql` — and every other byte of both goldens, including each one's narrowed inner-scan `projection`, MUST be unchanged, because that unchanged remainder is what distinguishes an accepted wire re-encoding from a lost shared-helper call
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: One classifier decides the request shape for both the dispatch and empty-result paths

* *GIVEN* the request-routing decision — grouped aggregate first, then single-group aggregate, then row scan, applying the same aggregate-column-type validation gates and the same HAVING-present hard-error decline — previously encoded twice, once in the non-empty dispatcher and once in the empty-result path
* *WHEN* the adapter plans a pushdown request, whether data files remain or every file is pruned
* *THEN* the request shape SHALL be computed once by one shared classifier that both paths consume
* *AND* each path SHALL render only its own SQL from the shared decision — the non-empty path its scan-driving SQL, the empty path its shape-correct empty response
* *AND* the classifier SHALL additionally resolve the grouped HAVING's merge-rendering, because an unmergeable HAVING removes the partial/merge grouped shape from the reachable set and so is a routing decision, and SHALL carry the rendered HAVING fragment on the grouped shape it returns
* *AND* the non-empty dispatch path SHALL splice that rendered fragment into its outer merge wrapper WITHOUT re-rendering it, and MUST NOT retain its own HAVING-rendering decline, so exactly one place decides whether a HAVING can be merged
* *AND* the classifier SHALL raise NO grouped-tier hard error at all: a grouped request that does not decompose — for any reason, including a non-numeric aggregate column type whether or not a HAVING is present — SHALL fall through to the qualified single-table wrapper shape on both paths, because that wrapper renders the HAVING itself rather than dropping it
* *AND* a grouped request whose HAVING cannot be merged SHALL surface the same qualified-single-table-wrapper shape on both paths — the scan-driving wrapper SQL on the non-empty path, the typed zero-row wrapper shape on the empty path
* *AND* the scan-driving SQL and the empty-result response for every request whose HAVING merges unchanged MUST each remain byte-identical to their pre-delta output EXCEPT for the scan-driving SQL's `storage` value's variant tag; the empty-result response carries no scan spec and MUST therefore be byte-identical with NO exception, which keeps the classifier's two-path agreement falsifiable across this refactor
<!-- /DELTA:CHANGED -->
