# Feature: Pushdown Planning — Join

Pushes a broadcast-eligible two-table inner equi-join into the node-local DataFusion scan by
replicating the smaller side's file list in the shard-invariant common spec, with a fall-through to
the unified unaccelerated fallback for every join outside the broadcast contract.

## Background

* The recorded clause "rendered via the same `crates/vs-expression` translator path used for
  single-table filters" was already inaccurate before this delta: the single-table path runs the
  filter tree through the type-rewrite pipeline (`apply_type_rewrites`) BEFORE handing it to the
  translator, and the broadcast site skipped that pass entirely and pre-screened only SYNTACTICALLY
  (`datafusion_renderable`). This delta makes the clause true rather than adding a new requirement:
  the broadcast site now runs the same pipeline behind the same owner
  (`classify_where_filter`), so "the same path" means the same path. Issue #215.
* The broadcast site's column-type universe is the UNION of both involved tables' columns, matched
  by bare column name, and it is read only AFTER `disjoint_schema_guard` has passed — which is
  exactly what makes a bare name resolve to one Exasol type. Broadcast rendering is side-agnostic
  bare-name (see this feature's own render contract), so a bare-name universe is the matching one.
  The ordering is therefore load-bearing, not incidental.
* This delta adds no new decline OUTCOME. A type-rewrite decline is routed through the SAME
  `Ok(None)` fall-through the syntactically-unrenderable-filter decline already takes, and that
  outcome stays owned by `vs-adapter/pushdown-declined-filter-self-apply`. Only the set of triggers
  widens. See `vs-adapter/pushdown-planning-like-type-coercion` for the per-surface type dispatch.
* A DATE-column LIKE is a REWRITE, not a decline, so it keeps the broadcast plan. Rendering the
  REWRITTEN tree rather than the raw one is what distinguishes "coerce and stay broadcast" from
  "decline and forfeit broadcast"; rendering the raw tree after a successful rewrite would silently
  discard the coercion and reintroduce the hard scan failure.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Broadcast join projection and filter are rendered per involved table

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter resolves the projection and renders the WHERE filter
* *THEN* the adapter SHALL resolve each projected column's Exasol output type from the involved table it belongs to, matching the column against that table's involved-table column metadata
* *AND* the scan-driving SQL's declared EMITS column list SHALL match the projected join output columns in order and type
* *AND* a WHERE filter over columns of either side SHALL be rendered via the same path used for single-table filters — the type-rewrite pipeline over the union of both involved tables' column metadata, THEN the `crates/vs-expression` translator over the pipeline's REWRITTEN tree — and carried in the common spec, REPLACING the recorded "rendered via the same `crates/vs-expression` translator path used for single-table filters", which named the translator alone and so omitted the type-rewrite pass the single-table path has always run and this site skipped (issue #215)
* *AND* that column-type universe SHALL be read only AFTER the disjoint-column-name guard has passed, because a bare column name resolves to exactly one Exasol type only once the two sides' names are known disjoint
* *AND* a filter that is PRESENT and non-trivial but that DECLINES — because the translator cannot express a node in the tree OR because the type-rewrite pipeline returned no tree — SHALL cause the adapter to decline the broadcast plan and take the unified unaccelerated fallback, exactly as an unrenderable join condition already does, because the broadcast SQL carries no outer `WHERE` in which the predicate could be applied
* *AND* the adapter SHALL distinguish an ABSENT or trivially-true filter, which leaves the broadcast plan eligible and emits no scan-spec filter, from a DECLINED one, which forfeits the broadcast plan
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec omits a declined predicate, because the result would carry extra rows — see `vs-adapter/pushdown-declined-filter-self-apply`
* *AND* a filter the pipeline REWRITES rather than declines — a DATE LIKE subject rewrapped as CAST-to-VARCHAR, a governed string function's argument coerced, a DECIMAL stringification trimmed — SHALL keep the broadcast plan eligible and SHALL be carried in the common spec in its REWRITTEN form, never its raw form
* *AND* a filter the pipeline leaves untriggered SHALL render byte-identically to its pre-change output, so no golden-SQL fixture over such a filter changes
<!-- /DELTA:CHANGED -->
