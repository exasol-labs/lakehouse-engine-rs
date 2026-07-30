# Feature: Pushdown Planning — Join Fallback

Reconstructs any inner join the broadcast path does not serve as an Exasol-side `INNER JOIN … ON`
chain over one sharded fan-out per involved table, with every wrapper clause rendered
table-qualified.

## Background

* This delta corrects which conjuncts reach a fan-out leg. A conjunct reaches a leg only if the leg
  can actually apply it, so a side-local conjunct the DataFusion dialect cannot render is residual
  and lands in the outer wrapper's `WHERE` rather than being pushed into a leg that then drops it.
  The set the legs receive and the set the outer wrapper renders stay exact complements, so the
  partition remains total and disjoint. See `vs-adapter/pushdown-declined-filter-self-apply`.
* The renderability screen governs the RENDER path only. Each side's Iceberg manifest-pruning
  predicate keeps EVERY side-local conjunct, renderable or not, because pruning only ever removes
  files that provably cannot match; narrowing that input would silently open more files and buy no
  correctness.
* The stale claim that the outer Exasol query "still applies the FULL `WHERE`" is corrected in the
  same change: it applies exactly the residual set, and the residual set is now defined so that
  every conjunct no leg applies is in it.
* The outer wrapper's `WHERE` is rendered from ONE renderer over ONE combined residual tree, and that
  render has THREE outcomes, not two. An ABSENT residual set emits no clause. A residual set that
  renders TRIVIALLY TRUE emits no clause and is not an error — the trivially-true-suppressing
  renderer returns nothing for it exactly as it does for an unrenderable one, so that renderer alone
  cannot decide the error. A residual set the NON-SUPPRESSING qualified render also cannot express is
  UNRENDERABLE and returns the wrapper's existing client-facing error. Only the third outcome errors.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Join conditions attach greedily by table-name set and side-local filters push into each leg

* *GIVEN* a unified unaccelerated fallback over N ≥ 2 involved tables with a set of join conditions and a WHERE filter
* *WHEN* the adapter renders the `INNER JOIN … ON` chain
* *THEN* the adapter SHALL attach each join condition to the earliest join point in the left-to-right chain at which every table the condition references is in scope, deciding scope by the SET of `tableName`s the condition touches — NEVER by column name, so shared column names across sides stay correctly qualified
* *AND* a join point at which no not-yet-attached condition becomes resolvable SHALL be rendered with `ON 1=1`
* *AND* a top-level WHERE conjunct SHALL be pushed INTO a side's fan-out leg as a DataFusion filter if and only if it references only that ONE table AND the DataFusion dialect can render it, so DataFusion performs row-group pruning and row filtering per leg for every conjunct the leg can actually apply
* *AND* every OTHER conjunct SHALL be RESIDUAL and remain in the outer wrapper's `WHERE`: cross-table, OR-spanning, untagged, column-free, or side-local-but-unrenderable — REPLACING the recorded "only the RESIDUAL WHERE conjuncts — cross-table, OR-spanning, or untagged — SHALL remain in the outer wrapper's `WHERE`", under which a side-local conjunct that failed to render was applied nowhere
* *AND* the partition SHALL stay total and disjoint, so no conjunct is dropped and none is applied twice
* *AND* the filter each leg receives SHALL already be screened as DataFusion-renderable, so the leg's own render cannot decline and no second renderability decision exists to drift from the first
* *AND* each side's Iceberg manifest-pruning predicate SHALL keep every side-local conjunct, renderable or not, so a render decline SHALL NOT change which files are opened
* *AND* a non-empty residual set that the NON-SUPPRESSING qualified Exasol render also cannot express SHALL return the wrapper's existing client-facing error, because the predicate can be applied nowhere and returning rows without it would be wrong
* *AND* a non-empty residual set that renders trivially true SHALL emit no outer `WHERE` and SHALL NOT error, so the error decision MUST NOT be taken from the trivially-true-suppressing render's empty result
* *AND* the returned result SHALL equal the result of the same inner join evaluated on a single node, for any assignment of conditions to join points
<!-- /DELTA:CHANGED -->
