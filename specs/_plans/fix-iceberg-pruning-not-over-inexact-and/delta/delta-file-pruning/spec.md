# Feature: Delta Plan-Time File Pruning

Translates the soundly-translatable nodes of the Exasol WHERE predicate into a
`delta_kernel::expressions::Predicate` handed to the Delta scan builder, so log replay drops files on
partition values and per-file min/max statistics before any Parquet byte is read — while the full
predicate stays applied above the scan as the sole source of row-level correctness.

<!-- DELTA:CHANGED -->
## Background

* This feature is the Delta sibling of `file-planning/pushdown-file-pruning`. Both formats combine
  translated nodes under `AND`, `OR`, `NOT`, and `BETWEEN` by the rules in that feature's
  Background, so a `NOT` negates only an exact child. Leaf translation, literal types, and the
  statistics contract differ per format and are stated here.
* The Delta library prunes, and the adapter only translates. The adapter hands the translated
  predicate to the Delta library, which drives both partition pruning and statistics-based skipping
  during log replay. The adapter keeps the files the library selects. It compares no bound and
  filters no resolved file itself.
* No per-file statistic reaches the scan spec. Pruning completes before a file entry exists, so the
  scan spec's file entries and logical fields carry no minimum, maximum, or count.
* Pruning is sound-not-complete, and correctness lives above the scan. Every emitted node is implied
  by the user predicate. A node that cannot be translated soundly is dropped, so the scan opens more
  files rather than skipping one that could hold a matching row. The full predicate is still
  evaluated: in the scan's DataFusion filter when the DataFusion dialect renders it, and otherwise
  in the adapter's own outer `WHERE` (`pushdown/pushdown-declined-filter-self-apply`).
* A predicate the Delta library cannot evaluate for a file keeps that file. A column without
  statistics, a predicate ineligible for skipping, and a NULL evaluation result all keep the file.
  An imperfect translation therefore costs pruning, never rows.
* Delta protocol § "Per-file Statistics" is what makes range pruning sound. It requires `maxValues`
  be "A value that is greater than or equal to all valid values present in this file for this
  column" and `minValues` be "A value that is less than or equal to all valid values", and states
  "These upper/lower bounds are sufficient information for data skipping". Its footnote "String
  columns are cut off at a fixed prefix length. Timestamp columns are truncated down to
  milliseconds" describes how a writer produces the value and does not relax the bound. Writers keep
  a truncated string maximum an upper bound, and the Delta library compensates a timestamp maximum
  for millisecond truncation. A writer that emitted a bare string prefix would defeat pruning
  undetectably. That trust is a deliberate trade-off that every Delta reader shares.
* Delta protocol § "Add File and Remove File" marks `stats` optional, so a file whose `add` action
  carries no statistics is always kept.
* An IN list prunes as a disjunction of equalities, because the Delta library prunes nothing for a
  native IN. An IN list with no translatable element imposes no constraint and never prunes every
  file.
* Min/max statistics exist by default for the first 32 leaf columns
  (`delta.dataSkippingNumIndexedCols`), and only for numeric, decimal, date, timestamp, and string
  types. A predicate over a non-partition column without them keeps every file.
* Under Delta column mapping (`name` or `id` mode), a predicate on the logical column name still
  prunes, because the Delta library maps the logical name to the physical statistics path.
* Exasol pre-normalises `>`→`<` and `>=`→`<=` (`file-planning/pushdown-file-pruning`). The
  translator still handles the greater forms and flips an operator whose column sits on the right.
* Apache Iceberg spec check: Iceberg requires that `upper_bounds` "must be greater than or equal to
  all non-null, non-Nan values in the column for the file" with no truncation caveat, while Delta's
  matching requirement carries the prefix-cut footnote handled above.
* Every error this feature surfaces is a clean query error, never an abnormal exit of the UDF VM,
  and no error text carries a credential value.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A NOT negates only a fully translated child

* *GIVEN* a query whose WHERE clause is a `NOT` over a conjunction that dropped an untranslatable conjunct, or over a `BETWEEN` that dropped a bound
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the reader SHALL hand the Delta library no predicate derived from that `NOT`, because negating a widened predicate would skip files that hold matching rows
* *AND* a `NOT` over a fully translated conjunction SHALL be negated and SHALL still prune
<!-- /DELTA:NEW -->
