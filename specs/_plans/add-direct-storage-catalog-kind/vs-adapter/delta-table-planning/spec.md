# Feature: Delta Table Planning

Resolves a Delta table's current version into the engine's scan spec through the shared format-reader seam. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta amends exactly ONE scenario, "The format reader is selected at one site and refuses a
  mismatched pairing", and is issue #407. The third table format arrives. The change discharges the
  clause predicting that a third format would be a compile error at the selection site. Every other
  scenario of this feature is unchanged. The Delta reader's own behavior is untouched.
* The Delta arm's format check keeps its full force. A direct-storage table carries the Parquet
  format tag. Routing one into the Delta reader still produces the recorded clear refusal rather
  than a log-not-found error.
* The selection site still MUST NOT match the catalog kind. `vs-adapter/direct-storage-table-planning`
  owns what the third variant carries. The scan source selects the third arm exactly as it selects
  the first two.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The format reader is selected at one site and refuses a mismatched pairing

* *GIVEN* a `ScanSource` whose variant pairs one resolved session with the table it reads
* *WHEN* the adapter selects the format reader for that source
* *THEN* the adapter SHALL match `ScanSource` EXHAUSTIVELY at exactly ONE site, which returns a boxed `FormatReader`, so adding a FOURTH table format or a FOURTH catalog kind is a compile error at that site rather than a silent fall-through, SUPERSEDING the recorded THIRD form of this clause, which issue #407's direct-storage variant discharged by being the third
* *AND* the Unity Catalog variant SHALL check the loaded table's FORMAT tag and SHALL return a `UdfError` naming the table and the reported format when it is not Delta, because Unity Catalog can report a non-Delta format and misrouting one into the Delta reader would surface a log-not-found error instead of a clear format refusal
* *AND* that check MUST NOT be replaced by an assumption that the Unity Catalog listing filter already excluded non-Delta tables, because the single-table load applies no listing filter
* *AND* that check SHALL keep refusing a table whose reported format is the Parquet tag the direct-storage kind produces, so a table of the third format routed into the Delta reader is refused by the guard that already exists rather than by a new one
* *AND* the selection site MUST NOT match `CatalogKind`, so the permitted-site set of `vs-adapter/catalog-kind-selection` — the enum with its resolver, the catalog-client construction site, credential validation, and the pushdown scan-source construction site — stays intact and gains no file
<!-- /DELTA:CHANGED -->
