# Feature: Refresh and Set Properties

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/refresh-and-set-properties/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/refresh-and-set-properties/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Refresh rebuilds the table map and preserves other adapter notes

* *GIVEN* a `refresh` request whose `schemaMetadataInfo.adapterNotes` carries the persisted notes from creation (`PARALLELISM_FACTOR`, the DataFusion threading and memory-budget entries, and `TABLE_MAP`)
* *WHEN* the adapter builds the `refresh` response
* *THEN* the adapter SHALL rebuild `TABLE_MAP` from the re-enumerated tables — a full rebuild, never a diff or patch of the prior map
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry when writing the rebuilt `TABLE_MAP`, including an `NR_OF_CORES` entry left behind by a schema created under an earlier adapter version, which survives unread and inert
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `schemaMetadata.adapterNotes`
<!-- /DELTA:CHANGED -->
