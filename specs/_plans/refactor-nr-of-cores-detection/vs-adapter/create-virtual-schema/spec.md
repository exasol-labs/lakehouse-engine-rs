# Feature: Create Virtual Schema

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/create-virtual-schema/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/create-virtual-schema/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema records the Exasol-name to Iceberg-identifier map in adapterNotes

* *GIVEN* a `createVirtualSchema` request that enumerates one or more tables in the configured namespace
* *WHEN* the adapter builds the `createVirtualSchema` response
* *THEN* the adapter SHALL record, inside the response's `schemaMetadata.adapterNotes` (a stringified JSON object), a `TABLE_MAP` entry mapping each uppercased `__`-flattened Exasol table name to its original-cased fully-qualified Iceberg identifier (dot-joined namespace segments plus table name)
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry (`PARALLELISM_FACTOR`, and the DataFusion threading and memory-budget entries) when writing `TABLE_MAP`
* *AND* the recorded map SHALL round-trip back to the adapter at pushdown time so a pushdown can recover the exact Iceberg identifier from the Exasol table name without re-listing the catalog
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `adapterNotes`
<!-- /DELTA:CHANGED -->
