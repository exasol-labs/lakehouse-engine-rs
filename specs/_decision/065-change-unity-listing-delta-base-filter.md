# Decisions: change-unity-listing-delta-base-filter

## ADR: Delta-base filter lives inside the Unity Catalog client

**ID:** delta-base-filter-inside-unity-catalog-client
**Plan:** change-unity-listing-delta-base-filter
**Status:** Accepted

### Context

The recorded invariant holds that the shared listing pipeline (`build_listing_virtual_tables`) is structurally incapable of branching on catalog kind — the only site that matches `CatalogKind` is the client-construction site. Issue #318's native Unity Catalog client shipped listing every entry `GET /tables` returns, including views and non-Delta formats, because the Delta/base filter was never implemented. Restoring that filter needs a home that does not reopen a second `CatalogKind`-matching site.

### Decision

Put the Delta-base filter inside `UnityCatalogSession::list_tables`. The client deserializes `data_source_format` and admits an entry as a neutral table iff its `table_type` is `MANAGED` or `EXTERNAL` (a neutral `Table`) AND its `data_source_format` is `DELTA`; every other entry is routed into `CatalogListing.skipped`. `data_source_format` is a crate-private wire field on `TableInfo` and never enters a neutral type. `build_listing_virtual_tables` stays kind-agnostic and untouched.

### Options Considered

| Option | Verdict |
|--------|---------|
| Filter inside the Unity Catalog client | ✓ Chosen — keeps the one shared decision owned by exactly one place per kind and keeps the neutral type and the pipeline kind-free |
| Filter in the shared listing pipeline | ✗ Rejected — forces a `CatalogKind` branch there, breaking the kind-agnostic invariant |
| Expose `data_source_format` on the neutral `CatalogTable` | ✗ Rejected — leaks a Unity wire concept into the kind-free neutral type and into the Iceberg path |

### Consequences

The Delta/base decision needs a Unity-specific wire field; deciding inside the client keeps that field crate-private and the shared pipeline free of any per-kind branch. Under the native Unity Catalog kind, createVirtualSchema now exposes only Delta-format base tables (`MANAGED`/`EXTERNAL` + `DELTA`); the Iceberg REST kind is unaffected.

## ADR: Carry the skip reason as neutral data; the adapter renders it per reason, not per catalog kind

**ID:** skip-reason-neutral-data-render-by-reason
**Plan:** change-unity-listing-delta-base-filter
**Status:** Accepted

### Context

Excluding a Unity Catalog entry needs a warning distinct from the Iceberg REST kind's byte-identical "not a loadable Iceberg table" warning, without adding a second `CatalogKind`-matching site to the adapter's warn loop and without losing the specific per-entry exclusion reason.

### Decision

Change `CatalogListing.skipped` from `Vec<CatalogTableIdent>` to `Vec<SkippedTable>`, where `SkippedTable { ident, reason: SkipReason }` and `SkipReason` is `NotLoadableIcebergTable | NotDeltaBaseTable { detail: String }`. The client that decides to skip sets the reason. The adapter's existing warn loop matches `reason` (neutral data, not `CatalogKind`) to render one `warn` line per entry: `NotLoadableIcebergTable` reproduces the legacy Iceberg line byte-for-byte; `NotDeltaBaseTable { detail }` renders a Unity line naming the excluded identifier and the disqualifying `table_type=…` or `data_source_format=…`. Message wording is intentionally co-owned: the client supplies the pre-formatted `detail` fragment, the adapter owns the log channel and the surrounding sentence — an opaque string keeps `data_source_format` off the neutral public surface while the adapter still controls the sentence structure.

### Options Considered

| Option | Verdict |
|--------|---------|
| Neutral `SkipReason` carried on the skipped entry, adapter renders per reason | ✓ Chosen — only option that keeps the Iceberg warning byte-identical, gives Unity a specific reason, and adds no `CatalogKind` branch |
| Generalize the shared warn message to be kind-neutral | ✗ Rejected — loses the specific per-entry reason and changes the Iceberg warning text |
| Keep `skipped: Vec<CatalogTableIdent>` and branch the warn loop on the resolved `CatalogKind` | ✗ Rejected — reintroduces a second `CatalogKind`-matching site and re-derives client knowledge in the adapter, a back-door leak |
| A fully-structured discriminator (`field: DisqualifyingField, value: String`) instead of an opaque `detail` string | ✗ Rejected — adds a third type to the deliberately-minimal `lakehouse-catalog` public surface, or names `data_source_format` as a matchable field on a public neutral type, either of which the crate-private wire-field rule forbids |

### Consequences

The Iceberg skipped-table warning stays byte-identical. The Unity warning names the specific exclusion reason without a new `CatalogKind` branch. The single-owner claim applies to the skip decision (client-owned); message wording is co-owned by design between the client (detail fragment) and the adapter (sentence and channel).
