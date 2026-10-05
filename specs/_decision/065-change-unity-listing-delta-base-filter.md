# Decisions: change-unity-listing-delta-base-filter

## ADR: Delta-base filter lives inside the Unity Catalog client

**ID:** delta-base-filter-inside-unity-catalog-client
**Plan:** change-unity-listing-delta-base-filter
**Status:** Accepted

### Context

The shared listing pipeline must not branch on catalog kind, and only the client-construction site matches the kind. The Unity Catalog client listed every entry, including views and non-Delta formats, because no Delta-base filter existed.

### Decision

The Unity Catalog client filters its listing. It admits an entry only when its table type is `MANAGED` or `EXTERNAL` and its data source format is `DELTA`, and it reports every other entry as skipped. The data source format stays a crate-private wire field, and the shared pipeline is unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Filter in the shared listing pipeline | Rejected: needs a catalog-kind branch there |
| Expose the data source format on the neutral table type | Rejected: leaks a Unity wire concept into the neutral type and the Iceberg path |

### Consequences

Under the Unity Catalog kind, createVirtualSchema exposes only Delta base tables. The Iceberg REST kind is unaffected.

## ADR: Carry the skip reason as neutral data; the adapter renders it per reason, not per catalog kind

**ID:** skip-reason-neutral-data-render-by-reason
**Plan:** change-unity-listing-delta-base-filter
**Status:** Accepted

### Context

A skipped Unity Catalog entry needs its own warning, distinct from the byte-identical Iceberg REST warning. The adapter's warn loop must not gain a second catalog-kind match, and the per-entry reason must survive.

### Decision

Each skipped entry carries a neutral skip reason set by the client that skipped it. The adapter renders one warning per entry by matching the reason, not the catalog kind. The Iceberg reason reproduces the legacy warning byte for byte. The Unity reason names the identifier and the disqualifying field, using a detail string supplied by the client. The client owns the skip decision, and the adapter owns the sentence and log channel.

### Options Considered

| Option | Verdict |
|--------|---------|
| Make the shared warning kind-neutral | Rejected: loses the per-entry reason and changes the Iceberg text |
| Branch the warn loop on catalog kind | Rejected: adds a second kind-matching site and leaks client knowledge into the adapter |
| Structured field-and-value discriminator instead of a detail string | Rejected: adds a type to the minimal catalog crate surface or exposes `data_source_format` on a public neutral type |

### Consequences

The Iceberg warning stays byte-identical, and the Unity warning names the exact reason with no new kind branch.
