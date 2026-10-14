# Decisions: change-unity-listing-delta-base-filter

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
