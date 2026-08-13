# Decisions: add-delta-table-planning

## ADR: `FormatReader` lives in the engine and each implementation owns its whole resolution

**ID:** format-reader-engine-owns-whole-resolution
**Plan:** `add-delta-table-planning`
**Status:** Accepted

### Context

Issue #319 adds Delta as a second table format that must resolve to the engine's `ScanSpec`. The
existing Iceberg file-planning code and the `CatalogClient` trait from #318 offered two candidate
homes. `lakehouse-catalog` MUST NOT name `iceberg`, `datafusion`, `arrow`, `parquet`, or
`object_store` (`vs-adapter/catalog-crate-structure`), and `delta_kernel` falls under that same
rule, so a file-planning method on `CatalogClient` was never reachable. The Iceberg and Delta
formats also need different metadata to reach their file lists — the Iceberg arm needs the
catalog's own `TableMetadata` to build an `iceberg::table::Table`, while the Delta arm needs only a
table-root URL plus a credentialed object store.

### Decision

Define `FormatReader` in `crates/lakehouse-engine/src/adapter/pushdown/format/` as the
per-table-format counterpart of `CatalogClient`. One method resolves a table's whole scan —
catalog request, storage credential, and file discovery — returning a single `ResolvedScan` value.
`CatalogClient` gains no method.

### Options Considered

| Option | Verdict |
|--------|---------|
| `FormatReader` trait in `lakehouse-engine`, each implementation owning its whole resolution | ✓ Chosen — keeps `lakehouse-catalog` free of `delta_kernel`/`iceberg`/`arrow`, and matches how the two formats need different pre-fetched metadata |
| A file-planning method on `CatalogClient` | ✗ Rejected — `lakehouse-catalog` MUST NOT name `iceberg`, `datafusion`, `arrow`, `parquet`, or `object_store`, and `delta_kernel` falls under the same rule |
| A shared caller pre-fetches table metadata and each reader only plans files | ✗ Rejected — the pre-fetch step would itself have to fork per format, reintroducing the fork the trait exists to remove |

### Consequences

A caller asks one question and learns nothing about catalog protocol, credential vending, or
whether files come from Iceberg manifests or a Delta JSON commit log. The recorded clause that
`CatalogClient`'s two listing operations stay shaped so adding an operation later is purely
additive holds unedited, because nothing is added to that trait.

## ADR: Format dispatch matches `ScanSource`, never `CatalogKind` and never a bare format tag

**ID:** format-dispatch-matches-scansource-not-catalogkind
**Plan:** `add-delta-table-planning`
**Status:** Accepted

### Context

`vs-adapter/catalog-kind-selection` freezes `CatalogKind`'s match sites with a source-level probe
asserting its variant names appear in no production module beyond the enum, its resolver, the
client construction site, credential validation, and the pushdown refusal. Format dispatch for
#319 needed a selection site that could not weaken that probe, while still carrying enough context
for each `FormatReader` implementation to do its own catalog work.

### Decision

Define one `ScanSource` enum whose each variant pairs a live catalog session with the table it
reads (`IcebergRest { session, catalog_props }`, `UnityDelta { session, table }`). `format_reader`
matches it exhaustively at exactly one site and fails loud, naming the table and its reported
format, when the Unity variant is handed a non-Delta table.

### Options Considered

| Option | Verdict |
|--------|---------|
| Match `ScanSource`, a resolved session plus a loaded table | ✓ Chosen — carries what each reader needs and adds no second `CatalogKind` match site |
| Match `CatalogKind` | ✗ Rejected — `vs-adapter/catalog-kind-selection`'s source-level probe forbids a second match site |
| Match a bare `TableFormat` tag | ✗ Rejected — a tag cannot carry the session each reader needs, and obtaining it for a Unity table would repeat `load_table`, double-loading |
| One input struct carrying both sessions as `Option`s | ✗ Rejected — every arm would read a field the other arm sets, turning an unset field into a runtime error instead of a type error |

### Consequences

`ScanSource` is not a second `CatalogKind`: the kind is a parsed virtual-schema property,
`ScanSource` a resolved session plus a loaded table. The variant name `UnityDelta` states the
Unity-implies-Delta coupling out loud rather than pretending to a generality the code does not
have; a second Delta-hosting catalog is the trigger to revisit it.

## ADR: `CatalogTable` gains a neutral format tag and an opaque vending key

**ID:** catalog-table-neutral-format-tag-opaque-vending-key
**Plan:** `add-delta-table-planning`
**Status:** Accepted
**Supersedes:** shared-catalog-client-trait-neutral-types

### Context

`CatalogTable` did not carry the table's format: Unity's listing computed `data_source_format` and
discarded it, and `table_id` was never deserialized. The Delta format reader needs both — a format
tag to select and verify the reader, and the vending key to request per-table temporary storage
credentials — without leaking a Unity Catalog concept across the crate boundary.

### Decision

Add a closed `TableFormat` enum (Iceberg, Delta) and `vended_credential_key: Option<String>` to
`CatalogTable`. The raw Unity `data_source_format` and `table_id` wire fields stay crate-private;
only their neutral projections cross. Unity's `load_table` fails loud on an absent or unrecognized
format.

### Options Considered

| Option | Verdict |
|--------|---------|
| Neutral `TableFormat` enum plus opaque `vended_credential_key` on `CatalogTable` | ✓ Chosen — carries the data the engine's dispatch needs without exposing a Unity-specific shape |
| A `CatalogClient::resolve_table_storage` method keeping `table_id` inside the crate | ✗ Rejected — the Iceberg REST arm's equivalent prefix lives engine-side, so this would force re-plumbing the shipped Iceberg path for zero benefit |
| A separate `UnityCatalogSession` method returning table plus storage | ✗ Rejected — duplicates `load_table`'s work on a second public entry point |
| Re-issue `GET /tables/{full_name}` inside the vending step to recover `table_id` | ✗ Rejected — breaks "resolve metadata once per query" |
| A newtype wrapping the vending key | ✗ Rejected — puts a Unity concept on the crate's enumerated public surface for no invariant the neutral table's own privacy does not already give |

### Consequences

Two decisions previously conflated under one prohibition are now separate: the listing-admission
decision (which entries are Delta base tables) stays owned inside the client, unchanged; the
table's format is data the engine's dispatch reads. This supersedes the recorded clause that
`data_source_format` "MUST NOT appear in any neutral type the engine can name" — withholding the
format would have forced the engine to assume Unity implies Delta rather than check it.

## ADR: `IcebergFormatReader` is a deliberately thin delegator, with the collapse scheduled for #320

**ID:** iceberg-format-reader-thin-delegator-scheduled-collapse
**Plan:** `add-delta-table-planning`
**Status:** Accepted

### Context

`resolve_file_list` is shipped, spec-covered, credential-carrying Iceberg planning code reached by
the single-table pushdown path, every join leg, and external test callers. #319 needed an
`IcebergFormatReader` behind the new `FormatReader` trait without risking a regression in that
code or forcing an edit to its callers.

### Decision

`IcebergFormatReader::resolve_scan` calls `resolve_file_list` unchanged and packs its five-tuple
into `ResolvedScan` with an absent Delta block. `resolve_file_list` keeps its name, `pub`
visibility, signature, and every call site. Collapsing it into the reader is deferred to #320,
which removes its direct callers when it routes production pushdown through this seam.

### Options Considered

| Option | Verdict |
|--------|---------|
| Thin delegating wrapper, collapse deferred to #320 | ✓ Chosen — zero-byte diff on the shipped Iceberg path, with a named, scheduled follow-up |
| Move `resolve_file_list`'s body into the reader now and delete the free function | ✗ Rejected — relocates ~160 lines of shipped code, edits every join leg and external test caller, and breaches the recorded clause that `resolve_file_list` alone keeps its name and `pub` visibility |
| Change only its return type to `ResolvedScan` | ✗ Rejected — edits every caller for a cosmetic gain and forfeits the zero-diff guarantee on the shipped path |

### Consequences

A function whose whole body calls another with the same arguments is normally the shallow-module
red flag this project deletes on sight. It is accepted here only because it buys a zero-byte diff
on the shipped path and only until #320 removes the direct callers — a scheduled follow-up rather
than an open-ended one.

## ADR: Delta log replay takes an injected object store

**ID:** delta-log-replay-injected-object-store
**Plan:** `add-delta-table-planning`
**Status:** Accepted

### Context

Delta log-replay correctness — active-file selection across commits, partition values,
deletion-vector references, column mapping — needed to be verifiable without a live S3 or MinIO
stack, while the live `unity-e2e` suite still needed to prove the credentialed-storage path over
S3.

### Decision

The replay step's signature takes an `Arc<dyn ObjectStore>` and a table-root URL and builds no
store itself. `DeltaFormatReader` builds the store; the replay step never does.

### Options Considered

| Option | Verdict |
|--------|---------|
| Injected `Arc<dyn ObjectStore>`, built by the caller | ✓ Chosen — makes replay correctness testable offline in a plain `cargo test`, reserving the live suite for what only the stack can prove |
| Replay step builds its own store from `StorageBackend` | ✗ Rejected — would force every replay test to require S3 or a mock, and would put store construction in two homes |

### Consequences

Replay correctness is exercised offline against the vendored fixtures over a local-filesystem
store. The live `unity-e2e` suite is reserved for the catalog resolve, the
temporary-table-credentials vend, and reading `_delta_log` over S3.
