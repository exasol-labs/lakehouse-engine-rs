# Decisions: add-delta-reader-gating-and-type-mapping

## ADR: Refuse struct, map, binary, and variant instead of completing the JSON-`VARCHAR` convention

**ID:** refuse-struct-map-binary-variant-not-json-varchar-convention
**Plan:** `add-delta-reader-gating-and-type-mapping`
**Status:** Accepted

### Context

Issue #322's scope text asked for the project's "incompatible Arrow types → JSON `VARCHAR`"
convention to be completed for Delta `struct`, `map`, `binary`, and `variant`. Verifying that
convention against `arrow-cast` 58.3's `can_cast_types` found it partly unreachable:
`(Struct(_), _) => false` makes `Struct → Utf8` unavailable, and `Map` reaches the
`(_, Utf8) => from_type.is_primitive()` arm as `false`. `raw_scan` registers the logical schema as
the DataFusion table schema and DataFusion validates physical-against-logical castability at file
open, so neither type ever reaches the per-value JSON conversion, on either table format.
`Binary → Utf8` IS castable but replaces every non-UTF-8 byte sequence with NULL — silent
corruption, not a completed convention. Every existing test asserting the JSON fallback used a
zero-field struct, which sidesteps the cast and hid this.

### Decision

`binary`, `struct`, `map`, and `variant` are refused by name at plan time, each with a reason naming
its own cause. `binary`, `struct`, and `map` cite issue #350; `variant` cites its own opaque
`(metadata BINARY, value BINARY)` binary encoding. Issue #322 is closed without shipping the
JSON-`VARCHAR` convention for these four types.

### Options Considered

| Option | Verdict |
|--------|---------|
| Refuse all four by name, citing #350 for three and the binary-encoding shape for `variant` | ✓ Chosen — matches what `arrow-cast` and DataFusion's own castability check actually allow |
| Complete the convention as the issue's scope text asked | ✗ Rejected — unreachable for `struct` and `map`, lossy for `binary` |
| Implement real JSON rendering in this plan | ✗ Rejected — a design problem spanning both table formats, filed as issue #350 |
| Keep the existing generic "issue #322" error text | ✗ Rejected — #322 is this plan; a closed issue cited in a shipped error reads as an unfixed gap with no owner |

### Consequences

Issue #350 owns designing real JSON rendering for `struct` and `map` on both table formats and
removing Delta's refusal once it lands. The asymmetry with the Iceberg path, which already maps
these to `Utf8` today, is named as deliberate rather than silent.

## ADR: Scope the Delta type refusal to the column, not the table

**ID:** scope-delta-type-refusal-to-column-not-table
**Plan:** `add-delta-reader-gating-and-type-mapping`
**Status:** Accepted

### Context

The `stats-all-types` fixture — vendored specifically for issue #322's type coverage — carries
`binary_col`, `map_col`, and `nested_struct` alongside 13 mappable columns. The shipped behavior
from PR #340 refused the whole table on any unmapped column, which made this fixture wholly
unqueryable and left issue #322's own E2E acceptance criterion ("a fixture table spanning varied
Delta types returns the expected Exasol types and values") unreachable with the fixtures on hand.
It also makes any real Delta table with one struct column unreachable over a column nobody
selected.

### Decision

A refused column is omitted from the logical schema and recorded on `ResolvedScan`. One adapter
gate refuses a pushdown request that reads or emits a refused column; every other request against
the same table plans normally. This supersedes the shipped table-scoped refusal.

### Options Considered

| Option | Verdict |
|--------|---------|
| Column-scoped refusal, recorded on `ResolvedScan` | ✓ Chosen — matches Iceberg, which refuses nothing, and makes the vendored fixture's own acceptance criterion reachable |
| Keep the shipped table-scoped refusal | ✗ Rejected — leaves `stats-all-types` wholly unqueryable and any real table with one struct column unreachable |
| Omit refused columns from the `createVirtualSchema` declaration | ✗ Rejected — duplicates the classification decision across two type vocabularies (Unity Catalog type names and `delta_kernel::DataType`) and silently shrinks `SELECT *` |
| Author a new all-mappable Spark fixture and keep table scope | ✗ Rejected — adds a network-dependent seed step the #325 harness deliberately avoided, for strictly less useful engine behavior |

### Consequences

A refused column is absent from the logical schema as defense in depth: a gate miss on any pushdown
path fails with a DataFusion unresolved-column error rather than emitting a silently-NULLed column.

## ADR: The refused-column gate reads a total recursive JSON walk, never a per-clause enumeration

**ID:** delta-refused-column-gate-total-recursive-json-walk
**Plan:** `add-delta-reader-gating-and-type-mapping`
**Status:** Accepted

### Context

The refusal gate needs the complete set of columns a pushdown request touches — through a WHERE
filter, a GROUP BY key, an ORDER BY key, an aggregate argument, a join condition, or the emitted
projection. A per-clause enumeration was considered and would need to list every such shape by
hand.

### Decision

The gate's referenced-column set is one recursive walk over the whole pushdown request JSON
collecting every `column` node's name, unioned with the final projection the adapter renders — but
that projection union applies ONLY when the request's own select list is absent or empty (a genuine
`SELECT *`), never to the synthetic full-base-row projection the adapter separately renders for an
aggregate select list or an untranslatable select-list item. That synthetic fallback is a
placeholder the scan never reads; each such item's own referenced columns already reach the
touched-column set through the request-JSON walk's aggregate-argument coverage. During
implementation this exact distinction was found missing: unioning the synthetic fallback
unconditionally made a bare `COUNT(*)` — which reads no column value — refuse against any table
carrying an unrelated refused column. The fix made the widened-vs-not distinction a first-class
`Option<&[ProjectionItem]>` argument rather than a separate boolean, so the invalid combination is
unrepresentable.

### Options Considered

| Option | Verdict |
|--------|---------|
| One recursive JSON walk unioned with the projection ONLY for a genuine `SELECT *` | ✓ Chosen — reached the real bug (`COUNT(*)` over-refusal) that a naïve union or a per-clause list both miss |
| Enumerate the clauses that can carry a column (select list, WHERE, GROUP BY, ORDER BY, aggregate arguments, join conditions) | ✗ Rejected — silently omits every pushdown capability added after it, and a miss routes a refused column into the scan rather than declining it |
| Union the full-base-row projection unconditionally, for `SELECT *`, aggregate select lists, and untranslatable items alike | ✗ Rejected — reproduced as a real bug: refuses `COUNT(*)`, which reads no column value, against a table carrying any unrelated refused column |

### Consequences

For `binary` specifically, a filter comparing the column as text with every non-UTF-8 value
silently NULL is exactly the failure this plan exists to prevent — the total walk's aggregate-argument
and filter coverage is what catches it. The join path attributes each column reference to its
declaring side (tagged references to their own side; untagged/ambiguous references charged to every
side, fail-safe) before intersecting with that side's own refused list, fixing a second real bug
found in code review: a request-global touched-column set let a refused column named against one
join side refuse a `SELECT` naming only the other side's identically-named mappable column.

## ADR: Pin the three Delta type-mapping sets to arrow's own castability answer with assertions

**ID:** pin-delta-type-sets-to-arrow-castability-with-assertions
**Plan:** `add-delta-reader-gating-and-type-mapping`
**Status:** Accepted

### Context

The native, text-rendered, and refused type sets are each a claim about `arrow-cast`'s
`can_cast_types` behavior. The existing `convert_tests`/`mapping_tests` assertions passed against a
convention that does not hold precisely because they used a zero-field struct, which sidesteps the
field-wise cast check `can_cast_types` performs on a populated struct.

### Decision

A unit test asserts `can_cast_types(physical, Utf8)` directly for a representative of each set:
`true` for `Binary`, `List(Int32)`, `Interval(YearMonth)`, `Interval(DayTime)`, and an
out-of-domain `Decimal128`; `false` for a POPULATED `Struct`, a `Map`, and a `List(Struct)`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Assert `can_cast_types` directly against a populated struct and the other representatives | ✓ Chosen — compiles the claim into a test, so an `arrow-cast` upgrade that changes an answer is a test failure, not a silent re-partition |
| State the castability facts in the spec prose alone | ✗ Rejected — is exactly what let the zero-field-struct blind spot pass unnoticed in the existing suites |

### Consequences

An `arrow-cast` upgrade that changes any of these three sets' membership now fails a test instead of
silently reclassifying a column's queryability.
