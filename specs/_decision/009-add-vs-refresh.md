# Decisions: add-vs-refresh

## ADR: Root cause is a wrong dispatch string, not a missing handler

**ID:** vs-refresh-dispatch-string-is-root-cause
**Plan:** `add-vs-refresh`
**Status:** Accepted

### Context

Issue #147 reported `ALTER VIRTUAL SCHEMA x REFRESH` failing with `unsupported VS request
type: refresh`. The dispatch in `crates/lakehouse-engine/src/adapter/mod.rs` matched
`Some("refreshVirtualSchema")`, but the Exasol protocol sends the literal string `refresh`,
verified against `virtual-schema-common-java`'s `virtual_schema_api.md`. The
`refreshVirtualSchema` arm never fired; every refresh fell through to the unsupported-type
error. `setProperties` had no arm at all.

### Decision

Treat #147 as a request-type-string bug, not a missing subsystem. Match the literal `"refresh"`
string and add a `"setProperties"` arm, both reusing the existing enumeration path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Match the real protocol strings (`refresh`, `setProperties`) | ✓ Chosen — the existing dispatch and enumeration are correct; only the recognized string and response label were wrong |
| Build a wholly new refresh subsystem | ✗ Rejected — the enumeration path already exists and works; a new subsystem would duplicate it |
| Assume the SDK exposes a typed refresh callback | ✗ Rejected — the protocol strings are the source of truth, verified against `virtual-schema-common-java` |

### Consequences

The fix is a small, verifiable dispatch correction rather than new subsystem code, keeping the
stateless-adapter architecture intact.

## ADR: Refresh and setProperties reuse the createVirtualSchema enumeration

**ID:** vs-refresh-reuses-create-virtual-schema-enumeration
**Plan:** `add-vs-refresh`
**Status:** Accepted

### Context

The only supported way to re-read the catalog was `DROP ... CASCADE` + `CREATE`, which
destroys dependent views and grants. The adapter is stateless (mission.md; CLAUDE.md
"Architecture boundaries" — no caching, no metadata persistence).

### Decision

Route `refresh` and `setProperties` through the existing `handle_create_virtual_schema`
enumeration: full namespace re-enumeration, `TABLE_MAP` rebuilt from scratch, unrelated
`adapterNotes` entries preserved.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse `handle_create_virtual_schema`'s full enumeration | ✓ Chosen — refresh is "re-run create", matching the DROP+CREATE workaround minus the destruction; no reinvented listing/mapping code |
| A separate refresh path that diffs prior `TABLE_MAP` against the catalog | ✗ Rejected — diffing introduces cross-request state the stateless architecture forbids |

### Consequences

Refresh and setProperties get correctness for free from the already-verified
`createVirtualSchema` path, at the cost of always paying full-namespace enumeration cost even
for a single-table `REFRESH TABLES <t>`.

## ADR: setProperties needs its own property-merge precedence

**ID:** vs-refresh-set-properties-merge-precedence
**Plan:** `add-vs-refresh`
**Status:** Accepted

### Context

The existing `get_properties` helper makes persisted `schemaMetadataInfo.properties` win,
which is correct when the request carries no properties (pushdown, refresh). `setProperties`
carries the changed properties and must let them win instead, and a `null` value must unset a
property — the opposite precedence.

### Decision

Add `merge_set_properties`, where the request's `properties` win over persisted properties on
conflict and a `null` value unsets that property. Use it only for `setProperties`; leave
`get_properties` unchanged for create/refresh/pushdown.

### Options Considered

| Option | Verdict |
|--------|---------|
| A dedicated `merge_set_properties` with request-wins precedence and null-unset | ✓ Chosen — matches the `setProperties` protocol contract without disturbing the persisted-wins precedence the other three callers rely on |
| Reuse `get_properties` for `setProperties` | ✗ Rejected — would silently ignore a `SET` that changes an existing property |

### Consequences

Two small, single-purpose merge helpers with distinct, non-overlapping precedence rules,
each matched to its caller's actual protocol contract.
