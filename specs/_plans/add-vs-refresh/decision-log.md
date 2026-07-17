# Decision Log: add-vs-refresh

## Interview

Planned headlessly (no live interview). GitHub issue #147 stands in as the interview record.

**Q:** What must change so an existing virtual schema can re-read the catalog in place?
**A:** (#147) `ALTER VIRTUAL SCHEMA x REFRESH` fails with `unsupported VS request type: refresh` and `ALTER VIRTUAL SCHEMA x SET ...` with `unsupported VS request type: setProperties`. Metadata is captured once at CREATE and never updated, so new tables never appear, a new column is not queryable, and a dropped column errors at scan time. The only workaround is `DROP ... CASCADE` + `CREATE`, which loses dependent views and grants. Implementing the adapter's `refresh` (and ideally `setProperties`) callback would fix it. Environment: lakehouse-engine v0.26.3, Exasol SaaS, Glue Iceberg REST.

**Q:** Is `setProperties` in scope or deferred?
**A:** Included. Both are named in the issue title; the namespace-change case is only reachable via `setProperties`; both reuse the same enumeration.

## Design Decisions

### [1] Root cause is a wrong dispatch string, not a missing handler

- **Decision:** Treat #147 as a request-type-string bug. The dispatch matches `Some("refreshVirtualSchema")`, but the Exasol protocol emits the literal `refresh`; the arm is dead code and every refresh falls through to the unsupported-type error. Fix by matching `"refresh"` and adding a `"setProperties"` arm.
- **Alternatives:** Build a wholly new refresh subsystem; assume the SDK exposes a typed refresh callback.
- **Rationale:** Verified the protocol strings against `virtual-schema-common-java` `virtual_schema_api.md`: request/response types are `refresh` and `setProperties` (not `refreshVirtualSchema`). The existing dispatch and enumeration are correct; only the recognised string and response label are wrong.
- **Promotes to ADR:** yes

### [2] Refresh and setProperties reuse the createVirtualSchema enumeration

- **Decision:** Route both through `handle_create_virtual_schema`; full namespace re-enumeration, `TABLE_MAP` rebuilt, unrelated `adapterNotes` preserved.
- **Alternatives:** A separate refresh code path that diffs prior `TABLE_MAP` against the catalog and patches only changes.
- **Rationale:** The adapter is stateless (mission.md, CLAUDE.md "Architecture boundaries" — no caching, no metadata persistence). Refresh is "re-run create", which is exactly the DROP+CREATE workaround minus the destruction. Diffing would introduce cross-request state the architecture forbids.
- **Promotes to ADR:** yes

### [3] Include setProperties, do not defer

- **Decision:** Ship `refresh` and `setProperties` in one plan.
- **Alternatives:** Ship `refresh` only; open a follow-up issue for `setProperties`.
- **Rationale:** Both request types are named in #147 and produce the same error class. The namespace-retarget case (`SET ICEBERG_NAMESPACE=...`) is only reachable via `setProperties`. Both reuse the same enumeration, so a separate follow-up would be near-duplicate work. This is a reversible scope choice, not an architectural divergence, so it is decided here rather than escalated.
- **Promotes to ADR:** no

### [4] Always full-namespace enumeration; echo requestedTables rather than honour it

- **Decision:** Always enumerate the whole namespace; when a `refresh` request carries `requestedTables`, echo it in the response so Exasol applies the requested subset; omit it for a full refresh.
- **Alternatives:** Honour `requestedTables` by listing only those tables from the catalog (a partial-listing code path).
- **Rationale:** Full enumeration matches `createVirtualSchema` and the DROP+CREATE workaround. Echoing `requestedTables` keeps partial-refresh SQL correct without a second listing path, because Exasol applies the echoed subset even when the response carries the full metadata.
- **Promotes to ADR:** no

### [5] setProperties needs its own property-merge precedence

- **Decision:** Add `merge_set_properties` where the request's `properties` win over persisted `schemaMetadataInfo.properties` and a `null` value unsets the property. Leave `get_properties` (persisted-wins) for create/refresh/pushdown.
- **Alternatives:** Reuse `get_properties` for setProperties.
- **Rationale:** `get_properties` makes persisted properties win, which is correct where the request carries no properties (pushdown, refresh). `setProperties` carries the changed properties and must let them win, and `null` must remove a property — the opposite precedence. A shared helper would silently ignore a `SET` that changes an existing property.
- **Promotes to ADR:** yes

### [6] No new Iceberg schema-handling surface

- **Decision:** Rely on the existing enumeration and `datafusion-scan/type-mapping`; add no new schema/type logic.
- **Alternatives:** Add refresh-specific schema-change detection.
- **Rationale:** Per the Iceberg table spec, `current-schema-id` "points to the schema by ID for use when reading table data" and columns are "selected by field id", so a re-read reflects added/dropped/renamed columns and type promotion automatically. The known field-id projection exception (#27) is unchanged and out of scope.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated in Revision Mode after plan-reviewer blockers, and by speq-implement after code review. -->
