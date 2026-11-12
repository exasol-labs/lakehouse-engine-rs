# Decision Log: add-capabilities-exclusion-property

## Interview

- Q: What do the Java adapters (`virtual-schema-common-java`/`-jdbc`) do? → A: Parse only in `getCapabilities`: split on `,`, trim, skip empty, exact case-sensitive enum lookup, subtract from advertised set. A misspelled name passes CREATE/SET/REFRESH and fails every query (`E-VSCJDBC-51`); a valid but unadvertised name is a no-op; no deny-list.
- Q: Which requests check the `EXCLUDED_CAPABILITIES` value? → A: Only `CREATE` and `SET`, the requests that write properties.
- Q: Should `REFRESH` keep the #534 name and value checks? → A: No. `REFRESH` validates no property; simplify the code. Only `CREATE` and `SET` validate.
- Q: Capability name matching? → A: Exact and case-sensitive, like Java and property names; a case or spelling miss gets a did-you-mean suggestion (default taken, user did not object).
- Q: Can any advertised capability be excluded, or are structural ones refused? → A: Any advertised name, like Java; no deny-list (default taken, user did not object).
- Q: Should refresh also drop the PERMISSION_CHECK/USER_MAPPING checks? → A: Yes, refresh validates no property.
- Q: Couple LIMIT/LIMIT_WITH_OFFSET/ORDER_BY_* exclusions? → A: No. ORDER BY in a VS is a known Exasol limitation; the property only subtracts listed names from the list, no special handling of capabilities that must be advertised together.
- Q: Cover the issue's "ORDER_BY_COLUMN excluded while LIMIT stays" example in E2E? → A: No. It was only an example; LIMIT and ORDER_BY exclusions are problematic combinations and stay out of E2E.
- Q: How to document the ORDER BY with LIMIT behavior? → A: In docs/semantic-differences.md, with a reference to Exasol's virtual schema limitations: https://docs.exasol.com/db/latest/database_concepts/virtual_schema/user_guide.htm#Virtualschemalimitations
- Q: What does REFRESH do with a saved invalid property? → A: It runs no check and has no special handling; a REFRESH whose properties cannot be used just fails. Only CREATE and SET validate, because they write properties.
- Correction to the first entry: the Java adapters run full property validation (names and values of the other properties) on CREATE, REFRESH, and SET; only the `EXCLUDED_CAPABILITIES` value goes unchecked until `getCapabilities`.
- Q: Final REFRESH rule? → A: REFRESH behaves as today and as the Java virtual schemas: it runs the same checks as CREATE and SET. We do not want the VS ecosystems to diverge.

## Design Decisions

### [1] An exclusion subtracts the listed names and nothing else

- **Decision:** `getCapabilities` returns the advertised list less exactly the names `EXCLUDED_CAPABILITIES` lists. No capability is withheld because another one is excluded, and CREATE and SET refuse no combination of advertised names.
- **Rationale:** The interview rejects coupling between capabilities. Excluding a capability hands its construct to Exasol, so Exasol's own virtual-schema behavior applies to it. Verified on Exasol 2026.2.0-nano.3 with a probe Lua adapter: Exasol never pushes a bare `LIMIT` with the offset dropped. When an `ORDER_BY_*` capability is advertised and `LIMIT` or `LIMIT_WITH_OFFSET` is not, Exasol pushes the `ORDER BY` without the limit, removes its own `ORDER BY`, applies `LIMIT n` locally, and ignores the `OFFSET`: `ORDER BY id LIMIT 5 OFFSET 3` over the ids 1 to 20 returns 1 to 5. Advertising all three capabilities, or none of them, returns 4 to 8. This corrects the Background of `pushdown-capabilities/pushdown-planning-order-by-capability`, which said Exasol strips the offset from the pushed request. Exasol pushes a limit only together with a pushed ordering, so a non-zero offset without `orderBy` cannot reach the adapter.
- **Consequences:**
  - The plan adds no guard for a non-zero offset without `orderBy`.
  - By user decision, no E2E case excludes `LIMIT`, `LIMIT_WITH_OFFSET`, `ORDER_BY_COLUMN`, or `ORDER_BY_EXPRESSION`. The E2E combinations assert rows only for request shapes the adapter plans.
  - `docs/semantic-differences.md` documents the `ORDER BY ... LIMIT n OFFSET m` effect, links Exasol's virtual schema limitations, and gives the workaround of also excluding `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`. `docs/tuning.md` states the effect and links that section.
- **Promotes to ADR:** no

### [2] Refresh runs the same property checks as create and set

- **Decision:** A `refresh` runs the property name and value checks of `createVirtualSchema` and `setProperties`, and the `EXCLUDED_CAPABILITIES` check joins them. The permission property checks on `refresh` stay as recorded. `getCapabilities` does not check the value and ignores an entry the adapter does not advertise.
- **Rationale:** `refresh` keeps its current behavior (#534), which matches the Java virtual schemas: they validate the properties on `createVirtualSchema`, `refresh`, and `setProperties`. The user does not want the virtual schema ecosystems to diverge.
- **Alternatives:** A `refresh` that checks nothing: diverges from the current behavior and from the Java virtual schemas.
- **Consequences:**
  - When a later adapter stops advertising a listed name, `REFRESH` and `SET` fail until a `SET` removes that entry. `getCapabilities` ignores the entry, so queries keep working. `docs/tuning.md` states this.
- **Promotes to ADR:** no

## Review Findings
