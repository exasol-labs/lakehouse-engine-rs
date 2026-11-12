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

## Design Decisions

### [1] An exclusion subtracts the listed names and nothing else

- **Decision:** `getCapabilities` returns the advertised list less exactly the names `EXCLUDED_CAPABILITIES` lists. No capability is withheld because another one is excluded, and CREATE and SET refuse no combination of advertised names.
- **Rationale:** The interview rejects coupling between capabilities. Excluding a capability hands its construct to Exasol, so Exasol's own virtual-schema behavior applies to it. Verified on Exasol 2026.2.0-nano.3 with a probe Lua adapter: Exasol never pushes a bare `LIMIT` with the offset dropped. When an `ORDER_BY_*` capability is advertised and `LIMIT` or `LIMIT_WITH_OFFSET` is not, Exasol pushes the `ORDER BY` without the limit, removes its own `ORDER BY`, applies `LIMIT n` locally, and ignores the `OFFSET`: `ORDER BY id LIMIT 5 OFFSET 3` over the ids 1 to 20 returns 1 to 5. Advertising all three capabilities, or none of them, returns 4 to 8. This corrects the Background of `pushdown-capabilities/pushdown-planning-order-by-capability`, which said Exasol strips the offset from the pushed request. Exasol pushes a limit only together with a pushed ordering, so a non-zero offset without `orderBy` cannot reach the adapter.
- **Consequences:**
  - The plan adds no guard for a non-zero offset without `orderBy`.
  - By user decision, no E2E case excludes `LIMIT`, `LIMIT_WITH_OFFSET`, `ORDER_BY_COLUMN`, or `ORDER_BY_EXPRESSION`. The E2E combinations assert rows only for request shapes the adapter plans.
  - `docs/semantic-differences.md` documents the `ORDER BY ... LIMIT n OFFSET m` effect, links Exasol's virtual schema limitations, and gives the workaround of also excluding `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`. `docs/tuning.md` states the effect and links that section.
- **Promotes to ADR:** no

### [2] Refresh reads a value the checks would reject as absent

- **Decision:** A `refresh` request runs no property name or value check. A tuning or table pattern value that CREATE or SET would reject reads as absent, so it takes its default.
- **Rationale:** The interview removes every property check from `refresh`. Failing on a value that `refresh` consumes would be the same check under another name. CREATE and SET reject every such value, so only a schema persisted before the checks existed can hold one, and the adapters of that time already read such a value as its default.
- **Alternatives:** Keep the previous tuning values from the persisted `adapterNotes`: adds a second source for values that every full and partial refresh rebuilds from the properties.
- **Promotes to ADR:** no

### [3] Refresh still needs the properties that locate the source

- **Decision:** `refresh` runs no check whose only purpose is validation: the property catalog's name and value checks (#534), the `EXCLUDED_CAPABILITIES` check, and the `PERMISSION_CHECK` and `USER_MAPPING` checks, including the catalog-kind and `/catalog` URI requirements of the permission check. It still fails when `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `MERGE_SCHEMA`, or `HIVE_PARTITIONING` cannot be used, because it reads them to list the source and declare its tables.
- **Rationale:** Listing cannot proceed without the catalog kind, the CONNECTION, and the namespace. Reading an unusable `MERGE_SCHEMA` or `HIVE_PARTITIONING` as absent would silently change the declared columns of a direct-storage table. A rejected table pattern reads as absent instead, because the patterns and their check arrived in one change (#544): only a schema whose properties another adapter script wrote can hold one. The permission properties have no effect on a `refresh`, which lists as the CONNECTION's identity.
- **Alternatives:** Read every unusable property as absent on `refresh`: an unusable `CATALOG_KIND` would select the Iceberg REST client for a different catalog.
- **Promotes to ADR:** no

## Review Findings
