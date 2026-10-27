# Decision Log: fix-vs-property-validation

## Interview

- Q: Which issue does this plan fix? → A: #534, unknown names and invalid tuning values are silently ignored.
- Q: Which requests validate properties? → A: createVirtualSchema, refresh, setProperties, as the Java JDBC adapter does (`JDBCAdapter` calls `validateProperties()` on those three). Pushdown and getCapabilities never validate.
- Q: Which properties does setProperties validate? → A: the merged set after `= null` unsets are applied, so `NAME = null` removes a bad property and passes.
- Q: Where does the check live? → A: once, in `handle_create_virtual_schema`, right after the effective properties are computed, because pushdown calls `get_properties` separately and must stay lenient.
- Q: What does an invalid tuning value do? → A: reject all. A value that fails to parse or falls outside the range fails the statement, for the 8 numeric properties, `DATAFUSION_THREADING_MODE` (AUTO or FIXED), and `ALLOW_HTTP` (true or false). The error names each property, value, and accepted range.
- Q: How are names matched? → A: one flat list of every property the adapter reads, for every `CATALOG_KIND` (incl. `USER_MAPPING`, `MERGE_SCHEMA`, `HIVE_PARTITIONING`). Case-sensitive exact match on the name Exasol passes. The error suggests the nearest known name by edit distance only when close.
- Q: Report one problem or all? → A: all unknown names and all invalid values in one error (the issue asks for each to be named; Java reports only the first).
- Q: Which error first when a typo causes a missing required property? → A: unknown-name error first (`CATALOG_CONECTION` reports "did you mean CATALOG_CONNECTION").
- Q: Legacy schemas with a stray property? → A: REFRESH fails until the user runs `ALTER VIRTUAL SCHEMA ... SET <NAME> = null`. Accepted cost of validating on refresh.

## Design Decisions

### [1] One property catalog parses each tuning value once, for the check and for its consumers

- **Decision:** A new module, `adapter/properties.rs`, declares every property name and the value rule of each tuning property. Its check returns the typed tuning values, and the create-time resolution reads those values instead of re-parsing the strings. The pushdown's `adapterNotes` reads parse through the same rules and keep their lenient default.
- **Rationale:** Each range is defined in one place, so the check and the resolution cannot disagree, and the create path keeps no lenient branch that the spec forbids. The module answers one question (which properties exist and which values each accepts), its callers get a typed value or one error, and it depends on nothing in the adapter. No accepted ADR covers property validation; `allow-http-threaded-as-vended-selector-parameter` keeps `ALLOW_HTTP` false by default, and this plan keeps that default.
- **Alternatives:** A name list next to the existing `resolve_*` parsers, which keep their `.ok().filter(..).unwrap_or(default)` chains. Rejected: every range would be written twice, and the lenient branch would stay reachable if the check ever moved.
- **Consequences:** A property the adapter starts to read later must be added to the catalog, or every CREATE that sets it fails. The first test that sets the property catches the omission.
- **Promotes to ADR:** no

### [2] Each tuning value is checked on its own; the other properties keep their own value checks

- **Decision:** The check rejects an invalid tuning value even when another property makes the value unused, such as `DATAFUSION_TARGET_PARTITIONS` under the `AUTO` threading mode. For `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `PERMISSION_CHECK`, `USER_MAPPING`, `MERGE_SCHEMA`, and `HIVE_PARTITIONING` the check verifies only the name, and the owning module keeps its value check and error text.
- **Rationale:** A check that depends on another property would fail a later `SET DATAFUSION_THREADING_MODE = 'FIXED'` on a value an earlier statement set. The owning modules' checks depend on the catalog kind, the CONNECTION address, or a template compile, which the property catalog does not know.
- **Alternatives:** Moving the value checks of those seven properties into the catalog, so one error lists every problem. Rejected: it duplicates context the owning modules hold and changes established error text.
- **Consequences:** A request with a tuning problem and a problem in one of those seven properties reports the tuning problem first. The second problem appears after the user fixes the first. A `MERGE_SCHEMA` value under a kind other than `DIRECT_STORAGE` stays unchecked, as `direct-storage/direct-storage-properties` requires.
- **Promotes to ADR:** no

## Review Findings
