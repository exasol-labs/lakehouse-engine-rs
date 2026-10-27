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

- **Decision:** A new module, `adapter/properties.rs`, declares every property name and the value rule of each tuning property. Its check returns the typed tuning values, and the create-time resolution reads those values instead of re-parsing the strings. The module parses properties only. The pushdown's `adapterNotes` reads stay in `adapter/mod.rs`, beside `build_adapter_notes`.
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

### [pr-feedback] Each whole-number property parses into a fixed-width type that sets its range

- **Finding:** The planned rules checked only a lower bound, so `DATAFUSION_THREADS_PER_UDF = '100000'` passed, and a 21-digit value overflowed the parse and got the misleading message `at least 1`.
- **Direction change:** The property catalog parses `PARALLELISM_FACTOR` into `NonZeroU8`; `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_TARGET_PARTITIONS`, and `S3_MAX_CONNECTIONS` into `NonZeroU16`; `INSTANCE_OVERHEAD_MB` into `u16`; `DATAFUSION_BATCH_SIZE` into `NonZeroU32`; and `JOIN_BROADCAST_MAX_BYTES` into `NonZeroU64`. The type's minimum and maximum are the accepted range, and every parse failure, overflow included, gets the error `expected a whole number from <minimum> to <maximum>`. The `MEMORY_POOL_FRACTION`, `DATAFUSION_THREADING_MODE`, and `ALLOW_HTTP` rules stay unchanged.
- **Promotes to ADR:** no

### [pr-feedback] The adapterNotes reads stay with the notes format in adapter/mod.rs

- **Finding:** The plan had the property catalog expose lenient parses for the pushdown's `adapterNotes` reads, so two modules would share the notes format, and a change to a property's type would also change how stored notes are read.
- **Direction change:** The property catalog parses properties only. The pushdown's `adapterNotes` reads stay unchanged in `adapter/mod.rs`, beside `build_adapter_notes`, which owns the notes format.
- **Promotes to ADR:** no

### [pr-feedback] S3_MAX_CONNECTIONS no longer accepts AUTO or 0

- **Finding:** `docs/tuning.md` shows `AUTO` as the default of `S3_MAX_CONNECTIONS`, so users write `S3_MAX_CONNECTIONS = 'AUTO'`, which the new check rejects.
- **Direction change:** The plan keeps the rejection: `AUTO` and `0` fail, and only an absent property selects the derived budget. `docs/tuning.md` shows the default as absent, and the changed scenarios of `scan-runtime/scan-execution-connection-concurrency` state that only an absent or empty value derives the budget.
- **Promotes to ADR:** no

### [pr-feedback] The error echoes at most 100 characters of a rejected value, with quotes doubled

- **Finding:** The invalid-value entry copied the value without a limit, so a long or multi-line value made the error unreadable, and a `'` in the value broke the quoting.
- **Direction change:** The entry echoes the first 100 characters of the value, followed by `…` when the value is longer, and doubles each `'`.
- **Promotes to ADR:** no

### [pr-feedback] The property-validation spec owns the list of 17 names

- **Finding:** `specs/architecture.md` § Interfaces repeated the property list, and the user-facing docs split the names over three pages.
- **Direction change:** `vs-adapter/property-validation` holds the only list in the specs, and the architecture delta's § Interfaces links to it. `docs/tuning.md` gets one table of all 17 properties, each row linking to the page that explains the property.
- **Promotes to ADR:** no

### [plan-review] The bench log greps drop the removed CLUSTER_NODES key

- **Finding:** After the plan lands, a spec step, two unit tests, and `docs/architecture.md:37` still name the removed note key `CLUSTER_NODES`, which breaks the no-trace rule that task 5.2 adds. The log greps of `bench/sweep.sh:27` and `bench/parallelism_sweep.sh:29` also match the key, and the rule does not cover bench scripts.
- **Direction change:** The user decided that the plan also removes the `CLUSTER_NODES` alternative from both bench greps (task 5.2). No engine or UDF code emits that token, so the alternative is removed and not renamed. The rule text in `AGENTS.md` stays unchanged.
- **Promotes to ADR:** no
