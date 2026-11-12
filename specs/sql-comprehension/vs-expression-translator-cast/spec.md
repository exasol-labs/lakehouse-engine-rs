# Feature: VS Expression Translator, CAST

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with CAST target-type rendering.

## Background

* Exasol sends a CAST as a `function_scalar_cast` node whose `dataType` object names the target type. A nested `function_scalar` node named `CAST` is accepted as an alternate encoding.
* The adapter renders a CAST in two SQL dialects: the DataFusion dialect for the scan SQL that DataFusion parses, and the Exasol dialect for the wrapper SQL that Exasol parses. A node the DataFusion dialect declines moves into Exasol-dialect SQL that the adapter writes, because Exasol never re-evaluates a delegated node (`pushdown/pushdown-declined-filter-self-apply`).
* The DataFusion dialect renders the exact target type or declines the node, except a text target. DataFusion has no type with the semantics of `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE`. Its only text type, `Utf8`, has no length limit, no blank padding, and no character set, and datafusion-sql rejects a length-qualified character target, so every `CHAR` and `VARCHAR` target renders as a bare `VARCHAR`.
* The Exasol dialect renders a target in the same spelling the adapter uses for an `EMITS` column. Exasol's bare `TIMESTAMP` is `TIMESTAMP(3)`, and Exasol 8.x parses no TIMESTAMP precision, so precision 3 renders as bare `TIMESTAMP`.
* Exasol sends a TIMESTAMP `dataType` as `{"type":"TIMESTAMP","withLocalTimeZone":<bool>,"fractionalSecondsPrecision":<0-9>}`. Exasol 8.x omits `fractionalSecondsPrecision`, which then means 3. An absent `withLocalTimeZone` means `false`.

## Scenarios

### Scenario: CAST renders the mapped target type per dialect

* *GIVEN* a `function_scalar_cast` node whose `dataType` names a valid Exasol target type
* *WHEN* the adapter renders the node in the DataFusion dialect or in the Exasol dialect
* *THEN* the DataFusion dialect SHALL render `CAST(<expr> AS <target>)` with `DECIMAL(p,s)` as `DECIMAL(p,s)`, `DOUBLE` as `DOUBLE`, `BOOLEAN` as `BOOLEAN`, `DATE` as `DATE`, `TIMESTAMP` per the fractional-seconds-precision scenario, and every `CHAR(n)` and `VARCHAR(n)` target, with or without an ASCII `characterSet`, as a bare, length-less `VARCHAR`, because datafusion-sql rejects a length-qualified character target and Arrow has only `Utf8`
* *AND* the DataFusion dialect SHALL decline a target of `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE`
* *AND* the Exasol dialect SHALL render `DECIMAL(p,s)`, `DOUBLE PRECISION`, `BOOLEAN`, `DATE`, `CHAR(n)`, `VARCHAR(n)`, `INTERVAL YEAR(p) TO MONTH`, `INTERVAL DAY(p) TO SECOND(f)`, `GEOMETRY(srid)`, `HASHTYPE(n BYTE)`, and the TIMESTAMP targets per the fractional-seconds-precision and local-time-zone scenarios, each with the parameters the `dataType` carries
* *AND* a `HASHTYPE(8 BIT)` target, which Exasol sends as `bytesize` 1, SHALL render as `HASHTYPE(1 BYTE)`
* *AND* the DataFusion dialect SHALL render a CAST of a BOOLEAN value, a column included, to a `CHAR` or `VARCHAR` target as `(CASE <expr> WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)`, to match Exasol
* *AND* a query whose WHERE predicate carries a CAST to `INTERVAL`, `GEOMETRY`, or `HASHTYPE`, for example `WHERE CAST(c AS GEOMETRY) IS NULL` or `WHERE CAST(ts - ts AS INTERVAL DAY(2) TO SECOND(3)) IS NOT NULL`, SHALL return the same rows, or fail with the same error, as native Exasol, because the adapter applies that CAST in its Exasol-dialect wrapper over the sharded scan; a select-list item of these types routes per `pushdown-capabilities/pushdown-planning-capability-extensions`
* *AND* the DataFusion dialect SHALL refuse a CAST whose `dataType` is incomplete, outside Exasol's limits, or names an unknown type (`pushdown/pushdown-planning`)
* *AND* a CAST to or from text SHALL reach the DataFusion dialect only when `pushdown-types/pushdown-planning-scan-text-conversion` lets the scan compute it

### Scenario: CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect

* *GIVEN* a `function_scalar_cast` node whose `dataType` is a TIMESTAMP without local time zone and with precision `p` in 0-9, where an absent `fractionalSecondsPrecision` means `p` = 3
* *WHEN* the adapter renders the node
* *THEN* the Exasol dialect SHALL render bare `TIMESTAMP` when `p` is 3 and `TIMESTAMP(p)` otherwise
* *AND* the DataFusion dialect SHALL render `TIMESTAMP(p)` when `p` is in `{0, 3, 6, 9}`, including `TIMESTAMP(3)` when the precision is absent, because the DataFusion SQL frontend parses exactly these four and reads a bare `TIMESTAMP` as nanoseconds
* *AND* when `p` is in `{1, 2, 4, 5, 7, 8}` the DataFusion dialect SHALL decline the node and MUST NOT render an approximated `TIMESTAMP(p')` for any `p'` in `{0, 3, 6, 9}`
* *AND* a declined node SHALL be computed by Exasol in the adapter's Exasol-dialect wrapper, where `p` renders as above, and SHALL NOT produce a client-facing error
* *AND* the `EMITS` type the adapter declares for a select-list item of this type SHALL be bare `TIMESTAMP` when `p` is 3 and `TIMESTAMP(p)` otherwise, because Exasol validates a pushdown response positionally against `selectListDataTypes` and treats both spellings of precision 3 as one type
* *AND* a declined select-list item SHALL route the whole request to the qualified single-table wrapper, so Exasol computes every select-list item and the per-shard `LIMIT` and bounded top-N are given up, while the sharded fan-out, the referenced-column projection narrowing, and the scan-side WHERE predicate are retained

### Scenario: The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR

* *GIVEN* a `function_scalar_cast` node whose `dataType` is `{"type":"CHAR","size":20,"characterSet":"ASCII"}`
* *WHEN* the node is rendered in the Exasol dialect, as it is in the qualified single-table wrapper, the N-scan join wrapper, and the grouped-merge wrapper
* *THEN* the CAST target SHALL render as `CHAR(20) ASCII`, appending ` ASCII` exactly when `characterSet` equals `ASCII` case-insensitively and no suffix otherwise
* *AND* the target MUST NOT render as `VARCHAR({size})`, which Exasol rejects as `Data type mismatch ... Expected CHAR(20) ASCII` and which also strips the value's blank padding, nor as a bare length-less `CHAR` or `VARCHAR`, which Exasol's parser rejects (`sqlCode 04000`)
* *AND* the Exasol dialect SHALL render a `VARCHAR` target as `VARCHAR({size})` with the same ` ASCII` suffix rule
* *AND* the Exasol dialect SHALL render the `size` Exasol sent unchanged, because a `size` outside Exasol's limits already fails the query (`pushdown/pushdown-planning`)
* *AND* a nested CHAR CAST, `CAST(CAST(<agg> AS CHAR(20) ASCII) AS CHAR(20) ASCII)`, the shape the grouped-merge wrapper can produce, SHALL render `CHAR(20) ASCII` at both levels
* *AND* the two dialects SHALL diverge on the same CHAR node: bare `VARCHAR` in the DataFusion dialect, `CHAR({size})` plus any suffix in the Exasol dialect

### Scenario: A CAST to TIMESTAMP without a precision compares at three fractional digits

* *GIVEN* a virtual table whose timestamp column holds values with more than three fractional digits
* *AND* a query that filters on `CAST(<column> AS TIMESTAMP)` with no precision, for example `WHERE CAST(ts AS TIMESTAMP) = TIMESTAMP '2024-01-01 00:00:00.123'`, on Exasol 8.x, whose request omits the precision, or on Exasol 2025.x
* *WHEN* the adapter pushes the predicate into the scan
* *THEN* the scan SHALL compare the cast value at three fractional digits, as Exasol's `TIMESTAMP` holds it
* *AND* the query SHALL return the same rows, and a `COUNT(*)` over it the same count, as native Exasol on both versions
* *AND* the predicate SHALL stay in the scan's WHERE clause

### Scenario: A CAST to TIMESTAMP WITH LOCAL TIME ZONE keeps its declared precision

* *GIVEN* a query that selects `CAST(ts AS TIMESTAMP(6) WITH LOCAL TIME ZONE)`, whose `dataType` carries `withLocalTimeZone: true` and `fractionalSecondsPrecision: 6`
* *WHEN* the adapter plans the pushdown
* *THEN* the DataFusion dialect SHALL decline the CAST, because DataFusion's plain timestamp cannot reproduce the session time zone semantics
* *AND* the adapter's Exasol-dialect wrapper SHALL render the target as `TIMESTAMP(6) WITH LOCAL TIME ZONE`, and as bare `TIMESTAMP WITH LOCAL TIME ZONE` at precision 3
* *AND* the query SHALL return the same values as native Exasol in the same session, on Exasol 2025.x with the precision and on Exasol 8.x without it
