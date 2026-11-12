# Feature: Excluded Capabilities

The virtual schema property `EXCLUDED_CAPABILITIES` withholds named capabilities from the adapter's `getCapabilities` response, so Exasol evaluates them itself (#622). An operator can stop pushing one capability that misbehaves or is slow for a workload without a new adapter release.

## Background

* Exasol sends the virtual schema's persisted properties in `schemaMetadataInfo.properties` with each `getCapabilities` request, and requests the capabilities again for each query it plans over a virtual table.
* Exasol never delegates an unadvertised capability. It evaluates the construct itself over the rows the pushdown returns.
* The advertised list is the `CAPABILITIES` list of `vs-adapter/create-virtual-schema`. A feature that states the adapter advertises a capability describes that list before this feature withholds any name.
* `vs-properties/property-name-validation` lists the property name, and `vs-properties/property-value-validation` owns the combined error and which requests check a value.

## Scenarios

### Scenario: An excluded capability is withheld from getCapabilities

* *GIVEN* a virtual schema whose `EXCLUDED_CAPABILITIES` is `' FN_PRED_LIKE ,, FN_AGG_COUNT_DISTINCT,'`
* *WHEN* Exasol sends a `getCapabilities` request
* *THEN* the response SHALL list every advertised capability except `FN_PRED_LIKE` and `FN_AGG_COUNT_DISTINCT`, in the advertised order, and SHALL withhold no capability that the value does not list
* *AND* the adapter SHALL split the value on `,`, remove the whitespace around each entry, and skip each empty entry
* *AND* the adapter SHALL match each entry exactly and case-sensitively against the advertised names
* *AND* a virtual schema without `EXCLUDED_CAPABILITIES` SHALL receive the full advertised list

### Scenario: An excluded capability is evaluated by Exasol

* *GIVEN* a virtual schema whose `EXCLUDED_CAPABILITIES` is `'FN_PRED_LIKE'`
* *WHEN* the user runs a query whose `WHERE name LIKE 'event-1%'` matches a strict subset of a virtual table's rows
* *THEN* the pushdown SQL that `EXPLAIN VIRTUAL` shows MUST NOT contain the `LIKE` predicate, because Exasol evaluates it over the returned rows
* *AND* the query SHALL return the rows it returns over a virtual schema without the exclusion

### Scenario: A changed exclusion applies from the next query

* *GIVEN* a virtual schema without `EXCLUDED_CAPABILITIES`, whose `LIKE` query pushes the `LIKE` predicate
* *WHEN* the user runs `ALTER VIRTUAL SCHEMA ... SET EXCLUDED_CAPABILITIES = 'FN_PRED_LIKE'`, then the `LIKE` query, then `ALTER VIRTUAL SCHEMA ... SET EXCLUDED_CAPABILITIES = null`, then the `LIKE` query again
* *THEN* the first query's pushdown SQL MUST NOT contain the `LIKE` predicate, and the second query's pushdown SQL SHALL contain it, with no `ALTER VIRTUAL SCHEMA ... REFRESH` in between
* *AND* the adapter SHALL read the exclusion from the properties of each `getCapabilities` request and MUST NOT record it in `adapterNotes`

### Scenario: Request shapes that an exclusion makes reachable return the same rows

* *GIVEN* a virtual schema whose `EXCLUDED_CAPABILITIES` withholds a capability that the adapter's planning depends on, such as `SELECTLIST_PROJECTION`, `AGGREGATE_SINGLE_GROUP`, `FN_AGG_COUNT`, `FN_AGG_COUNT_DISTINCT`, or `JOIN`
* *WHEN* the user runs a query that uses the withheld construct
* *THEN* each query SHALL return the rows it returns over a virtual schema without the exclusion
* *AND* the adapter MUST NOT return different rows for a request shape that only an exclusion makes reachable, because Exasol does not re-check a delegated capability

### Scenario: Create, refresh, and set reject an entry the adapter does not advertise

* *GIVEN* a `createVirtualSchema`, `refresh`, or `setProperties` request whose effective `EXCLUDED_CAPABILITIES` is `'FN_PRED_LIK, fn_pred_like, MADE_UP, FN_PRED_LIK'`
* *WHEN* the adapter checks the properties
* *THEN* the request SHALL fail with one entry `invalid value '<entry>' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name` per distinct entry that is not an advertised name, in the order the value lists them, in the one error of `vs-properties/property-value-validation`
* *AND* for an entry that is at most two single-character edits from an advertised name, compared without regard to case, the entry SHALL append `(did you mean '<capability>'?)` with the nearest advertised name per the nearest-name rule of `vs-properties/property-name-validation`, so `FN_PRED_LIK` and `fn_pred_like` suggest `FN_PRED_LIKE` and `MADE_UP` gets no suggestion
* *AND* the entry SHALL echo the rejected entry under the echo rule of `vs-properties/property-value-validation`
* *AND* the adapter SHALL accept every advertised capability name, alone or in any combination, so no advertised capability is refused
* *AND* the adapter SHALL accept whitespace around an entry and an empty entry, and a value that holds only empty entries, such as `' , '`, SHALL withhold nothing

### Scenario: getCapabilities ignores an entry the adapter does not advertise

* *GIVEN* a virtual schema whose persisted `EXCLUDED_CAPABILITIES` is `'NO_SUCH_CAPABILITY, FN_PRED_LIKE'`, which an earlier adapter version that advertised `NO_SUCH_CAPABILITY` accepted
* *WHEN* the user runs a `LIKE` query over one of its virtual tables
* *THEN* the `getCapabilities` response SHALL withhold `FN_PRED_LIKE` and ignore `NO_SUCH_CAPABILITY`
* *AND* the adapter MUST NOT fail a `getCapabilities` request because of the `EXCLUDED_CAPABILITIES` value, so the query SHALL return the table's rows
* *AND* a `refresh` or `setProperties` request SHALL fail with the entry for `NO_SUCH_CAPABILITY` until a `setProperties` request removes it from the value
