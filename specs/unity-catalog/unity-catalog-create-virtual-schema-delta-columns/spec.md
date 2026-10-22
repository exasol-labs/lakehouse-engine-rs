# Feature: Unity Catalog Create Virtual Schema: Delta Column Declaration

Extends `unity-catalog/unity-catalog-create-virtual-schema` with the declaration of a Delta table's columns from `type_json` and the column-mapping mode, including the refusal of an unusable column-mapping annotation.

## Background

* The scenarios here share the parent feature's Background.

## Scenarios

### Scenario: Create virtual schema declares each Delta column from its type_json and the table's column-mapping mode property

* *GIVEN* three Unity Catalog Delta tables, one per column-mapping mode, whose `properties` set `delta.columnMapping.mode` to `name`, to `id`, or not at all, and whose columns' `type_json` carry the `delta.columnMapping.physicalName` and `delta.columnMapping.id` metadata the table's Delta log declares, and a partitioned Delta table whose partition column carries `partition_index` 0
* *WHEN* createVirtualSchema completes
* *THEN* each column's note SHALL carry the binding key the mode in the table's `properties` selects, read from the column's `type_json` metadata, per `delta/delta-table-planning`
* *AND* each column's Exasol `dataType` SHALL still be mapped from its `type_json`, per the type-mapping scenarios of this feature
* *AND* a Delta table's partition columns SHALL be the columns that carry a `partition_index`, in that order, and each one's note SHALL carry its position
* *AND* the adapter SHALL read no Delta log, request no storage credential, and read no object storage for any table

### Scenario: A Delta column whose column-mapping metadata is unusable is refused in its note and the table stays listed

* *GIVEN* a Unity Catalog Delta table whose `properties` set `delta.columnMapping.mode` to `name` and whose column `value` has a `type_json` without `delta.columnMapping.physicalName`, and a second Delta table whose `properties` set `delta.columnMapping.mode` to `label`
* *WHEN* createVirtualSchema completes and queries run over both tables
* *THEN* createVirtualSchema SHALL succeed, both tables SHALL be listed, and neither SHALL appear in `SKIPPED_TABLES`
* *AND* the note of `value` SHALL carry a refusal naming the column, the `name` mode, and the missing key, so a query that reads `value` fails with that reason and a query that reads only the table's other columns succeeds
* *AND* every column note of the second table SHALL carry a refusal naming the unrecognized mode value, so every query over that table fails with that reason
* *AND* no refusal SHALL contain a credential value
