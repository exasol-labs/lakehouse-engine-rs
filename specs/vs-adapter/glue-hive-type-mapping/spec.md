# Feature: Glue Hive Type Mapping

Maps a Glue column's Hive type string to the engine's types. A parser turns the string into a Spark type, and the Spark-type classifier the Delta and Unity Catalog readers use types it, so one classifier types every catalog-declared column.

## Background

* AWS Glue API, `Column.Type`: "The data type of the `Column`." It is a free-form string. Hive and Trino write parameterized and nested forms such as `decimal(38,10)`, `varchar(1)`, `map<varchar(1),int>`, and `struct<x:int,y:string>` (verified live, #410).
* The Spark-type classifier and its refusal rules belong to `vs-adapter/delta-type-mapping`.
* Exasol has no nested type, so a nested column surfaces as a JSON `VARCHAR(2000000)`. This is an Exasol target-type limitation, not a specification gap.

## Scenarios

### Scenario: Every Hive primitive type maps to its Spark type

* *GIVEN* Glue columns typed `tinyint`, `smallint`, `int`, `integer`, `bigint`, `float`, `double`, `boolean`, `string`, `varchar(10)`, `char(5)`, `date`, `timestamp`, `decimal`, `decimal(10,2)`, and `DECIMAL( 38 , 10 )`
* *WHEN* the parser reads each type string
* *THEN* the parser SHALL yield `byte`, `short`, `integer`, `integer`, `long`, `float`, `double`, `boolean`, `string`, `string`, `string`, `date`, `timestamp_ntz`, `decimal(10,0)`, `decimal(10,2)`, and `decimal(38,10)`, ignoring letter case and whitespace around arguments
* *AND* the classifier SHALL give each column the Arrow tag it gives the same Spark type in a Delta table, so `decimal(38,10)` carries the string tag because its precision is outside Exasol's `DECIMAL` domain

### Scenario: Nested Hive types parse recursively and render as JSON text

* *GIVEN* Glue columns typed `array<int>`, `map<varchar(1),int>`, `struct<x:int,y:string>`, and `array<struct<a:decimal(5,2)>>`
* *WHEN* the parser reads each type string
* *THEN* the parser SHALL yield the Spark array, map, or struct type with every member typed recursively and declared nullable
* *AND* the classifier SHALL give each column the string tag and the nested descriptor the JSON renderer reads, so the column returns one JSON document per row

### Scenario: A binary, unrecognized, or malformed Hive type refuses only its column

* *GIVEN* a Glue table with an `int` column and columns typed `binary`, `struct<b:binary>`, `uniontype<int,string>`, `interval_day_time`, `map<int>`, and an empty type string
* *WHEN* the pushdown plans the table
* *THEN* the `binary` and `struct<b:binary>` columns SHALL be refused per `vs-adapter/binary-column-refusal`
* *AND* every unrecognized or malformed type SHALL refuse its column with a reason quoting the type string
* *AND* a refused column SHALL fail only the queries that read or emit it, and a table whose every column is refused SHALL be refused as a whole, per `vs-adapter/delta-type-mapping`

### Scenario: The listing declares each Glue column through the Spark listing mapping

* *GIVEN* the Glue columns of the three scenarios above
* *WHEN* createVirtualSchema lists the table
* *THEN* each column SHALL be declared with the Exasol type the Unity Catalog listing declares for the same Spark type, so `tinyint`, `smallint`, `int`, and `bigint` declare `DECIMAL(3,0)`, `DECIMAL(5,0)`, `DECIMAL(10,0)`, and `DECIMAL(20,0)`, `float` and `double` declare `DOUBLE PRECISION`, `date` declares `DATE`, and `decimal(10,2)` declares `DECIMAL(10,2)`
* *AND* `timestamp` SHALL declare the catalog-declared timestamp type of `datafusion-scan/type-mapping`
* *AND* every string, nested, unrecognized, or malformed column SHALL declare `VARCHAR(2000000)`, as a binary column does per `vs-adapter/binary-column-refusal`, so a column type never fails the listing
