# Feature: EXPORT INTO SCRIPT Into a Partitioned Iceberg Table

The export writes each source row into the partition that the table's current partition spec assigns to it. That holds when it appends to an existing partitioned table, and when `PARTITION_BY` makes the table it creates partitioned. Each data file holds the rows of one partition, so a virtual schema prunes the exported files by partition.

## Background

* Statement and options: export/export-options § Background. Appending: export/export-append-iceberg-table § Background. Read through a virtual schema: export/export-into-script § Background.
* `PARTITION_BY = '<field>, ...'` lists, in order, the partition fields of a table the export creates. A field is `<column>` (identity), `bucket(<n>, <column>)`, `truncate(<w>, <column>)`, `year(<column>)`, `month(<column>)`, `day(<column>)`, or `hour(<column>)`. Transform names may be in any letter case, `<n>` and `<w>` are integers of at least 1, and blanks around a name, a number, or a separator are ignored. A column is a name without blanks, `,`, `(`, `)`, or `"`, or a name in double quotes with `""` for a quote. A column matches the source column of the same name, or else the one source column whose name differs from it only in letter case. A field is named after its source column: `<column>` for identity, else `<column>_bucket`, `_trunc`, `_year`, `_month`, `_day`, or `_hour`.
* The source types each transform of `PARTITION_BY` takes, by the export's type mapping (export/export-iceberg-table § Background):

  | Transform | Exasol source types |
  |-----------|---------------------|
  | identity | every type the export maps |
  | `bucket` | `DECIMAL`, `VARCHAR`, `CHAR`, `DATE`, `TIMESTAMP` |
  | `truncate` | `DECIMAL`, `VARCHAR`, `CHAR` |
  | `year`, `month`, `day` | `DATE`, `TIMESTAMP` |
  | `hour` | `TIMESTAMP` |

* A writer buffers the rows of each partition and keeps them within a budget taken from the per-instance memory limit. When the buffered rows exceed the budget, it writes out the partition that buffers the most bytes, and it writes every partition's remaining rows when its input ends. Each such write-out goes into data files of its own, which follow the table's write properties (export/export-append-iceberg-table) and roll over at `write.target-file-size-bytes`. A writer whose rows exceed the budget can therefore give a partition several data files, which can stay below `write.target-file-size-bytes`.
* Iceberg table spec § Partitioning: "Partition values for a data file must be the same for all records stored in the data file." § Partition Transforms: "All transforms must return `null` for a `null` input value". `bucket` is "Hash of value, mod `N`" over `int`, `long`, `decimal`, `date`, `timestamp`, `timestamptz`, and `string`, among others. `truncate` is "Value truncated to width `W`" over `int`, `long`, `decimal`, and `string`, among others. `year`, `month`, and `day` take a date or a timestamp, and `hour` takes a timestamp. No transform but identity takes `double` or `boolean`.

## Scenarios

### Scenario: An append places every row in the partition that the table's spec assigns

* *GIVEN* a table that another engine created and filled, partitioned by identity, `bucket`, `truncate`, `year`, `month`, `day`, and `hour` fields over `int`, `long`, `decimal`, `string`, `date`, `timestamp`, and `timestamptz` columns
* *WHEN* a user exports into it, with several writers or with `SINGLE_COMMIT = 'true'`, a source whose rows hold the partition column values of the table's rows
* *THEN* every data file that the export writes SHALL hold the rows of one partition, and its partition values SHALL equal those the other engine gave its rows of the same column values
* *AND* the table, read through a virtual schema, SHALL hold its earlier rows unchanged and every source row once

### Scenario: A row with a NULL partition column lands in the partition whose value is null

* *GIVEN* a partitioned table that another engine created and filled, with rows that hold NULL in some or all of their partition columns
* *WHEN* a user exports into it rows that hold NULL in every partition column, and rows that hold NULL in one partition column only
* *THEN* each data file that the export writes SHALL carry a null partition value for each field whose column its rows hold as NULL, and the other engine's partition values for the rest
* *AND* every NULL SHALL read back as NULL through a virtual schema

### Scenario: A filter on a partition column reads only the exported files of matching partitions

* *GIVEN* a partitioned table that holds rows an export wrote into several partitions of each field
* *WHEN* a user queries it through a virtual schema with an equality filter on the column of one field, for each transform
* *THEN* the query SHALL scan no export data file whose partition cannot hold a matching row, and every export data file whose partition and column bounds can hold one
* *AND* the query SHALL return the same rows as the same query over the export's source

### Scenario: An append writes under the table's current partition spec

* *GIVEN* a partitioned table whose partition spec changed after rows were written, such as `identity(region)` replaced by `bucket(8, id)`
* *WHEN* a user exports a source into it
* *THEN* every data file that the export writes SHALL carry the current partition spec and the partition values that spec assigns
* *AND* the table, read through a virtual schema, SHALL hold its earlier rows unchanged and every source row once

### Scenario: PARTITION_BY creates a partitioned table

* *GIVEN* a table that does not exist
* *WHEN* a user exports a source into it with a `PARTITION_BY` of one identity, `bucket`, `truncate`, `year`, `month`, `day`, and `hour` field each, with several writers or with `SINGLE_COMMIT = 'true'`
* *THEN* the new table's partition spec SHALL hold those fields in that order, with the names and transforms that § Background states
* *AND* every data file SHALL carry the partition values that another engine gives the same column values under the same transforms, and every row SHALL read back unchanged through a virtual schema

### Scenario: An invalid PARTITION_BY fails the export before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it into a new table with a `PARTITION_BY` that is empty or not a field list, names an unknown transform, has a `bucket` or `truncate` width below 1, names a column the source lacks or a name that matches several source columns only by letter case, gives two fields the same name, gives a field the name of a source column other than its own, or puts two of `year`, `month`, `day`, and `hour` on one column
* *THEN* the export SHALL fail before any source row is read, with an error that names `PARTITION_BY`, the offending field, and the accepted form
* *AND* the export SHALL NOT create the table

### Scenario: A transform that the column's type does not take fails the export before any data is written

* *GIVEN* a source with rows
* *WHEN* a user exports it into a new table with a `PARTITION_BY` field whose transform does not take its column's type, such as `bucket` of a `DOUBLE` column or `hour` of a `DATE` column
* *THEN* the export SHALL fail with an error that names the field, its column, and the column's Exasol type
* *AND* the export SHALL NOT create the table

### Scenario: PARTITION_BY into an existing table is refused

* *GIVEN* a table that exists in the catalog, and a source whose rows raise an error when they are read
* *WHEN* a user exports it into that table with `PARTITION_BY`
* *THEN* the export SHALL fail before any source row is read, with an error that names the table and states that `PARTITION_BY` applies only to a table the export creates
* *AND* the table SHALL keep its snapshots unchanged

### Scenario: An export into many partitions completes within a bounded amount of memory

* *GIVEN* a source several times larger than a writer's budget, whose consecutive rows fall into different ones of 1,000 partitions
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'` into a table partitioned by those values
* *THEN* the export SHALL succeed with the source row count
* *AND* the table SHALL hold more data files than partitions, because the budget wrote partitions out before the input ended
* *AND* the table, read through a virtual schema, SHALL hold every source row once
