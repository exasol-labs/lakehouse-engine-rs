# Feature: EXPORT INTO SCRIPT Into a Partitioned Iceberg Table

The export writes each source row into the partition that the table's current partition spec assigns to it. That holds when it appends to an existing partitioned table, and when `PARTITION_BY` makes the table it creates partitioned. Each data file holds the rows of one partition, so a virtual schema prunes the exported files by partition.

<!-- DELTA:CHANGED -->
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

* A writer buffers the rows of each partition and keeps them within a budget taken from the per-instance memory limit. When the buffered rows exceed the budget, it writes the partition that buffers the most bytes into a data file, and it writes every partition's remaining rows when its input ends. Each such write-out goes into data files of its own, which follow the table's write properties (export/export-iceberg-table § Background) and roll over at `write.target-file-size-bytes`. A writer whose rows exceed the budget can therefore give a partition several data files, which can stay below `write.target-file-size-bytes`.
* Iceberg table spec § Partitioning: "Partition values for a data file must be the same for all records stored in the data file." § Partition Transforms: "All transforms must return `null` for a `null` input value". `bucket` is "Hash of value, mod `N`" over `int`, `long`, `decimal`, `date`, `timestamp`, `timestamptz`, and `string`, among others. `truncate` is "Value truncated to width `W`" over `int`, `long`, `decimal`, and `string`, among others. `year`, `month`, and `day` take a date or a timestamp, and `hour` takes a timestamp. No transform but identity takes `double` or `boolean`.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: An export into many partitions completes within a bounded amount of memory

* *GIVEN* a source several times larger than a writer's budget, whose consecutive rows fall into different ones of 1,000 partitions
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'` into a table partitioned by those values
* *THEN* the export SHALL succeed with the source row count
* *AND* the table SHALL hold more data files than partitions, because the budget wrote partitions out before the input ended
* *AND* the table, read through a virtual schema, SHALL hold every source row once
