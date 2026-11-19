[lakehouse-engine](../README.md) › [Docs](index.md) › Export

---

# Export

> **In development.** `EXPORT ... INTO SCRIPT` is not ready for use. Syntax, options and permissions may change.

`EXPORT ... INTO SCRIPT` writes the rows of an Exasol table, a query on virtual schema tables, or a query that joins both into a new Iceberg table or an existing one. It writes through the Iceberg REST catalog that a CONNECTION names, onto S3-compatible storage or Azure Data Lake Storage Gen2. For a new table it creates the namespace and the table, and into an existing table it appends. Exasol reports the number of written rows as the statement's row count.

Export is in development and opt-in, so the installer creates `LAKEHOUSE_EXPORT` only with `--with-export`. Releases from v0.58.4 contain it. On an earlier release, the script fails with `F-UDF-CL-RUST-9001: Loader error: no entry point found`.

## The statement

```sql
EXPORT <table> [(<columns>)]
INTO SCRIPT <schema>.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = '<connection>'
     TABLE_NAME = '<namespace>.<table>'
     [WRITERS_PER_NODE = '<n>']
     [SINGLE_COMMIT = '<true|false>']
     [ALLOW_HTTP = '<true|false>']
     [PARTITION_BY = '<field>, ...'];
```

The source is a table, a table with a column list, or a parenthesized query:

```sql
EXPORT (SELECT id, amount FROM sales.orders WHERE amount > 100)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
     TABLE_NAME = 'sales.orders_big';
```

`<schema>` is the schema that holds the engine scripts, `LHVS` by default. `CATALOG_CONNECTION` names the CONNECTION object that the virtual schema uses, as the virtual schema property of the same name does. A query column that is an expression needs an alias, because Exasol refuses it with `must name expression with a column alias`.

The statement fails before it reads any source row when any of these holds:

- It has an `AT` clause, named or inline (`AT '<address>' USER '<user>' IDENTIFIED BY '<password>'`). Use the `CATALOG_CONNECTION` option instead. The error never repeats an inline password.
- It carries `TRUNCATE`, `REPLACE`, or `CREATED BY`. The export never truncates or replaces a table.
- An option is missing, unknown, or outside its accepted form. The error names the option and its accepted form, and for an unknown option lists the accepted ones.
- It sets `WRITERS_PER_NODE` together with `SINGLE_COMMIT = 'true'`.
- Its `PARTITION_BY` is not a valid field list for the source (see [Partitioned tables](#partitioned-tables)), or it sets `PARTITION_BY` for a table that exists. The error names `PARTITION_BY` and the field, or the table.
- Two source columns have the same name, which Exasol refuses with `duplicate column name`, or a column has the name of an Iceberg metadata column, such as `_file` or `_pos`.
- The CONNECTION is one the export cannot write through (see [The CONNECTION](#the-connection)), or it names a plain HTTP address without `ALLOW_HTTP = 'true'`.
- The catalog is unreachable or refuses the CONNECTION's credentials. The error names the catalog.

### Options

| Option | Required | Accepted value | Default |
|--------|----------|----------------|---------|
| `CATALOG_CONNECTION` | yes | The name of a CONNECTION object. | none |
| `TABLE_NAME` | yes | `'<namespace>.<table>'`. The namespace may have several `.`-separated levels, and no part is empty. | none |
| `WRITERS_PER_NODE` | no | An integer of at least 1. | `1` |
| `SINGLE_COMMIT` | no | `true` or `false`, in any letter case. | `false` |
| `ALLOW_HTTP` | no | `true` or `false`, in any letter case. | `false` |
| `PARTITION_BY` | no | The partition fields of a table the export creates (see [Partitioned tables](#partitioned-tables)). Refused for a table that exists. | unpartitioned |

Only `ALLOW_HTTP = 'true'` lets the export send a request over plain HTTP: to the catalog address, the OAuth2 server, the AWS STS endpoint, the CONNECTION's storage endpoint, or a storage endpoint that the catalog vends. Without it, an `http://` address fails the statement with an error that names the CONNECTION field and `ALLOW_HTTP`. A vended `abfs://` table location also needs `ALLOW_HTTP = 'true'`, as it does for a virtual schema, although the export writes it over HTTPS. Without it, the writer fails with an error that names the table and `ALLOW_HTTP`.

## The table

### A new table

The export creates a format-version 2 Iceberg table at the location the catalog chooses, unpartitioned unless `PARTITION_BY` is set. Every column is optional and carries the source column's name as Exasol reports it, so an unquoted identifier arrives in upper case. The writers write Parquet data files and each commits its files once, as one snapshot. Every snapshot's summary carries `lakehouse.export-id`, which identifies the statement, and `lakehouse.export-writer`, which identifies the writer.

### Type mapping

| Exasol type | Iceberg type | Read back through a virtual schema |
|-------------|--------------|------------------------------------|
| `DECIMAL(p,s)` | `decimal(p,s)` | `DECIMAL(p,s)` |
| `DOUBLE` | `double` | `DOUBLE` |
| `BOOLEAN` | `boolean` | `BOOLEAN` |
| `VARCHAR(n)` | `string` | `VARCHAR(2000000)` |
| `CHAR(n)` | `string`, with the value's trailing blanks | `VARCHAR(2000000)` |
| `DATE` | `date` | `DATE` |
| `TIMESTAMP(p)`, p ≤ 6, and `TIMESTAMP` | `timestamp` | `TIMESTAMP(6)` |

Every value reads back unchanged, NULLs included. A wider read-back type, such as `TIMESTAMP(6)` for a `TIMESTAMP(3)` column, holds the same values.

Every other type fails the statement and creates no table. Exasol refuses a `GEOMETRY`, `HASHTYPE`, `INTERVAL`, or `TIMESTAMP WITH LOCAL TIME ZONE` column itself, with `Column type not supported: <type>`, for example `HASHTYPE(16 BYTE)`, or with `VM error: Unknown data type found`, which names no column. Which of the two a column gets varies between statements. A column that reaches the export fails with the export's own error, which names the column and its type and suggests a cast to `TIMESTAMP(6)`.

Exasol truncates `TIMESTAMP(7)` to `TIMESTAMP(9)` values to microseconds before the export reads them, without an error, so the export refuses these columns instead of writing changed values. A source with no rows reaches no writer, so its column types go unchecked: a zero-row source with such a column succeeds with a row count of 0 and creates no table.

### An existing table

When the table exists, the export appends the source rows to it. It never changes the table's schema, partition spec, properties, earlier snapshots, or data files. Every appended snapshot carries `lakehouse.export-id`.

Each source column fills the table column of the same name. A source column without one fills the one table column whose name differs only in letter case, so an unquoted Exasol name such as `ID` fills `id`. Column order does not matter. A table column of these types accepts a source column of these types:

| Exasol type | Iceberg column type |
|-------------|---------------------|
| `DECIMAL(p,s)` | `decimal(P,s)` with P ≥ p |
| `DECIMAL(p,0)` | `int`, `long` |
| `DOUBLE` | `double` |
| `BOOLEAN` | `boolean` |
| `VARCHAR(n)`, `CHAR(n)` | `string` |
| `DATE` | `date` |
| `TIMESTAMP(p)`, p ≤ 6 | `timestamp`, `timestamptz` |

Exasol `INTEGER` (`DECIMAL(18,0)`) and `BIGINT` (`DECIMAL(36,0)`) fill an `int` or a `long` column, and a value outside the column's range fails the export with an error that names the table and the column. A `timestamptz` column takes a `TIMESTAMP` as a date and time in UTC. A virtual schema reads `int` as `DECIMAL(10,0)`, `long` as `DECIMAL(20,0)`, and `timestamptz` as `TIMESTAMP` in UTC, so rows read through a virtual schema fill the table they came from when each of its columns has one of the Iceberg types above. No Exasol type fills a `float`, `time`, `uuid`, `fixed`, `binary`, nanosecond timestamp, `struct`, `list`, or `map` column, so a table with such a column takes no append.

A source that lacks a table column, even an optional one, has a column the table lacks, has a column whose type the table column does not accept, or has a name that matches several columns only by letter case fails the export. One error names the table and lists every mismatch, with the Exasol type of each source column and the Iceberg type of each table column. A NULL for a required table column fails the export with an error that names the table and the column.

The export appends to a partitioned table under its current partition spec (see [Partitioned tables](#partitioned-tables)). It refuses a table of format version 3 or later, with an error that names the table and its format version.

The export also refuses a table that keeps a dropped partition field under the name of a column. Apache Spark leaves this shape when it drops an identity partition field of a format version 1 table, such as `PARTITIONED BY (day)` followed by `DROP PARTITION FIELD day`. The commit library the export uses cannot commit to such a table, so the error names the table and the field before any write.

### Data files

The writers write Parquet data files, also into a table whose `write.format.default` names another format. Each data file follows the table's write properties:

| Property | Sets | Default |
|----------|------|-------------------|
| `write.parquet.compression-codec` | The codec: `zstd`, `snappy`, `gzip`, `brotli`, `lz4_raw`, `uncompressed`, or `lz4`, which the [Parquet format deprecates](https://parquet.apache.org/docs/file-format/data-pages/compression/#lz4): use `lz4_raw` instead | `zstd` |
| `write.parquet.compression-level` | The level of `zstd`, `gzip`, or `brotli` | The codec's own |
| `write.parquet.row-group-size-bytes` | The size at which a row group ends | 128 MiB |
| `write.parquet.page-size-bytes`, `write.parquet.page-row-limit`, `write.parquet.dict-size-bytes` | Page and dictionary sizes | 1 MiB, 20,000 rows, 2 MiB |
| `write.parquet.content-defined-chunking.*` | Content-defined chunking of pages, off unless `enabled` is `true` | Off |
| `write.target-file-size-bytes` | The size at which a writer starts a new data file | 512 MiB |

The row group size counts data before compression, and a row group holds at least one batch of 8,192 rows. A row group also ends at 1,048,576 rows.

A table the export creates carries `write.parquet.compression-codec = zstd`, `write.parquet.row-group-size-bytes = 134217728` (128 MiB), and `write.target-file-size-bytes = 268435456` (256 MiB), so every later writer of the table uses the same settings. To change these settings, set the properties on the table with another engine: every later append follows them.

An append follows the properties of the table it appends to and never changes them. A property the export cannot use fails the export before any data is written, with an error that names the table, the property, and its value, for example `invalid write property write.parquet.compression-codec = 'lzo'`. This covers the `lzo` codec, which the export cannot write, a row group size or page row limit of 0, and a value that is not a number where one is expected.

An export into an unpartitioned table writes each writer's rows into its open data file as they arrive, so a writer holds no rows beyond the row group in progress.

### Partitioned tables

The export writes each row into the partition that the table's current partition spec assigns to it, both when it appends to a partitioned table and when `PARTITION_BY` makes the table it creates partitioned. Each data file holds the rows of one partition and lies under the partition's path, such as `data/D_day=2024-01-15/`, so a virtual schema prunes the exported files by partition. A row whose partition column is NULL lands in the partition whose value is null, such as `data/D_day=null/`.

`PARTITION_BY` lists the partition fields of a new table in order:

```sql
EXPORT sales.orders
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
     TABLE_NAME = 'sales.orders_by_day'
     PARTITION_BY = 'day(ORDERED_AT), bucket(16, CUSTOMER_ID)';
```

A field is `<column>` (identity), `bucket(<n>, <column>)`, `truncate(<w>, <column>)`, `year(<column>)`, `month(<column>)`, `day(<column>)`, or `hour(<column>)`. Transform names may be in any letter case, `<n>` and `<w>` are integers of at least 1, and blanks around a name, a number, or a separator are ignored. A column is a name without blanks, `,`, `(`, `)`, or `"`, or a name in double quotes with `""` for a quote. It matches the source column of the same name, or else the one source column whose name differs from it only in letter case. A field is named after its source column: `<column>` for identity, else `<column>_bucket`, `_trunc`, `_year`, `_month`, `_day`, or `_hour`.

Each transform takes these source types:

| Transform | Exasol source types |
|-----------|---------------------|
| identity | every type the export writes |
| `bucket` | `DECIMAL`, `VARCHAR`, `CHAR`, `DATE`, `TIMESTAMP` |
| `truncate` | `DECIMAL`, `VARCHAR`, `CHAR` |
| `year`, `month`, `day` | `DATE`, `TIMESTAMP` |
| `hour` | `TIMESTAMP` |

The statement fails before it reads any source row when `PARTITION_BY` is empty or not a field list, names an unknown transform, has a width below 1, names a column the source lacks or a name that matches several source columns only by letter case, gives two fields the same name, gives a field the name of another source column, or puts two of `year`, `month`, `day`, and `hour` on one column. A transform that does not take its column's type fails the export at the writer, before it creates the table, with an error that names the field, the column, and the column's type. `PARTITION_BY` applies only to a table the export creates: with a table that exists, the statement fails before it reads any row, naming the table.

A writer into a partitioned table buffers the rows of each partition in memory, within a budget of half the per-instance memory limit after 200 MB of overhead, at least 64 MiB and at most 512 MiB, or 512 MiB when the limit is unknown. When the buffered rows exceed the budget, the writer writes the partition that buffers the most bytes into data files of its own, which start a new file at `write.target-file-size-bytes`. Each such write-out gets new files, so a partition's files can stay below `write.target-file-size-bytes`. A writer whose rows exceed the budget therefore gives a partition several data files, and a source whose consecutive rows fall into many different partitions gives more and smaller files. Rows reach the writers in turn, not by partition, so every writer can write a file for every partition. Fewer writers, such as `SINGLE_COMMIT = 'true'`, give fewer files.

## How an export runs

1. Before it reads any source row, the export makes catalog calls only. It authenticates and checks whether the table exists, which decides whether the writers create the table or append to it. For a new table it creates every missing level of the namespace. It writes nothing to storage yet, so a storage error, such as a wrong key or a missing write permission, shows only when the writers write. The namespace stays even when the export then fails or writes no rows.
2. The writers read the source rows. Each opens the table at its first row. For a new table the first writer creates it, and the others take the table that a writer of the same statement created. For an existing table each writer loads it and checks it and the source columns, so a column mismatch fails the export before any write. A source with no rows reaches no writer, so it creates no table and checks nothing.
3. Each writer writes its data files and commits them once. A commit that conflicts with another writer's, or whose answer is lost, is sent again only after the writer checks that the table holds no snapshot of its own, so its rows land once. A commit fails, with an error that names the table and what changed, when another engine changed the table's current schema or default partition spec after the writer checked it.

Two statements that export into the same table, which does not exist when either statement checks it, leave one winner. One succeeds, the other fails with an error that names the table, and the table holds only the winner's rows. A statement that checks after the other created the table appends to it.

## Writer counts

The export deals the source rows in turn to G writers, where G is the node count times `WRITERS_PER_NODE`, capped at 300. A source with at least G rows runs exactly G writers. A source with fewer rows runs one writer per row.

| Setting | Writers |
|---------|---------|
| Neither option | One per node |
| `WRITERS_PER_NODE = '<w>'` | The node count times `w`, at most 300 |
| `SINGLE_COMMIT = 'true'` | One for the whole cluster |

## SINGLE_COMMIT

`SINGLE_COMMIT = 'true'` makes a new table appear only together with all its rows: the new table has one snapshot and no earlier metadata version. An append adds exactly one snapshot, or none. The export runs one writer for the whole cluster, because Exasol has no step after the last writer that could commit the files of several writers at once.

Iceberg REST makes a new table appear with its data through staged table creation (`stage-create`): the writer stages the table, writes its data files, and creates the table together with its snapshot in one commit. Only `SINGLE_COMMIT` into a new table needs a catalog that accepts staged creation. An append needs none. Lakekeeper accepts it. On a catalog that refuses it, the export fails at the writer, with an error that names the catalog and states that `SINGLE_COMMIT` needs staged table creation, and creates no table. A catalog that ignores `stage-create` and creates the table at once also fails the export, with an error that names the catalog.

## When an export fails

- Without `SINGLE_COMMIT`, each writer commits on its own, and there is no final commit that could undo the others. A failed export can leave the commits of some writers in the table. When the first writer created the table before a storage error, the export leaves an empty table.
- A rerun of a failed export without `SINGLE_COMMIT` appends every row again on top of the commits that stayed. Before a rerun, find the committed snapshots by the failed statement's `lakehouse.export-id` and roll them back with another engine, or export with `SINGLE_COMMIT`.
- A NULL for a required column and a value outside an `int` or `long` column's range show only in the rows of the writer that reads them. With `SINGLE_COMMIT` the export then leaves the table unchanged. Without it, writers that committed first keep their commits.
- Parallel writers that append to one table retry conflicting commits up to the table's `commit.retry.num-retries`, and at least 20 times, as often as on a table the export creates. Many writers on one table can still exhaust it: lower `WRITERS_PER_NODE`, or raise the property above 20.
- Between two attempts of one commit a writer waits the table's `commit.retry.min-wait-ms` (default 100 ms), doubled after each retry up to its `commit.retry.max-wait-ms` (default 60 s), plus up to a tenth more at random.
- Each writer commits into the schema and partition spec it checked (see [How an export runs](#how-an-export-runs)). Each commit reloads the table first and fails if it was upgraded to format version 3. An upgrade that lands between that reload and the commit, and changes neither the snapshot, the schema, nor the partition spec, is not detected.
- A failed writer deletes the data files it wrote and did not commit, best effort. The files of a writer that crashed, was killed, or could not delete them remain. When the table exists they lie under the table location, where Iceberg orphan-file removal finds them. After a failed `SINGLE_COMMIT` export into a new table no table exists, so they need a cleanup in storage, under the location the catalog chose for the table.
- A writer whose commit outcome is unknown keeps its data files, because the table may hold them.

## The CONNECTION

The export uses the same CONNECTION as a virtual schema over the catalog (see [Catalogs](catalogs.md)), with these requirements:

- The catalog is an Iceberg REST catalog that needs no AWS SigV4 signing. A CONNECTION with `"use_sigv4": true`, as for AWS Glue's Iceberg REST endpoint, is refused.
- The storage is S3-compatible or ADLS Gen2, with the storage fields a virtual schema reads with: S3 keys, an IAM role, an Azure `account_name` with `account_key` or `sas_token`, or vended credentials. A CONNECTION that `CREATE VIRTUAL SCHEMA` refuses before it contacts the catalog, such as one with Azure storage fields and `aws_assume_role_arn`, is refused with the same error.
- The table location must be one the storage can address: `s3://` or `s3a://` for S3 keys or a role, `abfss://` for Azure fields, and any of these or `abfs://` for vended credentials. A writer refuses any other location, with an error that names the table and the scheme, before it writes a data file.
- The catalog identity may create namespaces and tables and commit to them. Static storage keys need write access to the warehouse location. A read-only CONNECTION cannot export.

With `"use_vended_credentials": true`, the writers use the storage credentials that the catalog vends for the table: S3 keys, or an ADLS SAS for the table location's host. A writer renews them through the catalog's `loadCredentials` endpoint five minutes before they expire, at the expiry the catalog states in `s3.session-token-expires-at-ms` or `adls.sas-token-expires-at-ms.<host>`, and signs every later storage request with the renewed credentials. Credentials without a stated expiry are used as vended and never renewed. Lakekeeper vends credentials for one hour, states their expiry, and offers `loadCredentials`. On a catalog without that endpoint, a writer that outlives its credentials fails, with an error that names the table and their expiry. An OAuth2 token is renewed before it expires.

With `aws_assume_role_arn`, a writer must finish within the role session, one hour by default, because the export does not renew assumed-role credentials.

## Export role

A user who exports needs three privileges:

- the `EXPORT` system privilege,
- `EXECUTE` on `LAKEHOUSE_EXPORT`,
- `ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_EXPORT`.

One role holds all three. The installer prints these lines in its next-step template when you pass `--with-export`, and runs none of them, because it runs before any CONNECTION exists. Run them as a DBA after you create the CONNECTION:

```sql
CREATE ROLE LAKEHOUSE_EXPORT_ROLE_LHVS;
GRANT EXPORT TO LAKEHOUSE_EXPORT_ROLE_LHVS;
GRANT EXECUTE ON SCRIPT LHVS.LAKEHOUSE_EXPORT TO LAKEHOUSE_EXPORT_ROLE_LHVS;
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_EXPORT TO LAKEHOUSE_EXPORT_ROLE_LHVS;

GRANT LAKEHOUSE_EXPORT_ROLE_LHVS TO <user>;
```

After a re-install, run the `GRANT EXECUTE` and `GRANT ACCESS` lines again, because `CREATE OR REPLACE SCRIPT` drops them. Grant the role only to users who may write through the CONNECTION's credentials. [Security](security.md#export-grants) explains why.

A user who holds every privilege except the script-scoped `ACCESS` grant gets an error that names the CONNECTION, `LAKEHOUSE_EXPORT`, and the user. No error and no generated SQL contains a value of the CONNECTION.

## Example: Lakekeeper

This example exports into a Lakekeeper warehouse that vends credentials, appends more rows, and reads the table back. The CONNECTION is the one of [Lakekeeper](catalogs.md#lakekeeper-oidc-via-keycloak--seaweedfs):

```sql
CREATE CONNECTION LAKEKEEPER_CREDS
  TO 'http://lakekeeper:8181/catalog'
  USER ''
  IDENTIFIED BY '{
    "warehouse":               "lakehouse_vended",
    "client_id":               "lakehouse",
    "client_secret":           "<secret>",
    "oauth2_server_uri":       "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
    "path_style":              true,
    "use_vended_credentials": true
  }';

EXPORT (SELECT id, amount FROM sales.orders)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = 'LAKEKEEPER_CREDS'
     TABLE_NAME = 'sales.orders_copy'
     ALLOW_HTTP = 'true';

EXPORT (SELECT amount, id FROM sales.orders_2025)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = 'LAKEKEEPER_CREDS'
     TABLE_NAME = 'sales.orders_copy'
     ALLOW_HTTP = 'true';

CREATE VIRTUAL SCHEMA SALES_LAKE
  USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEKEEPER_CREDS'
  NAMESPACE          = 'sales'
  ALLOW_HTTP         = 'true';

SELECT * FROM SALES_LAKE.ORDERS_COPY;
```

The stack serves plain HTTP, so both the export and the virtual schema set `ALLOW_HTTP`. Against HTTPS addresses, drop it.

## Exporting a virtual table

Exasol refuses `EXPORT <virtual table>` and asks for `EXPORT (SELECT * FROM ...)`. Wrap the virtual table in a query:

```sql
EXPORT (SELECT * FROM my_lakehouse.lineitem)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
     TABLE_NAME = 'ns.lineitem_copy';
```

A query may also join a virtual table with an Exasol table. The row count equals that of the same query run as a `SELECT`.
