[lakehouse-engine](../README.md) › [Docs](index.md) › Export

---

# Export

> **In development.** `EXPORT ... INTO SCRIPT` is not ready for use. Syntax, options and permissions may change.

`EXPORT ... INTO SCRIPT` writes the rows of an Exasol table, a query on virtual schema tables, or a query that joins both into a new Iceberg table. It writes through the Iceberg REST catalog that a CONNECTION names, onto S3-compatible storage, and creates the namespace and the table. Exasol reports the number of written rows as the statement's row count.

Export is in development and opt-in. No release contains it yet, so the installer creates `LAKEHOUSE_EXPORT` only with `--with-export`, and the script works only on an engine built from source. On an engine without export code, the script fails with `F-UDF-CL-RUST-9001: Loader error: no entry point found`.

## The statement

```sql
EXPORT <table> [(<columns>)]
INTO SCRIPT <schema>.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = '<connection>'
     TABLE_NAME = '<namespace>.<table>'
     [WRITERS_PER_NODE = '<n>']
     [SINGLE_COMMIT = '<true|false>']
     [ALLOW_HTTP = '<true|false>'];
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
- The table exists. The export only creates new tables.
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

Only `ALLOW_HTTP = 'true'` lets the export send a request over plain HTTP: to the catalog address, the OAuth2 server, the AWS STS endpoint, the CONNECTION's storage endpoint, or a storage endpoint that the catalog vends. Without it, an `http://` address fails the statement with an error that names the CONNECTION field and `ALLOW_HTTP`.

## The new table

The export creates a format-version 2 Iceberg table at the location the catalog chooses. Every column is optional and carries the source column's name as Exasol reports it, so an unquoted identifier arrives in upper case. The writers write Parquet data files and each commits its files once, as one snapshot. Every snapshot's summary carries `lakehouse.export-id`, which identifies the statement, and `lakehouse.export-writer`, which identifies the writer.

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

## How an export runs

1. Before it reads any source row, the export makes catalog calls only. It authenticates, refuses an existing table, and creates every missing level of the namespace. It writes nothing to storage yet, so a storage error, such as a wrong key or a missing write permission, shows only when the writers write. The namespace stays even when the export then fails or writes no rows.
2. The writers read the source rows. Each opens the table at its first row: the first creates it, and the others take the table that a writer of the same statement created. A source with no rows creates no table.
3. Each writer writes its data files and commits them once. A commit that conflicts with another writer's, or whose answer is lost, is sent again only after the writer checks that the table holds no snapshot of its own, so its rows land once.

Two statements that export into the same new table at the same time leave one winner. One succeeds, the other fails with an error that names the table, and the table holds only the winner's rows.

## Writer counts

The export deals the source rows in turn to G writers, where G is the node count times `WRITERS_PER_NODE`, capped at 300. A source with at least G rows runs exactly G writers. A source with fewer rows runs one writer per row.

| Setting | Writers |
|---------|---------|
| Neither option | One per node |
| `WRITERS_PER_NODE = '<w>'` | The node count times `w`, at most 300 |
| `SINGLE_COMMIT = 'true'` | One for the whole cluster |

## SINGLE_COMMIT

`SINGLE_COMMIT = 'true'` makes the table appear only together with all its rows: the new table has one snapshot and no earlier metadata version. The export runs one writer for the whole cluster, because Exasol has no step after the last writer that could commit the files of several writers at once.

Iceberg REST makes a table appear with its data through staged table creation (`stage-create`): the writer stages the table, writes its data files, and creates the table together with its snapshot in one commit. Only `SINGLE_COMMIT` needs a catalog that accepts staged creation. Lakekeeper accepts it. On a catalog that refuses it, the export fails at the writer, with an error that names the catalog and states that `SINGLE_COMMIT` needs staged table creation, and creates no table. A catalog that ignores `stage-create` and creates the table at once also fails the export, with an error that names the catalog.

## When an export fails

- Without `SINGLE_COMMIT`, each writer commits on its own, and there is no final commit that could undo the others. A failed export can leave the commits of some writers in the table. When the first writer created the table before a storage error, the export leaves an empty table.
- A failed writer deletes the data files it wrote and did not commit, best effort. The files of a writer that crashed, was killed, or could not delete them remain. Without `SINGLE_COMMIT` they lie under the table location, where Iceberg orphan-file removal finds them. After a failed `SINGLE_COMMIT` export no table exists, so they need a cleanup in storage, under the location the catalog chose for the table.
- A writer whose commit outcome is unknown keeps its data files, because the table may hold them.

## The CONNECTION

The export uses the same CONNECTION as a virtual schema over the catalog (see [Catalogs](catalogs.md)), with these requirements:

- The catalog is an Iceberg REST catalog that needs no AWS SigV4 signing. A CONNECTION with `"use_sigv4": true`, as for AWS Glue's Iceberg REST endpoint, is refused.
- The storage is S3-compatible. A CONNECTION with an Azure storage field (`account_name`, `account_key`, `sas_token`) is refused, and so is a table location whose scheme is not `s3` or `s3a`.
- The catalog identity may create namespaces and tables and commit to them. Static storage keys need write access to the warehouse location. A read-only CONNECTION cannot export.

With `"use_vended_credentials": true`, the writers use the storage credentials that the catalog vends for the new table. A writer renews them through the catalog's `loadCredentials` endpoint five minutes before they expire. Lakekeeper vends credentials for one hour and offers `loadCredentials`. On a catalog without that endpoint, a writer that outlives its credentials fails, with an error that names the table and their expiry. An OAuth2 token is renewed before it expires.

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

This example exports into a Lakekeeper warehouse that vends credentials, and reads the new table back. The CONNECTION is the one of [Lakekeeper](catalogs.md#lakekeeper-oidc-via-keycloak--seaweedfs):

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
