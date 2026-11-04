[lakehouse-engine](../README.md) › [Docs](index.md) › Export

---

# Export

`EXPORT ... INTO SCRIPT` runs the engine's `LAKEHOUSE_EXPORT` script over an Exasol table, a query on virtual schema tables, or a query that joins both. The script checks the statement before it reads any source row. Each writer then counts the rows it receives, and Exasol reports the sum as the statement's row count.

This release writes no data to the lakehouse. The export validates the statement, runs the source query, and reports how many rows reached the writers. No Iceberg, Delta, or Parquet file is created and no table in the catalog changes.

## The statement

```sql
EXPORT <table> [(<columns>)]
INTO SCRIPT <schema>.LAKEHOUSE_EXPORT
AT <connection>
WITH TABLE_NAME = '<namespace>.<table>'
     [WRITERS_PER_NODE = '<n>']
     [SINGLE_COMMIT = '<true|false>'];
```

The source is a table, a table with a column list, or a parenthesized query:

```sql
EXPORT (SELECT id, amount FROM sales.orders WHERE amount > 100)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
AT LAKEHOUSE_CATALOG_CREDS
WITH TABLE_NAME = 'ns.orders_big';
```

`<schema>` is the schema that holds the engine scripts, `LHVS` by default. `<connection>` is the CONNECTION object that the virtual schema uses.

The statement fails before it reads any source row when any of these holds:

- It has no `AT <connection>`, or it has an inline `AT '<address>' USER '<user>' IDENTIFIED BY '<password>'`. Name a CONNECTION object instead. The error never repeats an inline password.
- It carries `TRUNCATE`, `REPLACE`, or `CREATED BY`. The export only appends rows.
- An option is missing, unknown, or outside its accepted form. The error names the option and its accepted form, and for an unknown option lists the accepted ones.
- It sets `WRITERS_PER_NODE` together with `SINGLE_COMMIT = 'true'`.

### Options

| Option | Required | Accepted value | Default |
|--------|----------|----------------|---------|
| `TABLE_NAME` | yes | `'<namespace>.<table>'`. The namespace may have several `.`-separated levels, and no part is empty. | none |
| `WRITERS_PER_NODE` | no | An integer of at least 1. | `1` |
| `SINGLE_COMMIT` | no | `true` or `false`, in any letter case. | `false` |

## Export role

A user who exports needs four privileges:

- the `EXPORT` system privilege,
- `EXECUTE` on `LAKEHOUSE_EXPORT`,
- the `CONNECTION` privilege on the CONNECTION, which Exasol requires for `AT <connection>`,
- `ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_EXPORT`.

One role holds all four. The installer prints these lines in its next-step template and runs none of them, because it runs before any CONNECTION exists. Run them as a DBA after you create the CONNECTION:

```sql
CREATE ROLE LAKEHOUSE_EXPORT_ROLE_LHVS;
GRANT EXPORT TO LAKEHOUSE_EXPORT_ROLE_LHVS;
GRANT CONNECTION LAKEHOUSE_CATALOG_CREDS TO LAKEHOUSE_EXPORT_ROLE_LHVS;
GRANT EXECUTE ON SCRIPT LHVS.LAKEHOUSE_EXPORT TO LAKEHOUSE_EXPORT_ROLE_LHVS;
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_EXPORT TO LAKEHOUSE_EXPORT_ROLE_LHVS;

GRANT LAKEHOUSE_EXPORT_ROLE_LHVS TO <user>;
```

After a re-install, run the `GRANT EXECUTE` and `GRANT ACCESS` lines again, because `CREATE OR REPLACE SCRIPT` drops them. Grant the role only to users who may use the CONNECTION's credentials. [Security](security.md#export-grants) explains why.

A user who holds every privilege except the script-scoped `ACCESS` grant gets an error that names the CONNECTION, `LAKEHOUSE_EXPORT`, and the user. No error and no generated SQL contains a value of the CONNECTION.

## Writer counts

The export deals the source rows in turn to G writers, where G is the node count times `WRITERS_PER_NODE`, capped at 300. A source with at least G rows runs exactly G writers. A source with fewer rows runs one writer per row.

| Setting | Writers |
|---------|---------|
| Neither option | One per node |
| `WRITERS_PER_NODE = '<w>'` | The node count times `w`, at most 300 |
| `SINGLE_COMMIT = 'true'` | One for the whole cluster |

To read the writer count of an export, turn on the session profile and read the `GROUP BY` part, whose `OUT_ROWS` is the number of writers:

```sql
ALTER SESSION SET PROFILE = 'ON';
EXPORT ... ;
ALTER SESSION SET PROFILE = 'OFF';
FLUSH STATISTICS;
SELECT OUT_ROWS FROM EXA_USER_PROFILE_LAST_DAY
WHERE SESSION_ID = CURRENT_SESSION AND COMMAND_NAME = 'EXPORT' AND PART_NAME = 'GROUP BY';
```

## Unsupported column types

The export refuses a source column of type GEOMETRY, HASHTYPE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, or TIMESTAMP WITH LOCAL TIME ZONE, because Exasol does not pass these types to a UDF reliably. Cast such a column to a supported type, for example VARCHAR or TIMESTAMP, in a query source:

```sql
EXPORT (SELECT id, CAST(shape AS VARCHAR(2000000)) AS shape FROM geo.places)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
AT LAKEHOUSE_CATALOG_CREDS
WITH TABLE_NAME = 'ns.places';
```

The refusal carries one of two errors, and a run returns either one:

- The export's error, which names the column as the source names it (table column, column-list entry, or query alias) and its type: `LAKEHOUSE_EXPORT: column "<name>" has type <type>, which the export does not support; cast it to a supported type`. Exasol wraps it in its cast-error prefix (`[22018] ... invalid character value for cast`).
- Exasol's own `[22002] Column type not supported: <type>`, which does not name the column.

Exasol's UDF type check runs concurrently with the export's check, so the same statement can return either error. A table with several unsupported columns returns either error in repeated runs. Find the offending column from the type in the message and the source's column types.

## Empty sources

A source that returns no rows and has only supported column types exports 0 rows and reports a row count of 0.

A source with no rows and a column of an unsupported type may be refused with either error above, or may export 0 rows. The result varies between runs, so do not rely on it. Cast the column to export it.

## Exporting a virtual table

Exasol refuses `EXPORT <virtual table>` and asks for `EXPORT (SELECT * FROM ...)`. Wrap the virtual table in a query:

```sql
EXPORT (SELECT * FROM my_lakehouse.lineitem)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
AT LAKEHOUSE_CATALOG_CREDS
WITH TABLE_NAME = 'ns.lineitem_copy';
```

A query may also join a virtual table with an Exasol table. The row count equals that of the same query run as a `SELECT`.
