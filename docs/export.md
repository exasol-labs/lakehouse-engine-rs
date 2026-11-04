[lakehouse-engine](../README.md) › [Docs](index.md) › Export

---

# Export

> **In development.** `EXPORT ... INTO SCRIPT` is not ready for use. It checks the statement and counts the rows, but writes nothing to the lakehouse. Syntax, options and permissions may change.

`EXPORT ... INTO SCRIPT` runs the engine's `LAKEHOUSE_EXPORT` script over an Exasol table, a query on virtual schema tables, or a query that joins both. The script checks the statement before it reads any source row. Each writer then counts the rows it receives, and Exasol reports the sum as the statement's row count.

Export is in development and opt-in. No release contains it yet, so the installer creates `LAKEHOUSE_EXPORT` only with `--with-export`, and the script works only on an engine built from source. On an engine without export code, the script fails with `F-UDF-CL-RUST-9001: Loader error: no entry point found`.

This release writes no data to the lakehouse. The export validates the statement, runs the source query, and reports how many rows reached the writers. No Iceberg, Delta, or Parquet file is created and no table in the catalog changes.

## The statement

```sql
EXPORT <table> [(<columns>)]
INTO SCRIPT <schema>.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = '<connection>'
     TABLE_NAME = '<namespace>.<table>'
     [WRITERS_PER_NODE = '<n>']
     [SINGLE_COMMIT = '<true|false>'];
```

The source is a table, a table with a column list, or a parenthesized query:

```sql
EXPORT (SELECT id, amount FROM sales.orders WHERE amount > 100)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
     TABLE_NAME = 'ns.orders_big';
```

`<schema>` is the schema that holds the engine scripts, `LHVS` by default. `CATALOG_CONNECTION` names the CONNECTION object that the virtual schema uses, as the virtual schema property of the same name does.

The statement fails before it reads any source row when any of these holds:

- It has an `AT` clause, named or inline (`AT '<address>' USER '<user>' IDENTIFIED BY '<password>'`). Use the `CATALOG_CONNECTION` option instead. The error never repeats an inline password.
- It carries `TRUNCATE`, `REPLACE`, or `CREATED BY`. The export only appends rows.
- An option is missing, unknown, or outside its accepted form. The error names the option and its accepted form, and for an unknown option lists the accepted ones.
- It sets `WRITERS_PER_NODE` together with `SINGLE_COMMIT = 'true'`.

### Options

| Option | Required | Accepted value | Default |
|--------|----------|----------------|---------|
| `CATALOG_CONNECTION` | yes | The name of a CONNECTION object. | none |
| `TABLE_NAME` | yes | `'<namespace>.<table>'`. The namespace may have several `.`-separated levels, and no part is empty. | none |
| `WRITERS_PER_NODE` | no | An integer of at least 1. | `1` |
| `SINGLE_COMMIT` | no | `true` or `false`, in any letter case. | `false` |

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

After a re-install, run the `GRANT EXECUTE` and `GRANT ACCESS` lines again, because `CREATE OR REPLACE SCRIPT` drops them. Grant the role only to users who may use the CONNECTION's credentials. [Security](security.md#export-grants) explains why.

A user who holds every privilege except the script-scoped `ACCESS` grant gets an error that names the CONNECTION, `LAKEHOUSE_EXPORT`, and the user. No error and no generated SQL contains a value of the CONNECTION.

## Writer counts

The export deals the source rows in turn to G writers, where G is the node count times `WRITERS_PER_NODE`, capped at 300. A source with at least G rows runs exactly G writers. A source with fewer rows runs one writer per row.

| Setting | Writers |
|---------|---------|
| Neither option | One per node |
| `WRITERS_PER_NODE = '<w>'` | The node count times `w`, at most 300 |
| `SINGLE_COMMIT = 'true'` | One for the whole cluster |

## Empty sources

A source that returns no rows exports 0 rows and reports a row count of 0.

## Exporting a virtual table

Exasol refuses `EXPORT <virtual table>` and asks for `EXPORT (SELECT * FROM ...)`. Wrap the virtual table in a query:

```sql
EXPORT (SELECT * FROM my_lakehouse.lineitem)
INTO SCRIPT LHVS.LAKEHOUSE_EXPORT
WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
     TABLE_NAME = 'ns.lineitem_copy';
```

A query may also join a virtual table with an Exasol table. The row count equals that of the same query run as a `SELECT`.
