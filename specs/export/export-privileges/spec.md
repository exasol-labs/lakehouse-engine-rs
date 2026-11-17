# Feature: EXPORT INTO SCRIPT Privileges and Installation

The export script needs a script-scoped CONNECTION grant of its own. With `--with-export` the installer creates the script and prints the grants as a template. The script reads no CONNECTION value into any SQL or error text.

## Background

* Statement and options: export/export-options § Background.
* An exporting user needs the `EXPORT` system privilege, `EXECUTE` on `LAKEHOUSE_EXPORT`, and `GRANT ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_EXPORT`, granted directly or through a role.

## Scenarios

### Scenario: A user who holds the export privileges exports rows

* *GIVEN* a user who holds the privileges § Background lists through one role, and no other privilege on the CONNECTION
* *WHEN* the user exports a table they may read
* *THEN* the export SHALL succeed and report the table's row count

### Scenario: A missing script-scoped CONNECTION grant fails with a named error

* *GIVEN* a user who holds every export privilege except `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <schema>.LAKEHOUSE_EXPORT`
* *AND* a source whose rows raise an error when they are read
* *WHEN* the user exports it
* *THEN* the export SHALL fail before any source row is read
* *AND* the error SHALL name the CONNECTION, `LAKEHOUSE_EXPORT`, and the user

### Scenario: No CONNECTION value appears in the generated SQL or an error

* *GIVEN* a CONNECTION whose address, user, and password are known
* *WHEN* an export succeeds, or fails in any way this feature describes
* *THEN* the SQL that `LAKEHOUSE_EXPORT` generates SHALL carry the CONNECTION name and no value of the CONNECTION
* *AND* no error message SHALL contain the CONNECTION's password

### Scenario: The installer creates the export script and prints the export grants only on request

* *GIVEN* the install script has uploaded the engine `.so`
* *WHEN* it runs without `--with-export`
* *THEN* it SHALL create no `LAKEHOUSE_EXPORT` script and print no export role lines
* *WHEN* it runs with `--with-export`
* *THEN* it SHALL create `LAKEHOUSE_EXPORT` as a RUST SET script with dynamic input that returns `DECIMAL(18,0)` and references the same `.so` as the other scripts
* *AND* the printed next-step template SHALL create one export role, `LAKEHOUSE_EXPORT_ROLE_<schema>`, that holds the privileges § Background lists for the template's CONNECTION, and SHALL tell the operator to grant that role to each exporting user
* *AND* the template SHALL print the export role lines for every installing user, including `SYS`
* *AND* the install script SHALL NOT run any of the export role's statements
