# Feature: EXPORT INTO SCRIPT Privileges and Installation

The export script needs a script-scoped CONNECTION grant of its own. With `--with-export` the installer creates the script and prints the grants as a template. The script reads no CONNECTION value into any SQL or error text.

<!-- DELTA:CHANGED -->
## Background

* Statement and options: export/export-options § Background.
* An exporting user needs the `EXPORT` system privilege, `EXECUTE` on `LAKEHOUSE_EXPORT`, and `GRANT ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_EXPORT`, granted directly or through a role.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: A user who holds the export privileges exports rows

* *GIVEN* a user who holds the privileges § Background lists through one role, and no other privilege on the CONNECTION
* *WHEN* the user exports a table they may read
* *THEN* the export SHALL succeed and report the table's row count
