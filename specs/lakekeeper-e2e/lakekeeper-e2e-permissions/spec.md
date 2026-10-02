# Feature: Lakekeeper E2E Permissions (OpenFGA)

The Lakekeeper E2E stack enforces permissions through OpenFGA. Two Keycloak principals hold different table grants, and the suite checks what Lakekeeper answers for each. The scenarios extend `lakekeeper-e2e/lakekeeper-e2e-harness`.

## Background

* The scenarios run on the Docker stack of `lakekeeper-e2e/lakekeeper-e2e-harness`, which adds OpenFGA to `docker-compose.lakekeeper.yml`.
* The scenarios MUST fail (never skip) when the stack is unavailable.
* The suite runs behind the `lakekeeper-e2e` cargo feature.

## Scenarios

### Scenario: The Lakekeeper stack enforces permissions

* *GIVEN* the stack started from `docker-compose.yml` and `docker-compose.lakekeeper.yml`
* *WHEN* the harness provisions the catalog
* *THEN* Lakekeeper SHALL enforce permissions through OpenFGA, so a principal without a grant on a table is denied it
* *AND* the suite SHALL fail, not skip, when the stack reports any other authorization backend

### Scenario: Two principals hold different table grants

* *GIVEN* the provisioned fixture, two tables in one warehouse and two principals
* *WHEN* the harness asks Lakekeeper, as the operator, whether each principal may read each table
* *THEN* the first principal SHALL be allowed the first table and denied the second
* *AND* the second principal SHALL be allowed the second table and denied the first

### Scenario: The existing Lakekeeper scenarios pass with permissions enforced

* *GIVEN* the permission-enforcing stack
* *WHEN* the existing scenarios of this suite run
* *THEN* each SHALL pass unchanged, so enforcing permissions does not alter the static-credential, vended-credential, or join results
