# Feature: Lakekeeper E2E Harness (OIDC + SeaweedFS)

End-to-end test suite that verifies the lakehouse VS query path against a Lakekeeper Iceberg REST
catalog backed by SeaweedFS object storage. Unchanged by this plan. This section is unmarked, so
`speq record` keeps the recorded description. See `specs/lakekeeper-e2e/lakekeeper-e2e-harness/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/lakekeeper-e2e/lakekeeper-e2e-harness/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The allowall suite runs on the allowall authorizer

* *GIVEN* the stack started from `docker-compose.yml` and `docker-compose.lakekeeper.yml` only
* *WHEN* the harness provisions the catalog
* *THEN* `GET /management/v1/info` SHALL report `authz-backend` as `allow-all`
* *AND* the suite SHALL fail, not skip, when it reports any other backend, so a stack carrying the OpenFGA overlay of `lakekeeper-e2e/lakekeeper-authz-contract` never runs this suite unnoticed
<!-- /DELTA:NEW -->
