# Feature: Pushdown Planning — Capability Extensions Credential Reference

Extends `vs-adapter/pushdown-planning-capability-extensions`: the credential-reference guarantee of `vs-adapter/scan-spec-credential-reference` applies to this feature's generated SQL.

## Background

* The feature `vs-adapter/pushdown-planning-capability-extensions` generates pushdown SQL through the same builder paths as the features covered by `vs-adapter/scan-spec-credential-reference`.

## Scenarios

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `vs-adapter/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages
