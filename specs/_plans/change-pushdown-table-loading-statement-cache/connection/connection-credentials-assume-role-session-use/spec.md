# Feature: Assumed-Role Session Use

Defines where the session of an assumed AWS IAM role replaces the CONNECTION's own key pair: SigV4 catalog requests, native Glue requests, and non-vended object storage. The scan receives the session only inside the sealed envelope.

## Background

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The adapter assumes the role once per request and substitutes the session

* *GIVEN* the recorded scenario of this title
* *WHEN* this plan is recorded
* *THEN* this scenario SHALL be REMOVED, because a later pushdown request of a statement reuses the session that the first pushdown assumed
* *AND* it SHALL be REPLACED by "The adapter assumes the role once per statement and virtual schema and substitutes the session" below
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: The adapter assumes the role once per statement and virtual schema and substitutes the session

* *GIVEN* a CONNECTION that names a role and carries the base key pair
* *WHEN* the adapter handles one `createVirtualSchema`, `refresh`, `setProperties`, or `pushdown` request through that CONNECTION
* *THEN* a `createVirtualSchema`, `refresh`, or `setProperties` request, and the first pushdown request of a statement on a virtual schema, SHALL send exactly ONE `AssumeRole` call before its first catalog or object-storage request
* *AND* a later pushdown request of the same statement on the same virtual schema SHALL send no `AssumeRole` call and SHALL use the session that the first pushdown assumed
* *AND* the adapter SHALL use that session for every table the request resolves, including every side of a join, for catalog signing, Glue requests, non-vended object storage, and error redaction
* *AND* the CONNECTION's acceptance validation and the sealing key SHALL use the CONNECTION as stated, not the assumed session
* *AND* a CONNECTION that names no role SHALL cause no STS call, and the adapter MUST NOT reuse a session across statements or across virtual schemas
<!-- /DELTA:NEW -->
