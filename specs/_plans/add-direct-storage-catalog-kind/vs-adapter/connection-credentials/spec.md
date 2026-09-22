# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and object-storage credentials from a named Exasol CONNECTION object. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta amends the feature's trailing narrative paragraph and ONE scenario, and is issue #407.
  It changes no parsing rule, no selector, no storage-field spelling, and no error text for the two
  shipped catalog kinds.
* **`warehouse` stops being unconditionally required.** The narrative states it is the only
  unconditionally-required field. The `DIRECT_STORAGE` kind reaches no catalog, so a `warehouse` is
  meaningless under it. Supplying one is rejected rather than ignored. The requirement therefore
  becomes kind-scoped. Validation already has that shape: the acceptance rules are
  parameterized by the resolved `CatalogKind`.
* **A new sibling owns the direct-storage rules.** `vs-adapter/connection-credentials-direct-storage`
  specifies which fields that kind requires, which it rejects, and how the CONNECTION address agrees
  with the credential shape. It joins `connection-credentials-catalog-auth`,
  `connection-credentials-azure`, and `connection-credentials-unity-catalog` as a sibling of this
  feature. The narrative names it, so a reader finds the third kind's rules from here.
* **The shared projection and selector are UNCHANGED and are what the new kind reuses.** The nine
  storage field spellings, the Azure-versus-S3 selection rule, and the single `path_style` resolution
  keep exactly one owner each. The direct-storage kind adds a reader of that selector, not a second
  copy of it.
* The amended scenario is "One storage-credential projection and one selector serve both readers".
  Its reader count moves from two to three, because the direct-storage adapter path builds a store
  at `createVirtualSchema` time as well as at pushdown time. Every other clause of that scenario is
  carried unchanged.

The connection name is supplied as the VS property `CATALOG_CONNECTION`. The adapter
resolves it with `ctx.connection(name)`. The resolved `ConnectionObject.address` is the
catalog URI under a catalog kind and the storage base path under `DIRECT_STORAGE`. The resolved
`ConnectionObject.password` is a JSON object string carrying
the credential fields. The resolved password value MUST NEVER appear in any error
message, returned SQL, or log line. Both adapter entry points
(`createVirtualSchema`/`refreshVirtualSchema` and `pushdown`) resolve credentials through
this same path. `warehouse` is required under every CATALOG kind. `DIRECT_STORAGE` reaches no
catalog, so it REJECTS `warehouse`. That SUPERSEDES the unconditional form "`warehouse`
is the only unconditionally-required field". `vs-adapter/connection-credentials-direct-storage`
owns that kind's required, rejected, and address-agreement rules. Catalog
authentication and S3 storage credentials are fully orthogonal: any combination is valid,
including an unauthenticated catalog that vends S3 credentials and an OAuth-authenticated
catalog used with static S3 credentials (the catalog-auth modes themselves are specified in
`connection-credentials-catalog-auth`). The one conditional requirement is the AWS Glue SigV4
path: when `use_sigv4`
is true the static `access_key`, `secret_key`, and `region` are required (they sign the
catalog `load_table` request, ahead of any credential vending). `endpoint` stays optional.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One storage-credential projection and one selector serve both readers

* *GIVEN* the readers of a CONNECTION on the vending-disabled path — the adapter at plan time, the scan UDF at execution time under `vs-adapter/scan-spec-credential-reference`, and the adapter's table-enumeration path under `CatalogKind::DirectStorage`, which opens a store before any table exists
* *WHEN* any reader turns a resolved CONNECTION password into a storage backend
* *THEN* exactly ONE storage-credential projection type — declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, and `sas_token` — and exactly ONE selector over it SHALL serve EVERY reader, and no reader SHALL carry its own copy of either
* *AND* the projection SHALL carry `path_style` as an OPTION of boolean, preserving whether the CONNECTION stated a value, and the ONE selector SHALL be the single place that resolves an unstated value to `false`, so no two readers resolve the same absent field differently
* *AND* the parse step SHALL NOT resolve that value, because a parse that already substituted `false` would destroy the distinction the vended path reads
* *AND* that pair SHALL live in the crate that already owns `ConnectionCreds`, `StorageProps`, and `StorageBackend`, while `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` SHALL ALL STAY in the adapter module where `vs-adapter/catalog-crate-structure` pins them, so the scan path depends inward on a credential type and no function interpreting the Exasol CONNECTION object crosses the crate boundary
* *AND* the adapter's own CONNECTION-to-backend entry point SHALL reach that selector through the projection rather than re-implementing the selection, and the nine storage field spellings SHALL have exactly ONE reader, so no two readers normalise an empty or absent field differently
* *AND* the backend the readers derive from one CONNECTION password and one `allow_http` value SHALL be field-for-field EQUAL, asserted by a test over a password carrying every storage field, over one carrying empty strings, and over one omitting fields
* *AND* the scan-side reader SHALL apply the DERIVATION only and MUST NOT re-run the acceptance validation the adapter applies, because that validation is parameterized by the resolved `CatalogKind` and answers a plan-time question the adapter already answered for this query
* *AND* that acceptance validation SHALL require `warehouse` under every CATALOG kind and SHALL REJECT it under `DIRECT_STORAGE`, which reaches no catalog, SUPERSEDING this feature's recorded statement that `warehouse` is the only unconditionally-required field; `vs-adapter/connection-credentials-direct-storage` owns that kind's required, rejected, and address-agreement rules
* *AND* the selection rule — Azure when `account_name` is present with exactly one of `account_key` and `sas_token`, S3 otherwise — SHALL be UNCHANGED, so a CONNECTION resolves to the same backend it resolved to before this delta
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line from any reader
<!-- /DELTA:CHANGED -->
