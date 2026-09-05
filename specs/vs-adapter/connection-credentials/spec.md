# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and object-storage credentials from a named Exasol CONNECTION object instead of plain VS properties, so credentials are managed and access-controlled by Exasol (never typed inline into `CREATE VIRTUAL SCHEMA`), and so the engine can authenticate to a cloud Iceberg REST catalog (AWS Glue, Databricks) and its backing object storage. The credential set carried by the CONNECTION selects WHICH storage backend the scan reads through whenever `use_vended_credentials` is false, selects whether requests to the catalog are AWS SigV4-signed, and selects whether the engine requests short-lived vended credentials at table-load time. Under vending, the CONNECTION's storage CREDENTIALS are ignored while its configured store `endpoint` and `region` participate in ADDRESSING; `vs-adapter/pushdown-planning-cloud-credentials` specifies the effective storage and the precedence between the two sources. The REST-catalog authentication
mode (none / static bearer token / OAuth2 client-credentials) is carried on the same
CONNECTION and specified by the sibling feature `connection-credentials-catalog-auth`.
The Azure Data Lake Storage Gen2 credential shape is specified by the sibling feature
`connection-credentials-azure`. Parameterizing validation by the resolved `CatalogKind`
and the Unity Catalog reuse of these auth fields is specified by the sibling feature
`connection-credentials-unity-catalog`.

## Background

* **This delta discharges one deferral and changes no parsing or validation rule.** It implements issue #276, slice D of six (A-F). Every field this feature parses, every guard `validate_creds` applies, and every error text it produces are unchanged; what changes is only what a supplied storage credential MEANS once `use_vended_credentials` is true.
* **SUPERSEDES the "Vended Azure credentials are out of scope (issue #276, slice D)" bullet.** That bullet recorded that an Azure CONNECTION setting `use_vended_credentials` "is accepted and reads with its STATIC credentials", citing a tracked exception in `vs-adapter/pushdown-planning-cloud-credentials`. Vended Azure credentials are now IN scope, that exception is discharged, and no `#276` citation remains in this feature.
* **Under vending, a supplied storage credential is IRRELEVANT — ignored, not rejected.** `validate_creds` gains no rule about static storage fields appearing alongside `use_vended_credentials = true`. Rejecting the combination was considered and declined: the fields are optional on every path, a CONNECTION legitimately carries a static `region` and key pair for SigV4 catalog signing while vending its storage credentials, and adding a rejection would break exactly that shape. Saying "ignored" explicitly is the point of this bullet — an unstated irrelevance is the same silent ambiguity the rest of these rules exist to prevent.
* **The Azure-and-S3 mixed-fields rejection still applies under vending, deliberately.** A CONNECTION supplying both credential sets declares two incompatible intents, and that stays an error even though vending would read neither set. Relaxing the guard because the values happen to be unused would trade a loud, cheap error for a class of misconfiguration nobody can observe.
* **A vended-only Azure CONNECTION supplies NO Azure field, so no Azure guard fires.** Such a CONNECTION carries `warehouse`, its catalog-auth fields, and `use_vended_credentials = true` and nothing else. `validate_creds` accepts it, `storage_block` produces an S3 backend with every field empty, and the vended resolution never reads that backend — the table location's `abfss://` scheme selects the ADLS backend instead. This is why the vended path cannot be reached from `storage_block`'s output and had to become its own selector.
* **The SigV4 requirement on `access_key`, `secret_key`, and `region` is unchanged and stays independent of vending.** Those three sign the catalog `load_table` request before any credential is vended, so they are catalog-authentication inputs. What this delta separates is their second, previously conflated use: they no longer reach the scan's storage once vending is requested.
* **This delta separates CREDENTIALS from ADDRESSING under vending and is issue #330. It changes no parsing rule, no guard, and no error text.** `ConnectionCreds` gains no field and loses none; `validate_creds` gains no rule. What changes is only what a supplied `endpoint` and `region` MEAN once `use_vended_credentials` is true.
* **SUPERSEDES the `region` half of the SigV4-static-values bullet.** That bullet read: "**The SigV4 requirement on `access_key`, `secret_key`, and `region` is unchanged and stays independent of vending.** … What this delta separates is their second, previously conflated use: **they no longer reach the scan's storage once vending is requested.**" The static `access_key` and `secret_key` still do not reach the scan's storage under vending. The static `region` DOES, as addressing. The SigV4 REQUIREMENT itself is untouched — those three fields are still required when `use_sigv4` is true and still sign the catalog `load_table` request — so what widens is the consequence of supplying `region`, not the rule that demands it.
* **SUPERSEDES this feature's earlier description sentence "Under vending, the CONNECTION's storage credentials are ignored; `vs-adapter/pushdown-planning-cloud-credentials` specifies the effective storage."** This feature is where the CONNECTION's field vocabulary is defined, and it elsewhere groups `endpoint` and `region` with `access_key` and `secret_key` under "static S3 credentials" — so a summary saying the storage credentials are ignored under vending would contradict the scenario below in the SAME spec file. The corrected sentence names the split: credentials ignored, `endpoint` and `region` read as addressing.
* **The precedence rule itself stays single-homed and is CITED here, not restated.** `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set" is the one normative home for which source wins per field. Restating it here would recreate the duplicated-rule-in-two-homes failure this plan exists to remove — that duplication is why this delta is needed at all.
* **`path_style` is NOT admitted, and the reason is a type limitation.** `ConnectionCreds.path_style` is a plain `bool` defaulting to `true` and cannot express "unstated", so it stays out of the CONNECTION-wins rule and out of the vended selectors; its non-participation is specified in `vs-adapter/pushdown-planning-cloud-credentials`. Widening the field to an `Option<bool>` is a NON-GOAL of this delta.
* **The vending-DISABLED path is untouched.** `storage_block` reads `endpoint`, `region`, `path_style`, and the key pair exactly as before, with the same `true` `path_style` default.

The connection name is supplied as the VS property `CATALOG_CONNECTION`. The adapter
resolves it with `ctx.connection(name)`. The resolved `ConnectionObject.address` is the
catalog URI; the resolved `ConnectionObject.password` is a JSON object string carrying
the credential fields. The resolved password value MUST NEVER appear in any error
message, returned SQL, or log line. Both adapter entry points
(`createVirtualSchema`/`refreshVirtualSchema` and `pushdown`) resolve credentials through
this same path. `warehouse` is the only unconditionally-required field. Catalog
authentication and S3 storage credentials are fully orthogonal: any combination is valid,
including an unauthenticated catalog that vends S3 credentials and an OAuth-authenticated
catalog used with static S3 credentials (the catalog-auth modes themselves are specified in
`connection-credentials-catalog-auth`). The one conditional requirement is the AWS Glue SigV4
path: when `use_sigv4`
is true the static `access_key`, `secret_key`, and `region` are required (they sign the
catalog `load_table` request, ahead of any credential vending); `endpoint` stays optional.

## Scenarios

### Scenario: Adapter reads catalog and storage credentials from a CONNECTION object

* *GIVEN* a `CREATE VIRTUAL SCHEMA` that supplies `CATALOG_CONNECTION = '<conn_name>'`
* *AND* an Exasol CONNECTION named `<conn_name>` whose address is the Iceberg REST catalog URI and whose password is a JSON object holding `warehouse`, `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter handles a `createVirtualSchema` or `pushdown` request
* *THEN* the adapter SHALL call `ctx.connection('<conn_name>')` to obtain the credentials
* *AND* the adapter SHALL use the resolved address as the catalog URI and the parsed JSON password to build the catalog and storage configuration
* *AND* the adapter SHALL carry `<conn_name>` itself on the resolved configuration, so the pushdown planning layer can reference the CONNECTION by name under `vs-adapter/scan-spec-credential-reference` instead of embedding the credentials it supplied
* *AND* the adapter MUST NOT read `ACCESS_KEY`, `SECRET_KEY`, `SESSION_TOKEN`, `CATALOG_URI`, `S3_ENDPOINT`, or `S3_REGION` from plain VS properties when `CATALOG_CONNECTION` is present
* *AND* neither the resolved password text nor any credential parsed out of it SHALL appear in any error message or log line, and neither SHALL appear in PLAINTEXT in the returned SQL under either setting of `use_vended_credentials` — the vended path carries only the sealed envelope of `vs-adapter/scan-spec-credential-reference`

### Scenario: One storage-credential projection and one selector serve both readers

* *GIVEN* the two readers of a CONNECTION on the vending-disabled path — the adapter at plan time, and the scan UDF at execution time under `vs-adapter/scan-spec-credential-reference`
* *WHEN* either reader turns a resolved CONNECTION password into a storage backend
* *THEN* exactly ONE storage-credential projection type — declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, and `sas_token` — and exactly ONE selector over it SHALL serve both readers, and neither reader SHALL carry its own copy of either
* *AND* that pair SHALL live in the crate that already owns `ConnectionCreds`, `StorageProps`, and `StorageBackend`, while `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` SHALL ALL STAY in the adapter module where `vs-adapter/catalog-crate-structure` pins them, so the scan path depends inward on a credential type and no function interpreting the Exasol CONNECTION object crosses the crate boundary
* *AND* the adapter's own CONNECTION-to-backend entry point SHALL reach that selector through the projection rather than re-implementing the selection, and the nine storage field spellings SHALL have exactly ONE reader, so the two readers cannot normalise an empty or absent field differently
* *AND* the backend the two readers derive from one CONNECTION password and one `allow_http` value SHALL be field-for-field EQUAL, asserted by a test over a password carrying every storage field, over one carrying empty strings, and over one omitting fields
* *AND* the scan-side reader SHALL apply the DERIVATION only and MUST NOT re-run the acceptance validation the adapter applies, because that validation is parameterized by the resolved `CatalogKind` and answers a plan-time question the adapter already answered for this query
* *AND* the selection rule — Azure when `account_name` is present with exactly one of `account_key` and `sas_token`, S3 otherwise — SHALL be UNCHANGED, so a CONNECTION resolves to the same backend it resolved to before this delta
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line from either reader

### Scenario: Missing connection name is rejected with a clear, credential-safe error

* *GIVEN* a VS request whose properties do not include a non-empty `CATALOG_CONNECTION`
* *WHEN* the adapter handles the request
* *THEN* the adapter SHALL return an error stating that `CATALOG_CONNECTION` is required
* *AND* the error message MUST NOT contain any credential value

### Scenario: Malformed connection password is rejected without leaking the password

* *GIVEN* a CONNECTION whose password is not a parseable JSON object
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating the CONNECTION password is not a valid JSON object
* *AND* the error message MUST NOT contain the password text

### Scenario: Connection password missing required credential fields is rejected listing only the field names

* *GIVEN* a CONNECTION whose JSON password omits `warehouse`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `warehouse` as the missing required field
* *AND* the error message MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT report any of `endpoint`, `region`, `access_key`, or `secret_key` as missing, because those fields are optional

### Scenario: Static S3 credentials are optional regardless of catalog auth mode

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, does not enable `use_sigv4`, but omits `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL accept the password without reporting any of `endpoint`, `region`, `access_key`, or `secret_key` as missing
* *AND* the adapter SHALL treat each omitted S3 field as absent, independently of whether any catalog-auth field or `use_vended_credentials` is set

### Scenario: When SigV4 is enabled, access_key, secret_key, and region are required

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true and supplies `warehouse` but omits one or more of `access_key`, `secret_key`, and `region`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming the missing field(s) and stating they are required when SigV4 signing is enabled
* *AND* the adapter SHALL apply this guard even when `use_vended_credentials` is true, because the static `access_key`, `secret_key`, and `region` sign the catalog `load_table` request before any vended credentials are used
* *AND* `endpoint` SHALL remain optional even when `use_sigv4` is true
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: Optional credential fields default sensibly

* *GIVEN* a CONNECTION password that supplies `warehouse` but omits the optional `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `use_sigv4`, `use_vended_credentials`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` fields
* *WHEN* the adapter builds the storage and catalog configuration
* *THEN* the adapter SHALL treat `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` as absent
* *AND* the adapter SHALL default `use_sigv4` and `use_vended_credentials` to false so existing static-S3 MinIO/REST stacks behave exactly as before
* *AND* the adapter SHALL apply the supplied `path_style` value (defaulting to a value that preserves existing MinIO behaviour)
* *AND* the backend `storage_block` builds from this CONNECTION SHALL be the S3 variant, unchanged from before this delta, because S3 stays the no-static-storage-fields default and a CONNECTION that names no Azure field describes no Azure backend
* *AND* this SHALL hold whether or not `use_vended_credentials` is set, so an existing vended-S3 CONNECTION that supplies no static storage field at all yields exactly the backend it yielded before
* *AND* this clause SHALL be read as constraining `storage_block`'s output ONLY, and MUST NOT be read as constraining the EFFECTIVE scan storage: when `use_vended_credentials` is true the effective backend is selected from the table location's URI scheme and `storage_block`'s output is never read, so the same CONNECTION resolves to an ADLS scan backend for an `abfss://` table

### Scenario: Static storage credentials are ignored, not rejected, when vending is requested

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, sets `use_vended_credentials` to true, and supplies one credential set — either static S3 storage fields, or `account_name` plus exactly one of `account_key` and `sas_token`
* *WHEN* the adapter resolves the connection and the pushdown path resolves the effective scan storage for a table
* *THEN* the adapter SHALL accept the CONNECTION and MUST NOT report an error for supplying storage credentials alongside `use_vended_credentials`
* *AND* the adapter MUST NOT read any of `access_key`, `secret_key`, `session_token`, `account_name`, `account_key`, or `sas_token` into the effective scan storage for that table, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled — SUPERSEDING the recorded clause that named `endpoint`, `region`, and `path_style` in that same list, which is now correct for CREDENTIALS alone
* *AND* the adapter SHALL read the CONNECTION's `endpoint` and `region` into the effective scan storage as ADDRESSING when they are non-empty, under the ONE precedence rule specified in `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set", which this feature CITES rather than restates
* *AND* the adapter MUST NOT read the CONNECTION's `path_style` into the effective scan storage under vending, because that field is a plain boolean with a `true` default and cannot express "unstated"
* *AND* the adapter SHALL still apply the existing guard rejecting a CONNECTION that supplies BOTH Azure and static S3 storage fields, because that input declares two incompatible intents whether or not either is read
* *AND* the adapter SHALL still require `access_key`, `secret_key`, and `region` when `use_sigv4` is true, because those sign the catalog `load_table` request rather than reaching object storage — and under vending the `region` they supply now ALSO places the store, which repairs the Glue vended path rather than changing what the guard demands
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line
