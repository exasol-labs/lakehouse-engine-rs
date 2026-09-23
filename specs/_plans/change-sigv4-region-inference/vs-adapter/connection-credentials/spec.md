# Feature: Connection-Object Credential Source

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/connection-credentials/spec.md`.

<!-- DELTA:CHANGED -->
## Background

* **This delta discharges one deferral and changes no parsing or validation rule.** It implements issue #276, slice D of six (A-F). Every field this feature parses, every guard `validate_creds` applies, and every error text it produces are unchanged; what changes is only what a supplied storage credential MEANS once `use_vended_credentials` is true.
* **SUPERSEDES the "Vended Azure credentials are out of scope (issue #276, slice D)" bullet.** That bullet recorded that an Azure CONNECTION setting `use_vended_credentials` "is accepted and reads with its STATIC credentials", citing a tracked exception in `vs-adapter/pushdown-planning-cloud-credentials`. Vended Azure credentials are now IN scope, that exception is discharged, and no `#276` citation remains in this feature.
* **Under vending, a supplied storage credential is IRRELEVANT — ignored, not rejected.** `validate_creds` gains no rule about static storage fields appearing alongside `use_vended_credentials = true`. Rejecting the combination was considered and declined: the fields are optional on every path, a CONNECTION legitimately carries a static `region` and key pair for SigV4 catalog signing while vending its storage credentials, and adding a rejection would break exactly that shape. Saying "ignored" explicitly is the point of this bullet — an unstated irrelevance is the same silent ambiguity the rest of these rules exist to prevent.
* **The Azure-and-S3 mixed-fields rejection still applies under vending, deliberately.** A CONNECTION supplying both credential sets declares two incompatible intents, and that stays an error even though vending would read neither set. Relaxing the guard because the values happen to be unused would trade a loud, cheap error for a class of misconfiguration nobody can observe.
* **A vended-only Azure CONNECTION supplies NO Azure field, so no Azure guard fires.** Such a CONNECTION carries `warehouse`, its catalog-auth fields, and `use_vended_credentials = true` and nothing else. `validate_creds` accepts it, `storage_block` produces an S3 backend with every field empty, and the vended resolution never reads that backend — the table location's `abfss://` scheme selects the ADLS backend instead. This is why the vended path cannot be reached from `storage_block`'s output and had to become its own selector.
* **The SigV4 requirement on `access_key`, `secret_key`, and `region` is unchanged and stays independent of vending.** Those three sign the catalog `load_table` request before any credential is vended, so they are catalog-authentication inputs. What this delta separates is their second, previously conflated use: they no longer reach the scan's storage once vending is requested.
* **This delta separates CREDENTIALS from ADDRESSING under vending and is issue #330. It changes no parsing rule, no guard, and no error text.** `ConnectionCreds` gains no field and loses none; `validate_creds` gains no rule. What changes is only what a supplied `endpoint` and `region` MEAN once `use_vended_credentials` is true.
* **SUPERSEDES the `region` half of the SigV4-static-values bullet.** That bullet read: "**The SigV4 requirement on `access_key`, `secret_key`, and `region` is unchanged and stays independent of vending.** … What this delta separates is their second, previously conflated use: **they no longer reach the scan's storage once vending is requested.**" The static `access_key` and `secret_key` still do not reach the scan's storage under vending. The static `region` DOES, as addressing. The SigV4 REQUIREMENT itself is untouched — those three fields still sign the catalog `load_table` request when the CONNECTION states them — so what widens is the consequence of supplying `region`, not the rule that demands it.
* **SUPERSEDES this feature's earlier description sentence "Under vending, the CONNECTION's storage credentials are ignored; `vs-adapter/pushdown-planning-cloud-credentials` specifies the effective storage."** This feature is where the CONNECTION's field vocabulary is defined, and it elsewhere groups `endpoint` and `region` with `access_key` and `secret_key` under "static S3 credentials" — so a summary saying the storage credentials are ignored under vending would contradict the scenario below in the SAME spec file. The corrected sentence names the split: credentials ignored, `endpoint` and `region` read as addressing.
* **The precedence rule itself stays single-homed and is CITED here, not restated.** `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set" is the one normative home for which source wins per field. Restating it here would recreate the duplicated-rule-in-two-homes failure this plan exists to remove — that duplication is why this delta is needed at all.
* **This delta widens `path_style` to a tri-state and flips its unstated meaning from `true` to `false` (#130).** `ConnectionCreds.path_style` and `StorageCreds.path_style` become `Option<bool>`: absent = unstated, supplied = the operator chose. AMENDS three scenarios, ADDS one, SUPERSEDES two Background bullets.
* **SUPERSEDES "path_style is NOT admitted, and the reason is a type limitation".** The field CAN now express "unstated" and an explicitly stated value DOES participate in the CONNECTION-wins rule (`vs-adapter/pushdown-planning-cloud-credentials`).
* **SUPERSEDES "The vending-DISABLED path is untouched".** The `true` default is the defect #130 reports. An unstated `path_style` now resolves to `false`, matching AWS S3 client convention.
* **A resolved `false` DISCARDS a configured endpoint** (`build_undecorated_store`, `object_store.rs:226-231`, passes `endpoint` to `AmazonS3Builder` only inside `if storage.path_style`). This forces the new guard: a non-vended CONNECTION with an `endpoint` and no stated `path_style` is rejected rather than silently reaching the wrong host.
* **`StorageProps` keeps its `bool` and `true` serde default.** It is the resolved wire type; the adapter always serializes the field. Decision [5] in the decision-log records the rationale.
* **Neither the Iceberg table spec nor the Delta protocol constrain this choice.** `s3.path-style-access` appears nowhere in the Iceberg REST spec; Delta's `PROTOCOL.md` contains no `path-style`/`path_style`/`virtual-hosted`. Decision [9] records the evidence.
* **The SigV4 signing region is a separate value from the CONNECTION's `region`, and for a standard AWS Glue endpoint the two are independent even when both are present.** For a standard AWS Glue endpoint — an address with the `https` scheme whose host, compared case-insensitively, is exactly `glue.<region>.amazonaws.com`, where `<region>` is a commercial AWS region code (two letters, a hyphen, one or more letters, a hyphen, one or more digits, for example `us-east-1` or `ap-southeast-2`; port and path do not affect the match) — the signing region is ALWAYS the region that address names, even when `region` is also stated and even when the two differ, because a Glue catalog and the S3 buckets of its tables can sit in different regions and `region` places the store, not the signature. For any other address form — AWS GovCloud (US), AWS China, FIPS, dual-stack, VPC interface, a private or proxy host, or any `http` address — the signing region is the stated `region`. Only the SigV4 guard and catalog request signing read the signing region; `region` holds exactly what the CONNECTION states and independently places the S3 store.

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
path: when `use_sigv4` is true, the static `access_key`, the static `secret_key`, and a SigV4
signing region are required. These inputs sign the catalog `load_table` request, ahead of any
credential vending. A CONNECTION whose address is a standard commercial AWS Glue endpoint always
signs with the region that address names, whether or not `region` is also stated; any other
address requires a stated `region`. `endpoint` stays optional.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: When SigV4 is enabled, access_key, secret_key, and region are required

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true and supplies `warehouse` but omits one or more of `access_key`, `secret_key`, and `region`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming the missing field(s) and stating they are required when SigV4 signing is enabled
* *AND* the adapter SHALL apply this guard even when `use_vended_credentials` is true, because the static `access_key`, `secret_key`, and `region` sign the catalog `load_table` request before any vended credentials are used
* *AND* `endpoint` SHALL remain optional even when `use_sigv4` is true
* *AND* the error message MUST NOT contain any supplied credential value
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: When SigV4 is enabled, access_key, secret_key, and a signing region are required

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true and supplies `warehouse`
* *AND* the password omits `access_key`, omits `secret_key`, or omits `region` while the CONNECTION address is not a standard AWS Glue endpoint, per § Background
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming each omitted field among `access_key` and `secret_key`, and naming `region` when the CONNECTION supplies no signing region
* *AND* the error SHALL state that the named fields are required when SigV4 signing is enabled
* *AND* when the error names `region`, the error SHALL also state that a CONNECTION whose address is a standard AWS Glue endpoint of the form `https://glue.<region>.amazonaws.com` can omit `region`
* *AND* the adapter SHALL apply this guard even when `use_vended_credentials` is true, because the SigV4 inputs sign the catalog `load_table` request before any vended credential is used
* *AND* `endpoint` SHALL remain optional even when `use_sigv4` is true
* *AND* the error message MUST NOT contain any supplied credential value
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A standard AWS Glue endpoint supplies the SigV4 signing region when the CONNECTION omits region

* *GIVEN* a CONNECTION whose address is `https://glue.eu-west-1.amazonaws.com/iceberg`
* *AND* the CONNECTION's JSON password sets `use_sigv4` to true, supplies `warehouse`, `access_key`, and `secret_key`, and omits `region`
* *WHEN* the adapter resolves the connection and issues its SigV4-signed catalog requests
* *THEN* the adapter SHALL accept the CONNECTION without reporting `region` as missing, per the standard-AWS-Glue-endpoint definition in § Background
* *AND* the adapter SHALL sign every SigV4-signed catalog request for the region `eu-west-1`, covering both the namespace-enumeration requests and the `loadTable` request
* *AND* the adapter MUST NOT write the derived signing region into the CONNECTION's `region`, so `ConnectionCreds.region` SHALL stay empty and every other rule that reads it — the store-addressing precedence of `vs-adapter/pushdown-planning-cloud-credentials`, the S3 region `storage_block` builds, and the Azure-and-S3 mixed-fields guard — SHALL see it as unstated
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A standard AWS Glue endpoint signs the catalog request even when the CONNECTION states a different region

* *GIVEN* a CONNECTION whose address is `https://glue.eu-west-1.amazonaws.com/iceberg`
* *AND* the CONNECTION's JSON password sets `use_sigv4` to true and supplies `warehouse`, `access_key`, `secret_key`, and a `region` of `us-east-1`
* *WHEN* the adapter resolves the connection and issues its SigV4-signed catalog requests
* *THEN* the adapter SHALL accept the CONNECTION
* *AND* the adapter SHALL sign every SigV4-signed catalog request for `eu-west-1`, the region the address names, and NOT for the stated `us-east-1`
* *AND* `ConnectionCreds.region` SHALL stay `us-east-1` for every other rule that reads it, so the stated `region` places the S3 store in a region DIFFERENT from the one that signs the catalog request — the deployment shape where a Glue catalog and its tables' S3 bucket sit in different AWS regions
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Static storage credentials are ignored, not rejected, when vending is requested

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, sets `use_vended_credentials` to true, and supplies one credential set — either static S3 storage fields, or `account_name` plus exactly one of `account_key` and `sas_token`
* *WHEN* the adapter resolves the connection and the pushdown path resolves the effective scan storage for a table
* *THEN* the adapter SHALL accept the CONNECTION and MUST NOT report an error for supplying storage credentials alongside `use_vended_credentials`
* *AND* the adapter MUST NOT read any of `access_key`, `secret_key`, `session_token`, `account_name`, `account_key`, or `sas_token` into the effective scan storage for that table, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled
* *AND* the adapter SHALL read the CONNECTION's `endpoint`, `region`, and `path_style` into the effective scan storage as ADDRESSING when the CONNECTION states them, under the ONE precedence rule specified in `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set" — SUPERSEDING the recorded clause that named `endpoint` and `region` alone, and SUPERSEDING the recorded clause "the adapter MUST NOT read the CONNECTION's `path_style`... because that field is a plain boolean with a `true` default and cannot express 'unstated'", whose reason no longer holds (#130)
* *AND* "states them" SHALL mean non-empty for `endpoint` and `region` and PRESENT for `path_style`, because an absent boolean and an empty string are the two spellings of unstated that these field types admit
* *AND* the adapter SHALL still apply the existing guard rejecting a CONNECTION that supplies BOTH Azure and static S3 storage fields, because that input declares two incompatible intents whether or not either is read
* *AND* the adapter SHALL still apply the guard of § "When SigV4 is enabled, access_key, secret_key, and a signing region are required" when `use_sigv4` is true, because those inputs sign the catalog `load_table` request rather than reaching object storage
* *AND* a `region` the CONNECTION states SHALL ALSO place the store under vending, while a signing region derived from the CONNECTION address SHALL place no store
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line
<!-- /DELTA:CHANGED -->
