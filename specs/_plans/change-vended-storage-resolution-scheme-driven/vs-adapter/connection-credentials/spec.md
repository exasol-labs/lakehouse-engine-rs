# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and object-storage credentials from a named Exasol CONNECTION object instead of plain VS properties, so credentials are managed and access-controlled by Exasol (never typed inline into `CREATE VIRTUAL SCHEMA`), and so the engine can authenticate to a cloud Iceberg REST catalog (AWS Glue, Databricks) and its backing object storage. The credential set carried by the CONNECTION selects WHICH storage backend the scan reads through whenever `use_vended_credentials` is false, selects whether requests to the catalog are AWS SigV4-signed, and selects whether the engine requests short-lived vended credentials at table-load time. Under vending, the CONNECTION's storage credentials are ignored; `vs-adapter/pushdown-planning-cloud-credentials` specifies the effective storage. The REST-catalog authentication
mode (none / static bearer token / OAuth2 client-credentials) is carried on the same
CONNECTION and specified by the sibling feature `connection-credentials-catalog-auth`.

## Background

<!-- DELTA:NEW -->
* **This delta discharges one deferral and changes no parsing or validation rule.** It implements issue #276, slice D of six (A-F). Every field this feature parses, every guard `validate_creds` applies, and every error text it produces are unchanged; what changes is only what a supplied storage credential MEANS once `use_vended_credentials` is true.
* **SUPERSEDES the "Vended Azure credentials are out of scope (issue #276, slice D)" bullet.** That bullet recorded that an Azure CONNECTION setting `use_vended_credentials` "is accepted and reads with its STATIC credentials", citing a tracked exception in `vs-adapter/pushdown-planning-cloud-credentials`. Vended Azure credentials are now IN scope, that exception is discharged, and no `#276` citation remains in this feature.
* **Under vending, a supplied storage credential is IRRELEVANT — ignored, not rejected.** `validate_creds` gains no rule about static storage fields appearing alongside `use_vended_credentials = true`. Rejecting the combination was considered and declined: the fields are optional on every path, a CONNECTION legitimately carries a static `region` and key pair for SigV4 catalog signing while vending its storage credentials, and adding a rejection would break exactly that shape. Saying "ignored" explicitly is the point of this bullet — an unstated irrelevance is the same silent ambiguity the rest of these rules exist to prevent.
* **The Azure-and-S3 mixed-fields rejection still applies under vending, deliberately.** A CONNECTION supplying both credential sets declares two incompatible intents, and that stays an error even though vending would read neither set. Relaxing the guard because the values happen to be unused would trade a loud, cheap error for a class of misconfiguration nobody can observe.
* **A vended-only Azure CONNECTION supplies NO Azure field, so no Azure guard fires.** Such a CONNECTION carries `warehouse`, its catalog-auth fields, and `use_vended_credentials = true` and nothing else. `validate_creds` accepts it, `storage_block` produces an S3 backend with every field empty, and the vended resolution never reads that backend — the table location's `abfss://` scheme selects the ADLS backend instead. This is why the vended path cannot be reached from `storage_block`'s output and had to become its own selector.
* **The SigV4 requirement on `access_key`, `secret_key`, and `region` is unchanged and stays independent of vending.** Those three sign the catalog `load_table` request before any credential is vended, so they are catalog-authentication inputs. What this delta separates is their second, previously conflated use: they no longer reach the scan's storage once vending is requested.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Optional credential fields default sensibly

* *GIVEN* a CONNECTION password that supplies `warehouse` but omits the optional `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `use_sigv4`, `use_vended_credentials`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` fields
* *WHEN* the adapter builds the storage and catalog configuration
* *THEN* the adapter SHALL treat `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` as absent
* *AND* the adapter SHALL default `use_sigv4` and `use_vended_credentials` to false so existing static-S3 MinIO/REST stacks behave exactly as before
* *AND* the adapter SHALL apply the supplied `path_style` value (defaulting to a value that preserves existing MinIO behaviour)
* *AND* the backend `storage_block` builds from this CONNECTION SHALL be the S3 variant, unchanged from before this delta, because S3 stays the no-static-storage-fields default and a CONNECTION that names no Azure field describes no Azure backend
* *AND* this SHALL hold whether or not `use_vended_credentials` is set, so an existing vended-S3 CONNECTION that supplies no static storage field at all yields exactly the backend it yielded before
* *AND* this clause SHALL be read as constraining `storage_block`'s output ONLY, and MUST NOT be read as constraining the EFFECTIVE scan storage: when `use_vended_credentials` is true the effective backend is selected from the table location's URI scheme and `storage_block`'s output is never read, so the same CONNECTION resolves to an ADLS scan backend for an `abfss://` table
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Static storage credentials are ignored, not rejected, when vending is requested

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, sets `use_vended_credentials` to true, and supplies one credential set — either static S3 storage fields, or `account_name` plus exactly one of `account_key` and `sas_token`
* *WHEN* the adapter resolves the connection and the pushdown path resolves the effective scan storage for a table
* *THEN* the adapter SHALL accept the CONNECTION and MUST NOT report an error for supplying storage credentials alongside `use_vended_credentials`
* *AND* the adapter MUST NOT read any of `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, or `sas_token` into the effective scan storage for that table
* *AND* the adapter SHALL still apply the existing guard rejecting a CONNECTION that supplies BOTH Azure and static S3 storage fields, because that input declares two incompatible intents whether or not either is read
* *AND* the adapter SHALL still require `access_key`, `secret_key`, and `region` when `use_sigv4` is true, because those sign the catalog `load_table` request rather than reaching object storage
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line
<!-- /DELTA:NEW -->
