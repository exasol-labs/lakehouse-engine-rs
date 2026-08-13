# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and object-storage credentials from a named Exasol CONNECTION object instead of plain VS properties, so credentials are managed and access-controlled by Exasol (never typed inline into `CREATE VIRTUAL SCHEMA`), and so the engine can authenticate to a cloud Iceberg REST catalog (AWS Glue, Databricks) and its backing object storage. The credential set carried by the CONNECTION selects WHICH storage backend the scan reads through whenever `use_vended_credentials` is false, selects whether requests to the catalog are AWS SigV4-signed, and selects whether the engine requests short-lived vended credentials at table-load time. Under vending, the CONNECTION's storage CREDENTIALS are ignored while its configured store `endpoint` and `region` participate in ADDRESSING; `vs-adapter/pushdown-planning-cloud-credentials` specifies the effective storage and the precedence between the two sources. The REST-catalog authentication
mode (none / static bearer token / OAuth2 client-credentials) is carried on the same
CONNECTION and specified by the sibling feature `connection-credentials-catalog-auth`.

## Background

* **This delta adds the Azure credential shape and nothing else.** It implements issue #275, slice C of six (A-F) for Azure Data Lake Storage Gen2 (`abfss://`) support. Every existing scenario keeps its behaviour: a CONNECTION that supplies no Azure field is parsed, validated, and projected exactly as before, byte for byte.
* **Three new optional password fields, all read by the same `nonempty_str` rule as every existing field:** `account_name` (the Azure storage account, not a secret — it appears in every `abfss://` URI), `account_key` (a shared-key secret), and `sas_token` (a shared-access-signature secret; a SAS supplied inline in the CONNECTION is a secret exactly as an account key is). An empty-string value is "absent", the convention every other field already uses.
* **The credential SHAPE selects the backend; there is no `backend` field.** Adding one would be a second source of truth free to disagree with the credentials actually supplied. `vs-adapter/storage-backend-enum` already records that `storage_block` is the ONLY site that selects a backend from input when vending is disabled; this delta is the first input that makes that selection observable, and it adds no second decision point.
* **The presence of ANY Azure field — not just `account_name` — makes the CONNECTION an Azure CONNECTION.** Keying selection on `account_name` alone would let a CONNECTION that supplies `account_key` and forgets `account_name` fall silently back to S3 with the key ignored. Keying on any-of-three turns that same input into a named-field error.
* **Exactly one credential, never two.** `AdlsCred` has an account-key state and a SAS state and no "both" state, so a CONNECTION supplying both describes a backend the type cannot represent. Rejecting is the only reading that does not silently pick one.
* **Mixing Azure and static S3 fields is rejected rather than resolved.** A credentials path MUST NOT resolve an ambiguous input silently. This is the one rule the issue text does not name; it is here because the alternative — an undeclared precedence between two credential sets — is exactly the silent misconfiguration the rest of this feature exists to prevent.
* **`use_sigv4` together with Azure fields is NOT given its own guard.** SigV4 already requires `access_key`, `secret_key`, and `region`, so such a CONNECTION is rejected either by the mixed-fields rule (when those are supplied) or by the existing SigV4 rule (when they are not). A third guard would add a second error for an input that already fails loud.
* **`allow_http` does not reach the Azure backend in this slice.** It arrives from the `ALLOW_HTTP` VS property and is consumed only by the S3 payload. Azurite-emulator support (plain-HTTP Azure endpoints) is out of scope here and is not silently half-wired: the Azure backend carries no HTTP-scheme knob at all.
* Every error added by this delta names FIELD NAMES only. No `account_key`, `sas_token`, or any other supplied value appears in any error message, returned SQL, or log line.
* **This delta discharges one deferral and changes no parsing or validation rule.** It implements issue #276, slice D of six (A-F). Every field this feature parses, every guard `validate_creds` applies, and every error text it produces are unchanged; what changes is only what a supplied storage credential MEANS once `use_vended_credentials` is true.
* **SUPERSEDES the "Vended Azure credentials are out of scope (issue #276, slice D)" bullet.** That bullet recorded that an Azure CONNECTION setting `use_vended_credentials` "is accepted and reads with its STATIC credentials", citing a tracked exception in `vs-adapter/pushdown-planning-cloud-credentials`. Vended Azure credentials are now IN scope, that exception is discharged, and no `#276` citation remains in this feature.
* **Under vending, a supplied storage credential is IRRELEVANT — ignored, not rejected.** `validate_creds` gains no rule about static storage fields appearing alongside `use_vended_credentials = true`. Rejecting the combination was considered and declined: the fields are optional on every path, a CONNECTION legitimately carries a static `region` and key pair for SigV4 catalog signing while vending its storage credentials, and adding a rejection would break exactly that shape. Saying "ignored" explicitly is the point of this bullet — an unstated irrelevance is the same silent ambiguity the rest of these rules exist to prevent.
* **The Azure-and-S3 mixed-fields rejection still applies under vending, deliberately.** A CONNECTION supplying both credential sets declares two incompatible intents, and that stays an error even though vending would read neither set. Relaxing the guard because the values happen to be unused would trade a loud, cheap error for a class of misconfiguration nobody can observe.
* **A vended-only Azure CONNECTION supplies NO Azure field, so no Azure guard fires.** Such a CONNECTION carries `warehouse`, its catalog-auth fields, and `use_vended_credentials = true` and nothing else. `validate_creds` accepts it, `storage_block` produces an S3 backend with every field empty, and the vended resolution never reads that backend — the table location's `abfss://` scheme selects the ADLS backend instead. This is why the vended path cannot be reached from `storage_block`'s output and had to become its own selector.
* **The SigV4 requirement on `access_key`, `secret_key`, and `region` is unchanged and stays independent of vending.** Those three sign the catalog `load_table` request before any credential is vended, so they are catalog-authentication inputs. What this delta separates is their second, previously conflated use: they no longer reach the scan's storage once vending is requested.
* This delta (plan `add-native-unity-catalog-client`, issue #318) parameterizes credential validation by the resolved `CatalogKind`, so the `warehouse`-required rule applies under the Iceberg REST kind only. The catalog kind arrives as an explicit input rather than from the CONNECTION password JSON. Every guarantee below is BEHAVIORAL, not code-level: validation is refactored to take the kind as a parameter, and the Iceberg REST listing path it feeds is refactored behind the shared `CatalogClient` trait, so the promise is that a connection resolved under the default kind is accepted or rejected identically and produces byte-identical error text — not that the code producing it is untouched.
* **This delta separates CREDENTIALS from ADDRESSING under vending and is issue #330. It changes no parsing rule, no guard, and no error text.** `ConnectionCreds` gains no field and loses none; `validate_creds` gains no rule. What changes is only what a supplied `endpoint` and `region` MEAN once `use_vended_credentials` is true.
* **SUPERSEDES the `region` half of the SigV4-static-values bullet.** That bullet read: "**The SigV4 requirement on `access_key`, `secret_key`, and `region` is unchanged and stays independent of vending.** … What this delta separates is their second, previously conflated use: **they no longer reach the scan's storage once vending is requested.**" The static `access_key` and `secret_key` still do not reach the scan's storage under vending. The static `region` DOES, as addressing. The SigV4 REQUIREMENT itself is untouched — those three fields are still required when `use_sigv4` is true and still sign the catalog `load_table` request — so what widens is the consequence of supplying `region`, not the rule that demands it.
* **SUPERSEDES this feature's earlier description sentence "Under vending, the CONNECTION's storage credentials are ignored; `vs-adapter/pushdown-planning-cloud-credentials` specifies the effective storage."** This feature is where the CONNECTION's field vocabulary is defined, and it elsewhere groups `endpoint` and `region` with `access_key` and `secret_key` under "static S3 credentials" — so a summary saying the storage credentials are ignored under vending would contradict the scenario below in the SAME spec file. The corrected sentence names the split: credentials ignored, `endpoint` and `region` read as addressing.
* **The precedence rule itself stays single-homed and is CITED here, not restated.** `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set" is the one normative home for which source wins per field. Restating it here would recreate the duplicated-rule-in-two-homes failure this plan exists to remove — that duplication is why this delta is needed at all.
* **`path_style` is NOT admitted, and the reason is a type limitation.** `ConnectionCreds.path_style` is a plain `bool` defaulting to `true` and cannot express "unstated", so it stays out of the CONNECTION-wins rule and out of the vended selectors; its non-participation is specified in `vs-adapter/pushdown-planning-cloud-credentials`. Widening the field to an `Option<bool>` is a NON-GOAL of this delta.
* **The vending-DISABLED path is untouched.** `storage_block` reads `endpoint`, `region`, `path_style`, and the key pair exactly as before, with the same `true` `path_style` default.
* **This delta (issue #331) adds ONE rule to this feature's rule list and changes no parsing rule, no other guard, and no existing error text.** The new rule rejects a CONNECTION supplying a `token` together with a complete `client_id`/`client_secret` pair. Its normative home is the sibling feature `vs-adapter/connection-credentials-catalog-auth` § "A CONNECTION supplying both a static token and OAuth2 client credentials is rejected", which owns the catalog-auth modes and their mutual exclusion; this feature CITES it rather than restating it. `ConnectionCreds` gains no field and loses none.
* **The new rule sits AFTER the SigV4 rules and is disjoint from the OAuth2-completeness rule.** Placing it after the SigV4 rules keeps every SigV4 error byte-identical: a CONNECTION enabling `use_sigv4` alongside any catalog-auth field is already rejected by the SigV4-versus-catalog-auth exclusion and never reaches the new rule. Placing it before the OAuth2-completeness rule is a readability choice with no behavioural consequence, because the new rule requires all three fields while the completeness rule requires exactly one of the pair — no input satisfies both.
* **SUPERSEDES the rule enumeration in scenario "Credential validation is parameterized by the resolved catalog kind."** That clause listed six rules as applying under `CatalogKind::IcebergRest` with BEHAVIOR UNCHANGED. The list now also carries the token-versus-OAuth exclusion, which is NEW rather than behaviour-unchanged and which applies under BOTH kinds. Leaving the enumeration alone would let it read as exhaustive while omitting the one rule this delta adds.

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

### Scenario: Azure account-key credentials select the ADLS storage backend

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, a non-empty `account_name`, and a non-empty `account_key`, and omits `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, and `sas_token`
* *WHEN* the adapter resolves the connection and builds the storage configuration
* *THEN* the adapter SHALL accept the password without reporting any missing field
* *AND* the resolved storage backend SHALL be the ADLS variant carrying the supplied `account_name` and an account-key credential holding the supplied `account_key`
* *AND* the adapter MUST NOT produce an S3 backend for this CONNECTION
* *AND* the supplied `account_key` MUST NOT appear in any error message, returned SQL, or log line

### Scenario: Azure inline-SAS credentials select the ADLS storage backend

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, a non-empty `account_name`, and a non-empty `sas_token`, and omits `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, and `account_key`
* *WHEN* the adapter resolves the connection and builds the storage configuration
* *THEN* the adapter SHALL accept the password without reporting any missing field
* *AND* the resolved storage backend SHALL be the ADLS variant carrying the supplied `account_name` and a SAS credential holding the supplied `sas_token`
* *AND* the adapter SHALL treat the `sas_token` value as a secret on every path that treats `account_key` as one, because a SAS supplied inline in the CONNECTION grants the same access
* *AND* the supplied `sas_token` MUST NOT appear in any error message, returned SQL, or log line

### Scenario: An Azure CONNECTION without exactly one account name and one credential is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse` and at least one of `account_name`, `account_key`, and `sas_token`, in one of three malformed shapes: `account_name` absent while a credential is present; `account_name` present while BOTH `account_key` and `sas_token` are present; or `account_name` present while NEITHER is present
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that an Azure CONNECTION requires `account_name` and exactly one of `account_key` and `sas_token`
* *AND* the error SHALL name the offending field names and MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT fall back to the S3 backend for any of the three shapes, because a malformed Azure credential set is an error and not an absent one

### Scenario: A CONNECTION mixing Azure and static S3 credential fields is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, at least one of `account_name`, `account_key`, and `sas_token`, AND at least one of `endpoint`, `region`, `access_key`, `secret_key`, and `session_token`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that Azure and S3 storage credentials cannot both be supplied on one CONNECTION
* *AND* the error SHALL name the supplied Azure field names and the supplied S3 field names and MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT apply a precedence rule between the two credential sets, because an undeclared precedence resolves an ambiguous credentials input silently

### Scenario: Adapter reads catalog and storage credentials from a CONNECTION object

* *GIVEN* a `CREATE VIRTUAL SCHEMA` that supplies `CATALOG_CONNECTION = '<conn_name>'`
* *AND* an Exasol CONNECTION named `<conn_name>` whose address is the Iceberg REST catalog URI and whose password is a JSON object holding `warehouse`, `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter handles a `createVirtualSchema` or `pushdown` request
* *THEN* the adapter SHALL call `ctx.connection('<conn_name>')` to obtain the credentials
* *AND* the adapter SHALL use the resolved address as the catalog URI and the parsed JSON password to build the catalog and storage configuration
* *AND* the adapter MUST NOT read `ACCESS_KEY`, `SECRET_KEY`, `SESSION_TOKEN`, `CATALOG_URI`, `S3_ENDPOINT`, or `S3_REGION` from plain VS properties when `CATALOG_CONNECTION` is present

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

### Scenario: Credential validation is parameterized by the resolved catalog kind

* *GIVEN* the mode-aware credential contract whose rule 1 makes `warehouse` the only unconditionally-required field
* *WHEN* the adapter resolves a CONNECTION under a resolved `CatalogKind` — `IcebergRest` by default, `UnityCatalogNative` when `CATALOG_KIND` selects it
* *THEN* the credential validation SHALL take the resolved `CatalogKind` as an input, and the `warehouse`-required rule SHALL apply under `CatalogKind::IcebergRest` ONLY, because a native Unity Catalog is addressed by `catalog.schema.table` and carries no Iceberg warehouse identifier
* *AND* under `CatalogKind::IcebergRest` every rule of this feature that predates the token-versus-OAuth exclusion — the `warehouse` requirement, the Azure/S3 mutual exclusion, the Azure-shape rules, the SigV4-versus-catalog-auth exclusion, the SigV4 required-fields rule, and the OAuth2 completeness rule — SHALL apply with BEHAVIOR UNCHANGED, so a connection resolved under the default kind produces byte-identical acceptance and byte-identical errors to before the `CatalogKind` parameter was introduced, even though the validation entry point itself gains that parameter
* *AND* the token-versus-OAuth exclusion SHALL apply under BOTH kinds and is the ONE rule of this feature that is not behaviour-unchanged, because it rejects a CONNECTION both kinds previously accepted; it is specified by `vs-adapter/connection-credentials-catalog-auth` and CITED here, so the kind-parameterized entry point carries no per-kind copy of it
* *AND* the `CatalogKind` SHALL arrive as an explicit validation input rather than being read from the CONNECTION password JSON, because the catalog kind is a virtual-schema property and not a credential field
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line under either kind

### Scenario: A Unity Catalog CONNECTION reuses the existing auth fields without a new credential field

* *GIVEN* a CONNECTION resolved under `CatalogKind::UnityCatalogNative` whose JSON password supplies at most one of a non-empty `token` and a `client_id`/`client_secret` pair, and may supply `oauth2_server_uri` and `scope`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` on the credentials through the SAME parsing this feature already applies, adding no new CONNECTION password field for Unity Catalog authentication
* *AND* the adapter SHALL accept a Unity Catalog CONNECTION that supplies none of those auth fields, because OSS Unity Catalog runs with authentication disabled
* *AND* the "at most one" precondition of this scenario SHALL be ENFORCED rather than assumed: a Unity Catalog CONNECTION supplying a `token` together with a complete `client_id`/`client_secret` pair SHALL be rejected by the same kind-independent rule that rejects it under `CatalogKind::IcebergRest`, specified by `vs-adapter/connection-credentials-catalog-auth`
* *AND* the resolved `token` and `client_secret` values MUST NOT appear in any error message, returned SQL, or log line
