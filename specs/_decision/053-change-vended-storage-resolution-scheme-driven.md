# Decisions: change-vended-storage-resolution-scheme-driven

## ADR: Two selectors on disjoint inputs, not one selection site

**ID:** two-vended-static-selectors-on-disjoint-inputs
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted
**Supersedes:** storage-backend-exhaustive-variant-naming-owners

### Context

`vs-adapter/storage-backend-enum` named `storage_block` as the only place a backend is selected from input. The vended path now selects its variant from the table location's URI scheme, so that clause is misleading. A single selector would need CONNECTION parsing deferred past `loadTable` for every request, and still could not serve a request that resolves no table.

### Decision

`storage_block` stays the static selector, reads the CONNECTION credential shape, and runs only when `use_vended_credentials` is false. `resolve_vended_storage` is the vended selector, reads the table location scheme from the `loadTable` response, and runs only when vending is true. One decision point, the `use_vended_credentials` branch in `resolve_file_list`, chooses between them.

### Options Considered

| Option | Verdict |
|--------|---------|
| One selector that receives the URI scheme | Rejected: the scheme is known only after `loadTable`, which runs later, once per table, and never on `createVirtualSchema` |
| The vended arm mutates the payload `storage_block` returned | Rejected: the feature already forbids reaching into the payload to finish construction |
| Call the scheme switch "not a selection" and keep the one-site clause | Rejected: a false clause on a credentials path is worse than superseding it |

### Consequences

The spec clause becomes "exactly two selectors on disjoint inputs, one decision point". No path runs both selectors, and neither overrides the other.

## ADR: Delete the `base: &StorageBackend` parameter

**ID:** resolve-vended-storage-drops-base-backend-parameter
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

`resolve_vended_storage` overlaid vended values field by field onto the backend `storage_block` had already chosen. That made it a second selector reading the first's output and created six per-field absence-and-preservation conventions.

### Decision

`resolve_vended_storage` takes the load result, the anchor, and `allow_http`, and returns a `StorageBackend`. It takes no backend and no CONNECTION-derived value. `allow_http` is a virtual-schema property, not a CONNECTION field.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `base` and read only `allow_http` from it | Rejected: leaves one CONNECTION-derived read under vending |
| Add a `StorageBackend::allow_http()` accessor and keep `base` | Rejected: has no meaningful answer for an `Adls` base whose credentials are irrelevant |

### Consequences

"No CONNECTION storage field is read under vending" is a property of the signature. The six absence conventions drop to none, and no future edit can reintroduce a per-field preservation rule.

## ADR: `ALLOW_HTTP` stays the operator's consent gate for plaintext transport

**ID:** allow-http-threaded-as-vended-selector-parameter
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

Deriving `allow_http` from the vended endpoint's scheme would permit plaintext to any endpoint a catalog names as `http://`. The shipped rule defaults `allow_http` to false when `ALLOW_HTTP` is absent, so it permits plaintext to no endpoint. A misconfigured or compromised catalog could then put STS credentials in cleartext with no operator control.

### Decision

The resolved `ALLOW_HTTP` virtual-schema property is passed into `resolve_vended_storage` as its own boolean. A vended `http://` `s3.endpoint` or an `abfs://` anchor is honoured only when it is true. Otherwise the function returns a `UdfError::User` naming the plaintext scheme and the `ALLOW_HTTP` property.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive `allow_http` from the vended endpoint's scheme | Rejected: a security regression in the default configuration |
| Read it from the base backend or an accessor | Rejected: reopens the CONNECTION-derived read that `resolve-vended-storage-drops-base-backend-parameter` closes |

### Consequences

Passing the value adds a 4-tuple to `resolve_connection_config` (2 call sites) and one boolean to four functions, following the convention for `s3_max_connections` and the DataFusion tuning knobs. Removing `ALLOW_HTTP` from the vended path was a planner decision, not an interview outcome.

## ADR: A vended payload naming neither a region nor an endpoint is an error

**ID:** vended-s3-requires-region-or-endpoint-else-error
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

The CONNECTION can no longer backfill an absent vended value, so `client.region` and `s3.endpoint` are the only values that place an S3 store. With both empty, the adapter would silently address an AWS store as a region-less URL.

### Decision

The S3 arm requires a non-empty `client.region` or a non-empty `s3.endpoint`, and otherwise returns a `UdfError::User` naming both keys.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave an absent region empty | Rejected: the silent failure this change removes |
| Require `client.region` always | Rejected: breaks the Lakekeeper vended path, which places its store by endpoint |
| Condition the rule on `s3.path-style-access` | Rejected: encodes the engine's builder logic in the catalog crate |
| Error only for an AWS-hosted `s3://` URI | Rejected: needs AWS endpoint conventions in the catalog crate, and a wrong answer fails silently |

### Consequences

The interview did not ask for this rule. It extends its "requested but not satisfied is an error" principle from the keys to the address. Whether AWS Glue's vended response carries `client.region` was unverified at planning time, so task 4.2 (the Glue vended-payload assertions) carries a blocking verification obligation.

## ADR: Per-side scheme selection needs a plan-time join guard scoped to variant and account, not full backend equality

**ID:** join-backend-guard-scoped-to-variant-and-account
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

Each join side resolves its own storage, but the scan spec keeps only the primary side's storage, and the registration backend is shared by every side. That was safe only while both sides took their variant from one `storage_block` output. With per-side scheme selection, an `s3://` fact joined to an `abfss://` dimension would run the S3 arm over Azure files, and two `abfss://` sides on different accounts would read one through the other's account and SAS. `validate_sides_share_one_store` misses both cases because different schemes produce different registry keys.

### Decision

`plan_join` calls a pure function, `validate_sides_share_one_backend`, right after per-side resolution and before the empty-side shortcut. It compares each side's storage with the first side's by variant and, for `Adls`, by `account_name`, and errors naming the differing variants and accounts with no credential value.

### Options Considered

| Option | Verdict |
|--------|---------|
| Inline guard in `plan_join` | Rejected: the backends are outputs of live catalog I/O, so no unit test could supply them |
| Guard on full backend equality, including per-prefix vended credentials | Rejected: could break every vended join against a catalog that mints per-table STS keys, which is unverified |
| Leave the collapse unguarded | Rejected: reads one side's files through another side's backend with no error |

### Consequences

Joins whose sides select the same backend are unchanged. The existing per-prefix vended-credential collapse in `join_fan_out_scan_spec` stays out of scope, tracked as issue #294, because widening the guard needs its own live verification.
