# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

## Background

<!-- DELTA:NEW -->
* **SUPERSEDES the "PRE-EXISTING and accurately-scoped defect this delta does NOT introduce and does NOT fix" bullet.** That bullet recorded that `join_fan_out_scan_spec` discards a per-prefix vended credential difference, that whether any target catalog vends genuinely per-prefix credentials for two tables in one warehouse was UNVERIFIED, and that widening the guard needed its own live verification. Issue #294 is that work. The collapse is FIXED — each side's `effective_storage` is now carried per side into the scan spec — so the defect is DISCHARGED, not re-scoped, and no `#294` citation remains in this feature.
* **The same-backend guard's SCOPE is unchanged; only its justification is restated.** The comparison stays on the backend VARIANT and, for `Adls`, `account_name`. The reason is no longer "a credential difference is a deferred defect" but "a credential difference is now SERVED per side, while a variant or account difference remains UNSERVEABLE": an `AmazonS3Builder` cannot address an `abfss://` URI, and two ADLS containers of one account collapse onto a single DataFusion object-store registry key whose formula this engine cannot change (`get_url_key`, `datafusion-execution-54.1.0/src/object_store.rs:266-274`, whose own test asserts `s3://username:password@host:123` keys as `s3://host:123` at `:330-332`).
* **Per-side credential resolution is the Iceberg REST protocol's own model, and this delta stops discarding its result.** `StorageCredential` requires a `prefix`, documented as "Indicates a storage location prefix where the credential is relevant. Clients should choose the most specific prefix (by selecting the longest prefix) if several credentials of the same type are available", and `storage-credentials` is an ARRAY. `select_credential_source` already runs that longest-prefix selection per side, against each side's own table location as the anchor. Nothing about that selection changes.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A join whose sides resolve to different storage backends is rejected at plan time

* *GIVEN* a broadcast-eligible or fallback join under ONE CONNECTION whose credentials set `use_vended_credentials` to true
* *AND* two or more involved tables whose locations do not all select the same storage backend — an `s3://` fact with an `abfss://` dimension, or two `abfss://` sides naming DIFFERENT storage accounts
* *WHEN* the adapter has resolved every side's file list and effective storage, before it builds any scan-driving SQL
* *THEN* the adapter SHALL compare every side's resolved backend against the first side's — the variant, and for the ADLS variant the `account_name` — and SHALL return a `UdfError::User` when they differ
* *AND* that error SHALL name the differing backends by variant and, for ADLS, by storage account, and MUST NOT contain any credential value, vended secret, or SAS token
* *AND* the adapter SHALL scope the comparison to the variant and ADLS account ONLY, and MUST NOT compare full backend equality, because two sides that differ only in CREDENTIAL are now served — each side's own backend is carried into the scan spec and read through its own object store (`datafusion-scan/scan-execution-join`)
* *AND* the adapter MUST NOT justify that narrow scope as a deferred defect, because the per-prefix credential collapse it once deferred is fixed
* *AND* this check MUST run at plan time rather than being left to the scan's own store-registration precondition, because two sides on different schemes resolve to DIFFERENT registry keys and so never reach the existing same-key conflict check
* *AND* a join whose sides all select the SAME backend variant SHALL keep its current plan, whether or not the two sides' credentials are equal
<!-- /DELTA:CHANGED -->
