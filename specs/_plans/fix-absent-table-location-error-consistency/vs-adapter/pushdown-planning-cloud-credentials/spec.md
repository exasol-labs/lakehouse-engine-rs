# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

## Background

* **This delta MOVES the absent-table-location rule out of this feature and amends ONE clause of the scheme-selection scenario; nothing else changes.** The rule is path-independent — it holds with vending disabled, with vending enabled, and on every join side — so `vs-adapter/pushdown-planning` now owns it and this feature references it. No scenario of this feature changes its behaviour, and the vending-DISABLED path is untouched.
* **SUPERSEDES the "An ABSENT table location is its own error" Background bullet.** That bullet concluded "The vended resolution therefore reports the missing location directly, and the 'no CONNECTION storage value is read under vending' guarantee holds on every path." The first half becomes false: the missing location is reported by file resolution BEFORE the vended/static split, so the vended resolution never sees one. The bullet's substance — that the REST `warehouse` is a routing identifier which may not stand in for a location, and that substituting it would make it the one CONNECTION-derived string reaching the backend variant, the credential-source prefix match, and the ADLS SAS host — is PRESERVED by `vs-adapter/pushdown-planning`'s owning bullets rather than duplicated here. The second half still holds and is unaffected.
* **`resolve_vended_storage` needs no change and keeps its signature.** It remains a TOTAL function over its anchor input, so a direct call with an empty anchor still returns a `UdfError::User` naming an unsupported scheme. What the upstream rejection removes is the need for a DISTINCT absent-location error inside the vended arm, not the totality that clause guarantees — a distinction that matters because the totality is what keeps a third `StorageBackend` variant from silently falling through.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The storage backend under vending is selected from the table location's URI scheme

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *AND* a `loadTable` response whose table metadata `location` is an absolute URI
* *WHEN* the adapter resolves the effective scan storage for that table
* *THEN* the adapter SHALL select the storage backend from that location's URI scheme ALONE, mapping `s3://` and `s3a://` to the S3 backend and `abfss://` and `abfs://` to the ADLS backend
* *AND* the adapter MUST NOT consult the CONNECTION credential shape, the backend `storage_block` selected, or any virtual-schema property to make this selection, so a CONNECTION carrying static Azure credentials and a CONNECTION carrying static S3 credentials resolve to the SAME backend for the same table location
* *AND* for any other scheme — and for a location that carries no `<scheme>://` prefix at all — the adapter SHALL return a `UdfError::User` naming the unsupported scheme and MUST NOT fall back to a default backend
* *AND* an ABSENT table location SHALL never reach this selection, because `vs-adapter/pushdown-planning` rejects a `loadTable` response carrying no location BEFORE the vended/static split — a path-independent rule this feature references rather than restates, so the prohibition on substituting the CONNECTION's `warehouse` binds the non-vended path too
* *AND* that error MUST NOT contain any credential value
* *AND* the mapping SHALL be a TOTAL function over its input: every one of the four accepted schemes yields a backend and EVERY other input, including the empty string, yields a `UdfError::User` — so the catch-all branch is REQUIRED here rather than forbidden, because the match is over a scheme string and not over a `StorageBackend`
* *AND* because this site does not match on `StorageBackend`, adding a THIRD variant to that enum SHALL NOT break this site's build, and this clause states that plainly rather than claiming a compile-time guarantee it cannot deliver
* *AND* a source-level probe in `crates/lakehouse-catalog/tests/catalog_public_surface.rs` SHALL therefore EXTRACT the variant list from `storage.rs`'s `enum StorageBackend` source — already reachable there through the `CATALOG_SOURCES` `include_str!` table — and assert that every extracted variant name appears in `vended.rs`, so a third variant left unreachable from vending fails that probe instead of becoming a silent gap
* *AND* a probe holding a HARDCODED variant list SHALL NOT satisfy the preceding clause, because such a list keeps passing after a third variant is added, which is the exact silent gap the probe exists to prevent
* *AND* when the selected scheme is a plaintext one — `abfs://`, or `s3://`/`s3a://` resolving to a vended plain-`http://` endpoint — the adapter SHALL honour it only when the `ALLOW_HTTP` virtual-schema property is true, and otherwise SHALL return a `UdfError::User` naming the plaintext scheme and the `ALLOW_HTTP` property with no credential value
<!-- /DELTA:CHANGED -->
