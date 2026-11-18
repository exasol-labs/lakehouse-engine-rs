<!-- DELTA:CHANGED -->
# Feature: Vended Storage Resolution

When the CONNECTION sets `use_vended_credentials` to true, the adapter resolves the effective scan storage from a `loadTable` response. The feature covers the `X-Iceberg-Access-Delegation` header, store-address resolution, backend selection from the table location's URI scheme, host-based selection of a vended Azure SAS, the error when the catalog vends no usable credential, and joins whose sides resolve to different backends.
<!-- /DELTA:CHANGED -->

## Background

* The adapter loads a table with one self-issued `loadTable` GET, as often as `file-planning/pushdown-statement-state` states. The raw `LoadTableResult` feeds both Iceberg file planning and vended-credential extraction, because `RestCatalog::load_table` drops the response `config` and `storage_credentials`.
* One credential-source selection serves both backends: the longest `storage_credentials` entry whose non-empty `prefix` prefixes the table location, else the flat `config` map. The Iceberg REST spec says clients choose the longest prefix. A matched entry is authoritative for the whole credential set, so a key it omits does not fall back to `config`. The prefix comparison lowercases the URI scheme on both sides and compares everything after `://` byte-exactly.
* The backend comes from the table location's URI scheme alone: `s3://` and `s3a://` select S3, `abfss://` and `abfs://` select ADLS, and any other or missing scheme is an error naming the scheme. The Iceberg table spec makes `location` an absolute path, so a location without a scheme is a malformed catalog response.
* Under vending, credentials (the S3 key pair and session token, or the ADLS SAS) come from the selected credential source alone and are never backfilled from the CONNECTION. Addressing (`endpoint`, `region`) comes from the CONNECTION when it states a non-empty value, else from the selected source, resolved per field. When neither states one, the field is empty and the AWS default chain applies.
* `path_style` resolves in three ordered steps: the CONNECTION's stated value, else the source's `s3.path-style-access`, else whether an `endpoint` was resolved. The last step stays because `register_side_store` gates `endpoint` on `path_style`. Both catalog kinds share this precedence through `s3_backend`.
* The vended ADLS SAS key is host-suffixed on the wire (`adls.sas-token.<host>`) and flat in the backend (`adls.sas-token`), because the downstream reader accepts only the flat form. The host comparison with the table location's host is case-insensitive (RFC 3986 §3.2.2). The account name is the first dot-separated label of that host, taken verbatim, because `iceberg-storage-opendal` compares it byte-exactly against the account in each file URI as a wrong-account guard.
* A catalog cannot downgrade the transport on its own authority. A resolved plaintext address, either a vended or CONNECTION-configured `http://` S3 endpoint or an `abfs://` ADLS location, is honoured only when the `ALLOW_HTTP` virtual-schema property is true (default false). `ALLOW_HTTP` is resolved outside the catalog crate and passed in as a plain boolean.
* Each side of a join resolves its own effective storage and backend, and the adapter never compares them at plan time. A backend difference or an ADLS storage-account difference yields distinct DataFusion registry keys. Two ADLS containers of one account collapse onto one key, and the scan's store-registration precondition refuses that case (`scan-aggregation/scan-execution-join`).
* A credential never appears in returned SQL or in an error message, except that a vended value appears in the SQL as the sealed envelope's ciphertext (`storage-access/scan-spec-credential-reference`). `redact_credentials` matches the `adls.sas-token` label by case-insensitive substring and truncates to the next delimiter. Every error path returns `Result` and never panics, because a panic in a UDF is an abnormal VM exit that kills every sibling VM of the statement part. Every error names the location's storage host, scheme, or expected config key.
* `file-planning/pushdown-planning-file-resolution` rejects an absent table location before the vended and static split, so this feature never sees one.
* See `storage-access/pushdown-planning-cloud-credentials` for SigV4 catalog signing, the Glue REST prefix, and the static-credential path.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: One concept-level call resolves the effective scan storage from a loadTable response

* *GIVEN* a `loadTable` response for a table whose CONNECTION sets `use_vended_credentials` to true
* *WHEN* the adapter or the export resolves the table's vended storage
* *THEN* this behavior SHALL be removed; its observable rules stay in this feature's Background and scenarios, and the export's use of vended credentials is in `export/export-catalog-storage`
<!-- /DELTA:REMOVED -->
