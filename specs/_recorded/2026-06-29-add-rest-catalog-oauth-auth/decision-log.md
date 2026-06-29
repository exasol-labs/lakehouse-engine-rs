# Decision Log: add-rest-catalog-oauth-auth

Date: 2026-06-29

## Interview

**Q:** Which authentication modes does this feature need to support?
**A:** BOTH — a static bearer token AND OAuth2 client credentials.

**Q:** When using OAuth/token catalog auth, are static S3 credentials (`access_key`, `secret_key`, `endpoint`, `region`) still required in the CONNECTION?
**A:** Optional — the catalog vends them. S3 creds should be optional and fall back to `use_vended_credentials` when OAuth/token is configured.

**Q:** Should OAuth scope be a configurable field in the CONNECTION password JSON?
**A:** Yes — an optional field.

**Note (post-interview correction, verified against `iceberg-catalog-rest` 0.9.1):** The
interview framed S3 creds as "optional when OAuth/token is configured." Source review showed
catalog auth and S3 storage credentials are fully orthogonal — `authenticate()` (`client.rs:211`)
supports a no-auth mode and `use_vended_credentials` governs S3 vending independently (even an
unauthenticated catalog can vend). The S3 fields are therefore made UNCONDITIONALLY optional, not
conditionally on auth mode. Separately, `oauth2_server_uri` is consulted only on the
client-credentials path (`exchange_credential_for_token`, `client.rs:112`), never with a static
`token`, and is optional even there (defaults to `{uri}/v1/oauth/tokens`).

## Design Decisions

### [1] Auth fields live on `ConnectionCreds`, never on `CatalogProps`/`StorageProps`

- **Decision:** Add `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope` (all `Option<String>`) to `ConnectionCreds` and keep them strictly within the planning layer. Catalog auth props are injected inside `build_rest_catalog`, which already receives `creds` via `resolve_file_list`.
- **Alternatives:** Widen `CatalogProps` (and thus `ScanSpec`) with the auth fields. Rejected because `CatalogProps` is serialized into `ScanSpec` and crosses the stateless UDF boundary; catalog secrets must never cross it, and the scan UDF never calls the catalog.
- **Rationale:** Preserves the architecture boundary "UDFs are stateless and never discover/authenticate to catalogs"; keeps secrets out of the boundary payload.
- **Promotes to ADR:** yes

### [2] Presence-driven auth-mode detection, no explicit `auth_mode` enum

- **Decision:** Detect the catalog-auth mode by presence: `token` set → token mode; `client_id`+`client_secret` set → client-credentials mode; neither → no-auth. No discriminator field. (`has_catalog_auth` is used only for the SigV4 mutual-exclusivity check, NOT to gate S3 requiredness.)
- **Alternatives:** A distinct `auth_mode` discriminator field in the password JSON. Rejected as heavier and a backward-compat risk for existing JSON payloads.
- **Rationale:** Mirrors the existing `use_sigv4` / `use_vended_credentials` flag style; keeps the password JSON ergonomic and backward-compatible (no new mandatory field).
- **Promotes to ADR:** no

### [3] SigV4 and catalog token/OAuth are mutually exclusive and rejected at validation

- **Decision:** Reject a CONNECTION that enables `use_sigv4` while also supplying a catalog-auth field, with a credential-safe error.
- **Alternatives:** Let SigV4 take precedence and silently ignore token/OAuth (the SigV4 path bypasses `RestCatalogBuilder`, so the props would be dropped). Rejected as a silent misconfiguration trap.
- **Rationale:** For an engine meant to be operated, an explicit error beats silently ignored credentials.
- **Promotes to ADR:** yes

### [4] S3 fields optional at the base level; `warehouse` the only unconditionally-required field

- **Decision:** Reduce base required-field validation to `warehouse` only. The four S3 fields (`endpoint`, `region`, `access_key`, `secret_key`) become optional at the base level, independent of catalog auth and `use_vended_credentials`. This also corrects pre-existing over-strictness in `REQUIRED_CRED_KEYS`. (The SigV4 path retains a conditional requirement on three of them — see [4d].)
- **Alternatives:** (a) Keep all five always required — rejected, forces dummy S3 values for vended/token/OAuth catalogs. (b) Require S3 only when no catalog auth is present — rejected, because catalog auth and S3 vending are orthogonal (a no-auth catalog can still vend; `client.rs:211`), so a conditional rule mismodels the crate's behaviour and still rejects valid no-auth-vended configs.
- **Rationale:** Matches the crate's actual orthogonality of auth vs storage credentials; loosening to optional only widens acceptance and never rejects a previously valid password (backward compatible).
- **Promotes to ADR:** yes

### [4b] `use_vended_credentials` stays independent of auth mode

- **Decision:** Do NOT default `use_vended_credentials` to true under catalog auth; it remains an independent flag defaulting to false, set explicitly by the operator.
- **Alternatives:** Auto-enable vending whenever catalog auth is present (an earlier draft). Rejected after source review: vending is orthogonal to auth, so auto-coupling would surprise operators who authenticate to a catalog but supply static S3 creds.
- **Rationale:** Preserves the established independent semantics of `use_vended_credentials`; avoids hidden behaviour changes.
- **Promotes to ADR:** no

### [4c] Three catalog-auth modes; `oauth2_server_uri`/`scope` only on the credential path

- **Decision:** Model three modes (no-auth / static token / OAuth2 client-credentials). Inject `oauth2-server-uri` and `scope` props only on the client-credentials path, and only when supplied; never inject them in token mode.
- **Alternatives:** Inject `oauth2-server-uri` whenever any auth field is present. Rejected — `get_token_endpoint()` (`catalog.rs:172`) is read only inside `exchange_credential_for_token` (`client.rs:112`); a static `token` is used directly as the bearer header and never consults it, so injecting it in token mode is dead config.
- **Rationale:** Matches the crate's exact prop consumption; keeps token mode minimal and correct.
- **Promotes to ADR:** no

### [4d] SigV4 enabled ⟹ `access_key`/`secret_key`/`region` required (orthogonal to vending)

- **Decision:** When `use_sigv4` is true, require `access_key`, `secret_key`, and `region` to be present and non-empty, rejecting with a credential-safe error that names the missing field(s) and references SigV4. Apply this regardless of `use_vended_credentials`. `endpoint` is excluded — it is not fed to the catalog signer.
- **Alternatives:** Rely on decision [4]'s warehouse-only validation for all cases. Rejected: the Glue path signs the `load_table` request with exactly `access_key`/`secret_key`/`region` (`sign_request`, `pushdown.rs:157-164`, service `glue`) BEFORE vended creds are swapped in, so a `use_sigv4` connection missing them previously caught by the flat `REQUIRED_CRED_KEYS` would now pass validation and fail later with an opaque signing error — a regression on the Glue path.
- **Rationale:** Restores the safety net that the old unconditional required-keys list provided for the SigV4 path, scoped precisely to the three fields the signer actually consumes; keeps the non-SigV4 cases as loose as [4] specifies. Verified that the guard must hold even with vending on (static creds sign the catalog request first).
- **Promotes to ADR:** yes

### [5] Use literal `iceberg-catalog-rest` 0.9.1 prop key strings

- **Decision:** Inject the literal keys `"token"`, `"credential"` (`"client_id:client_secret"`), `"oauth2-server-uri"`, `"scope"` verified against the pinned crate source.
- **Alternatives:** Wait for the crate to export named constants. Rejected — 0.9.1 exports none for these keys; the literals are stable per the Iceberg REST spec and the pinned version.
- **Rationale:** Unblocks the feature on the current dependency; keys are spec-stable.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
