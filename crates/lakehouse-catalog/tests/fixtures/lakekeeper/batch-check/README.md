# Lakekeeper `batch-check` fixtures

Recorded exchanges with `POST /management/v1/action/batch-check`, captured from the
`lakekeeper-e2e` stack. They are input for #415 (the adapter's permission client) and describe what
Lakekeeper answers, not what the adapter does.

Pinned versions: Lakekeeper `v0.13.1`, OpenFGA `v1.14.2`, Keycloak `26.0.7`. A version change needs a
recapture.

## Files

| File | Caller | Identity checked | Table | Answer |
|------|--------|------------------|-------|--------|
| `allowed.json` | `lakehouse` (operator) | `lakehouse-reader-a` | `authz_alpha` | 200, `allowed: true` |
| `denied.json` | `lakehouse` (operator) | `lakehouse-reader-a` | `authz_beta` | 200, `allowed: false` |
| `missing.json` | `lakehouse` (operator) | `lakehouse-reader-a` | `authz_missing` | 200, `allowed: false` |
| `cannot-inspect.json` | `lakehouse-reader-b` | `lakehouse-reader-a` | `authz_alpha` | 403 `CannotInspectPermissions`, no `results` |

Every request carries `error-on-not-found: false` and one `read_data` table check with the id
`read-data`. A denied and a missing table answer identically.

Each file is one JSON object: `case`, `caller` (the Keycloak client id), `request` (the body sent),
and `response` (`status` and `body`; a body that is not JSON is a JSON string).

## Placeholders

Live ids change with every stack, so a file holds placeholders in their place.

| Placeholder | Stands for |
|-------------|------------|
| `<warehouse-id>` | the id of warehouse `lakehouse_authz` |
| `<table-id:authz_alpha>` | the table id of that table (also `authz_beta`) |
| `<principal:lakehouse-reader-a>` | the Lakekeeper user id of that Keycloak client (also `-reader-b`, `-checker`, and `lakehouse`) |
| `<error-id>` | the value after `Error ID: ` in an error stack |

## Drift check and regeneration

`authz_batch_check_fixtures_match_live_contract` in
`crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` normalizes each live exchange (the known live
warehouse, table and principal ids and the `Error ID:` value become the placeholders above) and compares
it with its file on the request, `status`, `results`, `error.type`, and `error.code`. Message text may
differ. It also fails when a file holds a client secret, a token prefix, or a live id.

```bash
LH_LAKEKEEPER_FIXTURE_CAPTURE=1 make test-e2e-lakekeeper
```

writes the normalized live exchanges over the files instead of comparing. A second run changes nothing.

## Findings for #415

Each finding says how it is backed. "Asserted" means a test in the `lakekeeper-e2e` suite fails when
Lakekeeper changes. "Planning probe 2026-09-30" means it was observed on a probe stack (Lakekeeper
`v0.13.1`, OpenFGA `v1.8.16`) and is not suite-asserted; the ones marked "re-run" were also repeated
by hand against OpenFGA `v1.14.2` during implementation.

### Endpoint

- `POST /management/v1/action/batch-check` takes `{"checks": [...], "error-on-not-found": bool}` and
  answers `{"results": [{"id", "allowed"}]}`. A check is `{"id", "identity": {"user": "<id>"},
  "operation": {"table": {"warehouse-id", "namespace", "table", "action": {"action": "read_data"}}}}`.
  Asserted by `authz_batch_check_fixtures_match_live_contract`.
- The management API is mounted at the server root, beside the catalog path: from the catalog URI
  `http://host:port/catalog`, `http://host:port/management/v1/info` answers 200. Asserted by
  `authz_management_api_is_mounted_beside_catalog_path_on_local_topology`, which covers the unprefixed
  local topology only.
- Lakekeeper builds both bases from the same base URL, and `x-forwarded-prefix` moves them together.
  This comes from Lakekeeper source analysis (v0.13.1) and is not suite-asserted. A planning probe
  2026-09-30, re-run, saw the catalog base in `/catalog/v1/config` (`overrides.uri`) move to
  `/lk/catalog` under `x-forwarded-prefix: /lk`; the management base was not re-checked live. The
  routes stay at the root, so a gateway that rewrites paths is the case that breaks deriving the
  management base from the catalog URI. No config value names the management API, so #415 still needs
  an explicit management-base property for path-rewriting gateways.

### Principal ids

- A direct client-credentials login has the user id `oidc~<sub>`, the primary provider's id and the
  token subject. On Keycloak such a token has no `oid`, so `sub` is an opaque UUID;
  `preferred_username` (`service-account-<client-id>`) plays no part. Asserted by
  `authz_direct_login_principal_id_is_idp_prefix_and_token_subject`.
- A grant to a never-registered id (`oidc~template-user@corp`) takes effect, and a check for that id
  answers `allowed: true`. Asserted by `authz_direct_login_principal_id_is_idp_prefix_and_token_subject`.
- Not proven, open question for #415 (#TBD): that the id produced from an Exasol user through #415's
  proposed `USER_MAPPING` template matches an existing grant. The suite proves the id of a direct login
  and that Lakekeeper accepts an arbitrary grant id, not what a template yields for a given Exasol user.
- `LAKEKEEPER__OPENID_SUBJECT_CLAIM=preferred_username` changes the id to
  `oidc~service-account-<client-id>`. Planning probe 2026-09-30, not suite-asserted. Re-run.
- A malformed identity (`alice`) answers 422 with a plain-text body naming the expected format
  `<idp_id>~<user-id>`. Planning probe 2026-09-30, not suite-asserted. Re-run.
- A check with no `identity` answers for the caller. Planning probe 2026-09-30, not suite-asserted.
  Re-run.

### Privilege to check another identity

A check that names another identity needs `can_read_assignments` on each checked object. Without it
the whole batch fails with 403 `CannotInspectPermissions` and no `results` (`cannot-inspect.json`).

- No assignment, server `admin` only, and warehouse `select` only all fail with 403. Asserted by
  `authz_check_for_another_identity_is_forbidden_without_grant_management`.
- Warehouse `manage_grants` is enough across a warehouse, and answers equal the operator's. Asserted by
  `authz_warehouse_manage_grants_allows_checking_another_identity`. It is not the minimum, and
  `manage_grants` also permits writing grants.
- Project `security_admin` is enough. Planning probe 2026-09-30, not suite-asserted. Re-run.
- Narrower options, probed against OpenFGA `v1.14.2` during implementation (task 1.7), not
  suite-asserted:

  | Checker holds | Result |
  |---------------|--------|
  | `manage_grants` on the checked table | 200 |
  | `manage_grants` on the namespace | 200 |
  | `ownership` on the checked table or on the warehouse | 200 |
  | `pass_grants` and `select` together on the checked table (the `can_grant_select` path) | 200 |
  | project `project_admin` | 200 |
  | `pass_grants` alone, `select` alone, or `describe` on the checked table | 403 |
  | `pass_grants` on the namespace or the warehouse, alone or with warehouse `select` | 403 |
  | `manage_grants` on a different table | 403 |
  | project `data_admin` | 403 |

  The guard applies to each checked object, so one unauthorized table fails the whole batch.

### Missing tables and errors

- With `error-on-not-found: false`, a missing table answers `allowed: false`, the same body as a denied
  table. Asserted by `authz_batch_check_fixtures_match_live_contract`.
- With `error-on-not-found: true`, a missing table fails the whole batch with 404
  `NoSuchTableException`. Planning probe 2026-09-30, not suite-asserted. Re-run.
