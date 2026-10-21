[lakehouse-engine](../README.md) › [Docs](index.md) › Security

---

# Security

The catalog CONNECTION carries credentials that reach object storage and the catalog.

## Privilege model

A virtual schema runs its scripts with the privileges of its **owner**, not of the querying user. Every script and CONNECTION grant therefore goes to the owner.

### Reader grants

A user who only queries the virtual schema needs two grants:

```sql
GRANT CREATE SESSION TO <reader>;
GRANT SELECT ON SCHEMA <vs> TO <reader>;
```

A reader needs no `EXECUTE` on a script, no grant on the CONNECTION, and no role membership. A CONNECTION grant to a reader does not let the reader query the virtual schema, because only the owner's grants count. Do not grant one: together with `EXECUTE` on the scan script it lets the reader bypass the adapter (see [Plan visibility versus plan execution](#plan-visibility-versus-plan-execution)).

### Owner grants

An owner with the system privileges `EXECUTE ANY SCRIPT` and `ACCESS ANY CONNECTION`, such as `SYS` or a user with the `DBA` role, needs no grant. Any other owner, a user or a role, needs:

- `EXECUTE` on `LAKEHOUSE_ADAPTER`, `LAKEHOUSE_SCAN`, and `LAKEHOUSE_DISTRIBUTE_FILES`. The owner of the scripts' schema holds it already.
- `ACCESS` on the CONNECTION for `LAKEHOUSE_ADAPTER` and `LAKEHOUSE_SCAN`, because both scripts read it.
- `CREATE VIRTUAL SCHEMA`, if it creates the virtual schema itself.

The owner can hold each grant directly or through a role it is a member of. Grant them before `CREATE VIRTUAL SCHEMA`, because creating the virtual schema already runs the adapter as its owner. Choose one of two forms.

**Per script.** These grants cover only the engine's scripts. A re-install drops them (see [Re-installing](#re-installing)).

```sql
GRANT EXECUTE ON SCRIPT <schema>.LAKEHOUSE_ADAPTER          TO <owner>;
GRANT EXECUTE ON SCRIPT <schema>.LAKEHOUSE_SCAN             TO <owner>;
GRANT EXECUTE ON SCRIPT <schema>.LAKEHOUSE_DISTRIBUTE_FILES TO <owner>;
GRANT ACCESS ON CONNECTION <conn> FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER TO <owner>;
GRANT ACCESS ON CONNECTION <conn> FOR SCRIPT <schema>.LAKEHOUSE_SCAN    TO <owner>;
```

**Per schema.** These two grants survive a re-install:

```sql
GRANT EXECUTE ON SCHEMA <schema> TO <owner>;
GRANT ACCESS ON CONNECTION <conn> FOR SCHEMA <schema> TO <owner>;
```

They cover every script in `<schema>`, including scripts created later. An owner that can create a script in `<schema>` can therefore read the CONNECTION's credentials through a script of its own. Use the per-schema form only when the owner cannot create scripts in `<schema>`.

The install script prints the per-script CONNECTION grants as a template. It puts them on the role `LAKEHOUSE_ENGINE_ROLE_<schema>` and grants that role to the installing user. That user owns `<schema>`, so it needs no `EXECUTE` grant, and it owns the virtual schemas it creates. The role is for owners only. Never grant it to a reader.

### A role as owner

A role can own the virtual schema. Give the role the owner grants above, create the virtual schema as `SYS` or as a user that holds the same grants, then hand it to the role:

```sql
ALTER VIRTUAL SCHEMA <vs> CHANGE OWNER <role>;
```

Readers still need only the reader grants, not membership in the role. Every member of the role acts as the owner: it can refresh the virtual schema and change its properties, including `PERMISSION_CHECK` and `USER_MAPPING`. It also holds the role's `EXECUTE` and CONNECTION grants, so it can run a scan plan directly and bypass the adapter (see [Plan visibility versus plan execution](#plan-visibility-versus-plan-execution)). Grant the role only to administrators.

### Re-installing

Re-running the installer replaces the scripts with `CREATE OR REPLACE SCRIPT`. That drops every per-script grant on them, `EXECUTE` and `ACCESS` alike, so queries fail until you issue the grants again. Per-schema grants survive. A re-install never changes a reader's grants.

`CREATE OR REPLACE CONNECTION` drops every grant on the CONNECTION. `ALTER CONNECTION` keeps them, so change an existing CONNECTION with `ALTER CONNECTION`.

### Missing grants

A missing owner grant fails every reader's query with one of these errors:

| Error contains | Cause |
|----------------|-------|
| `insufficient privileges for calling adapter script` | The owner lacks `EXECUTE` on `LAKEHOUSE_ADAPTER`. |
| `CONNECTION '<conn>' could not be resolved` | The CONNECTION does not exist, or the owner lacks `ACCESS` on it for `LAKEHOUSE_ADAPTER`. |
| `insufficient privileges for calling script` | The owner lacks `EXECUTE` on `LAKEHOUSE_SCAN` or `LAKEHOUSE_DISTRIBUTE_FILES`. |
| `the scan cannot access CONNECTION '<conn>'` | The owner lacks `ACCESS` on the CONNECTION for `LAKEHOUSE_SCAN`. |

A scan that fits in one shard, such as a scan of one data file, does not call `LAKEHOUSE_DISTRIBUTE_FILES`. A missing `EXECUTE` on it therefore fails only the queries whose scan spans several shards.

To check the grants, run these queries as a DBA. Unquoted names are stored in uppercase.

```sql
-- The owner, and the roles it holds
SELECT SCHEMA_OWNER FROM EXA_DBA_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '<VS>';
SELECT GRANTED_ROLE FROM EXA_DBA_ROLE_PRIVS WHERE GRANTEE = '<OWNER>';

-- ACCESS grants on the CONNECTION
SELECT GRANTEE, FOR_OBJECT_TYPE, FOR_OBJECT_SCHEMA, FOR_OBJECT_NAME
FROM EXA_DBA_RESTRICTED_OBJ_PRIVS
WHERE OBJECT_NAME = '<CONN>';

-- EXECUTE grants on the scripts or their schema
SELECT GRANTEE, OBJECT_TYPE, OBJECT_SCHEMA, OBJECT_NAME
FROM EXA_DBA_OBJ_PRIVS
WHERE PRIVILEGE = 'EXECUTE' AND (OBJECT_SCHEMA = '<SCHEMA>' OR OBJECT_NAME = '<SCHEMA>');
```

Each owner grant needs a row whose `GRANTEE` is the owner or one of its roles. A `SCHEMA` row covers every script in that schema. The owner of `<SCHEMA>` holds `EXECUTE` without a row. Without DBA rights, read the same columns from `EXA_USER_*` and `EXA_ROLE_*` in place of `EXA_DBA_*`.

### Credentials in plan text

`EXPLAIN VIRTUAL` and pushdown-path errors carry the pushdown SQL verbatim, and credential values never appear in it. See [Plan visibility versus plan execution](#plan-visibility-versus-plan-execution) for what else that plan text exposes and why reading it is safe while running it is not.

## Plan visibility versus plan execution

Any user who can query the virtual schema can read the pushdown plan that `EXPLAIN VIRTUAL` returns, so the plan is not a secret. Safety comes from `EXECUTE` on `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` being separate from `SELECT` on the virtual schema. Running a script against real data needs both that grant and `ACCESS` on the CONNECTION for the script, per script or per schema (see [Owner grants](#owner-grants)). The two gates are independent, and neither replaces the other.

Granting `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN`, or `EXECUTE ANY SCRIPT`, to a querying user makes every adapter-side predicate advisory for that user once they also hold connection access: they can submit a hand-edited plan directly instead of the one the adapter generated. `LAKEHOUSE_DISTRIBUTE_FILES` is not an independent path to that outcome. Every generated plan calls it only inside the `FROM` clause of the `LAKEHOUSE_SCAN` call, so a user without `EXECUTE` on `LAKEHOUSE_SCAN`, or `EXECUTE ANY SCRIPT`, is denied before the distributor runs, even with `EXECUTE` on the distributor itself.

A reader with only `SELECT` on the virtual schema still sees, in the plan text: the table root, the bucket layout, file names and byte sizes, and the catalog CONNECTION name. None of that is a credential. A sealed vended-credential envelope, when present, travels in the plan as ciphertext, readable but not openable without the CONNECTION password (see [Sealed vended-credential envelope](#sealed-vended-credential-envelope-378) below).

## Lakekeeper permission check (#415)

With `PERMISSION_CHECK = 'LAKEKEEPER'`, the adapter maps the querying Exasol user to a Lakekeeper principal through `USER_MAPPING` and refuses a query over any table that principal may not read. The check runs when the adapter plans a query, so it covers `SELECT` and `EXPLAIN VIRTUAL`. Creating, refreshing, or altering a virtual schema lists tables as the CONNECTION's identity and checks no user.

A user with `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN`, or `EXECUTE ANY SCRIPT`, bypasses this check. That user submits a scan plan directly, so the adapter never plans it and Lakekeeper is never asked. See [Plan visibility versus plan execution](#plan-visibility-versus-plan-execution). Grant those privileges only to users who may read every table the CONNECTION can read.

### Setup

The check needs the Iceberg REST catalog kind, a catalog URI that ends in `/catalog` (a gateway that rewrites paths is not supported), and a Lakekeeper server with an authorization backend such as OpenFGA. Every other catalog kind fails with `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind`.

Grant the CONNECTION's identity `manage_grants` on the warehouse or on the namespace. Lakekeeper answers a check for another identity only when the caller holds `can_read_assignments` on each checked table. Without that grant, every query fails with 403 `CannotInspectPermissions`. The same grant lets the identity manage grants, so the CONNECTION's client secret is a grant-administration credential.

```sql
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  NAMESPACE          = 'default'
  PERMISSION_CHECK   = 'LAKEKEEPER'
  USER_MAPPING       = 'oidc~{{ user|lower }}@corp.net';
```

To turn the check on for an existing virtual schema, set `USER_MAPPING` first, because the adapter refuses `PERMISSION_CHECK = 'LAKEKEEPER'` without it:

```sql
ALTER VIRTUAL SCHEMA MY_LAKEHOUSE SET USER_MAPPING = 'oidc~{{ user|lower }}@corp.net';
ALTER VIRTUAL SCHEMA MY_LAKEHOUSE SET PERMISSION_CHECK = 'LAKEKEEPER';
```

Grant each table in Lakekeeper to the principal that `USER_MAPPING` produces for the Exasol user. Lakekeeper accepts a grant to a principal that has never logged in. A Lakekeeper server started with `LAKEKEEPER__OPENID_SUBJECT_CLAIM=preferred_username` builds the Lakekeeper user id from the token's `preferred_username` claim, so a mapping can reproduce a user name instead of an opaque subject.

### USER_MAPPING

`USER_MAPPING` is a Jinja-syntax template with one variable, `user`. The adapter renders it with MiniJinja, so the template can use MiniJinja's built-in filters and tests, such as `lower`, `replace`, and `startingwith`. Exasol reports an undelimited user name in uppercase, so `ALICE` is the value for `alice`. The trimmed output is the Lakekeeper user id. For an OIDC login that id is `oidc~<subject>`, where the subject comes from the token's `oid` claim, then its `sub` claim. The adapter assumes no id format.

- `oidc~{{ user|lower|replace("_", ".") }}@corp.net` maps `ALICE_COOPER` to `oidc~alice.cooper@corp.net`.
- `oidc~{% if user is startingwith("BI_") %}svc-reporting{% else %}{{ user|lower }}{% endif %}@corp.net` maps `BI_TABLEAU` and `BI_POWERBI` to the one principal `oidc~svc-reporting@corp.net`.

The adapter refuses a query when the request names no user, when the template fails to render, or when the id is empty or holds a whitespace or control character. A template that does not compile rejects create, refresh, and `SET`. The adapter trusts the template author and checks neither the uniqueness nor the owner of a principal. A template holds up to 100,000 characters. On Exasol 2025.1.16 a longer value is accepted by `SET`, but reading it from `EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` drops the session.

### What the check enforces

A user can query a table only if Lakekeeper allows the mapped principal to read it. This holds for every query shape, including aggregates, filters, and joins. Each table in a join counts separately, so a join succeeds only when the user may read both tables.

The adapter asks Lakekeeper once per query, before any data is read. If the user may not read one or more tables, the whole query is refused, and the error names each table the user may not read and no other. Lakekeeper reports a missing table as denied, so the error cannot tell a missing table from a forbidden one. Example error for a join without grants:

```text
the Lakekeeper permission check refuses the query: Exasol user 'LK_PERM_JOINONE', mapped by 'USER_MAPPING' to the Lakekeeper principal 'oidc~lk.joinone@lakehouse.test', may not read e2e_lakehouse.fact_orders, e2e_lakehouse.dim_customer. Lakekeeper denies read_data on each, or the table does not exist. Only a Lakekeeper grant that names the principal 'oidc~lk.joinone@lakehouse.test' lets this user read a table
```

If Lakekeeper is unreachable or does not answer within 30 seconds, every query on the virtual schema is refused.

### What the check does not enforce

- **Table listing.** Every user who may query the virtual schema sees all table names, column names, and column types, including those of tables the user cannot read. Querying such a table is refused.
- **Storage access.** Data is read with the CONNECTION's storage credentials, including credentials that Lakekeeper vends, and these are not scoped to the querying user. The check is enforced by the adapter, not by the catalog or the storage layer.
- **Users with script privileges.** A user with `EXECUTE` on the scan script bypasses the check, as described above. That includes every member of a role that owns the virtual schema. A user without that privilege cannot obtain a scan plan or run one captured by another user. The [Privilege model](#privilege-model) lists the grants that matter.
- **Changing the settings.** The virtual schema's owner, every member of an owning role, a user with `ALTER` on the virtual schema, and a user with `ALTER ANY VIRTUAL SCHEMA` can set `PERMISSION_CHECK` to an empty value or change `USER_MAPPING`. Either change turns the check off, or changes the principal, for every user of the virtual schema. A user with only `SELECT` on the virtual schema cannot change either property.

### Out of scope

The check does not provide row-level or column-level authorization, which need a policy engine. It does not cover other catalog kinds, and it does not scope storage credentials per user.

## Sealed vended-credential envelope (#378)

A vended credential has no CONNECTION name to reference. It travels as AES-256-GCM ciphertext (HKDF-SHA256 key from the CONNECTION password, fresh 96-bit nonce). Vending without key material is refused at plan time.

An assumed-role CONNECTION's session credentials travel sealed in the same envelope, like vended ones. The base key pair that signs `AssumeRole` never enters the scan spec.

## AWS Glue credentials and Lake Formation

The Glue kind (`CATALOG_KIND = 'GLUE'`) reads with the IAM credentials of the CONNECTION. The same `access_key`, `secret_key`, and optional `session_token` sign the Glue requests and read S3. When the CONNECTION names `aws_assume_role_arn`, the role's session replaces them for both, and the scan spec carries the session sealed. The adapter reads no credential from the environment, a profile file, or instance metadata. Without a role, the scan spec carries the CONNECTION name, never a credential value.

The adapter evaluates no Lake Formation grant. It uses no Lake Formation credential vending. The Glue kind rejects `use_vended_credentials`. A querying user sees every table, partition, and object that the IAM policy of the CONNECTION's credentials, or of its assumed role, allows. Lake Formation column filters, row filters, and cell filters do not apply. Grant the credentials only the IAM actions that the [Glue section of Catalogs](catalogs.md#aws-glue-data-catalog-catalog_kind--glue) lists.

A `warehouse` that names another account's `CatalogId` is untested (#TBD). To read another account's catalog, assume a role in that account.

## Rotation

Every query re-resolves the CONNECTION, with no cache and no restart. Rotate with `ALTER CONNECTION`, which keeps the grants (see [Re-installing](#re-installing)). A sealed envelope of an in-flight vended query fails the AEAD open, so rotate when no vended query is in flight.
