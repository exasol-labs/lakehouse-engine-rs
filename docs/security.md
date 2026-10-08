[lakehouse-engine](../README.md) › [Docs](index.md) › Security

---

# Security

The catalog CONNECTION carries credentials that reach object storage and the catalog.

## Privilege model

Both scripts resolve the CONNECTION by name (`ctx.connection()`). Each needs a script-scoped grant:

```sql
GRANT ACCESS ON CONNECTION <conn> FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER TO <vs-owner>;
GRANT ACCESS ON CONNECTION <conn> FOR SCRIPT <schema>.LAKEHOUSE_SCAN   TO <vs-owner>;
```

The grantee is the **virtual schema's OWNER**, not the querying user. Run these **before** `CREATE VIRTUAL SCHEMA`. A DBA owner (`SYS`) needs none of this. `CREATE OR REPLACE CONNECTION` and `CREATE OR REPLACE SCRIPT` both drop the grant — re-issue after re-running the installer. `ALTER CONNECTION` preserves grants.

End users of the virtual schema hold none of the above. `GRANT CREATE SESSION` plus `GRANT SELECT
ON SCHEMA <vs>` is the entire grant set a querying user ever needs — no `EXECUTE ON SCRIPT`, no
`GRANT ACCESS ON CONNECTION`, no role membership. Re-running the installer never touches a reader's
grants: `CREATE OR REPLACE SCRIPT` and `CREATE OR REPLACE CONNECTION` only drop privileges held on
the scripts and the CONNECTION, and a reader holds none of those. The re-grant burden described
above falls entirely on the VS owner/operator.

`EXPLAIN VIRTUAL` and pushdown-path errors carry the pushdown SQL verbatim — credential VALUES must never appear in it. See [Plan visibility versus plan execution](#plan-visibility-versus-plan-execution) for what else that plan text exposes and why reading it is safe while running it is not.

## Plan visibility versus plan execution

The pushdown plan `EXPLAIN VIRTUAL` returns is readable by any user who can query the virtual schema — it is not a secret. Safety comes from `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` being separable from `SELECT` on the virtual schema: running a script against real data needs BOTH that grant AND the script-scoped `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` grant from `## Privilege model` above — two independent gates, neither a substitute for the other.

Granting `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN`, or `EXECUTE ANY SCRIPT`, to a querying user makes every adapter-side predicate advisory for that user once they also hold connection access: they can submit a hand-edited plan directly instead of the one the adapter generated. `LAKEHOUSE_DISTRIBUTE_FILES` is not an independent path to that outcome — every generated plan calls it only from inside the `LAKEHOUSE_SCAN` invocation's `FROM` clause, so a user without `LAKEHOUSE_SCAN` execute (or `EXECUTE ANY SCRIPT`) is denied before the distributor script is ever reached, even when granted execute on it directly (verified live: see `specs/_plans/add-pushdown-plan-execution-boundary/notes/A.md` §6).

A reader with only `SELECT` on the virtual schema still sees, in the plan text: the table root, the bucket layout, file names and byte sizes, and the catalog CONNECTION name. None of that is a credential. A sealed vended-credential envelope, when present, travels in the plan as ciphertext — readable but not openable without the CONNECTION password (see [Sealed vended-credential envelope](#sealed-vended-credential-envelope-378) below).

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
- **Users with script privileges.** A user with `EXECUTE` on the scan script bypasses the check, as described above. A user without that privilege cannot obtain a scan plan or run one captured by another user. The [Privilege model](#privilege-model) lists the grants that matter.
- **Changing the settings.** The virtual schema's owner, a user with `ALTER` on the virtual schema, and a user with `ALTER ANY VIRTUAL SCHEMA` can set `PERMISSION_CHECK` to an empty value or change `USER_MAPPING`. Either change turns the check off, or changes the principal, for every user of the virtual schema. A user with only `SELECT` on the virtual schema cannot change either property.

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

Every query re-resolves the CONNECTION — no cache, no restart. Use `ALTER CONNECTION` (preserves grants). Sealed envelopes of in-flight vended queries fail the AEAD open — rotate when no vended query is in flight.
