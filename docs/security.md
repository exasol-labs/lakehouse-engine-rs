[lakehouse-engine](../README.md) › [Docs](index.md) › Security

---

# Security

The catalog CONNECTION (`LAKEHOUSE_CATALOG_CREDS` in [Install](install.md) and
[Catalogs](catalogs.md)) carries the credentials that reach object storage and the catalog. This
page covers who needs access to it, how to grant that access safely, what a `SELECT`-only user of
the Virtual Schema can and cannot read back, and how to rotate the credential without downtime.

## Privilege model: script-scoped connection access

`LAKEHOUSE_SCAN` resolves the CONNECTION by name at scan time (`ctx.connection()`), rather than
receiving its credentials inline. `LAKEHOUSE_ADAPTER` resolves the same CONNECTION at plan time.
Resolving a CONNECTION by name requires a grant naming both the connection and the script:

```sql
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER TO <vs-owner>;
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT <schema>.LAKEHOUSE_SCAN   TO <vs-owner>;
```

### The grantee is the VIRTUAL SCHEMA OWNER, not the querying user

Exasol evaluates the `ACCESS ON CONNECTION ... FOR SCRIPT` check against the virtual schema's owner
when the script is reached through VS-rewritten pushdown SQL. So the two statements above are
**deployment-time statements, issued once per (connection, script, owner)**, not per reader.

The check *is* evaluated against the session user when a script is invoked **directly**
(`SELECT <schema>.LAKEHOUSE_SCAN(...)`), which is not how a virtual-schema query reaches the scan.

**Adding a reader is therefore plain RBAC and involves nothing connection-related:**

```sql
GRANT SELECT ON SCHEMA <MY_LAKEHOUSE> TO <new-user>;
```

Two broader alternatives are rejected:

- **`GRANT ACCESS ANY CONNECTION TO <user>`** — lets the grantee's own script resolve ANY connection
  on the instance, reopening the exact leak this engine closes.
- **`GRANT ACCESS ON CONNECTION ... FOR SCRIPT ... TO PUBLIC`** — the grant cannot later be scoped
  down or revoked from one user without revoking it from all.

## Recommended pattern: a deployment-scoped role held by the VS owner

`deploy/scripts/install.sh` prints the grants as a role (schema-qualified so two independent
installs on one cluster do not collide):

```sql
CREATE ROLE LAKEHOUSE_ENGINE_ROLE_LHVS;
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_ADAPTER TO LAKEHOUSE_ENGINE_ROLE_LHVS;
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_SCAN   TO LAKEHOUSE_ENGINE_ROLE_LHVS;
GRANT LAKEHOUSE_ENGINE_ROLE_LHVS TO <vs-owner>;
```

**Order matters.** Run these before `CREATE VIRTUAL SCHEMA`: the adapter resolves the CONNECTION
while the virtual schema is being created, so a non-DBA owner without the `LAKEHOUSE_ADAPTER` grant
cannot create it at all.

A direct grant to the owner (no role) works the same way for single-owner deployments.

### What a non-DBA installer needs

| Privilege | Why |
|---|---|
| `CREATE VIRTUAL SCHEMA` | system privilege for the `CREATE VIRTUAL SCHEMA` statement |
| `EXECUTE ON SCRIPT <schema>.LAKEHOUSE_ADAPTER` | the `USING <schema>.LAKEHOUSE_ADAPTER` clause |
| `ACCESS ON CONNECTION <c> FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER` | the adapter resolves the CONNECTION at create time and on every later query |
| `ACCESS ON CONNECTION <c> FOR SCRIPT <schema>.LAKEHOUSE_SCAN` | the scan resolves it per shard |

The CONNECTION's own owner can issue both `ACCESS ON CONNECTION` grants without DBA help.

**A DBA owner needs none of this** — `SYS` holds every CONNECTION implicitly, and Exasol refuses
`GRANT ACCESS ON CONNECTION ... TO SYS` and `GRANT <role> TO SYS` (SQL state `42500`).

### `CREATE ROLE` has no `IF NOT EXISTS` form

Check `EXA_ALL_ROLES` before re-running on an existing deployment:

```sql
SELECT ROLE_NAME FROM EXA_ALL_ROLES WHERE ROLE_NAME = 'LAKEHOUSE_ENGINE_ROLE_LHVS';
```

### Who ends up authorized

The grant delegates the credential to every user who can query the virtual schema. A user holding
only `CREATE SESSION` and `SELECT` on the virtual schema's schema — and no `EXECUTE` or connection
privilege — can query the VS successfully. So `GRANT SELECT ON SCHEMA <vs>` is the real
authorization boundary: scope the CONNECTION's credential to the warehouse prefix this deployment
needs.

### `CREATE OR REPLACE` drops the grant

**Both** `CREATE OR REPLACE CONNECTION` and `CREATE OR REPLACE SCRIPT` drop the script-scoped grant.
After either statement, queries fail with a named error. Re-issue both `GRANT ACCESS ON CONNECTION`
statements after re-running the installer or replacing the CONNECTION. `ALTER CONNECTION` does NOT
drop the grants — see [Rotation](#rotation) below.

## What a `SELECT`-only user can and cannot read

`EXPLAIN VIRTUAL` and pushdown-path errors carry the pushdown SQL verbatim — the reason credential
VALUES must never appear inside it. `EXPLAIN VIRTUAL` on Exasol 2025.2.1 returns `PUSHDOWN_SQL` and
`PUSHDOWN_JSON` (same `sql` value nested), both covered by the same fix.

Profiling (`EXA_USER_PROFILE_LAST_DAY`) and audit (`EXA_DBA_AUDIT_SQL`) `SQL_TEXT` carry only the
user's own literal statement, never the VS-rewritten pushdown SQL. `EXA_DBA_CONNECTIONS` exposes no
password column. Neither is a credential leak vector.

## The sealed vended-credential envelope (#378)

A credential the catalog vends per query (`use_vended_credentials`) has no name a scan UDF can
reference, so it travels inside the pushdown SQL as an AES-256-GCM ciphertext, never in plaintext.
The key is derived (HKDF-SHA256) from the same CONNECTION's password.

**This is a deliberately bounded guarantee.** It defeats a plaintext read of the pushdown SQL — not
offline cryptanalysis against a low-entropy password. Two facts make the bound acceptable: the
protected values are short-lived and prefix-scoped, and the key material is exactly the secret
`ACCESS ON CONNECTION` already reveals.

**Vending without key material is refused.** A CONNECTION password with no non-empty secret field
(`token`, `client_secret`, `secret_key`, `session_token`, `account_key`, or `sas_token`) would
derive a guessable key. The engine refuses at plan time with a clear error naming the configuration
and both remedies. A non-empty `access_key` alone does not satisfy the gate — an AWS access key id
is an identifier, not a secret.

## Rotation

Every query re-resolves the CONNECTION — no cache to invalidate and no restart needed.

| Fact | Basis |
|---|---|
| Rotate with `ALTER CONNECTION <c> TO '<uri>' USER '<u>' IDENTIFIED BY '<json>'` | `ALTER` preserves the script-scoped grants. |
| Do NOT rotate with `CREATE OR REPLACE CONNECTION` | Drops the grants (see [above](#create-or-replace-drops-the-grant)). |
| Zero-downtime rotation requires the PROVIDER to accept both secrets during the switch | Exasol holds one password per CONNECTION; register the new secret at the provider first. |
| In-flight queries may straddle the switch | Each shard resolves independently; both values must stay valid for the longest running query. |
| A rotation invalidates sealed envelopes of in-flight vended queries | The shard fails the AEAD open with a named error carrying no plaintext. Rotate when no vended query is in flight. |

## See also

- [Install: Point the VS at your data](install.md#point-the-vs-at-your-data)
- [Catalogs](catalogs.md)
