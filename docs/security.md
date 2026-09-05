[lakehouse-engine](../README.md) › [Docs](index.md) › Security

---

# Security

The catalog CONNECTION (`LAKEHOUSE_CATALOG_CREDS` in [Install](install.md) and
[Catalogs](catalogs.md)) carries the credentials that reach object storage and the catalog. This
page covers who needs access to it, how to grant that access safely, what a `SELECT`-only user of
the Virtual Schema can and cannot read back, and how to rotate the credential without downtime.

## Privilege model: script-scoped connection access

`LAKEHOUSE_SCAN` resolves the CONNECTION by name at scan time (`ctx.connection()`), rather than
receiving its credentials inline. `LAKEHOUSE_ADAPTER` resolves the same CONNECTION at plan time, as
it always has. Resolving a CONNECTION by name requires a grant naming both the connection and the
script, so a deployment needs one per script:

```sql
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER TO <vs-owner>;
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT <schema>.LAKEHOUSE_SCAN   TO <vs-owner>;
```

### The grantee is the VIRTUAL SCHEMA OWNER, not the querying user

This is the single fact to get right, and it is the opposite of what a per-user reading of the
`GRANT` syntax suggests. Verified live on Exasol 2025.2.1, in both directions, with a non-DBA
virtual-schema owner:

- A user holding only `CREATE SESSION` and `SELECT ON SCHEMA <vs>` — and **no** connection
  privilege, no role, nothing else — queries the virtual schema successfully.
- Revoking `ACCESS ON CONNECTION ... FOR SCRIPT <schema>.LAKEHOUSE_SCAN` from the **owner** breaks
  that same user's query, with the owner's privileges being the only thing that changed.
- Granting it to the **querying user** while it is revoked from the owner does **not** restore the
  query. The querying user's own privileges are not what the check reads.

Exasol evaluates the check against the virtual schema's owner when the script is reached through
VS-rewritten pushdown SQL — the only path a `SELECT ... FROM <vs>.<table>` query takes. So the two
statements above are **deployment-time statements, issued once per (connection, script, owner)**,
not per reader.

The check *is* evaluated against the session user when a script is invoked **directly**
(`SELECT <schema>.LAKEHOUSE_SCAN(...)`), which is why the syntax reads per-grantee. That path is not
how a virtual-schema query reaches the scan.

**Adding a reader is therefore plain RBAC and involves nothing connection-related:**

```sql
GRANT SELECT ON SCHEMA <MY_LAKEHOUSE> TO <new-user>;
```

Two broader alternatives exist and are both rejected, verified live against Exasol 2025.2.1:

- **`GRANT ACCESS ANY CONNECTION TO <user>`** — a blanket system privilege. A user holding only this
  privilege can write their OWN script and read back the full CONNECTION password in plaintext, for
  ANY connection on the instance, not only `LAKEHOUSE_CATALOG_CREDS`. This reopens the exact leak
  this engine closes and must never be granted for this purpose.
- **`GRANT ACCESS ON CONNECTION ... FOR SCRIPT ... TO PUBLIC`** — the script-scoped form granted to
  every current and future user. The script-scoping itself holds (a `PUBLIC`-holding user's own
  separate script still fails against the connection), but the grant cannot later be scoped down or
  revoked from one user without revoking it from all.

The script-scoped grant, and only the script-scoped grant, is the recommended shape: it is provably
narrower than `ACCESS ANY CONNECTION` (a grantee's own script cannot resolve the connection; only
the two named scripts can) and revocable per grantee, unlike `PUBLIC`.

## Recommended pattern: a deployment-scoped role held by the VS owner

`deploy/scripts/install.sh` prints the grants as a role, not as a direct grant. Its next-step
template looks like this (the role name is schema-qualified, so two independent installs on one
cluster do not collide), and the role is held by the principal that will **own** the virtual schema:

```sql
CREATE ROLE LAKEHOUSE_ENGINE_ROLE_LHVS;
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_ADAPTER TO LAKEHOUSE_ENGINE_ROLE_LHVS;
GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_SCAN   TO LAKEHOUSE_ENGINE_ROLE_LHVS;
GRANT LAKEHOUSE_ENGINE_ROLE_LHVS TO <vs-owner>;
```

**Order matters.** Run these before `CREATE VIRTUAL SCHEMA`: the adapter resolves the CONNECTION
while the virtual schema is being created, so a non-DBA owner without the `LAKEHOUSE_ADAPTER` grant
cannot create it at all (the failure is
`CONNECTION '<c>' could not be resolved: ... MT_IMPORT response missing connection_information`,
SQL state `22002`).

Verified live end to end with a non-DBA owner: a role carrying both grants, granted to the **owner**
and to nobody else, let a separate `SELECT`-only user query the virtual schema and get the same rows
a DBA session got; revoking the scan grant from the role made that same user's query fail; re-granting
restored it. The inverse — role on the querying user, revoked from the owner — fails. The per-script
scoping is a property of the `FOR SCRIPT` grant itself; it holds identically whether the grantee is a
role, `PUBLIC`, or a single user.

Grant the role to a **further virtual schema owner** — someone who will run `CREATE VIRTUAL SCHEMA`
against this CONNECTION themselves — and to nobody else:

```sql
GRANT LAKEHOUSE_ENGINE_ROLE_LHVS TO <other-vs-owner>;
```

A direct grant to the owner (`GRANT ACCESS ON CONNECTION ... FOR SCRIPT ... TO <vs-owner>`, with no
role in between) remains possible and works exactly the same way. It is the documented exception,
for a single-owner deployment or a one-off grant, not the default.

### What a non-DBA installer needs

Every deployment and test path in this repository provisioned as `sys` — a DBA that holds every
CONNECTION implicitly — until the credential-exposure regression test deliberately stood up a
non-DBA owner, which is why the requirements below went unexercised for so long. An installer who is
**not** a DBA and will own the virtual schema needs, on themselves (or on a role they hold):

| Privilege | Why |
|---|---|
| `CREATE VIRTUAL SCHEMA` | system privilege for the `CREATE VIRTUAL SCHEMA` statement |
| `EXECUTE ON SCRIPT <schema>.LAKEHOUSE_ADAPTER` | the `USING <schema>.LAKEHOUSE_ADAPTER` clause |
| `ACCESS ON CONNECTION <c> FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER` | the adapter resolves the CONNECTION at create time **and** on every later query |
| `ACCESS ON CONNECTION <c> FOR SCRIPT <schema>.LAKEHOUSE_SCAN` | the scan resolves it per shard |

The CONNECTION's own owner can issue both `ACCESS ON CONNECTION` grants without DBA help, so a
non-DBA owner who also creates the CONNECTION (`CREATE CONNECTION` system privilege) can
self-provision the whole grant set.

**A DBA owner needs none of this**, and Exasol refuses to give it to `SYS` anyway. Both forms are
rejected outright with SQL state `42500` (verified live):

- `GRANT ACCESS ON CONNECTION ... FOR SCRIPT ... TO SYS` → `cannot grant connections to SYS`
- `GRANT <role> TO SYS` → `cannot grant roles to SYS`

So skip the whole grant block when installing as `SYS`.

### `CREATE ROLE` has no `IF NOT EXISTS` form

Verified live: unlike `CREATE SCHEMA IF NOT EXISTS` (which this installer already relies on
elsewhere), `CREATE ROLE` has no idempotent form. Re-running the next-step template against an
existing deployment fails if the role already exists. Check first:

```sql
SELECT ROLE_NAME FROM EXA_ALL_ROLES WHERE ROLE_NAME = 'LAKEHOUSE_ENGINE_ROLE_LHVS';
```

and skip the `CREATE ROLE` line if it returns a row.

### Who ends up authorized

The grant is issued once, on the owner — but the population it *delegates the credential to* is
every user who can query the virtual schema. Those are two different things, and the second one is
the security-relevant one.

Verified live: a fresh user holding `CREATE SESSION` and `SELECT` on the virtual schema's schema —
and explicitly NO `EXECUTE` privilege on anything and NO connection privilege of any kind, both
confirmed absent through `EXA_DBA_OBJ_PRIVS` and `EXA_DBA_CONNECTION_PRIVS` — ran a real query
through the virtual schema and it succeeded, returning the same rows a DBA session got. **`EXECUTE
ON SCRIPT <schema>.LAKEHOUSE_SCAN` is not a separate prerequisite for querying the virtual schema,
and neither is any connection grant.**

So `GRANT SELECT ON SCHEMA <vs>` is the real authorization boundary for reading data through the
CONNECTION's credential: whoever holds it gets the owner's credential used on their behalf. Scope
the connection's own credential to a storage prefix no wider than the warehouse this deployment
actually needs, and treat `SELECT` on the virtual schema as the privilege that hands out that
reach.

### `CREATE OR REPLACE` drops the grant; two statements do it, not one

Verified live on Exasol 2025.2.1: **both** `CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS`
and `CREATE OR REPLACE SCRIPT <schema>.<script>` — for either script; the second is what an engine
upgrade/redeploy runs, and `deploy/scripts/install.sh` reissues both on every run — drop the
`ACCESS ON CONNECTION ... FOR SCRIPT` grant, regardless of which grantee holds it (a role, `PUBLIC`,
or a direct owner grant all behave the same). After either statement, queries through the virtual
schema fail with the scan's own error naming the connection and the grant (SQL state `22002`, no
credential value), or — for a directly-invoked script — with `insufficient privileges for using
connection <c> in script <p>` (SQL state `22001`). Re-issue **both**
`GRANT ACCESS ON CONNECTION ... FOR SCRIPT ...` statements (to the role, if using the recommended
pattern) after re-running the installer or replacing the CONNECTION. `ALTER CONNECTION ...
IDENTIFIED BY` does NOT drop the grants — see [Rotation](#rotation) below.

## What a `SELECT`-only user can and cannot read

`EXPLAIN VIRTUAL` and any error raised on the pushdown path both carry the pushdown SQL verbatim,
which is why credential VALUES must never appear inside it — the reason this engine references the
CONNECTION by name (and seals a vended credential — see [below](#the-sealed-vended-credential-envelope-378))
instead of embedding it. `EXPLAIN VIRTUAL` on Exasol 2025.2.1 returns four columns: `PUSHDOWN_ID`,
`PUSHDOWN_SQL`, `PUSHDOWN_JSON`, and `PUSHDOWN_INVOLVED_TABLES`. `PUSHDOWN_JSON` carries the same
`sql` value nested as `{"sql":…,"type":"pushdown"}` — it is the same leak surface as `PUSHDOWN_SQL`,
already covered by the same fix.

Two other surfaces were checked live and do NOT carry it, worth stating explicitly since a reader
would otherwise have to assume rather than find it recorded:

- **`EXA_USER_PROFILE_LAST_DAY`**, read by the least-privilege user itself (needs neither
  `ACCESS ANY CONNECTION` nor `SELECT ANY DICTIONARY`): after `ALTER SESSION SET PROFILE = 'ON'`, the
  marked query, and a DBA-issued `FLUSH STATISTICS`, every returned row's `SQL_TEXT` was
  byte-identical to the user's own literal `SELECT` — never the VS-rewritten pushdown statement, no
  `LAKEHOUSE_SCAN` call, no credential value — even though `EXPLAIN VIRTUAL` for the identical query
  DID carry both seeded credential values.
- **`EXA_DBA_AUDIT_SQL`** is not reachable by a `SELECT`-only user at all: the same query was refused
  with `insufficient privileges for accessing view EXA_DBA_AUDIT_SQL` (SQL state `42500`) — it
  requires `SELECT ANY DICTIONARY`. Read as `sys`, it carried the same negative result: only the
  user's own literal statement, never the rewritten pushdown SQL.

So profiling is least-privilege-reachable and carries nothing; audit carries nothing and a
`SELECT`-only user cannot reach it either. Neither is a credential leak vector for either static or
vended credentials. `EXPLAIN VIRTUAL` and pushdown-path error text are the real leak surfaces, and
both are addressed by referencing the CONNECTION by name (static credentials) or sealing the value
(vended credentials).

`EXA_DBA_CONNECTIONS` exposes `CONNECTION_NAME`, `CONNECTION_STRING`, `USER_NAME`, `PUBLIC_KEY`,
`CREATED`, and `CONNECTION_COMMENT` — no password column, verified live — and a `SELECT`-only user
is refused the view outright (SQL state `42500`).

## The sealed vended-credential envelope (#378)

A credential the catalog vends per query (`use_vended_credentials`) has no name a scan UDF can
reference the way a static CONNECTION credential can, so it still travels inside the pushdown SQL —
but only as an AES-256-GCM ciphertext, never in plaintext. The envelope's key is derived
(HKDF-SHA256) from the same CONNECTION's password, so the scan UDF's own grant-gated
`ctx.connection()` read supplies the key it needs to open the envelope, with no separate secret to
manage.

**This is a deliberately bounded guarantee, not a maximum-security cryptographic envelope.** Its
goal is to defeat a plaintext read of the pushdown SQL through `EXPLAIN VIRTUAL` or pushdown-path
error text — not to withstand a determined cryptanalytic attacker holding the ciphertext and
unlimited compute against a low-entropy password. The gate that decides whether the envelope ships
tests **non-emptiness** of a secret field on the CONNECTION password, not entropy: the engine cannot
measure the strength of an arbitrary password without rejecting legitimate secrets. This means the
bound also rests on the operator's own secret strength — a `token` of a single character satisfies
the gate as readily as a strong one. Two facts make the bound acceptable rather than an oversight:
the protected values are short-lived and scope-limited (vended per query, expiring on the catalog's
own schedule, scoped to the prefix the catalog vended them for), and the key material is exactly the
secret an attacker would already need `ACCESS ON CONNECTION` to read.

**Vending without any key material is refused, not shipped under a weakened guarantee.** A
CONNECTION password carrying none of `token`, `client_secret`, `secret_key`, `session_token`,
`account_key`, or `sas_token` non-empty (a no-auth catalog whose password holds only
`{"warehouse":"…"}` is the canonical shape) would derive a guessable key. Rather than seal under
that key, the engine refuses the query at pushdown-planning time with a clear error naming the
configuration and both remedies: configure catalog authentication, or disable
`use_vended_credentials`. A non-empty `access_key` alone does not satisfy the gate — an AWS access
key id is an identifier, not a secret.

## Rotation

The engine holds no state between queries: every query re-resolves the CONNECTION, so there is no
cache to invalidate and no restart or redeployment needed after a rotation.

| Fact | Basis |
|---|---|
| Rotate in place with `ALTER CONNECTION <c> TO '<uri>' USER '<u>' IDENTIFIED BY '<json>'` | Verified live: the statement succeeds against a real CONNECTION and the owner's `ACCESS ON CONNECTION ... FOR SCRIPT` grants SURVIVE — the next call resolves the rotated connection, with the new value. |
| Do NOT rotate with `CREATE OR REPLACE CONNECTION` | Verified live: the replacement DROPS the script-scoped grants (see [above](#create-or-replace-drops-the-grant-two-statements-do-it-not-one)). This is the form the installer and the test harness use for provisioning, so a re-provision requires re-issuing every grant. |
| The secret is not recoverable from the catalog afterwards | `EXA_DBA_CONNECTIONS` has no password column, and a least-privilege user is refused the view outright (SQL state `42500`). |
| Zero-downtime rotation requires the PROVIDER to accept both secrets during the switch | Exasol holds exactly one password per CONNECTION, so there is no overlap window on the Exasol side. Register the new secret at the identity provider or cloud account first, `ALTER` the CONNECTION second, revoke the old secret third. |
| In-flight queries may straddle the switch | Each scan shard resolves the CONNECTION itself, so both the old and new value must stay valid for the duration of the longest running query. Make an `ALTER` that changes the store `endpoint` or `region` (not just the secret) only when no query is in flight — a later shard would otherwise read a different store mid-query. |
| A rotation invalidates the sealed envelopes of in-flight VENDED queries | The envelope key is derived from the password, so a shard unsealing after the rotation fails with a named error carrying no plaintext — it cannot read stale or wrong credentials. Rotate when no vended query is in flight. |
| A `client_secret` (catalog-auth) rotation is narrower than a storage-credential rotation | The catalog-auth secret never crosses the UDF boundary; only the adapter reads it, once per pushdown request, at plan time. An already-minted bearer token stays valid for its own lifetime — that is the provider's contract, not the engine's. |

## See also

- [Install: Point the VS at your data](install.md#point-the-vs-at-your-data) — the next-step
  template this page's privilege model documents.
- [Catalogs](catalogs.md) — the CONNECTION fields, including the six secret-bearing fields the
  sealed envelope's key-material gate checks.
