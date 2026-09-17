# Spike: can OPA enforce row-level access in lakehouse-engine using only the Exasol user name?

**Verdict up front.** Plan-time enforcement is sound and needs no per-user catalog identity, but
**Trino's OPA contract cannot be adopted as-is**: it returns a SQL expression *string* in the Trino
dialect, and **0 of 9** realistic row filters planned in the engine's DataFusion when spliced in
verbatim (`evidence/05`, PROBE 1). After an adapter-side identifier rewrite, 7 of 9 planned, but 2
of those 7 are *silently unsound* rather than correct, and the single documented column-mask example
from Trino's own docs returns the **unmasked value** with no error anywhere. The feature is real;
the contract is not. Full verdict and costing in [§7](#7-verdict).

Everything below is backed by a transcript in `evidence/` or a quoted source file. Where something
was reasoned about rather than executed it says so explicitly.

Tracked property this spike relies on and did **not** re-verify: a user holding only
`CREATE SESSION` plus `SELECT` on the virtual schema can run queries and `EXPLAIN VIRTUAL` but
cannot execute the captured pushdown SQL ("insufficient privileges for calling script"), so an
adapter-injected filter cannot be stripped by the user. GitHub issue #402.

## Reproducing

```bash
scripts/run-all.sh          # everything; downloads OPA into .bin/ (gitignored)
scripts/10-capture-row-filters.sh
scripts/20-capture-column-masks.sh
scripts/30-failure-modes.sh
scripts/40-latency.sh
scripts/50-translate-probe.sh          # cargo; df-probe/ is NOT a workspace member
scripts/60-exasol-identity.sh          # needs: docker compose up -d exasol
```

| Evidence | What it holds |
|---|---|
| `evidence/00-trino-contract.txt` | Verbatim Trino source excerpts defining the request/response types |
| `evidence/01-row-filters.txt` | 12 live `GetRowFilters` request/response pairs from OPA 1.20.2 |
| `evidence/02-column-masks.txt` | 5 per-column plus 1 batch `GetColumnMask` pairs |
| `evidence/03-failure-modes.txt` | 9 failure classes, each with HTTP status and body |
| `evidence/04-latency.txt` | 300-call round-trip distribution, two measurement modes |
| `evidence/05-datafusion-translation.txt` | 7 probes against real DataFusion 54.1, incl. negative controls |
| `evidence/06-exasol-identity.txt` | Live Exasol 2025.1.16: what identity attributes exist |
| `evidence/07-lakekeeper-refutation.txt` | All 18 Lakekeeper `.rego` files searched for the row-filter contract |

Versions: OPA 1.20.2 (Rego v1), DataFusion 54.1 with `parquet,sql,unicode_expressions` (the engine's
own feature set, `crates/lakehouse-engine/Cargo.toml:32`), Exasol 2025.1.16, Trino `master` as of
2026-09-17.

---

## 1. What exactly does OPA return?

**A SQL expression string in the Trino dialect, and nothing else.** The entire payload type is four
lines (`evidence/00`, from `schema/OpaViewExpression.java`):

```java
public record OpaViewExpression(String expression, Optional<String> identity)
```

`expression` is a bare `String`. Trino parses it with its own parser, in the catalog/schema context
of the filtered table:

```java
ViewExpression.builder().catalog(catalogName).schema(schemaName).expression(expression)
```

There is no structured form, no AST, no type annotation, no dialect field. The dialect is Trino SQL
by construction, never declared.

**Row filters** (`opa.policy.row-filters-uri`) return a *list*, and the plugin applies all of them,
so multiple filters are conjunctive. Live capture (`evidence/01`, case `orders_multi`):

```json
{"result": [{"expression": "o_totalprice < 100000"}, {"expression": "region = 'EU'"}]}
```

Note the order is not the order the policy declares them in. Rego `contains` builds a *set*, so the
list order is not contractual. Harmless for AND, not safe to rely on otherwise.

**Column masks** return at most one expression per column, either one request per column
(`{"result": {"expression": "NULL"}}`) or batched with an index
(`{"result": [{"index": 0, "viewExpression": {...}}]}`). Both captured in `evidence/02`.

**Who decides the column names: the policy, with no help from the request.** This is the sharpest
finding of item 1. `getRowFilterExpressionsFromOpa` builds its resource with
`new TrinoTable(table)`, and that constructor passes `columns` and `properties` as `null`:

```java
public TrinoTable(CatalogSchemaTableName table) {
    this(table.getCatalogName(), ..., null, null);
}
```

`@JsonInclude(NON_NULL)` then drops both fields, so the row-filter request carries **only three
strings**: catalog, schema, table. Confirmed on the wire (`evidence/01`, every case):

```json
{"action":{"operation":"GetRowFilters","resource":{"table":
  {"catalogName":"lakehouse","schemaName":"sales","tableName":"orders"}}}}
```

Negative control: a policy rule reading `input.action.resource.table.columns` evaluates to undefined
and the endpoint answers `{}` (`evidence/03`, case H). The policy author must therefore hard-code
column names and their types from out-of-band knowledge of the table. A schema change silently turns
a working filter into a planning error, or worse (see [§2](#2-can-we-translate-what-it-returns)).

Column *mask* requests are the exception: they do carry `columnName` and `columnType`
(`schema/TrinoColumn.java`), e.g. `"columnType":"DECIMAL(15,2)"` in `evidence/02`.

### Prior art check: Lakekeeper (refuted as a row-filter source, confirmed as table-level)

The brief asked to confirm or refute rather than assume. **Refuted, with a full-repository search.**
All 18 `.rego` files in `lakekeeper/lakekeeper` were concatenated (2,645 lines) and searched for
`rowFilters|columnMask|batchColumnMasks|GetRowFilters|GetColumnMask`: zero matches
(`evidence/07`). The bridge answers a boolean `allow` for catalog, schema, table, view and function
operations, delegating to Lakekeeper's `/management/v1/action/batch-check`. `SelectFromColumns` and
`FilterColumns` give column-level **allow/deny**, which is access control, not masking. So the
original claim holds: Lakekeeper is table-level (plus column allow/deny), and contributes nothing to
row filtering.

---

## 2. Can we translate what it returns?

**Verbatim: no, 0 of 9.** After an identifier rewrite: 7 of 9 plan, of which **5 are sound** and
**2 plan but are wrong or unverifiable**. `evidence/05`.

The engine already treats the scan filter as raw SQL text, which is why this looked promising:

```rust
/// DataFusion SQL WHERE predicate fragment, already translated.
pub filter: Option<String>,          // scan/spec.rs:1114
```

spliced at three sites, e.g. `scan/raw_scan.rs:563-571`:

```rust
let mut sql = format!("SELECT {select_clause} FROM ({inner})");
if let Some(filter) = &spec.common.filter && !filter.is_empty() {
    sql.push_str(" WHERE "); sql.push_str(filter);
}
```

### 2.1 Verbatim splicing fails on every single filter

`inner` is built by `build_alias_items` (`scan/sql_support.rs:15-28`), which exposes every column as
`"parquet_name" AS "UPPERCASE_NAME"`. DataFusion normalises unquoted identifiers to lowercase, so an
OPA filter written the way every policy author writes it cannot resolve:

```
REJECTED A simple equality   PLAN: Schema error: No field named region. Column names are case
  sensitive. ... Valid fields are "REGION", "O_TOTALPRICE", ...
```

All nine failed this way (`evidence/05` PROBE 1). Negative controls in PROBE 3 confirm the cause
rather than assuming it: bare lowercase, mixed case, and *quoted* lowercase all fail, while the
uppercase-quoted form succeeds, and an unknown column fails too.

### 2.2 After an identifier rewrite: what survives

| Shape | OPA returned | Verdict |
|---|---|---|
| A equality | `region = 'EU'` | **sound**, 4 rows |
| B two filters, ANDed | `o_totalprice < 100000`, `region = 'EU'` | **sound**, 4 rows |
| C IN list | `region IN ('EU','UK','CH')` | **sound**, 6 rows |
| D NULL / 3-valued logic | `(classification IS NULL OR classification <> 'SECRET')` | **sound**, 6 rows |
| E date + interval | `o_orderdate >= DATE '2024-01-01' AND o_orderdate < current_date - INTERVAL '7' DAY` | plans, **unsound** |
| F subquery / mapping table | `region IN (SELECT region FROM security.acl.user_region WHERE user_name = CURRENT_USER)` | **rejected** |
| G engine function | `regexp_like(o_comment, '^(?i)eu-')` | plans, **unverified** |
| H session function | `owner = CURRENT_USER` | **rejected** |
| I with `identity` override | `dept = 'FINANCE'` | plans; the override is **unimplementable** |

**Sound: 5 of 9 (56%).** Detail on the four that are not:

**E is unsound, and the engine already knows why.** `current_date` plans fine, but it is evaluated
inside the shard, on the container clock, in UTC, once per shard invocation. The engine deliberately
withdrew exactly this family of functions from its advertised capabilities
(`adapter/capabilities.rs:117-132`):

> `FN_CURRENT_DATE`/`FN_CURRENT_TIMESTAMP`/`FN_SYSDATE`/`FN_SYSTIMESTAMP` (the now-family) are NOT
> advertised ... Measured live against Exasol 2025.2.1: a pushed `SYSTIMESTAMP` returned a value ~2
> hours off native ... and `GROUP BY SYSTIMESTAMP` over a two-file table returned **two distinct
> timestamps** against one statement-constant native value.

A clock call in an OPA filter re-introduces precisely what that withdrawal removed, and the
withdrawal cannot stop it: the capability registry gates what *Exasol* pushes down, and an
OPA-supplied string never passes through it. Two shards straddling midnight would filter
differently within one query. `evidence/05` PROBE 6c shows `current_date` evaluating inside the
shard.

**F cannot work, and this is structural, not a missing feature.** `PLAN: Error during planning:
table 'security.acl.user_region' not found`. The scan UDF's `SessionContext` has exactly one
registered table, `scan_target` (`scan/partial_agg.rs:42`), holding that shard's assigned Parquet
files. There is no catalog inside the scan for a subquery to reach, and registering one would
violate "resolve metadata once per query, in the VS layer, never once per node" and
"UDFs are stateless and disposable" (`CLAUDE.md`). Entitlement-table policies are the common
real-world shape and they are out, unless the adapter resolves the mapping table at plan time and
inlines the result as a literal IN list, which is a different feature with its own size limit.

**G plans but is unverified.** `regexp_like` exists in both dialects with different engines: Trino
uses `java.util.regex`, DataFusion the Rust `regex` crate. Trino was not run here, so PROBE 6b is
one-sided and only shows DataFusion answering. Useful boundary result: Java lookahead `(?=eu)` and
backreference `(\w)\1` fail loudly in DataFusion, which is the safe outcome. The dangerous class is
a pattern both engines accept and read differently, and this spike did not establish whether such a
pattern exists.

**H is rejected in the most alarming possible way.** DataFusion 54.1 has no `current_user` session
function, so it parses the token as a *column reference*: `Schema error: No field named
current_user`. Today that is a planning error. On a table that happens to have a column named
`current_user`, the same filter would plan and compare a column to itself. Any identity function a
policy writes must be resolved to a literal by the adapter before the string reaches the scan.

**I's `identity` field is unimplementable here, and that is fine.** In Trino, `identity` means
"evaluate this filter as user X" so a filter can reference a column the requester cannot see. The
engine has one service account and no per-user identity, so there is nothing to switch to. The
expression body still works; the field must be treated as an explicit reject, never ignored, because
ignoring it silently changes the policy's meaning.

### 2.3 Column masks: the documented example silently leaks the column

This is the worst result in the spike. Trino's own OPA documentation gives this as *the* column-mask
example:

```text
columnMask := {"expression": "'****' || substring(user_name, -3)"} if { ... }
```

In Trino, `substring(str, -3)` returns the last 3 characters, so `alice` masks to `****ice`. In
DataFusion the same call, same arity, plans without complaint and returns (`evidence/05` PROBE 6a):

```
| OWNER | MASKED    |
| alice | ****alice |
| bob   | ****bob   |
| carol | ****carol |
```

The full value, with a decorative prefix. No error, no warning, no plan failure. A masking policy
copied verbatim from Trino's documentation would ship a total disclosure of the column it was
written to protect. Mask results overall (`evidence/05` PROBE 5):

| Mask | Result |
|---|---|
| `NULL` | plans, correct |
| `CASE WHEN ... THEN col ELSE NULL END` | plans, correct |
| `'****' \|\| substring(x, -3)` | plans, **silently unmasked** |
| `to_hex(sha256(to_utf8(x)))` | rejected: `Invalid function 'sha256'` |
| `sha256(x)`, `md5(x)` | rejected (DataFusion's `crypto_expressions` feature is off) |
| `to_utf8(x)`, `cardinality(...)` | rejected (Trino-only names) |

Hash-based masking, one of the most common real policies, is unavailable without enabling
`crypto_expressions`, and even then the digest would differ from Trino's rendering.

### 2.4 A second, independent hazard: SQL-comment truncation

`raw_scan.rs` appends `ORDER BY` and `LIMIT` *after* the spliced filter, on one line. A filter
ending in `--` therefore comments them out. Executed, `evidence/05` PROBE 7:

```
filter without a comment: Ok(2) rows
filter ending in `-- x` : Ok(4) rows   <-- ORDER BY and LIMIT swallowed
```

Multi-statement injection is blocked (`"; DROP TABLE"` fails with *the context currently only
supports a single SQL statement*), so this is not arbitrary execution. It is still a policy source
silently rewriting the query plan. Any accepted filter must be parenthesised at minimum, and really
ought to be parsed rather than trusted.

### 2.5 What this means for the contract

The honest fraction is **5 of 9 sound after a rewrite the adapter would have to write, 0 of 9 as
delivered**, and the failures are not uniformly loud. Loud failures are tolerable. `substring(x, -3)`
returning the unmasked column, and `current_date` differing between shards, are not: a policy
engine whose output can be *quietly wrong* is worse than no policy engine, because it is believed.

**The string contract is the defect, not OPA.** OPA is perfectly capable of returning a structured
predicate: it is a JSON engine, and Rego can emit any JSON shape. A contract in which the policy
returns something like
`{"op":"in","column":"REGION","values":["EU","UK"]}` would route through the engine's existing
`vs-expression` translator, which already owns dialect rendering, already declines names it cannot
translate (`TRANSLATED_SCALAR_FNS` gates dispatch, `crates/vs-expression/src/lib.rs:119-251`), and
already has the decline-then-self-apply plumbing (`classify_where_filter`,
`adapter/pushdown/support.rs:1178`). That path converts every failure above into an explicit
decline. It is also strictly less expressive than Trino's, which is the point.

---

## 3. Is the Exasol user name enough as the policy input?

**Not for realistic policies, and the gap is closable from inside Exasol with no catalog identity.**

What a policy needs, read off the captured inputs: Trino sends `{"user": ..., "groups": [...]}`
(`schema/TrinoIdentity.java`) plus a `queryId` and a software-stack block, and an operator-supplied
static context map from `opa.context-file`. Every non-trivial published example keys on **groups**,
not on the user name, for the obvious operational reason that a policy enumerating individual users
does not survive contact with an organisation. The spike's own policy demonstrates the alternative
and its failure mode: it keys a region off a literal user map, and an unlisted user (`carol`) gets
**no filter at all** (`evidence/01`, last case) rather than an error.

A bare user name is technically sufficient (the policy can hold the mapping itself) and practically
unusable at any scale.

**Where groups can come from, verified live against Exasol 2025.1.16** (`evidence/06`):

- **Exasol roles are readable and are the natural group source.** `SYS.EXA_DBA_ROLE_PRIVS` returns
  `GRANTEE, GRANTED_ROLE, ADMIN_OPTION` for any user. Live: `OPA_ALICE,OPA_EU_STAFF`. Negative
  control, asking for a role the user does not hold, returns 0 rows rather than an error.
- **The transitive closure is computable from that one view.** Role-to-role grants appear in the
  same view (`OPA_EU_STAFF,OPA_AUDITORS`). `EXA_ROLE_ROLE_PRIVS` is the wrong view for this: it
  showed 0 rows because it only covers roles the querying session itself holds.
- **There is no user attribute store.** `EXA_ALL_USERS` has exactly four columns: `USER_NAME`,
  `CREATED`, `USER_CONSUMER_GROUP`, `USER_COMMENT`. Negative control: `SYS.EXA_USER_ATTRIBUTES` does
  not exist (`SQL state 42000, object not found`), so the probe distinguishes "empty" from
  "fabricated".
- **External identity links exist as a bridge to a real IdP.** `EXA_DBA_USERS` carries
  `DISTINGUISHED_NAME` (LDAP DN), `KERBEROS_PRINCIPAL` and `OPENID_SUBJECT`. Empty in this
  password-auth container, but on an IdP-backed cluster these are the join key from an Exasol user to
  LDAP groups or OIDC claims, resolvable by OPA itself through `http.send` or a bundle, with no
  per-user catalog credential anywhere.

**Reachability, reasoned not executed.** The adapter is a UDF, so it can read those views over
connect-back (`ExaConnection::query`, already used by the engine), and it already performs outbound
HTTP for catalog access through `reqwest` (`crates/lakehouse-catalog/Cargo.toml:30`), which compiles
into the same `.so`. An OPA call therefore adds no new capability and no new dependency. Not executed
here: no adapter code was written, per the brief.

One hard failure mode to design for: `UdfContext::current_user()` returns `Option<String>` and is
documented as `None` "when the DB did not report it"
(`exasol-udf-sdk-0.26.1/src/context.rs:200-205`). No identity means no decision, which must be a
refusal.

---

## 4. Composition with pushdown

**Every shape can carry the filter safely, and the engine's existing splice points are already in
the right places.** Demonstrated against real DataFusion over a two-shard fixture, each sharded
result compared to the single-node reference answer (`evidence/05` PROBE 4):

| Shape | Where the filter goes | Sharded vs single-node |
|---|---|---|
| Plain row scan | outer `WHERE` over the aliased inner (`raw_scan.rs:563`) | n/a, direct |
| Single-group partial/merge | `WHERE` of the same SELECT that aggregates (`partial_agg.rs:365-376`) | 4 = 4 |
| GROUP BY partial/merge | same, before `GROUP BY` (`partial_agg.rs:238-252`) | `FINANCE 4, SALES 2` both sides |
| COUNT(DISTINCT) | ANDed into the per-shard DISTINCT scan (`support.rs:392-396`) | 1 = 1 |
| Broadcast join | after the join (`join_scan.rs:206-212`) | 4 both placements |
| ORDER BY + LIMIT (TopN) | before `ORDER BY`/`LIMIT` (`raw_scan.rs:563-591`) | `80, 50` both sides |

The aggregate builders splice the filter into the `WHERE` of the very SELECT that computes the
aggregate, so the filter is pre-aggregation by construction. The failure the brief warns about was
reproduced deliberately to show it is a real distinction and not a theoretical one: moving the same
filter to the merge step instead of the shard scan gave **0** where the correct answer is **4**
(PROBE 4b).

Broadcast join deserves a note. The engine puts the filter *after* the join, which is safe here
because an inner equi-join commutes with a conjunctive single-side predicate. Both placements gave 4
(PROBE 4e). This holds for the inner-equi-join contract the engine actually pushes down; it would
not hold for an outer join, and the engine pushes none.

**Two injection points, not one.** This is the one structural surprise. The single-table path has a
clean chokepoint: `base: CommonScanSpec` is constructed once (`pushdown/mod.rs:360-380`) and `filter`
flows from there into all four dispatch arms (`Grouped`, `GroupByWrapper`, `SingleGroupAgg`,
`RowScan`). But the join gate returns *before* it:

```rust
JoinShape::Join(join) => { return plan_join(...).await; }   // mod.rs:160-178
```

and the join path computes per-leg predicates of its own via `leg_local_filter` /
`render_df_filter_qualified` (`joins/rendering.rs`). A row filter must be attached to the correct
leg there, separately. A single-point implementation would enforce nothing on joins.

**Three shapes needing explicit handling, none of them unsafe if handled:**

- **`declined_filter` route** (`mod.rs:384`). When the request's own WHERE cannot be translated, the
  adapter self-applies it in the outer wrapper and passes `None` as the scan filter. An OPA filter
  must be passed as `Some(...)` on this route, not dropped with it.
- **Empty-result short-circuit** (`mod.rs:255-256`, before `base` exists). A table with zero active
  files returns an empty result. Safe: an empty answer discloses nothing.
- **`refused_columns`** (`ensure_no_refused_column_referenced`, `mod.rs:249`). If a policy filters on
  a column the format reader refused to map, the filter cannot be applied. That must be a refusal,
  never a skipped filter.

---

## 5. Failure modes

**The transport fails closed. The policy layer fails OPEN, and that is the finding.**

Captured, with HTTP status and body (`evidence/03`):

| Case | HTTP | Body | Consequence in Trino's consumer |
|---|---|---|---|
| A healthy, user has a policy | 200 | `{"result":[{"expression":"region = 'EU'"}]}` | filter applied |
| B **user the policy says nothing about** | 200 | `{"result":[]}` | **no filter, full access** |
| C **mistyped rule name** | 200 | `{}` | **no filter, full access** |
| D **mistyped package path** | 200 | `{}` | **no filter, full access** |
| E **Rego runtime error (1/0)** | 200 | `{"result":[]}` | **no filter, full access** |
| F same, `strict-builtin-errors=true` | 200 | `{"result":[]}` | **no filter, full access** |
| G non-string expression (`42`) | 200 | `{"result":[{"expression":42}]}` | codec coercion, see below |
| H policy reads an absent input field | 200 | `{}` | rule undefined |
| I **OPA unreachable** | none | `curl: (7) Failed to connect` | exception, query fails |

Two mechanics explain the fail-open column. First, a Rego *partial set* rule (`rowFilters contains
...`) is always **defined**, evaluating to the empty set when no element body succeeds, so a crashed
or silent policy is byte-identical to a permissive one. `strict-builtin-errors=true` did not change
this. Second, Trino's consumer flattens the remaining distinction away:

```java
result = ImmutableList.copyOf(requireNonNullElse(result, ImmutableList.of()));
```

so `{}` and `{"result":[]}` both become "no filters". Worth noting that Trino's
`OpaQueryException.PolicyNotFound` branch fires only on HTTP 404, and OPA's `/v1/data` API returned
**200** for both a nonexistent rule and a nonexistent package, so that branch is effectively
unreachable for these endpoints.

By contrast, transport and HTTP-level failures do fail closed: `parseOpaResponse` throws
`OpaServerError` on any non-2xx and `DeserializeFailed` on an unparseable body, and an unreachable
server surfaces as `QueryFailed` (`evidence/00`).

**What the engine would have to do, per case.** None of these are satisfiable by adopting the
contract as-is; each needs an explicit rule.

1. **OPA unreachable, timeout, non-2xx, unparseable body.** Refuse the pushdown request with a
   user-visible error. Never return a spec. Exasol does not re-plan on an adapter error, so a refusal
   is a hard query failure, which is the correct outcome. No caching of a previous decision:
   `CLAUDE.md` forbids cross-call state, and a cached allow is a stale allow.
2. **Untranslatable filter** (F, H, an unknown function, an `identity` override). Refuse. Do not fall
   back to applying the request's own predicate only, and do not route through the existing
   `declined_filter` self-apply path, which exists to preserve *the user's* semantics and would here
   discard the policy's.
3. **Nothing returned for a user.** This is the one that cannot be fixed inside Trino's contract,
   because "allowed everything" and "policy did not answer" are the same bytes. The engine must
   require an explicit affirmative envelope, for example
   `{"decision": "allow"|"deny"|"filter", "filters": [...]}`, and treat a missing or unrecognised
   `decision` as deny. Adopting `rowFilters contains ...` as the contract means accepting that a
   typo in a policy path grants full table access with an HTTP 200.
4. **Non-string or malformed expression** (G). Reject on type. *Reasoned, not executed:* Jackson's
   default coercion would turn `42` into the string `"42"`, which then fails as a non-boolean
   predicate. DataFusion's equivalent refusal was executed: `Cannot create filter with non-boolean
   predicate 'O_TOTALPRICE' returning Float64` (`evidence/05` PROBE 3).
5. **No identity.** `ctx.current_user()` returning `None` must refuse before any OPA call.
6. **Policy filters a refused column.** Refuse, per [§4](#4-composition-with-pushdown).

A corollary worth stating plainly: because failure and permission are indistinguishable on this
contract, **the engine cannot verify that the policy layer is working**. A monitoring signal
(decision IDs, a policy-version echo, an explicit deny-by-default envelope) is not a nice-to-have
here, it is the only way to tell enforcement from silence.

---

## 6. Cost

**Negligible, and no batching needed.** 300 calls to a loopback OPA with policies loaded from disk
(`evidence/04`):

| Measurement | median | p95 | p99 | max |
|---|---|---|---|---|
| one `curl` process per call | 0.556 ms | 0.968 ms | 1.845 ms | 2.617 ms |
| all calls in one process, keep-alive | **0.203 ms** | 0.324 ms | 0.852 ms | 1.672 ms |

The keep-alive figure is the right one for a client that holds a connection. Projected plan-time
cost: 0.20 ms for 1 table, 1.02 ms for 5, 4.06 ms for 20. Against a plan that already resolves
catalog metadata and lists files over the network, this is not measurable. Caveat: loopback sidecar
with in-memory policy. A remote OPA, or a policy using `http.send` to reach an IdP, changes this
completely and the measurement would have to be redone against that deployment.

**One call per table, not one per query.** `OpaAccessControl.getRowFilters(context, tableName)` takes
a single table and Trino's analyzer calls it once per table in the query. `OpaConfig` declares batch
URIs for generic filtering (`opa.policy.batched-uri`) and for column masking
(`opa.policy.batch-column-masking-uri`) but **none for row filters** (`evidence/00`).

**Why the batch endpoints exist at all**, from Trino's docs: generic batching covers operations that
fan out over a *list of objects* ("Trino sends one request to OPA for each object, and then creates
a filtered list of permitted objects", for catalogs, schemas, tables, columns), and batch column
masking exists because "when working with very wide tables this can result in a performance
degradation", one request per column. Row filters have neither problem: one per table, and queries
have few tables. The absence of a batch row-filter endpoint is a design consequence, not an omission.

The engine's constraint that the call happen once per query at plan time is satisfied for free: the
adapter's pushdown handler runs once per request, and folding the answer into the shard-invariant
`CommonScanSpec` means every one of the G shards receives it without re-asking. The relevant cost is
therefore not latency but a hard dependency: OPA becomes a synchronous, fail-closed prerequisite for
planning any query, alongside the catalog.

---

## 7. Verdict

**Can this ship without per-user catalog identity? Yes, and the single service account is genuinely
untouched. But not by adopting Trino's OPA contract, and the epic as framed should not proceed.**

What the evidence supports:

- The enforcement model is sound. One plan-time call, answer folded into the shard-invariant spec,
  filter placed pre-aggregation in every pushdown shape, verified equal to the single-node answer in
  all six shapes (`evidence/05` PROBE 4), and unstrippable by the user (#402). Catalog authentication
  never enters the picture: the decision is made from the Exasol user name, and the service account
  keeps doing exactly what it does now.
- The identity input is adequate and closable. User name from `ctx.current_user()`, groups from
  `SYS.EXA_DBA_ROLE_PRIVS` including the transitive closure, verified live, and an IdP bridge via
  `DISTINGUISHED_NAME`/`OPENID_SUBJECT` if attributes beyond roles are ever needed
  (`evidence/06`).
- Cost is not a factor: 0.2 ms per table.

What kills the contract, not the idea:

1. **0 of 9 filters work as delivered; 5 of 9 are sound after a rewrite the adapter must own.**
2. **Some failures are silent.** Trino's own documented mask example returns the unmasked column
   (`evidence/05` PROBE 6a). `current_date` in a filter diverges per shard, the exact defect the
   engine already fixed by withdrawing that capability (`capabilities.rs:117-132`). A filter ending
   in `--` deletes the engine's `ORDER BY` and `LIMIT` (PROBE 7).
3. **The contract cannot express "deny" or "the policy layer is down".** Unknown user, mistyped rule
   name, mistyped package, and a Rego division by zero all return HTTP 200 and mean "unrestricted"
   (`evidence/03` B, C, D, E). A policy-path typo is a silent grant of full table access. This is not
   fixable downstream: the information is not on the wire.

What to build instead, if the epic proceeds. The change is to the *contract*, and it reuses machinery
that already exists:

- **A structured predicate, not a SQL string.** OPA returns JSON; Rego can emit any shape. Route it
  through `vs-expression`, which already owns dialect rendering and already declines unknown function
  names by gate rather than by accident (`TRANSLATED_SCALAR_FNS`, `lib.rs:119-251`). Every silent
  failure in [§2](#2-can-we-translate-what-it-returns) becomes an explicit decline.
- **An affirmative envelope.** `{"decision": "allow"|"deny"|"filter", ...}`, with anything else,
  including absence, meaning deny. This is the only way to distinguish silence from permission.
- **Two injection points**, single-table (`mod.rs:360-380`) and per-leg join (`plan_join`), plus the
  `declined_filter` route and the `refused_columns` interaction.
- **Column masking scoped down or dropped.** With `crypto_expressions` off, hashing masks do not
  plan at all; with them on, the digest still would not match Trino's. `NULL` and `CASE` masks work.
  Anything richer needs the same structured treatment as filters.

A rough shape of the cost, for planning only and not a committed estimate: the OPA client, identity
resolution, and the envelope are small and well-bounded. The structured-predicate contract plus its
`vs-expression` integration is the bulk, and the join-leg injection point is the part most likely to
be underestimated. Sizing that properly is a `/speq:plan` exercise, not a spike conclusion.

**If the requirement is specifically "reuse Trino's OPA policies unchanged", the answer is no,
with evidence.** The dialect does not survive the crossing, the entitlement-table shape cannot
survive it at all, and the failure semantics are unsafe by construction.
