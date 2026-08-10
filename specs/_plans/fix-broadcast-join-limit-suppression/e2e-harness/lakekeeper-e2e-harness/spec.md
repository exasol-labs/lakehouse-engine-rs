# Feature: Lakekeeper E2E Harness (OIDC + MinIO)

End-to-end test suite that verifies the lakehouse VS query path against a
Lakekeeper Iceberg REST catalog — the open-source, OpenID-secured, multi-warehouse
Rust catalog — backed by MinIO object storage, proving real interoperability rather
than a connectivity smoke test. The suite authenticates to the catalog with the
engine's existing OAuth2 client-credentials CONNECTION fields (an external Keycloak
IdP issues the token), resolves tables through Lakekeeper's per-warehouse
`overrides.prefix`, and reads data files under both static S3 credentials and
Lakekeeper's default vended (STS) credentials. It is additive: the existing
unauthenticated `exasol-e2e` baseline is unchanged, and this suite runs behind its
own `lakekeeper-e2e` cargo feature.

## Background

<!-- DELTA:CHANGED -->
* This bullet SUPERSEDES the preceding Background bullet "**The shared test harness declares no row cap by default, and this suite's connection must stay uncapped — not because a cap is inert, but because it is not.** The shared WebSocket test client (`crates/lakehouse-engine/tests/common/exasol_ws.rs`) sends Exasol's own documented default — `0`, no limit — unless a call site declares a cap through `ExaConn::capped_result_sets(n)` …". **The shared test harness declares no row cap by default, and this suite's connection must stay uncapped — not because a cap is inert, but because it is not.** The shared WebSocket test client (`crates/lakehouse-engine/tests/common/exasol_ws.rs`) sends Exasol's own documented default — `0`, no limit — unless a call site declares a cap through `ExaConn::capped_result_sets(n)`. A declared `resultSetMaxRows` cap DOES reach the adapter as a pushdown `limit` on a real query execution, confirmed by directly capturing the adapter's incoming request (bypassing `EXPLAIN VIRTUAL`, which is a separate exchange that cannot observe this) across all seven statement shapes measured, including the broadcast-eligible inner equi-join. Since issue #307 a pushed `limit` no longer disqualifies broadcast for a join: a bare `LIMIT` and a bare-projected-column `ORDER BY` both stay on the broadcast path, and only the four surviving forcing conditions (an aggregate select item, a non-empty `GROUP BY`, `aggregationType = "group_by"`, or a non-null `HAVING`), a `limit` offset with no `orderBy`, and an unrenderable or unprojected sort key fall back to the unaccelerated two-scan (`LHS_T0`/`LHS_T1`) wrapper (`vs-adapter/pushdown-planning-join`). This suite's own connection never calls `capped_result_sets`, so its broadcast join test is unaffected in practice — but that is because the connection stays uncapped by choice, not because the mechanism doesn't exist. See `docs/debugging-pushdown.md`'s measured shape matrix for the full comparison, including the broadcast-join row. Verifying the broadcast path at row-fetch time and not only at `EXPLAIN VIRTUAL` time is still valuable as a genuine end-to-end check — it confirms the joined rows actually come back through the broadcast plan, not merely that the plan was selected.
<!-- /DELTA:CHANGED -->

## Scenarios

The scenario below is reproduced verbatim and UNMARKED as required structural context — this delta changes only the Background bullet above and no scenario, so no `DELTA:*` marker applies and `/speq:record` leaves this scenario untouched. It is the broadcast-join test the Background bullet refers to, which stays broadcast on the harness's uncapped default.

### Scenario: A two-table broadcast join over a vended-credential warehouse returns correct rows

* *GIVEN* the `sts-enabled` Lakekeeper warehouse seeded with BOTH star-schema tables through the OIDC-secured catalog, and one virtual schema over that warehouse's namespace whose CONNECTION supplies OAuth2 catalog auth, sets `use_vended_credentials` true, and supplies NO static S3 storage field
* *AND* a harness connection that declares no row cap, which is the harness default and therefore requires no opt-out call at the call site
* *WHEN* a user runs an inner equi-join of the two tables through that one virtual schema
* *THEN* the adapter SHALL plan a broadcast fan-out, so the pushed SQL carries the compact scan-spec join block and NOT the two-scan `LHS_T0` / `LHS_T1` unaccelerated wrapper
* *AND* that broadcast fan-out SHALL hold when the joined rows are fetched, not only when the plan is inspected through `EXPLAIN VIRTUAL`, because row-fetch-time verification is the only check that confirms the broadcast plan was actually executed rather than merely selected
* *AND* the adapter SHALL resolve a vended credential for EACH table independently, sending `X-Iceberg-Access-Delegation: vended-credentials` on each side's `loadTable` request
* *AND* the emitted scan spec SHALL carry the fact side's vended backend as its whole-spec `storage` value and the dimension side's vended backend inside the join block, so neither side's credential is discarded
* *AND* the joined rows SHALL equal the join computed independently from the two tables read un-joined through the same virtual schema, because a one-warehouse fixture has no second warehouse to cross-check against
* *AND* no vended credential value SHALL appear in any returned SQL string surfaced by the test or in any test output
* *AND* the test MUST fail (not skip) when the Docker stack is unavailable
