# Feature: Lakekeeper E2E Harness (OIDC + MinIO)

End-to-end test suite that verifies the lakehouse VS query path against a Lakekeeper Iceberg
REST catalog — the open-source, OpenID-secured, multi-warehouse Rust catalog — backed by MinIO
object storage, proving real interoperability rather than a connectivity smoke test.

## Background

<!-- DELTA:CHANGED -->
* **The shared test harness declares no row cap by default, so this suite's broadcast join needs no opt-out call.** The shared WebSocket test client (`crates/lakehouse-engine/tests/common/exasol_ws.rs`) sends Exasol's own documented default — `0`, no limit — unless a call site declares a cap through `ExaConn::capped_result_sets(n)`. That structural fact is why this scenario's `.unbounded_result_sets()` call was removable — not any effect a cap has on broadcast-join eligibility. A live capped-versus-uncapped capture across seven statement shapes, including the broadcast-eligible inner equi-join, found the `pushdownRequest`, the full adapter exchange, and the generated scan SQL byte-identical between the capped and uncapped capture of every shape: a declared `resultSetMaxRows` cap was never shown to reach the adapter as a pushdown `limit` or to suppress broadcast eligibility on this Exasol version (`docker.io/exasol/docker-db:2025.2.1`) — it only truncates the delivered result set at the statement level. See `docs/debugging-pushdown.md`'s measured shape matrix ("Declared row cap versus pushdown `limit` (measured)") for the full comparison, including the broadcast-join row. Verifying the broadcast path at row-fetch time and not only at `EXPLAIN VIRTUAL` time is still valuable as a genuine end-to-end check — it confirms the joined rows actually come back through the broadcast plan, not merely that the plan was selected — independent of any cap mechanism.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->
