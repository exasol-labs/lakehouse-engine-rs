# Feature: Format-Neutral Pushdown Resolution

Resolves every pushdown request's tables through one per-request session and one format-neutral seam. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta amends three scenarios and adds one, and is issue #407. It adds the third catalog kind to the per-request resolver. It also makes a request's leg resolutions concurrent.
* Under the direct-storage kind the per-request session is an object store rather than a catalog client, so the one-session-per-request rule holds with a store in that role.
* The concurrency change is shared code, so the Iceberg and Delta join paths gain the same latency reduction. Leg-index order is the whole contract: every later step indexes a side by its leg rather than by its name.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One catalog session per request serves every table the request resolves

* *GIVEN* a broadcast-join pushdown request whose two legs name two different tables in one virtual schema, under any catalog kind
* *WHEN* the adapter resolves both legs
* *THEN* the adapter SHALL build the request's catalog session EXACTLY ONCE and SHALL resolve both legs through it, so the request performs no more catalog authentication round-trips than a single-table request over the same virtual schema
* *AND* the adapter MUST NOT build a second session per leg, per shape, or per format reader
* *AND* the number of catalog round-trips an Iceberg request performs SHALL be unchanged from before this feature
* *AND* under a catalog kind whose session is an OBJECT STORE rather than a catalog client, the adapter SHALL build that store EXACTLY ONCE per request and SHALL share it across every leg, so a two-table join opens one store and its admission limiter bounds the whole request rather than each leg
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The catalog kind is matched at one added construction site and nowhere else

* *GIVEN* the resolved catalog kind and the recorded rule that its variant names appear in no production module beyond the enum's own declaration, its resolver, the catalog-client construction site, credential validation, and the pushdown scan-source construction site
* *WHEN* the pushdown path builds the per-request scan-source resolver
* *THEN* the adapter SHALL match the catalog kind EXHAUSTIVELY at exactly ONE site in the pushdown path, which yields the per-request resolver, so a FOURTH catalog kind is a compile error there rather than a silent fall-through
* *AND* that site SHALL REPLACE the pushdown refusal in the recorded list of production sites permitted to name a catalog-kind variant, so the permitted-site count is unchanged and no per-request-shape fork is introduced
* *AND* that list SHALL be held by EXHAUSTIVE matching plus review rather than by a source-level probe, SUPERSEDING the recorded clause that the probe asserting it SHALL be updated to name this site instead of the refusal: no such probe was ever built, and `vs-adapter/catalog-kind-selection` records why this plan declines to build one
* *AND* the private already-resolved-session value the resolver carries SHALL gain one variant per catalog kind and SHALL be matched exhaustively wherever it is read, and it MUST NOT name a catalog-kind variant, so it adds no site to the permitted list
* *AND* the format-reader selection site MUST NOT match the catalog kind, because it matches the scan source
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Capability advertisement stays blind to the catalog kind

* *GIVEN* three virtual schemas over the same adapter, one created under each catalog kind
* *WHEN* Exasol requests the adapter's capabilities for each
* *THEN* the adapter SHALL return the SAME capability set for all three, and MUST NOT read, branch on, or receive the catalog kind while assembling it
* *AND* a capability the adapter advertises SHALL therefore be one it satisfies under EVERY kind, because Exasol re-applies nothing it delegated and a kind-conditional capability would return wrong rows rather than a deferred check
* *AND* the compile-time probe pinning that the capability set is assembled without the catalog kind SHALL stay intact and unweakened
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A request's table resolutions run concurrently rather than one after another

* *GIVEN* a broadcast-join pushdown request whose legs each resolve one table through the format-reader seam, resolved today by a loop that awaits each leg before starting the next
* *WHEN* the adapter resolves the request's legs
* *THEN* the adapter SHALL drive every leg's resolution CONCURRENTLY over the request's ONE shared session, so a two-leg join's plan-time latency approaches its slowest leg rather than the sum of its legs
* *AND* the resolved sides SHALL be returned in LEG-INDEX order, unchanged, because a self-join's two occurrences share a table name and every later step indexes a side by its leg rather than by its name
* *AND* the concurrency SHALL apply to EVERY catalog kind and EVERY table format, because the loop is shared code, so the Iceberg and Delta join paths gain the same reduction
* *AND* the generated SQL and the serialized per-shard scan specs for every join request MUST be BYTE-IDENTICAL to their pre-change output, so the committed join golden strings pass unedited
* *AND* the error a failing leg produces SHALL be the same error that leg produces today, and a request with more than one failing leg SHALL surface one of those errors rather than a combined or masked one
* *AND* the per-request in-flight object-store bound SHALL be unchanged by the concurrency, because the request's ONE admission-limited store already bounds every read its legs issue
<!-- /DELTA:NEW -->
