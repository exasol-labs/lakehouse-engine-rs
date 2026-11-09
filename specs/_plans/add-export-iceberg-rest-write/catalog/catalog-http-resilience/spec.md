# Feature: Catalog HTTP Resilience

Every HTTP request that the adapter sends to an Iceberg REST catalog, a Lakekeeper management API, a Unity Catalog, or an OAuth2 token endpoint stops waiting on a catalog that stays silent and retries a transient failure. A catalog that hangs therefore cannot block a `createVirtualSchema`, `REFRESH`, `SET PROPERTIES`, or pushdown request indefinitely. An answer that keeps arriving is read to its end, and one brief overload or outage of the catalog does not fail the request (#448).

<!-- DELTA:CHANGED -->
## Background

* A catalog request is one HTTP request with its retries, of these kinds: the Iceberg REST OAuth2 client-credentials grant, `/v1/config` lookup, namespace and table listings, and `loadTable`, SigV4-signed ones to the AWS Glue Iceberg REST endpoint included; the Lakekeeper batch-check; and the Unity Catalog OAuth machine-to-machine grant, table listings, get-table load, and temporary-credentials request; and, for the export, the Iceberg REST namespace creation, table creation, staged table creation, load with all snapshots, `loadCredentials`, and commit.
* The read timeout is 10 seconds. It is the longest an attempt waits for its answer's headers, counted from the attempt's start with its connection included, and then for each next part of the answer body.
* A transient failure is an attempt that reaches the 10-second connect timeout or the read timeout before its answer's headers arrive, a connection that is refused, reset, or aborted, an HTTP/1.1 or HTTP/2 connection that the peer closes before the answer begins, with or without a TLS `close_notify`, as a proxy does when it reloads or ends an idle keep-alive connection (#538), or an answer with HTTP status 408 (Request Timeout), 429 (Too Many Requests), 502 (Bad Gateway), 503 (Service Unavailable), or 504 (Gateway Timeout).
* A DNS resolution failure and a TLS failure, one that the TLS library reports, are not transient.
* RFC 9110 § 10.2.3: `Retry-After = HTTP-date / delay-seconds`.
* A request states whether it is safe to repeat. Every request that only reads is safe, the POST-shaped reads included (the OAuth2 grants, the batch-check, the temporary-credentials request), and so is staged table creation, which reserves nothing. Namespace creation, table creation, and a commit change catalog state and are not safe: the client sends each once, and the operation that can check its own outcome decides whether to send it again.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A request that changes catalog state is sent once after a transient failure

* *GIVEN* a catalog that answers a commit, a table creation, or a namespace creation with HTTP 504 and would answer a second attempt with a valid answer
* *WHEN* the client sends that request
* *THEN* the client SHALL send it once and return the failure, without a second attempt
* *AND* a request that is safe to repeat and gets the same answer SHALL still succeed at its second attempt
<!-- /DELTA:NEW -->
