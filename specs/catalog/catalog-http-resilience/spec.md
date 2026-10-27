# Feature: Catalog HTTP Resilience

Every HTTP request that the adapter sends to an Iceberg REST catalog, a Lakekeeper management API, a Unity Catalog, or an OAuth2 token endpoint ends within a fixed time and retries a transient failure. A catalog that hangs therefore cannot block a `createVirtualSchema`, `REFRESH`, `SET PROPERTIES`, or pushdown request indefinitely, and one brief overload or outage of the catalog does not fail the request (#448).

## Background

* A catalog request is one HTTP request with its retries, of these kinds: the Iceberg REST OAuth2 client-credentials grant, `/v1/config` lookup, namespace and table listings, and `loadTable`, SigV4-signed ones to the AWS Glue Iceberg REST endpoint included; the Lakekeeper batch-check; and the Unity Catalog OAuth machine-to-machine grant, table listings, get-table load, and temporary-credentials request.
* A transient failure is a connection attempt that reaches the 10-second connect timeout, a connection that is refused, reset, or aborted, a TLS connection that the peer closes without sending `close_notify`, or an answer with HTTP status 408 (Request Timeout), 429 (Too Many Requests), 502 (Bad Gateway), 503 (Service Unavailable), or 504 (Gateway Timeout).
* A DNS resolution failure and a TLS failure, one that the TLS library reports, are not transient.
* RFC 9110 § 10.2.3: `Retry-After = HTTP-date / delay-seconds`.

## Scenarios

### Scenario: A catalog request that gets no complete answer fails within 30 seconds

* *GIVEN* a catalog request to a catalog that accepts the connection and never completes its answer, or to an address that never completes a connection
* *WHEN* the adapter sends the request
* *THEN* each connection attempt SHALL end after at most 10 seconds
* *AND* the request SHALL fail no later than 30 seconds after its first attempt started, its retries, its waits, and the read of the answer body included
* *AND* when the 30 seconds run out, the error SHALL state that the catalog sent no complete answer within 30 seconds, retries included
* *AND* the error MUST NOT contain the CONNECTION's `token`, `client_secret`, `access_key`, `secret_key`, or `session_token`, or the session's bearer token

### Scenario: A transient failure is retried with exponential backoff and jitter

* *GIVEN* a catalog request whose first attempts fail with a refused connection, a connection that the catalog resets after it reads the request, a connection that the catalog resets during the TLS handshake without a TLS library error, a connection that the catalog closes after it reads the request without sending a TLS `close_notify`, a connection attempt that reaches the connect timeout, or an answer with HTTP 408, 429, 502, 503, or 504 that carries no `Retry-After` header, and whose next attempt answers HTTP 200
* *WHEN* the adapter sends the request
* *THEN* the adapter SHALL send the request again after each transient failure, with at most 5 attempts in total
* *AND* the wait before retry n SHALL be at least 125 × 2^(n−1) milliseconds and at most 250 × 2^(n−1) milliseconds, chosen at random within that range
* *AND* each attempt SHALL carry the method, URL, headers, and body of the first attempt
* *AND* the request SHALL succeed with the HTTP 200 answer, as if the catalog had answered the first attempt

### Scenario: Retry-After sets the wait, and a wait past the bound ends the retries

* *GIVEN* a catalog request whose answer has a transient HTTP status and carries `Retry-After` as delay-seconds, such as `1`, or as an IMF-fixdate HTTP-date, such as `Sun, 06 Nov 1994 08:49:37 GMT`
* *WHEN* the adapter decides when to send the request again
* *THEN* the adapter SHALL wait the time the header states instead of its backoff wait, and an IMF-fixdate in the past SHALL mean no wait
* *AND* a `Retry-After` value that is neither delay-seconds nor an IMF-fixdate SHALL be ignored, and the backoff wait SHALL apply
* *AND* an HTTP-date in the obsolete RFC 850 form, such as `Sunday, 06-Nov-94 08:49:37 GMT`, or the asctime form, such as `Sun Nov  6 08:49:37 1994`, SHALL be ignored the same way, and the backoff wait SHALL apply, a scoped deviation from RFC 9110 § 5.6.7, which states that a recipient parsing a timestamp in an HTTP field "MUST accept all three HTTP-date formats"
* *AND* when a wait would end after the 30 seconds of the request, the adapter MUST NOT wait and SHALL fail the request at once with the answer it holds

### Scenario: A failure that is not transient fails the request at the first attempt

* *GIVEN* a catalog request whose first attempt answers HTTP 400, 401, 403, 404, 409, 419, or 500, or fails with a DNS resolution failure, such as for a host name under `.invalid`, with a TLS failure, such as an `https://` request to a server that answers in plain HTTP, with a connection that the catalog closes before the answer's status line by a plain TCP close or after a TLS `close_notify`, or with another transport error that is not transient
* *WHEN* the adapter sends the request
* *THEN* the adapter SHALL make exactly one attempt
* *AND* the adapter SHALL hand that answer or error to the caller's own handling unchanged, so the caller decides, for example, that an HTTP 404 is a skipped table or an error
* *AND* a DNS or TLS failure SHALL keep the error text of a single attempt, including the `dns error` cause or the statement that the server did not answer with TLS

### Scenario: Retries that run out fail with the last failure

* *GIVEN* a catalog request whose every attempt answers HTTP 503 with a body that echoes a CONNECTION credential, or fails with a refused connection
* *WHEN* the adapter sends the request
* *THEN* the adapter SHALL make exactly 5 attempts
* *AND* the request SHALL then fail with the error that a single answer or connection failure of the same kind produces: the HTTP status and the redacted body of the last answer, or every cause that the HTTP client reports for the last connection failure
* *AND* the error MUST NOT contain the echoed credential

### Scenario: Every catalog request kind is bounded and retried

* *GIVEN* an Iceberg REST CONNECTION in OAuth2 mode, an AWS Glue Iceberg REST CONNECTION with `use_sigv4`, a virtual schema with `PERMISSION_CHECK = 'LAKEKEEPER'`, and a Unity Catalog CONNECTION in OAuth machine-to-machine mode
* *WHEN* each catalog request kind of the Background answers its first attempt with HTTP 503 and its second attempt with a valid answer
* *THEN* each request SHALL succeed at its second attempt, so the enumeration, the pushdown's table loads, the batch-check, and the temporary-credentials request each complete
* *AND* a re-sent SigV4-signed request SHALL carry the `Authorization` and `x-amz-date` headers of its first attempt, because a SigV4 signature stays valid for 5 minutes
* *AND* a re-sent OAuth2 grant SHALL carry the same form body as its first attempt

### Scenario: A failed loadTable names the table it loads

* *GIVEN* a `loadTable` GET on a `CatalogSession` for the table `orders` in the namespace `sales.eu`, issued either by the enumeration of a `createVirtualSchema`, `REFRESH`, or `SET PROPERTIES` request or by a pushdown's table resolution
* *WHEN* the GET fails with an HTTP status other than 404, with a transport error, with the 30-second bound of this feature running out, or with an answer that does not parse as a `loadTable` result
* *THEN* the returned error SHALL name the table by its dotted catalog identifier `sales.eu.orders`, the form its identifier note records
* *AND* the error SHALL carry the cause: the HTTP status and the redacted answer body, every cause the HTTP client reports, the statement of the 30-second bound, or the parse failure
* *AND* a pushdown's `loadTable` that answers HTTP 404 SHALL fail with an error that names the table in the same way
* *AND* naming the table MUST NOT change which enumeration loads are skipped, so an enumeration `loadTable` that answers HTTP 404 SHALL still skip the table with the `NotLoadableIcebergTable` reason
