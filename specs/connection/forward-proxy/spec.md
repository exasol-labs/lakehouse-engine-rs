# Feature: Forward Proxy

Every outbound request of `LAKEHOUSE_ADAPTER` and `LAKEHOUSE_SCAN` honors the `HTTPS_PROXY`, `HTTP_PROXY`, and `NO_PROXY` environment variables, so a deployment that reaches the catalog, the object store, or AWS only through a forward proxy works (#539).

## Background

* An operator sets the variables per script with `%env` lines in the script source. Exasol starts the UDF process with them in its environment. Each script needs its own lines, because the adapter and the scan reach different hosts.
* A request covered here is any request of a catalog client, an object-store client, the AWS Glue client, or the AWS STS client, including clients added later.
* The proxy is the only proxy mechanism. No virtual schema property or CONNECTION field names a proxy.

## Scenarios

### Scenario: Catalog, storage, and STS requests go through the proxy

* *GIVEN* `%env HTTP_PROXY` on both scripts, naming a working proxy
* *WHEN* `CREATE VIRTUAL SCHEMA`, `REFRESH`, and a `SELECT` run against Iceberg REST, Lakekeeper with OAuth2 and its permission check, Unity Catalog, direct storage, or an assumed-role CONNECTION
* *THEN* every request of the adapter and of the scan SHALL go through the proxy
* *AND* the `SELECT` SHALL return the rows it returns without a proxy, positional deletes and deletion vectors applied

### Scenario: HTTPS endpoints are reached through a CONNECT tunnel

* *GIVEN* `%env HTTPS_PROXY` on both scripts, and a CONNECTION to AWS Glue, AWS STS, S3, or ADLS over `https://`
* *WHEN* a virtual schema is created and queried
* *THEN* each request SHALL reach its host through a `CONNECT` tunnel of the proxy

### Scenario: NO_PROXY hosts bypass the proxy

* *GIVEN* a proxy variable naming a port nothing listens on, and `NO_PROXY` listing every host the CONNECTION names
* *WHEN* `CREATE VIRTUAL SCHEMA` and a `SELECT` run
* *THEN* both SHALL succeed without contacting the proxy

### Scenario: An unreachable proxy fails CREATE without leaking its credentials

* *GIVEN* `%env HTTP_PROXY=http://user:password@host:port` on the adapter, where nothing listens on the port
* *WHEN* `CREATE VIRTUAL SCHEMA` runs
* *THEN* the statement SHALL fail with an error that names the proxy as `http://host:port`
* *AND* the error MUST NOT contain the proxy's user name or password
