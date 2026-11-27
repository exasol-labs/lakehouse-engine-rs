# Feature: DNS Cache

Every outbound client of `LAKEHOUSE_ADAPTER`, `LAKEHOUSE_SCAN`, and `LAKEHOUSE_EXPORT` resolves a host name through one DNS cache per UDF VM process, which looks names up through the node's system resolver. Every invocation the VM process runs shares the cache, so a burst of connections to one host costs at most one lookup per VM within the cache's lifetime of an answer (#693, #701).

## Background

* A client covered here is any catalog client, Iceberg metadata reader, table-file reader or writer, object-store client, the AWS Glue client, or the AWS STS client, including clients added later.
* A lookup goes through the node's system resolver, so `/etc/nsswitch.conf`, `/etc/hosts`, and `/etc/resolv.conf` apply as for any other process on the node.
* The cache keeps an answer for 30 seconds and a failed lookup for 10 seconds. A client receives a host's addresses in random order, so its connections spread across them.
* No virtual schema property or CONNECTION field configures the cache.

## Scenarios

### Scenario: A host name the node's resolver knows resolves on every network path

* *GIVEN* a CONNECTION whose catalog, AWS STS, and object-store addresses use host names that only the node's own resolver configuration resolves
* *WHEN* `CREATE VIRTUAL SCHEMA` and a `SELECT` run
* *THEN* both SHALL succeed
* *AND* the `SELECT` SHALL return the table's rows

### Scenario: Concurrent and repeated requests for one host share one lookup

* *GIVEN* a UDF VM process whose cache holds no answer for a host
* *WHEN* several clients of the process connect to that host at the same time, and again within 30 seconds
* *THEN* the process SHALL look the host up once
* *AND* every connection SHALL receive that lookup's addresses

### Scenario: A failed lookup is cached and names the host

* *GIVEN* a host name the node's resolver cannot resolve
* *WHEN* a client connects to it twice within 10 seconds
* *THEN* the process SHALL look the host up once
* *AND* each connection SHALL fail with an error that names the host
