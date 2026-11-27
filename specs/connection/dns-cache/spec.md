# Feature: DNS Cache

Every outbound client of `LAKEHOUSE_ADAPTER`, `LAKEHOUSE_SCAN`, and `LAKEHOUSE_EXPORT` resolves a host name through a DNS cache that keeps each answer for its DNS TTL. A connection to a host whose answer is cached sends the node's DNS server no lookup (#693).

## Background

* A client covered here is any catalog client, table-file reader or writer, object-store client, the AWS Glue client, or the AWS STS client, including clients added later.
* A lookup reads the node's `/etc/hosts` first, then asks the DNS servers of the node's `/etc/resolv.conf`. It reads no other name service. When `/etc/resolv.conf` names no DNS server, the catalog clients and the table-file readers and writers send their lookups to Google's public DNS servers, and the other clients resolve only the names of `/etc/hosts`.
* The cache lives as long as its client. An object-store client receives a host's addresses in random order, so its connections spread across them.
* No virtual schema property or CONNECTION field configures the cache.

## Scenarios

### Scenario: A host name of the node's hosts file resolves on every network path

* *GIVEN* a CONNECTION whose catalog, AWS STS, and object-store addresses use host names that only the node's `/etc/hosts` lists
* *WHEN* `CREATE VIRTUAL SCHEMA` and a `SELECT` run
* *THEN* both SHALL succeed
* *AND* the `SELECT` SHALL return the table's rows
