# Decisions: add-native-unity-catalog-client

## ADR: Bespoke thin Unity Catalog REST client over the standard API

**ID:** bespoke-unity-catalog-rest-client
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

The engine needs a Unity Catalog client to list catalogs, schemas, and tables and to load table metadata, with PAT and Databricks OAuth machine-to-machine authentication. The catalog crate already carries an HTTP client and serde for the Iceberg REST path, and no mature standalone Rust Unity Catalog client exists.

### Decision

Build a thin Unity Catalog client in the catalog crate over the standard Unity Catalog API. The client passes a PAT through as a bearer token and mints and refreshes OAuth machine-to-machine tokens itself. One client serves both OSS and Databricks-managed Unity Catalog.

### Options Considered

| Option | Verdict |
|--------|---------|
| `unitycatalog` and `unitycatalog-client` crates | Rejected: reserved placeholders, not real crates |
| `roeap/unitycatalog-rs` | Rejected: dead, folded into delta-kernel-rs |
| delta-kernel-rs Unity Catalog crates | Rejected: they target the `delta/v1` API, which has no list endpoints and is gated on Databricks behind an allowlisted User-Agent |

### Consequences

The client fits the existing REST-catalog shape and adds no dependency. Both auth modes end in a bearer header, so the only real work is the OAuth exchange and lifecycle. The delta-kernel crates stay on a watch-list only for the coordinated-commits risk.

## ADR: CATALOG_KIND as a virtual-schema property that selects a client at one construction site

**ID:** catalog-kind-single-construction-site
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

Unity Catalog is a second catalog kind. A virtual schema needs a way to select its catalog without breaking existing Iceberg REST virtual schemas or making every operation re-decide the kind.

### Decision

A `CATALOG_KIND` virtual-schema property selects the catalog kind, defaulting to Iceberg REST. The kind is read from the properties, not from the CONNECTION credentials, and is matched exhaustively at exactly one site: the one that constructs the catalog client. After construction, createVirtualSchema runs one listing pipeline for both kinds.

### Options Considered

| Option | Verdict |
|--------|---------|
| Kind field inside the CONNECTION password JSON | Rejected: the kind is a routing decision, not a credential |
| Match the kind at every operation site | Rejected: duplicates the listing pipeline, and the two paths can diverge |

### Consequences

Existing virtual schemas keep their behavior with no configuration change. A third kind is a build failure at the one construction site, not a silent fall-through.

## ADR: Unity Catalog vending is a third backend-selection site

**ID:** unity-catalog-vending-third-selector
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

The Storage Backend Enum invariant allows exactly two backend-selection sites, which read disjoint inputs. Unity Catalog credential vending returns a response shape that neither site's input covers.

### Decision

Unity Catalog vending is a third backend-selection site. It reads the Unity Catalog temporary-credentials response and selects the variant from the storage-location scheme. The "exactly two sites" and "no third selector" clauses of the invariant are superseded, and the scheme-to-variant decision lives in one place shared by both vended selectors.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reshape Unity Catalog credentials into an Iceberg `LoadTableResult` | Rejected: couples Unity Catalog to a provider type it does not use |

### Consequences

Scan-path wiring of the third selector is deferred to #319/#320. The invariant is revised so it stays true.

## ADR: Unity Catalog auth reuses the existing CONNECTION credential fields

**ID:** unity-catalog-auth-reuses-connection-fields
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

Unity Catalog needs PAT, OAuth machine-to-machine, and unauthenticated (OSS) modes. The CONNECTION password JSON already parses the token, client ID, client secret, OAuth server URI, and scope fields for Iceberg REST.

### Decision

Unity Catalog auth reuses the existing CONNECTION credential fields and adds none. Validation depends on the catalog kind: `warehouse` is required only for Iceberg REST, SigV4 is rejected for Unity Catalog, and other Iceberg rules are unchanged. A Unity Catalog CONNECTION with no auth field is accepted for OSS.

### Options Considered

| Option | Verdict |
|--------|---------|
| New Unity Catalog credential fields | Rejected: the existing fields already express standard OIDC client-credentials ending in a bearer |

### Consequences

The auth mode is selected from which fields are present. Iceberg REST acceptance and error text are unchanged.

