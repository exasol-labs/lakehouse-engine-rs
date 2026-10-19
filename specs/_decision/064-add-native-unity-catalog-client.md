# Decisions: add-native-unity-catalog-client

## ADR: Bespoke thin Unity Catalog REST client over the standard API

**ID:** bespoke-unity-catalog-rest-client
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

The engine needs a Unity Catalog client to list catalogs, schemas, and tables and to load table metadata, with PAT and OAuth machine-to-machine authentication. No mature standalone Rust client exists.

### Decision

Build a thin client in the catalog crate over the standard Unity Catalog API. It passes a PAT through as a bearer token and mints and refreshes OAuth machine-to-machine tokens itself. One client serves both OSS and Databricks-managed Unity Catalog.

### Options Considered

| Option | Verdict |
|--------|---------|
| `unitycatalog` and `unitycatalog-client` crates | Rejected: placeholder names, not real crates |
| `roeap/unitycatalog-rs` | Rejected: abandoned, folded into delta-kernel-rs |
| delta-kernel-rs Unity Catalog crates | Rejected: they target an API with no list endpoints that Databricks gates behind an allowlisted User-Agent |

### Consequences

- The client fits the existing REST-catalog shape and adds no dependency.
- Both auth modes end in a bearer header, so the only real work is the OAuth exchange and token lifecycle.

## ADR: CATALOG_KIND as a virtual-schema property that selects a client at one construction site

**ID:** catalog-kind-single-construction-site
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

Unity Catalog is a second catalog kind. A virtual schema must select its catalog without breaking existing Iceberg REST virtual schemas or making every operation re-decide the kind.

### Decision

A `CATALOG_KIND` virtual-schema property selects the catalog kind and defaults to Iceberg REST. The kind is matched exhaustively at one site, the one that constructs the catalog client. After construction, one listing pipeline serves both kinds.

### Options Considered

| Option | Verdict |
|--------|---------|
| Kind field inside the CONNECTION password JSON | Rejected: the kind is a routing decision, not a credential |
| Match the kind at every operation site | Rejected: duplicates the listing pipeline, and the two paths can diverge |

### Consequences

- Existing virtual schemas keep their behavior.
- A third kind fails the build at the construction site instead of falling through silently.

## ADR: Unity Catalog vending is a third backend-selection site

**ID:** unity-catalog-vending-third-selector
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

The storage-backend invariant allows exactly two backend-selection sites. Unity Catalog credential vending returns a response shape that neither site reads.

### Decision

Unity Catalog vending is a third backend-selection site. It reads the temporary-credentials response and selects the variant from the storage-location scheme. The invariant is revised to allow it, and the scheme-to-variant decision lives in one place shared by both vended selectors.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reshape Unity Catalog credentials into an Iceberg `LoadTableResult` | Rejected: couples Unity Catalog to an Iceberg type it does not use |

### Consequences

- The invariant stays true after revision.

## ADR: Unity Catalog auth reuses the existing CONNECTION credential fields

**ID:** unity-catalog-auth-reuses-connection-fields
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

Unity Catalog needs PAT, OAuth machine-to-machine, and unauthenticated (OSS) modes. The CONNECTION credential JSON already parses the token, client ID, client secret, OAuth server URI, and scope fields for Iceberg REST.

### Decision

Unity Catalog auth reuses the existing CONNECTION credential fields and adds none. Validation depends on the catalog kind: `warehouse` is required only for Iceberg REST and SigV4 is rejected for Unity Catalog. A Unity Catalog CONNECTION with no auth field is accepted for OSS.

### Options Considered

| Option | Verdict |
|--------|---------|
| New Unity Catalog credential fields | Rejected: the existing fields already express standard OIDC client-credentials ending in a bearer |

### Consequences

- The auth mode follows from which fields are present.
- Iceberg REST validation is unchanged.

