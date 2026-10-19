# Decisions: refactor-storage-backend-enum

## ADR: Externally-tagged lowercase serde representation for the storage backend

**ID:** storage-backend-externally-tagged-lowercase-serde
**Plan:** `refactor-storage-backend-enum`
**Status:** Accepted

### Context

The storage backend travels in the scan spec and is read only by the same deploy's `.so`, so no cross-version compatibility is needed. An Azure variant is coming, so the encoding must tell variants apart unambiguously.

### Decision

The storage backend serializes externally tagged with a lowercase key, for example `{"s3":{...}}`, with the payload unchanged. The tag lands while only one variant exists.

### Options Considered

| Option | Verdict |
|--------|---------|
| `#[serde(untagged)]` | Rejected: picks the variant by trial deserialization, so the credentials used depend on which shape parses first and a field error becomes a generic mismatch |
| Internally tagged | Rejected: same change as external tagging with no gain |

### Consequences

- Landing the tag while one variant exists makes every byte outside the tag proof that nothing else changed.

