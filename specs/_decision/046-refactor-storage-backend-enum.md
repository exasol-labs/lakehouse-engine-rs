# Decisions: refactor-storage-backend-enum

## ADR: Externally-tagged lowercase serde representation for the storage backend

**ID:** storage-backend-externally-tagged-lowercase-serde
**Plan:** `refactor-storage-backend-enum`
**Status:** Accepted

### Context

`StorageBackend` travels in the scan spec's shard-invariant blob and is consumed only by the same deploy's `.so`, so there is no cross-version wire-compatibility requirement. Slice C of issue #274 adds an Azure variant, so the encoding must discriminate variants unambiguously.

### Decision

`StorageBackend` serializes externally tagged with a lowercase key, giving `{"s3":{...}}` with the payload bytes unchanged. The tag lands in this slice, not in slice C.

### Options Considered

| Option | Verdict |
|--------|---------|
| `#[serde(untagged)]` | Rejected: picks the variant by trial deserialization, so which credentials are used depends on which shape parses first, and a real field error becomes "data did not match any variant" |
| Internally tagged | Rejected: same churn as external tagging with no gain |

### Consequences

Five features needed a narrow `storage`-value carve-out in their byte-identical-output gates. Landing the tag while one variant exists makes every byte outside the tag proof that nothing else moved.

