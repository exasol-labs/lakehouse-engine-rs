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

## ADR: The exhaustive variant-naming owner list is capped at five permitted modules

**ID:** storage-backend-exhaustive-variant-naming-owners
**Plan:** `refactor-storage-backend-enum`
**Status:** Accepted

### Context

The original clause of `vs-adapter/storage-backend-enum` permitted only the enum's methods and the registration function to match a variant, which the plan's own tasks violated, and it named no single owner for backend selection.

### Decision

Five production owners may name a variant: the enum's methods, the engine's registration function, `resolve_vended_storage`'s S3 arm (credential overlay only, never changing the variant), `storage_block` (construction at CONNECTION parsing), and `CommonScanSpec`'s manual `impl Default` (construction, not selection). Any `#[cfg(test)]` module is also permitted. `storage_block` is the only place a backend is selected from input.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the original two-owner clause | Rejected: unsatisfiable against the plan's own design |

### Consequences

A fixed `S3` placeholder construction is not selection from input and does not count against the sole-selection clause. The test carve-out covers any `#[cfg(test)]` module, not only each crate's support module.
