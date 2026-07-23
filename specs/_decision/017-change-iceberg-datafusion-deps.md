# Decisions: change-iceberg-datafusion-deps

## ADR: Pin iceberg 0.10.0 via crates.io registry version, not git tag

**ID:** pin-iceberg-0-10-0-via-crates-io-registry-version-not-git-tag
**Plan:** change-iceberg-datafusion-deps
**Status:** Accepted
**Supersedes:** pin-iceberg-0-10-0-rc-2-via-git-tag-not-a-crates-io-exact-version-pin

### Context

The workspace pinned `iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal` to the git tag
`v0.10.0-rc.2` because crates.io published no version past 0.9.1 at plan time. crates.io now
publishes the final `0.10.0` release, so the prior ADR's Context premise — the version is
unpublished — no longer holds.

### Decision

Replace the git-tag dependency (`{ git = "…", tag = "v0.10.0-rc.2" }`) with the registry version
`"0.10.0"` (`^0.10.0`) for all three iceberg crates. Retain
`iceberg-storage-opendal`'s `default-features = false, features = ["opendal-s3"]`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Registry version `"0.10.0"` (`^0.10.0`) | ✓ Chosen — the release is on crates.io; matches house style for other released crates (`arrow = "58"`, `roaring = "0.11"`) |
| Keep the git source pinned at the final `v0.10.0` tag | ✗ Rejected — a git source is no longer warranted once the release is on crates.io |
| Exact-pin `"=0.10.0"` | ✗ Rejected — deviates from house style, which uses caret ranges for released crates |

### Consequences

Dependency resolution becomes reproducible and cache-friendly through the crates.io registry
instead of a git checkout. A future GA/RC bump remains an explicit, reviewed edit, consistent with
the intent of the superseded ADR — it changes only the pin mechanism (registry vs. git), not the
review discipline around bumping the iceberg version.
