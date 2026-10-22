# Decisions: change-iceberg-datafusion-deps

## ADR: Pin iceberg via the crates.io registry release, not a git tag

**ID:** pin-iceberg-0-10-0-via-crates-io-registry-version-not-git-tag
**Plan:** change-iceberg-datafusion-deps
**Status:** Accepted

### Context

A released iceberg version is on crates.io, so a git pin is no longer needed.

### Decision

The workspace uses the crates.io registry release for the iceberg crates, with a caret range, not a git tag.

### Options Considered

| Option | Verdict |
|--------|---------|
| Git source at the final tag | Rejected: a git source is unwarranted once the release is on crates.io |
| Exact pin | Rejected: house style uses caret ranges for released crates |

### Consequences

- Version bumps are explicit, reviewed edits.
