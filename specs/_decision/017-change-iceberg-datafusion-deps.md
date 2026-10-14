# Decisions: change-iceberg-datafusion-deps

## ADR: Pin iceberg 0.10.0 via crates.io registry version, not git tag

**ID:** pin-iceberg-0-10-0-via-crates-io-registry-version-not-git-tag
**Plan:** change-iceberg-datafusion-deps
**Status:** Accepted
**Supersedes:** pin-iceberg-0-10-0-rc-2-via-git-tag-not-a-crates-io-exact-version-pin

### Context

The iceberg crates were pinned to a git tag because crates.io had no matching release. The final release is now on crates.io.

### Decision

The workspace uses the crates.io registry release for the iceberg crates, with a caret range, not a git tag.

### Options Considered

| Option | Verdict |
|--------|---------|
| Git source at the final tag | Rejected: a git source is unwarranted once the release is on crates.io |
| Exact pin | Rejected: house style uses caret ranges for released crates |

### Consequences

Version bumps remain explicit, reviewed edits.
