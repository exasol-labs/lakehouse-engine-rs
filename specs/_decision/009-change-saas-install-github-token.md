# Decisions: change-saas-install-github-token

## ADR: Replace `gh`-CLI distribution with `GITHUB_TOKEN` + plain `curl`

**ID:** replace-gh-cli-with-github-token-curl
**Plan:** `change-saas-install-github-token`
**Status:** Accepted

### Context

The recorded plan `add-saas-install-script` (`specs/_recorded/005-add-saas-install-script`)
fetched the installer and downloaded release assets through the pre-authenticated `gh` CLI,
because the lakehouse-engine-rs repository is private. That choice required every user, CI
runner, and agent to install `gh` and run `gh auth login` before the one-liner worked. Its
Consequences table recorded "Require `gh` for private-repo access" as the chosen row and
"`GITHUB_TOKEN` + plain `curl`" as the rejected alternative; the choice was never promoted to
a standalone ADR. The new target user assumes repository access is already provisioned and
expects a standard `curl | bash` install, not a second authenticated CLI.

### Decision

Replace every `gh` invocation — script fetch, `releases/latest` resolution, and release asset
download — and the `gh auth status` preflight with `curl` against the documented GitHub REST
API, authenticated by a `GITHUB_TOKEN`/`--github-token` bearer token. Drop `gh` as a
prerequisite everywhere; send the same token header to both the private engine repo and the
public SLC repo, keeping exactly one authenticated code path.

### Options Considered

| Option | Verdict |
|--------|---------|
| `GITHUB_TOKEN` + plain `curl` for all GitHub access | Chosen — a provisioned-token user expects a portable `curl \| bash` one-liner; one code path avoids dual-path drift |
| Keep `gh` (the prior recorded choice) | Superseded — forces every user and CI runner to install and authenticate a second CLI |
| Keep `gh`, add `GITHUB_TOKEN`/`curl` as an optional fallback | Rejected — a dual path doubles the test surface and drifts over time |

### Consequences

Every GitHub access — script fetch, version resolution, asset download — now depends on a
supplied `GITHUB_TOKEN`/`--github-token` instead of a pre-authenticated `gh` session. The
installer's prerequisite list drops to `exapump` and `curl`. A no-jq bash-regex helper resolves
release-asset numeric ids by name, and asset downloads follow GitHub's redirect to signed
storage with `curl -L` while never forwarding the `Authorization` header past that redirect.
</content>
