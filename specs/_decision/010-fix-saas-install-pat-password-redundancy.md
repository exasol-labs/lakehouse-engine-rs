# Decisions: fix-saas-install-pat-password-redundancy

## ADR: Derive the SaaS REST PAT from the resolved connectivity credential; drop `--pat`/`EXASOL_PAT`

**ID:** derive-pat-from-connectivity-credential-not-separate-flag
**Plan:** `fix-saas-install-pat-password-redundancy`
**Status:** Accepted

### Context

`specs/_recorded/005-add-saas-install-script`'s Consequences table recorded a deliberate choice:
require the SaaS PAT via a separate `--pat`/`EXASOL_PAT` input, rejecting "derive PAT from
`~/.exapump/config.toml` profile" with the rationale "REST and SQL are distinct auth surfaces;
explicit input avoids brittle config parsing." Hands-on testing of the shipped, not-yet-merged
installer (PR #141) against a live SaaS database found the premise doesn't hold for this
installer's scope: it only ever targets Exasol SaaS, and on SaaS the PAT *is* the SQL password —
there is exactly one secret, not two auth surfaces. Every connectivity mode (`--profile`, `--dsn`,
`--host`/`--user`/`--password`) therefore already carries the exact value `--pat` was asking the
user to type again.

### Decision

Remove `--pat`/`EXASOL_PAT`. Add `resolve_pat()`, run once connectivity mode is validated, that
derives `RESOLVED_PAT` from whichever mode was chosen: `--password` directly (host mode), the
DSN's password segment percent-decoded (dsn mode), or the named profile's `password` key read
straight out of `exapump`'s own config file at `${EXAPUMP_CONFIG:-$HOME/.exapump/config.toml}`
(profile mode — confirmed via `strings $(command -v exapump)` that `EXAPUMP_CONFIG` is the exact
override `exapump` itself honors, and via `exapump profile show` that it masks the password so
can't be used to retrieve it; the installer parses the TOML section directly instead, with the
same bounded bash-regex scan style already used for JSON field extraction).

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive `RESOLVED_PAT` per connectivity mode; parse `exapump`'s own config file for profile mode | Chosen — the PAT and the SaaS SQL password are the identical secret for this installer's SaaS-only scope; asking for both is pure duplication |
| Keep `--pat`/`EXASOL_PAT` as a separate required input (the original 005 ADR) | Superseded — its stated rationale ("distinct auth surfaces") is false for every connectivity mode this installer supports |
| Add a `jq`/toml-parsing library dependency to read the profile | Rejected — the installer's stated prerequisite list is bash+curl+exapump only; a single bounded-regex scan (mirroring the existing JSON extractor) needs no new dependency |
| Fall back silently through DSN → profile → empty when derivation fails | Rejected — masks a genuine misconfiguration (e.g. a DSN with no password) as a confusing downstream 401/403 from the SaaS REST API instead of a clear installer-side error |

### Consequences

The installer now needs exactly one credential per connectivity mode instead of two. Profile-mode
users get one fewer flag to pass and one fewer place to leak the same secret. The trade-off is a
new coupling to `exapump`'s config-file location and TOML shape (`${EXAPUMP_CONFIG:-$HOME/.exapump/config.toml}`,
flat `[profile]` sections, a `password` key) — if `exapump` ever changes that location or format,
`resolve_pat()`'s profile branch must change with it. This supersedes the specific Consequences-table
row in `specs/_recorded/005-add-saas-install-script/decision-log.md` (and the promoted
`specs/_decision/008-add-saas-install-script.md`) that chose the separate `--pat` input over
config-file derivation.
