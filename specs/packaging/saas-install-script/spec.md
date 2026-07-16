# Feature: SaaS One-Command Install Script — Preflight, Connectivity, and Targeting

A single Bash installer provisions lakehouse-engine onto an Exasol SaaS database with one
command. This feature covers the installer's front door: prerequisite and authentication
checks, required-input validation, SaaS REST target selection, and artifact-version
resolution — everything that runs before any upload or DDL step. See
`packaging/saas-install-script-slc-registration` for the RUST `SCRIPT_LANGUAGES`
read-modify-write, and `packaging/saas-install-script-deploy` for artifact upload, script
DDL, the fingerprint smoke test, and the stdin-piped invocation contract.

## Background

* The script is a single POSIX-compatible Bash file targeting Bash 3.2+ (stock macOS)
  and stock Linux; it uses only `bash`, `curl`, and `exapump` — no assumed package
  manager, no `jq`, and no `gh` CLI.
* `exapump` and `curl` are stated prerequisites, not bootstrapped by the script; a missing
  prerequisite is a fail-fast error, never a silent skip.
* The lakehouse-engine-rs repository is private (INTERNAL); every access to its release
  assets or contents goes through the GitHub REST API over `curl`, authenticated with a
  GitHub token supplied as `GITHUB_TOKEN` (or `--github-token`) in an `Authorization` bearer
  header. The language-container-rs repository is public; the installer sends the same token
  header to it (harmless for a public repo), so there is exactly one authenticated code path
  and no dependency on the `gh` CLI.
* SaaS control-plane REST calls authenticate with a SaaS personal access token (PAT)
  passed as `EXASOL_PAT` (or `--pat`); `--account-id` and `--database-id` are required
  inputs because the SaaS API exposes no route to discover them.
* SQL runs through `exapump sql` in exactly one connectivity mode: a named profile
  (`--profile`) or a direct connection (`--dsn`/`EXAPUMP_DSN`, or `--host`/`--user`/`--password`
  assembled into a DSN). The two modes are a validated either/or.
* The SaaS REST base defaults to `https://cloud.exasol.com`; `--staging` selects
  `https://cloud-staging.exasol.com`. These are the only two supported targets; the installer
  accepts no arbitrary base-URL override.

## Scenarios

### Scenario: Missing prerequisite fails fast with a remediation pointer

* *GIVEN* `exapump` or `curl` is absent from `PATH`
* *WHEN* the script runs
* *THEN* the script MUST exit non-zero before making any network call or SQL statement
* *AND* the script MUST print which tool is missing and the URL of its install instructions
* *AND* the script MUST NOT attempt to install the missing tool itself

### Scenario: Missing GitHub token fails fast

* *GIVEN* neither the `GITHUB_TOKEN` environment variable nor the `--github-token` flag supplies a non-empty token
* *WHEN* the script runs
* *THEN* the script MUST exit non-zero before making any GitHub REST call or downloading any release asset
* *AND* the error message MUST name both `GITHUB_TOKEN` and `--github-token`, and state the token is required for private lakehouse-engine-rs access
* *AND* the script MUST NOT print the token value

### Scenario: Connectivity mode is a validated either/or

* *GIVEN* the required account, database, and PAT inputs are supplied
* *WHEN* the user supplies both a `--profile` and a direct-connection input, or neither
* *THEN* the script MUST reject the invocation with a non-zero exit
* *AND* the script MUST state that exactly one connectivity mode is required
* *AND* the script MUST proceed past this check only when exactly one mode is supplied

### Scenario: Missing required identifiers fail fast

* *GIVEN* the script is invoked without `--account-id`, without `--database-id`, or without a PAT (`EXASOL_PAT`/`--pat`)
* *WHEN* the script runs
* *THEN* the script MUST exit non-zero before making any network call
* *AND* the error message MUST name the missing input and where to obtain it (the SaaS web console)

### Scenario: Artifact versions default to the latest release and honour overrides

* *GIVEN* no version flags are supplied
* *WHEN* the script resolves artifact versions
* *THEN* the script SHALL resolve the latest lakehouse-engine-rs release tag through the GitHub REST API over `curl` (`GET https://api.github.com/repos/<repo>/releases/latest` with the `GITHUB_TOKEN` bearer `Authorization` header), parsing `tag_name` with the existing no-jq bash-regex field extractor
* *AND* the script SHALL resolve the latest language-container-rs (public repo) release tag through the same `curl` GitHub REST API path and the same token header, adding no additional authentication step or prerequisite of its own
* *AND* WHEN `--lakehouse-version` or `--slc-version` is supplied THEN the script SHALL use that exact version instead of resolving the latest
* *AND* the script SHALL print the resolved engine and SLC versions before uploading

### Scenario: Target environment defaults to production and honours an override

* *GIVEN* no target-override flag is supplied
* *WHEN* the script issues SaaS REST calls
* *THEN* the script SHALL address `https://cloud.exasol.com`
* *AND* WHEN `--staging` is supplied THEN the script SHALL address `https://cloud-staging.exasol.com` for every SaaS REST call
* *AND* the script SHALL accept no arbitrary base-URL override; `--staging` toggles between exactly these two targets
</content>
