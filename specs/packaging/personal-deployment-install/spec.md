# Feature: Exasol Personal Deployment Install

The install script gains a `--deployment` flag that targets an Exasol Personal instance by name, mirroring the lc-rs install.sh pattern. The deployment backend (local or cloud) is discriminated at runtime from the deployment directory's `deployment.json`. Local backend copies artifacts over SSH into the VM's BucketFS directory; cloud backend resolves connection details from the descriptor and falls through to the existing BucketFS HTTP upload path.

## Background

- Exasol Personal deployments live under `$HOME/.exasol/personal/deployments/<name>/`
- Each deployment directory contains `deployment.json` (connection details, backend type) and `secrets.json` (DB password)
- Local backend: no BucketFS HTTP endpoint; artifacts travel over SSH; SQL port is assigned per deployment and read from `deployment.json`
- Cloud backend: exposes ordinary BucketFS HTTP endpoint; requires `--bfs-write-password`
- The lc-rs project already implements this pattern; this feature mirrors it with the addition of architecture-aware asset selection
- Bash 3.2+ compatibility required (stock macOS); `jq` is required for deployment descriptor parsing

## Scenarios

### Scenario: --deployment with local backend installs over SSH

* *GIVEN* the install script is invoked with `--deployment my-local-db`
* *AND* the deployment descriptor at `$HOME/.exasol/personal/deployments/my-local-db/deployment.json` has `"backend": "local"`
* *WHEN* the install runs
* *THEN* the script MUST resolve host, port, user, and password from the deployment descriptor and secrets
* *AND* the script MUST copy artifacts to the VM over SSH using the deployment's node key
* *AND* the script MUST register with `ALTER SYSTEM` (not `ALTER SESSION`)
* *AND* the SCRIPT_LANGUAGES update MUST preserve all pre-existing language entries

### Scenario: --deployment with cloud backend uses BucketFS HTTP upload

* *GIVEN* the install script is invoked with `--deployment my-cloud-db --bfs-write-password secret`
* *AND* the deployment descriptor has a non-`local` backend (e.g. `"aws"`)
* *WHEN* the install runs
* *THEN* the script MUST resolve host, port, user, and password from the deployment descriptor and secrets
* *AND* the script MUST fall through to the existing BucketFS HTTP upload path

### Scenario: --deployment cloud without --bfs-write-password fails

* *GIVEN* the install script is invoked with `--deployment my-cloud-db` and no `--bfs-write-password`
* *AND* the deployment descriptor has a non-`local` backend
* *WHEN* argument validation runs
* *THEN* the script MUST exit with a non-zero status
* *AND* the error message MUST state that `--bfs-write-password` is required for cloud deployments

### Scenario: --deployment requires jq

* *GIVEN* the install script is invoked with `--deployment my-db`
* *AND* `jq` is not on PATH
* *WHEN* the deployment path begins
* *THEN* the script MUST exit with a non-zero status
* *AND* the error message MUST name `jq` as the missing prerequisite

### Scenario: CLI flags override deployment descriptor values

* *GIVEN* the install script is invoked with `--deployment my-db --host override.example --password override`
* *WHEN* connection details are resolved
* *THEN* the explicit `--host` and `--password` values MUST override the deployment descriptor values
* *AND* unoverridden fields (port, user) MUST still resolve from the descriptor

### Scenario: Missing deployment directory fails

* *GIVEN* the install script is invoked with `--deployment nonexistent`
* *AND* no directory exists at `$HOME/.exasol/personal/deployments/nonexistent/`
* *WHEN* deployment resolution begins
* *THEN* the script MUST exit with a non-zero status
* *AND* the error message MUST name the expected directory path
