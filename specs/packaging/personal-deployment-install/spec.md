# Feature: Exasol Personal Deployment Install

The install script's `--deployment` flag targets an Exasol Personal instance by name. The deployment backend (local or cloud) is read at runtime from the deployment directory's `deployment.json`. The local backend writes the engine `.so` into the deployment's BucketFS directory and installs the Rust SLC with `exasol slc custom`. The cloud backend resolves connection details from the descriptor and uses the BucketFS HTTP upload path.

## Background

- Exasol Personal deployments live under `$HOME/.exasol/personal/deployments/<name>/`
- Each deployment directory contains `deployment.json` (connection details, backend type) and `secrets.json` (DB password)
- The local backend has no BucketFS HTTP endpoint. BucketFS is the host directory `local/runtime/exa/bucketfs/`, and the SQL port is read from `deployment.json`
- The cloud backend exposes an ordinary BucketFS HTTP endpoint and requires `--bfs-write-password`
- `jq` is required for deployment descriptor parsing

## Scenarios

### Scenario: --deployment with local backend installs through the deployment directory

* *GIVEN* the install script is invoked with `--deployment my-local-db`
* *AND* the deployment descriptor at `$HOME/.exasol/personal/deployments/my-local-db/deployment.json` has `"backend": "local"`
* *WHEN* the install runs
* *THEN* the script MUST resolve host, port, user, and password from the deployment descriptor and secrets
* *AND* the script MUST write the engine `.so` into the deployment's BucketFS directory and install the SLC with `exasol slc custom`
* *AND* the SCRIPT_LANGUAGES registration MUST preserve all pre-existing language entries

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
