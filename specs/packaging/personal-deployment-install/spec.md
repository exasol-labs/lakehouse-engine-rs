# Feature: Exasol Personal Deployment Install

The install script gains a `--deployment` flag that targets an Exasol Personal instance by name, mirroring the lc-rs install.sh pattern. The deployment backend (local or cloud) is discriminated at runtime from the deployment directory's `deployment.json`. Local backend installs the engine bundled inside the Rust SLC through the `exasol` launcher; cloud backend resolves connection details from the descriptor and falls through to the existing BucketFS HTTP upload path.

## Background

- Exasol Personal deployments live under `$HOME/.exasol/personal/deployments/<name>/`
- Each deployment directory contains `deployment.json` (connection details, backend type) and `secrets.json` (DB password)
- Local backend: no BucketFS HTTP endpoint; SQL port is assigned per deployment and read from `deployment.json`
- Only Exasol Personal 2.3 and later is supported. Earlier versions, which exposed the VM over SSH (`connection.sshPort`, `local/node_access.pem`), are deliberately out of scope
- Local backend: no SSH access exists, and files placed in the shared BucketFS host directory are not visible inside the UDF sandbox. The only supported entry point is `exasol slc custom install|update`, which imports a container tarball, registers its alias in `SCRIPT_LANGUAGES`, and restarts the database. The engine `.so` therefore rides inside the SLC rootfs at `udf/liblakehouse_engine.so` and scripts load it via `%udf_object /udf/liblakehouse_engine.so` (verified live on Personal 2.3.0)
- Cloud backend: exposes ordinary BucketFS HTTP endpoint; requires `--bfs-write-password`
- The lc-rs project already implements this pattern; this feature mirrors it with the addition of architecture-aware asset selection
- Bash 3.2+ compatibility required (stock macOS); `jq` is required for deployment descriptor parsing

## Scenarios

### Scenario: --deployment with local backend installs through the launcher

* *GIVEN* the install script is invoked with `--deployment my-local-db`
* *AND* the deployment descriptor at `$HOME/.exasol/personal/deployments/my-local-db/deployment.json` has `"backend": "local"`
* *AND* the `exasol` launcher CLI is on PATH
* *WHEN* the install runs
* *THEN* the script MUST resolve host, port, user, and password from the deployment descriptor and secrets
* *AND* the script MUST append the engine `.so` to the downloaded Rust SLC tarball at `./udf/liblakehouse_engine.so` without extracting the SLC rootfs
* *AND* the script MUST run `exasol slc custom update --alias RUST` when `exasol slc list --json` reports a custom SLC with alias `RUST`, and `exasol slc custom install --alias RUST --language rust` otherwise, passing the deployment directory explicitly
* *AND* the created scripts MUST reference `%udf_object /udf/liblakehouse_engine.so`
* *AND* the script MUST NOT issue its own `ALTER SYSTEM SET SCRIPT_LANGUAGES`, because the launcher owns that registration, and the SCRIPT_LANGUAGES entries other than `RUST` MUST survive
* *AND* the version smoke test MUST still run

### Scenario: The launcher transport rejects --skip-slc and BucketFS flags

* *GIVEN* a local deployment
* *WHEN* the install script is invoked with `--skip-slc`
* *THEN* the script MUST exit with a non-zero status before any download
* *AND* the error message MUST name `--skip-slc`, because the engine `.so` is installed inside the SLC
* *AND* any `--bfs-host`, `--bfs-port`, `--bfs-bucket` or `--bfs-write-password` MUST likewise fail naming the flag, because no BucketFS upload happens

### Scenario: A local deployment requires the exasol launcher CLI

* *GIVEN* a local deployment
* *AND* the `exasol` launcher CLI is not on PATH
* *WHEN* the prerequisite check runs
* *THEN* the script MUST exit with a non-zero status before any download
* *AND* the error message MUST name `exasol` as the missing prerequisite

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
