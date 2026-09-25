# Feature: Exasol Personal Deployment Install

The install script gains a `--deployment` flag that targets an Exasol Personal instance by name, mirroring the lc-rs install.sh pattern. The deployment backend (local or cloud) is discriminated at runtime from the deployment directory's `deployment.json`. Local backend writes the engine `.so` into the deployment's host-side BucketFS directory and installs the Rust SLC through the `exasol` launcher; cloud backend resolves connection details from the descriptor and falls through to the existing BucketFS HTTP upload path.

## Background

- Exasol Personal deployments live under `$HOME/.exasol/personal/deployments/<name>/`
- Each deployment directory contains `deployment.json` (connection details, backend type) and `secrets.json` (DB password)
- Local backend: no BucketFS HTTP endpoint; SQL port is assigned per deployment and read from `deployment.json`
- Only Exasol Personal 2.3 and later is supported. Earlier versions, which exposed the VM over SSH (`connection.sshPort`, `local/node_access.pem`), are deliberately out of scope
- Local backend: no SSH access and no BucketFS HTTP endpoint. The deployment exposes the database's `/exa` directory at `local/runtime/exa`; creating `local/runtime/exa/bucketfs/<service>/<bucket>/` creates that bucket, the engine registers it in `local/runtime/exa/bucketfs.conf` within seconds, and UDFs read its files under `/buckets/<service>/<bucket>/` (documented in the Exasol Personal virtual-schemas guide; verified live on Personal 2.3.0)
- Local backend SLC: the only supported entry point is `exasol slc custom install|update`, which imports a container tarball, registers its alias in `SCRIPT_LANGUAGES`, and restarts the database
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
* *AND* the script MUST write the engine `.so` to `local/runtime/exa/bucketfs/bfsdefault/<bucket>/udf/liblakehouse_engine.so` under the deployment directory, replacing any earlier file by rename
* *AND* the script MUST install the downloaded Rust SLC tarball unchanged, running `exasol slc custom update --alias RUST` when `exasol slc list --json` reports a custom SLC with alias `RUST` and `exasol slc custom install --alias RUST --language rust` otherwise, passing the deployment directory explicitly
* *AND* the script MUST wait until `bucketfs.conf` registers `bfsdefault/<bucket>` before creating scripts, failing with an error naming the bucket when it never appears
* *AND* the created scripts MUST reference `%udf_object buckets/bfsdefault/<bucket>/udf/liblakehouse_engine.so`
* *AND* the script MUST NOT issue its own `ALTER SYSTEM SET SCRIPT_LANGUAGES`, because the launcher owns that registration, and the SCRIPT_LANGUAGES entries other than `RUST` MUST survive
* *AND* the version smoke test MUST still run

### Scenario: --skip-slc on a local deployment replaces only the engine

* *GIVEN* a local deployment whose `RUST` custom SLC is already installed
* *WHEN* the install script is invoked with `--skip-slc`
* *THEN* the script MUST NOT download the SLC or run any `exasol slc custom` command, so the database is not restarted
* *AND* the engine `.so` MUST still be written and the scripts created

### Scenario: A local deployment rejects BucketFS HTTP flags

* *GIVEN* a local deployment
* *WHEN* the install script is invoked with `--bfs-host`, `--bfs-port`, or `--bfs-write-password`
* *THEN* the script MUST exit with a non-zero status naming the flag, because the deployment has no BucketFS HTTP endpoint
* *AND* `--bfs-bucket` MUST select the bucket directory and MUST be rejected unless it matches `[A-Za-z0-9._-]+` and is neither `.` nor `..`

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
