# Feature: SaaS Install Script — RUST SLC Registration

The SaaS installer registers the Rust Script Language Container by reading the database's
persisted `SCRIPT_LANGUAGES` system value and appending or replacing a single fixed RUST
segment, never overwriting the whole value. Split out of `packaging/saas-install-script`
to keep the idempotent read-modify-write logic separate from the installer's preflight and
deploy scenarios. See `packaging/saas-install-script` for prerequisite/connectivity checks
and `packaging/saas-install-script-deploy` for the artifact upload and script DDL that
follow SLC registration.

## Background

* The RUST `SCRIPT_LANGUAGES` alias segment is fixed and version-independent — the SLC version
  lives in the uploaded tarball content, not in the alias string. The appended or replaced segment
  is exactly `RUST=localzmq+protobuf:///uploads/default/rustslc?lang=rust#buckets/uploads/default/rustslc/exaudf/exaudfclient`,
  pointing at the `rustslc` name at the SaaS bucket path `uploads/default` where the SLC tarball is
  uploaded (the SaaS BucketFS service/bucket is always `uploads/default`).
* Every `exapump sql` invocation the registration step issues MUST run with stdin redirected
  from `/dev/null`, consistent with the installer's stdin-piped invocation contract (see
  `packaging/saas-install-script-deploy`).

## Scenarios

### Scenario: SLC registration appends the RUST language without overwriting existing languages

* *GIVEN* a database whose persisted `SCRIPT_LANGUAGES` system value already lists other languages (e.g. `PYTHON3`, `JAVA`) and no `RUST` entry
* *WHEN* the SLC registration step runs
* *THEN* the script SHALL read the current `SCRIPT_LANGUAGES` system value from `EXA_PARAMETERS` before writing
* *AND* the script SHALL issue `ALTER SYSTEM SET SCRIPT_LANGUAGES` with the existing languages preserved and the exact segment `RUST=localzmq+protobuf:///uploads/default/rustslc?lang=rust#buckets/uploads/default/rustslc/exaudf/exaudfclient` appended
* *AND* the script MUST NOT drop or reorder any pre-existing language entry
* *AND* WHEN the `ALTER SYSTEM SET` fails for insufficient privilege THEN the script MUST surface that the account user lacks admin rights and exit non-zero

### Scenario: Re-running replaces the existing RUST segment idempotently

* *GIVEN* a database whose `SCRIPT_LANGUAGES` already contains a `RUST=` entry from a prior run
* *WHEN* the SLC registration step runs again
* *THEN* the script SHALL replace the single existing `RUST=` segment in place with `RUST=localzmq+protobuf:///uploads/default/rustslc?lang=rust#buckets/uploads/default/rustslc/exaudf/exaudfclient`
* *AND* the resulting `SCRIPT_LANGUAGES` MUST contain exactly one `RUST=` entry equal to that exact segment
* *AND* every non-RUST entry MUST remain unchanged
