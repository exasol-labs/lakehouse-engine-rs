# AGENTS.md

Spec-driven development with mission in: @specs/mission.md

## Testing

- Integration and E2E tests run against a local Exasol Docker database. Start the container yourself, do not ask the user.
- Tests must fail, not skip, when Exasol is unavailable.
- Connection strings must set `validateservercertificate=0`, because the Docker image uses a self-signed certificate.
- Read `specs/testing.md` before adding or changing an E2E suite, fixture, or harness helper. It holds the coverage rule, the suite layout, and the ops rules for the orphan sweeps and the benchmark catalog.

Project specifics:

- `make test-e2e` runs the E2E suite against the local container. Use `exapump` for all Exasol and BucketFS interaction.
- Reproduce a reported bug against the Docker Exasol container before fixing it. Do not trust an issue's repro, a capability list, or code inspection alone.
- Verify any claim about SQL capabilities, syntax, or pushdown reachability against a live Exasol (`EXPLAIN VIRTUAL`, a pushed query, or an E2E test). `capabilities.rs` and documentation are not evidence.
- A stray `bench/.env` redirects `make bench` and `bench/run.sh` to a remote target, and `BENCH_TARGET=docker` alone does not undo it. Before debugging a hung bench run, move `bench/.env` aside.

## Code quality

- `cargo fmt --all` and `cargo clippy --all-targets` must pass with zero warnings before committing.

## Workflow

- New features are tracked as GitHub issues. A human opens the issue, agents do not. Reference it in the implementing commit (`Closes #<n>`). If no issue exists, ask for one.
- PR titles follow Conventional Commits, `<type>(<scope>): <description>`, with type one of `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `perf`. The title describes the final state of the change. A plan-only PR for a new feature is still `feat(...)`.
- A plan that touches scanning, pushdown, or schema and type handling is checked against the governing Iceberg table spec or Delta Lake protocol. Quote the normative section. Fix each deviation in the same plan, or record it in the spec delta as a scoped exception. A deviation forced by an Exasol type limit (no struct, list, or map) is a named trade-off, not a gap.
- Planning never opens GitHub issues. Cite an existing issue as `(#83)`, otherwise mark `(#TBD)` and list it as an open question.

## Code style

- A comment states a non-obvious why: an invariant, an external-system quirk, or a spec or issue constraint. Keep it to 1 or 2 lines. Never restate the code, narrate history, or add banners. Update or delete comments when behavior changes.
- A test implementing a spec scenario carries one `/// Scenario: <title>` line per scenario, quoting the title verbatim.
- Unit tests live in a sibling file, never in the production file: `foo.rs` declares `#[cfg(test)] #[path = "foo_tests.rs"] mod tests;`, and `foo/mod.rs` uses `foo/foo_tests.rs`. The name must match `[0-9a-zA-Z_-]+[_-]tests.rs`, or `cargo llvm-cov` counts it as production code. Test helpers live in the `_tests.rs` file without `#[cfg(test)]`. Only a test-visibility `pub use` re-export stays in the production module.
- Prefer Serena's symbolic tools (`find_symbol`, `find_referencing_symbols`, `replace_symbol_body`, `rename_symbol`) over grep, Read, and Edit for code files. Load Serena and call `initial_instructions` first.

## Architecture rules

- The Virtual Schema (VS) stays thin: query translation, pushdown analysis, parallelization planning, and result schema mapping. Execution logic lives in DataFusion.
- UDFs are stateless and disposable. They hold no cache, persist no metadata, and keep no state across calls.
- The VS resolves metadata once per query, never once per node, and passes each UDF an explicit file list. A UDF never discovers files itself.
- Each node scans only its assigned files. Assignments never overlap.
- `ScanSpec`, `FileEntry`, and `LogicalField` stay format-neutral. Widen an existing field before adding one, and never add a format-specific struct or `Option<FormatXSpec>`. Format knowledge lives in the plan-time `FormatReader`, and the scan side dispatches on field content, never on format identity.
- Once the adapter advertises a capability, Exasol delegates it fully and never re-checks it. There is no Exasol-side fallback. The adapter must generate equivalent SQL for anything within an advertised capability that it cannot push into the DataFusion scan, because omitting it returns wrong rows.
- Arrow types never cross the `.so` boundary, because Arrow `TypeId` is not stable across it. Only SDK `Value`s and Arrow IPC bytes (`ctx.emit_batch`) cross it.

## Exasol UDF behavior

Read `specs/udf-context.md` before changing the shard count or fan-out shape.

- Groups drive UDF invocations, not OS processes. A node runs a fixed pool of VMs sized to `NR_OF_CORES`, and groups are multiplexed onto it.
- Never shard on `IPROC()`. It yields one group per node and idles the other cores. Shard on `GROUP BY shard_key` with `G = node_count × parallelism_factor`, capped at 300 (Exasol's `max_dynamic_group_count`). At or below 300 groups are distributed round-robin, above it they are hash-partitioned and unbalanced. Clamp G to between 1 and the file count. Read the node count from `ctx.node_count()`.
- `ctx.memory_limit()` returns the per-instance limit in bytes (`0` means unknown). The engine stalls new VMs at 80% of it, so size the DataFusion memory pool to about 0.6 of it.
- Stream DataFusion results one `RecordBatch` at a time and drop each batch before fetching the next. The raw scan path calls `ctx.emit_batch(&batch)`, and the partial-aggregate path converts its single row to `Vec<Value>` and calls `ctx.emit`.
- `ctx.emit` flushes at 4,000,000 bytes (`EMIT_BUFFER_LIMIT_BYTES`), not 4 MiB. Always flush at the end of `run()`.
- `MT_EMIT` is a synchronous request and reply over the SLC's ZMQ REQ socket. A short, no-retry timeout treated as fatal breaks the lockstep when the engine is slow to acknowledge, and the VM exits abnormally.
- `cleanup VM failed: VM crashed` (SQL state 22002) is an abnormal native VM exit, not necessarily OOM and not a Rust panic. Check RSS against the limit, the OS OOM killer, core dumps, and the panic log first. The engine SIGKILLs every sibling VM when one dies, so find the earliest death.
- The SLC and the `.so` must use the same `exasol-udf-sdk` version and the same rustc, or the fingerprint check fails at load.
- To capture UDF output, set `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<host>:<port>'` to a listener the cluster nodes can reach. `%udf_debug_level debug` in the script source enables SLC tracing. The redirect destabilizes multi-leg join queries, so diagnose those with single-leg repros.

## Data types

Exasol has no arrays, lists, structs, or maps. `specs/scan-types/type-mapping/spec.md` owns the mapping. Two rules are easy to miss:

- Iceberg `timestamptz` maps to plain `TIMESTAMP`, because Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` type (`sqlCode 22002`).
- Types Exasol cannot hold (List, Struct, Map, Union, Binary, Duration, Time, Interval, Decimal256, and Decimal128 beyond precision 36) become `VARCHAR(2000000)` JSON, produced inside the UDF.

## Build

- Build the `.so` only with `make cross-udf-build`, inside `rust:1.94-trixie` (glibc 2.41, matching the SLC). A host `cargo build --release` writes a host-glibc `.so` that fails to load in Exasol. Host `cargo test` is fine.
- `crates/lakehouse-catalog` and `crates/vs-expression` compile into `crates/lakehouse-engine`'s cdylib, so one `.so` exports all three entry points.
- Pin `exasol-udf-sdk` and `exasol-udf-macros` only in the root `[workspace.dependencies]`. Enable `emit-arrow` for `ctx.emit_batch`.

## Deployment state

- The OpenTofu state of the `deploy/{data,cluster,trino,lakekeeper}-stack` stacks lives in S3, never locally. Never commit the bucket name, because it embeds the AWS account id and this repo is public. Each `providers.tf` declares an empty `backend "s3" {}`, and the real values come from a gitignored `backend.hcl` (copy `backend.hcl.example`).
- Dependent stacks also read the bucket from `var.tofu_state_bucket` in a gitignored `terraform.tfvars`.
- A local backend loses state when a disposable worktree is dropped while the AWS resources keep running. Never revert to one.
- Select the workspace with `tofu workspace select "$ENV" || tofu workspace new "$ENV"`.
- `data-stack`'s `terraform.tfvars` must set `enable_emr_serverless = true`. The default is `false`, and a bare plan or apply offers to destroy the live EMR Serverless application.
