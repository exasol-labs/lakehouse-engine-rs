# Decisions: add-timestamp-precision-versioning

## ADR: The Delta declaration path is `unity_type_name_to_exasol`, not `compatible_exasol_type`

**ID:** delta-timestamp-declaration-path-is-unity-type-name-to-exasol
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

Issue #359's scope text and its own planning interview both named `compatible_exasol_type` as the
generic Arrow-to-Exasol resolver that also covers Delta table schema declarations, alongside
`iceberg_primitive_to_exasol` for Iceberg. A workspace-wide call-site census found
`arrow_to_exasol_type` has zero production call sites — already recorded by
`datafusion-scan/type-mapping-module-structure` — and `compatible_exasol_type`'s only production
consumer is the `needs_json_fallback` boolean, whose answer for `Timestamp(_, _)` is `false` at every
precision. A Delta table in fact reaches `createVirtualSchema` only through the Unity Catalog kind,
where `unity_type_name_to_exasol` maps the Spark type names `TIMESTAMP`/`TIMESTAMP_NTZ`.

### Decision

Honor the plan's Iceberg+Delta scope by version-gating `iceberg_primitive_to_exasol` (Iceberg) and
`unity_type_name_to_exasol` (Delta). Leave `arrow_to_exasol_type`/`compatible_exasol_type` untouched,
and record that exclusion as a scenario rather than leaving it silent.

### Options Considered

| Option | Verdict |
|--------|---------|
| Version-gate `unity_type_name_to_exasol` for Delta | ✓ Chosen — it is the one production function that actually declares a Delta timestamp column's Exasol type |
| Thread the gate through `compatible_exasol_type`, per the issue's literal wording | ✗ Rejected — that function has no production consumer whose answer a precision parameter could change |
| Thread the gate through both and accept a redundant parameter | ✗ Rejected as over-engineering — adds a parameter that cannot change any observable answer |

### Consequences

Delta timestamps get microsecond precision through the function that actually declares them, with no
dead parameter added to an unreached resolver. The exclusion of `arrow_to_exasol_type` is recorded as
a scenario so a reader who knows the issue named it does not mistake the omission for an oversight.

## ADR: `TimestampPrecision` owns the version rule and both declaration strings

**ID:** timestamp-precision-enum-owns-version-rule-and-declaration-strings
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`iceberg_primitive_to_exasol` and `unity_type_name_to_exasol` each independently hardcoded the
literal `"TIMESTAMP"` — the exact shape that let the catalog-decimal guard drift into four copies
before issue #329 consolidated it into one. A version-gated declaration needs one place that decides
the threshold and both resulting strings, so the two producers cannot diverge.

### Decision

Add a two-variant `Copy` enum, `TimestampPrecision::{Millisecond, Microsecond}`, to
`crates/lakehouse-engine/src/types/mapping.rs`, owning `from_database_version(&str)` and both
declaration strings. Both producers read it; neither keeps its own `"TIMESTAMP"` literal.

### Options Considered

| Option | Verdict |
|--------|---------|
| Named `Copy` enum owning the rule and both strings | ✓ Chosen — variant names carry meaning at every call site, and the module already owns Exasol's own type domain (`exasol_representable_catalog_decimal`) |
| A `bool supports_parameterized_timestamp` parameter | ✗ Rejected — inverts silently at any of five call sites |
| A free function returning `&'static str` | ✗ Rejected — leaves the version rule and the two strings without one named owner |
| A broader `EngineFeatures` struct | ✗ Rejected as over-engineering — generalizes for a second version-gated decision that does not exist |

### Consequences

An Iceberg `timestamp` and a Delta `timestamp` are declared at the same precision by construction,
not by coincidence. A future version-gated decision has a precedent to extend rather than a template
to widen prematurely.

## ADR: The version STRING crosses into `types/mapping.rs`; the `UdfContext` does not

**ID:** timestamp-precision-version-string-crosses-not-udfcontext
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`types/mapping.rs` reads no ambient state and performs no I/O today. Threading `&dyn UdfContext` into
it to reach `database_version()` would make the type-mapping module depend on the adapter's runtime
context, reversing the direction the dependency has always pointed. `cluster_nodes_from_context` is
the recorded precedent for reading a context value once at the adapter edge and passing a plain value
onward, but it earns its own name by normalizing `0` to `1` — a wrapper here would only forward one
call.

### Decision

`handle_create_virtual_schema` reads `ctx.database_version()` exactly once, inline, and threads the
resolved `TimestampPrecision` as a plain parameter through `build_listing_virtual_tables` →
`column_source_type_to_exasol` → both producers. `TimestampPrecision::from_database_version` takes
`&str`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read inline at the adapter edge, thread a plain `Copy` value | ✓ Chosen — keeps `types/mapping.rs` free of `UdfContext`, matching `cluster_nodes_from_context`'s shape |
| Thread `&dyn UdfContext` into `types/mapping.rs` | ✗ Rejected — makes the type-mapping module perform I/O and read ambient state it has never needed |
| Add a `timestamp_precision_from_context(ctx)` wrapper | ✗ Rejected — a wrapper whose whole body forwards one call adds a name without adding a decision |
| Read the version again in the scan UDF entry point | ✗ Rejected — the scan's `EMITS` types already arrive in the pushdown request's own `dataType` JSON; a second read would give one decision two owners |

### Consequences

The type-mapping module stays a pure function of its inputs. The scan UDF entry point needs no
version read at all, so the decision has exactly one owner across both entry points.

## ADR: Empty and unparseable versions both take the microsecond default

**ID:** timestamp-precision-empty-and-unparseable-default-to-microsecond
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`UdfContext::database_version()` returns `String::new()` on a context that does not populate
handshake metadata, and no call site for it exists anywhere in the repo today, so its unpopulated
behavior is untested in practice. A version-gated declaration needs a defined answer for a version
string that is empty or that fails to parse.

### Decision

An empty string and any string whose leading dot-separated component does not parse as an integer
both yield `TIMESTAMP(6)` — one default arm, not two — rather than falling back to the conservative
bare `TIMESTAMP`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Default both to `TIMESTAMP(6)` (the modern declaration) | ✓ Chosen — the user's explicit, deliberate choice, opposite of the recommended conservative default |
| Default both to bare `TIMESTAMP` (the conservative option) | ✗ Rejected — silently truncates every timestamp value on any engine the parse misjudges |
| Distinguish empty from unparseable with two separate arms | ✗ Rejected — neither input carries information the other lacks, and two arms invite drift |

### Consequences

A hypothetical engine that rejects `TIMESTAMP(6)` fails loudly at `createVirtualSchema` rather than
silently truncating — the trade-off the user chose. Live capture against Exasol 8.29.13 later showed
this risk does not materialize as a loud failure on that engine (it clamps instead), which corrects
the rationale's claimed failure mode without changing the decision itself.

## ADR: The E2E precision expectation is an independent oracle read from the live session

**ID:** timestamp-precision-e2e-oracle-independent-of-production-rule
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`cargo test --features exasol-e2e` runs against whatever Exasol stack happens to be up. An
`EXASOL_IMAGE`-derived expectation silently picks the wrong arm whenever that variable is absent or
stale — the same class of failure a stray `bench/.env` produces. Separately, a test that computes its
expected declaration by calling the very production rule under test cannot fail when that rule is
wrong.

### Decision

One shared helper in `crates/lakehouse-engine/tests/common/` reads the running engine's own version
from the live session and maps it to an expected precision with its own explicit version-to-precision
table. The helper MUST NOT call `TimestampPrecision::from_database_version` and MUST NOT read
`EXASOL_IMAGE`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Independent oracle reading the live session, own explicit table | ✓ Chosen — cannot pass by construction when the production rule is wrong, and is correct regardless of how the stack was started |
| Call the production parser directly (one owner, no duplication) | ✗ Rejected — a test computing its expectation by calling the rule under test cannot fail when that rule is wrong |
| Read `EXASOL_IMAGE`, which the CI matrix leg already sets | ✗ Rejected — an absent or stale variable silently selects the wrong assertion arm in local runs |

### Consequences

The production rule's own inputs are covered separately by a unit matrix over concrete version
strings, so the oracle and the rule form two independent checks on one behavior rather than one check
duplicated twice.

## ADR: One CI matrix leg keeps the status-check name `E2E`

**ID:** timestamp-precision-ci-matrix-keeps-e2e-check-name
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`E2E` is a required status check on `main`'s branch-protection ruleset. The core `e2e` CI job needs a
second leg running an `8.29.x` image alongside the existing `2025.x` leg, but a matrixed job whose
legs both carry new names would leave the ruleset waiting on a check that never reports again,
blocking every PR until an admin edits the ruleset. `upload-artifact@v7` also rejects a name already
used by another upload in the same run, which the workflow already records for `e2e-azure`'s
`exa-logs-azure`.

### Decision

Matrix entries carry the image, the status-check name, and the failure-log artifact name. The
`2025.x` leg's check name stays exactly `E2E`; the `8.29.x` leg gets a new, distinct check name that
an admin adds to `main`'s ruleset as a separate operator action, and a distinct failure-log artifact
name.

### Options Considered

| Option | Verdict |
|--------|---------|
| Matrix one job, keep one leg named exactly `E2E` | ✓ Chosen — preserves the existing required-check requirement with no ruleset edit needed to stay green |
| Rename both legs and update the ruleset | ✗ Rejected — blocks every PR until an admin intervenes, between the rename landing and the ruleset edit |
| Add an aggregate gate job named `E2E` that `needs: [e2e]` | ✗ Rejected — without `if: always()` it reports *skipped* when its dependency fails, and GitHub counts a skipped required check as satisfied, silently unblocking a red E2E |
| Duplicate the job body into a second sibling job | ✗ Rejected — copies roughly 60 lines with no YAML-anchor support to keep the two copies in step |

### Consequences

Adding the `8.29.x` leg to the ruleset's required checks is an explicit, tracked operator action — the
same class of follow-up issue #336 already tracks for `e2e-azure` — rather than an assumed side
effect of the matrix landing.

## ADR: `CLAUDE.md` gets a one-line pointer, not a second copy of the version rule

**ID:** timestamp-precision-claude-md-pointer-not-second-rule-copy
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`CLAUDE.md`'s Data types table carried a stale `| Timestamp(_, _) | TIMESTAMP |` row — the same class
of failure issue #359 itself reports, a statement nobody re-checked after the code moved. Only one of
two possible copies of a version-gated rule can be test-backed:
`TimestampPrecision::from_database_version`'s unit matrix pins the spec's version, and nothing pins a
second prose copy in `CLAUDE.md`.

### Decision

Task 12 rewrites the single `Timestamp(_, _)` row of `CLAUDE.md`'s Data types table to name both
declarations and point at the `datafusion-scan/type-mapping` spec for the exact rule. `CLAUDE.md`
keeps the invariant facts it already carries and does not restate the version parse, the `>= 2025`
threshold, or the empty/unparseable fallback.

### Options Considered

| Option | Verdict |
|--------|---------|
| One-line pointer to the owning spec | ✓ Chosen — keeps exactly one test-backed owner for the rule, and gives an agent only what it needs before reading further |
| Restate the full version-gated rule in `CLAUDE.md` | ✗ Rejected — recreates the exact drift failure mode issue #359 reports, since only one copy would stay test-backed |
| Leave the row untouched, let the spec alone carry the change | ✗ Rejected — an auto-loaded project instruction that contradicts the shipped code misleads every future session |

### Consequences

`CLAUDE.md` stays a pointer document for this rule rather than a second source of truth, so the next
version-gated change to timestamp declaration only has one place to update.
