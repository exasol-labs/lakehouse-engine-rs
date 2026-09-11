# Plan: fix-path-style-default

Closes [#130](https://github.com/exasol-labs/lakehouse-engine-rs/issues/130).

## Summary

Widen the CONNECTION's `path_style` credential from a boolean to a tri-state, so an absent field means
unstated rather than `true`, and resolve unstated to `false` on the non-vended path to match AWS client
convention. An explicitly stated value then wins on the vended path too, which discharges the type
limitation that kept `path_style` out of the CONNECTION-wins addressing rule.

## Design

### Context

`StorageCreds::from_json` (`crates/lakehouse-catalog/src/creds.rs:214-217`) resolves an absent
`path_style` to `true`. AWS S3 and every AWS SDK address a bucket virtual-hosted by default, so a
CONNECTION that names a `region` and no `endpoint` gets path-style addressing where an operator
expects the AWS behaviour. The `true` default came from MinIO fixtures, not from a decision about
the general CONNECTION path.

The boolean type has a second cost. `ConnectionCreds.path_style` cannot distinguish "the operator
set `false`" from "the operator said nothing", so the vended resolution cannot admit it: a silent
default would override the response's `s3.path-style-access` on every CONNECTION that omits the
key. Two recorded clauses name that limitation as the reason for excluding the field
(`specs/vs-adapter/connection-credentials/spec.md:23` and
`specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:97`), and ADR
`066-fix-vended-storage-shared-policy` § "path_style does not read the CONNECTION" records the
rejection. One type change discharges both concerns.

One verified code fact shapes the whole design. `build_undecorated_store`
(`crates/lakehouse-engine/src/scan/object_store.rs:226-231`) passes `endpoint` to
`AmazonS3Builder` only inside `if storage.path_style`. A resolved `false` therefore DISCARDS the
configured endpoint and lets the builder derive `https://<bucket>.s3.<region>.amazonaws.com` from
the region. In this engine `path_style` is not only an addressing-style knob. It is also the gate on
whether the endpoint is used at all.

- **Goals** — Make an unstated `path_style` mean `false` on the non-vended path. Let a stated value
  win on the vended path. Keep an operator from silently reaching the wrong host across the upgrade.
- **Non-Goals** — Changing `StorageProps`, the resolved wire type. Changing how `s3.path-style-access`
  is read off a vended response. Changing `endpoint` or `region` resolution. Changing any credential
  rule on any path.

### Decision

`ConnectionCreds.path_style` and `StorageCreds.path_style` become `Option<bool>`. The parse step
preserves the tri-state. Two resolution points consume it, one per path.

#### Architecture

```
CONNECTION JSON
  │  StorageCreds::from_json   (preserves tri-state: absent -> None)
  ▼
StorageCreds.path_style: Option<bool>          ConnectionCreds.path_style: Option<bool>
  │                                              │
  │ StorageCreds::backend()                      │ StaticStoreAddress::from(&ConnectionCreds)
  │   the ONE non-vended selector,               ▼
  │   read by BOTH the adapter and             StaticStoreAddress { endpoint, region, path_style }
  │   the scan UDF                               │
  ▼                                              │ s3_backend()  the ONE vended policy home,
None -> false                                    ▼   reached by the Iceberg REST and Unity arms
Some(v) -> v                                   connection.or(vended).unwrap_or(!endpoint.is_empty())
  │                                              │
  └──────────────► StorageProps.path_style: bool ◄┘   (RESOLVED, unchanged, always serialized)
```

`validate_creds` gains one guard. It rejects a CONNECTION that supplies a non-empty `endpoint`,
states no `path_style`, and does not enable vending. Without that guard the default flip converts
every such CONNECTION from working to silently reading an AWS host.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Tri-state at the parse boundary, resolution at the selector | `StorageCreds::from_json` vs `StorageCreds::backend` | Both readers of one CONNECTION resolve an absent field through one selector, so they cannot disagree |
| CONNECTION-wins, then response, then derivation | `s3_backend` | The same three-step shape `endpoint` and `region` already use, with the endpoint-presence derivation demoted to last resort |
| Capability-narrowed parameter | `StaticStoreAddress` | Widening by an addressing field leaves the probe's forbidden credential spellings untouched |
| Reject rather than silently resolve | `validate_creds` | A dropped endpoint is a wrong-host read, and this repo prefers a named plan-time error over a silent fallback |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Unstated resolves to `false` on the non-vended path | Resolve unstated to `!endpoint.is_empty()`, mirroring the vended derivation | Issue #130 and the interview both specify `false`. A derivation would make the field's meaning depend on a second field, so adding an endpoint later would silently change addressing mode |
| Guard the one combination the flip newly breaks | Accept the silent breakage, as the interview anticipated | The verified endpoint-drop makes the failure a wrong-host read rather than a failed connection. A named error delivers the accepted outcome and tells the operator the one field to set |
| `StorageProps` keeps `bool` and its `true` serde default | Flip it to `false` for consistency | The field carries `#[serde(default = "default_true")]` with no `skip_serializing_if`, so the adapter always writes it and that default is reachable only from a hand-written fixture. Flipping it changes `StorageProps::default()`, which about thirty scan integration fixtures use to mean "a local path-style store" |
| Resolve in `StorageCreds::backend` | Resolve in `parse_creds` | `backend` is the one selector both readers call, and a parse that already substituted `false` would destroy the distinction the vended path reads |
| Widen `StaticStoreAddress` | Pass a fourth bare parameter to `s3_backend` | The type is the recorded one-construction home for CONNECTION values crossing into vended resolution, and a bare parameter would put that decision back at each call site |

This change introduces no new module, interface, or module boundary, so `/speq:design-philosophy`'s
Quick Diagnostic does not apply. It widens one existing type, moves one resolution point into the
selector that already owns the non-vended derivation, and adds one guard to an existing validator.
The one diagnostic question it does raise is answered above: exactly one module owns what an unstated
`path_style` means per path, and `StorageProps` is not a second owner.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/connection-credentials | CHANGED | `specs/_plans/fix-path-style-default/vs-adapter/connection-credentials/spec.md` |
| vs-adapter/pushdown-planning-cloud-credentials | CHANGED | `specs/_plans/fix-path-style-default/vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| vs-adapter/storage-backend-enum | CHANGED | `specs/_plans/fix-path-style-default/vs-adapter/storage-backend-enum/spec.md` |
| vs-adapter/catalog-crate-public-surface-extensions | CHANGED | `specs/_plans/fix-path-style-default/vs-adapter/catalog-crate-public-surface-extensions/spec.md` |

## Impact

**This release contains a BREAKING behaviour change for existing CONNECTION objects. Operators must
read § Migration before upgrading.**

- **A non-vended CONNECTION that supplies an `endpoint` and omits `path_style` stops working and now
  fails with a named error.** This is the self-hosted MinIO and Ceph shape. Before this change the
  omitted field resolved to `true` and the store was reached. After it, the CONNECTION is rejected at
  plan time with an error naming `path_style`. The operator fixes it by adding `"path_style": true`.
  The failure is loud and the fix is one field.
- **A non-vended CONNECTION that supplies no `endpoint` and omits `path_style` changes resolved
  behaviour from path-style to virtual-hosted addressing.** This is the AWS and AWS Glue shape, and
  the change is the repair issue #130 asks for. No error is raised, because nothing is discarded.
- **A vended CONNECTION that STATES `path_style` changes behaviour: its value now wins over the
  response's `s3.path-style-access`.** Before this change the CONNECTION's value was discarded on
  that path. A vended CONNECTION that omits `path_style` resolves exactly the value it resolves
  today.
- **No change for any CONNECTION that already states `path_style` on the non-vended path.** Every
  in-repo E2E and bench CONNECTION already states it, so no in-repo suite observes the flip without
  an explicit fixture edit.
- **Version bump: MINOR, not the patch a `fix` type defaults to.** `lakehouse-engine` goes to
  `0.46.0` and `lakehouse-catalog` to `0.3.0`. An operator upgrading across this version may have to
  edit a CONNECTION, and a patch-level bump would hide that. `lakehouse-catalog` also changes its
  published `StaticStoreAddress` shape.
- **Documentation changes are operator-facing.** `docs/catalogs.md`'s credential table currently
  states `path_style` defaults to `true`. That line, and the `install.md` recipe, must state the new
  default and the endpoint rule.

## Migration

| Current CONNECTION shape | Behaviour before | Behaviour after | Operator action |
|---|---|---|---|
| `endpoint` set, `path_style` absent, vending off | Path-style against the configured endpoint | Rejected at plan time, error names `path_style` | Add `"path_style": true` |
| `endpoint` set, `path_style: true`, vending off | Path-style against the configured endpoint | Unchanged | None |
| `endpoint` set, `path_style: false`, vending off | Endpoint discarded, virtual-hosted AWS host from `region` | Unchanged | None |
| `endpoint` absent, `path_style` absent, vending off | Path-style with an empty endpoint | Virtual-hosted from `region`, the AWS convention | None |
| `endpoint` absent, `path_style: false`, vending off | Virtual-hosted from `region` | Unchanged | None |
| `path_style` stated, vending on | CONNECTION value discarded, vended or derived value used | CONNECTION value wins | Remove `path_style` to keep the vended value |
| `path_style` absent, vending on | Vended value, else endpoint-presence derivation | Unchanged | None |

The installer's printed CONNECTION template (`deploy/scripts/install.sh:1478-1484`) names `warehouse`,
`region`, `access_key`, and `secret_key` and no `endpoint`. It therefore lands in the fourth row and
is improved by this change rather than broken, so it needs no edit.

## Test Disposition

The vended and non-vended spec deltas both cite this section. Every listed test is named with its
file and its disposition.

### Must change, because they encode the shipped `true` default

| Test | File | Disposition |
|---|---|---|
| `read_connection_parses_uri_and_creds` | `crates/lakehouse-engine/src/adapter/connection_tests.rs:43` | `minimal_password()` at `:27` supplies an `endpoint` and omits `path_style`, which the new guard rejects. Add `"path_style": true` to that fixture, assert `Some(true)`, and delete the stale `:56` comment "path_style defaults to true (MinIO behaviour preserved)" |
| `optional_fields_default` | `crates/lakehouse-engine/src/adapter/connection_tests.rs:188` | Assert the parsed `path_style` is `None` for a warehouse-only password |
| `optional_fields_set_when_supplied` | `crates/lakehouse-engine/src/adapter/connection_tests.rs:222` | Assert `Some(false)` rather than a bare `false` |
| `storage_block_maps_creds_to_storage_props` | `crates/lakehouse-engine/src/adapter/connection_tests.rs:250` | Keeps asserting a resolved `true`, because its fixture now states `path_style: true` |
| `absent_optional_fields_default_and_still_select_s3` | `crates/lakehouse-engine/src/adapter/connection_tests.rs:403` | Assert the resolved `path_style` is `false` for a CONNECTION stating no storage field |
| `path_style_composes_the_vended_override_with_the_resolved_endpoint` | `crates/lakehouse-catalog/src/storage_tests.rs:624` | Extend its case table with the CONNECTION-stated step, which now precedes the vended value. Its `address` helper at `:443` takes the added field |
| `static_store_address_is_reachable_and_declares_no_credential_field` | `crates/lakehouse-catalog/tests/catalog_public_surface.rs:603` | Read the added accessor, so the external vantage proves it reachable |
| `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend` | `crates/lakehouse-catalog/tests/catalog_public_surface.rs:331-363` | Its doc comment names the parameter shape and must name the third field |
| `resolve_uc_vended_storage_signature_takes_only_a_credential_free_store_address` | `crates/lakehouse-catalog/tests/catalog_public_surface.rs:377` | Its doc comment names the parameter shape and must name the third field |

### Must NOT change, because they are this plan's characterization gate

| Test | File | Property pinned |
|---|---|---|
| `vended_endpoint_without_path_style_stays_reachable_by_the_scan` | `crates/lakehouse-catalog/src/vended_tests.rs:961` | A vended endpoint that no source stated a style for still resolves `true`, so the endpoint reaches the S3 builder |
| `vended_explicit_path_style_false_wins_over_the_endpoint_coupled_default` | `crates/lakehouse-catalog/src/vended_tests.rs:988` | A response-stated `false` still beats the endpoint-presence derivation |
| `resolve_vended_storage_unparseable_path_style_without_an_endpoint_is_false` | `crates/lakehouse-catalog/src/vended_tests.rs:931` | An unparseable vended value with no endpoint still resolves `false` |
| `vended_storage_adopts_endpoint_and_path_style_from_flat_config` | `crates/lakehouse-catalog/src/vended_tests.rs:525` | The flat-`config` read is unchanged |
| Every longest-prefix and `storage-credentials`-before-`config` assertion | `crates/lakehouse-catalog/src/vended_tests.rs` | The Iceberg REST compliance evidence, which MUST NOT weaken |
| Every golden SQL fixture carrying `"path_style":true` | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs:2461`, `crates/lakehouse-engine/tests/shared_type_reexports.rs:98` and `:121`, `crates/lakehouse-engine/src/scan/spec_tests.rs:1485`, `:1527`, `:1569` | Their fixtures state `path_style: true`, so the resolved wire bytes are unchanged |
| Every `StorageProps` fixture using `..Default::default()` | About thirty files under `crates/lakehouse-engine/tests/scan_*.rs` | `StorageProps` is unchanged, so none of them move |

### Mechanical literal updates, no assertion change

`ConnectionCreds` literals stating `path_style` become `Some(...)`. Sixteen sites (in fourteen files):
`crates/lakehouse-engine/tests/common/e2e_harness.rs:377`,
`crates/lakehouse-engine/tests/e2e_unity_test.rs:341`,
`crates/lakehouse-engine/tests/shared_type_reexports.rs:60`,
`crates/lakehouse-engine/tests/cloud_e2e_test.rs:178`,
`crates/lakehouse-engine/tests/common/lakekeeper.rs:512`,
`crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs:25`,
`crates/lakehouse-engine/src/adapter/pushdown/format/format_tests.rs:15`,
`crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs:16`,
`crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs:80`,
`crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs:2048` and `:3355`,
`crates/lakehouse-engine/src/scan/sealed_tests.rs:13`,
`crates/lakehouse-catalog/tests/catalog_public_surface.rs:72`,
`crates/lakehouse-catalog/src/creds_tests.rs:40`,
`crates/lakehouse-catalog/src/test_support_tests.rs:21` and `:90`.

`StorageCreds` literals stating `path_style` become `Some(...)`. Two sites:
`crates/lakehouse-catalog/src/creds_tests.rs:91` and `:177`.

Two of those sites copy a test-harness boolean into `ConnectionCreds` and need a wrapping rather than a
literal edit: `crates/lakehouse-engine/tests/common/lakekeeper.rs:512` and
`crates/lakehouse-engine/tests/cloud_e2e_test.rs:178` read `password.path_style`, which stays a plain
boolean on `CatalogConnectionPassword`.

`CatalogConnectionPassword` (`crates/lakehouse-engine/tests/common/stack.rs:307`) keeps its plain
boolean and always serializes the key, so every E2E CONNECTION JSON already states `path_style`
explicitly. No E2E connection JSON needs a new field.

## Implementation Tasks

### 1. Credential tri-state and non-vended resolution

- [ ] 1.1 Widen `ConnectionCreds.path_style` and `StorageCreds.path_style` to `Option<bool>` in `crates/lakehouse-catalog/src/creds.rs`, keeping both `Debug` impls rendering the field's value.
- [ ] 1.2 Change `StorageCreds::from_json` to preserve the tri-state: an absent or non-boolean `path_style` yields `None`.
- [ ] 1.3 Resolve `None` to `false` inside `StorageCreds::backend`, the one selector both readers call, and state in its doc comment why the resolution lives there rather than in the parse step.
- [ ] 1.4 Update `From<&ConnectionCreds> for StorageCreds` and `parse_creds` (`crates/lakehouse-engine/src/adapter/connection.rs:246-283`) for the widened field.
- [ ] 1.5 Add the `validate_creds` guard: reject a CONNECTION supplying a non-empty `endpoint`, stating no `path_style`, with `use_vended_credentials` false. The message names `path_style`, names both values and what each reaches, and carries no credential value.
- [ ] 1.6 Update the sixteen `ConnectionCreds` and `StorageCreds` literal sites listed in § Test Disposition so the workspace compiles.
- [ ] 1.7 Write the `from_json` tri-state tests: absent yields `None`, `true` yields `Some(true)`, `false` yields `Some(false)`, and a non-boolean value yields `None`.
- [ ] 1.8 Write the `backend` resolution tests: `None` resolves to `false`, `Some(v)` resolves to `v`, and the adapter-side and scan-side readers derive a field-for-field equal backend from a password omitting `path_style`.
- [ ] 1.9 Write the guard tests: the rejection fires for endpoint-without-`path_style`; it does not fire for an explicit `false` beside an endpoint, for no endpoint, or with vending enabled; and the message names no credential value.
- [ ] 1.10 Update the five `connection_tests.rs` assertions named in § Test Disposition and delete the stale `:56` comment.

### 2. Vended CONNECTION-wins merge

- [ ] 2.1 Widen `StaticStoreAddress` (`crates/lakehouse-catalog/src/storage.rs:271-307`) with a non-`pub` `path_style: Option<bool>`, add its accessor, and extend the single `From<&ConnectionCreds>` conversion.
- [ ] 2.2 Change `s3_backend`'s `path_style` resolution to the three-step chain: the address's stated value, else the vended value, else the endpoint-presence derivation. Update its doc comment so the demotion of the derivation to last resort is stated, not inferred. [expert]
- [ ] 2.3 Confirm the Unity arm needs no edit: `uc_vended_s3` (`crates/lakehouse-catalog/src/unity/vended.rs:160`) already yields `path_style: None` into the same shared `s3_backend`, so the override reaches both catalog kinds from one implementation.
- [ ] 2.4 Write the precedence tests: a stated `Some(false)` beats a vended `true`, a stated `Some(true)` beats a vended `false`, `None` falls through to the vended value, and `None` with no vended value falls through to the endpoint-presence derivation.
- [ ] 2.5 Write a Unity-arm precedence test proving a stated CONNECTION value wins there through the same shared function.
- [ ] 2.6 Extend `crates/lakehouse-catalog/tests/catalog_public_surface.rs`: read the added accessor from the external vantage, and confirm the two source-level probes cover the third field without a hardcoded field list.

### 3. Operator documentation

- [ ] 3.1 Update `docs/catalogs.md:45` to state the new default and the endpoint rule, and check every recipe in that file states `path_style` where it configures an `endpoint`.
- [ ] 3.2 Check `docs/install.md:185` and the installer template at `deploy/scripts/install.sh:1478-1484` against § Migration, and edit only what § Migration shows is wrong.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Credential tri-state and non-vended resolution | 1.1-1.10 | — | spec delta `vs-adapter/connection-credentials`; `crates/lakehouse-catalog/src/creds.rs`, `crates/lakehouse-catalog/src/creds_tests.rs`, `crates/lakehouse-engine/src/adapter/connection.rs`, `crates/lakehouse-engine/src/adapter/connection_tests.rs`, and the sixteen literal sites in § Test Disposition |
| B: Vended CONNECTION-wins merge | 2.1-2.6 | A (shares `crates/lakehouse-catalog/src/creds.rs` and `crates/lakehouse-catalog/tests/catalog_public_surface.rs`) | spec deltas `vs-adapter/pushdown-planning-cloud-credentials`, `vs-adapter/storage-backend-enum`, `vs-adapter/catalog-crate-public-surface-extensions`; `crates/lakehouse-catalog/src/storage.rs`, `crates/lakehouse-catalog/src/storage_tests.rs`, `crates/lakehouse-catalog/src/vended_tests.rs`, `crates/lakehouse-catalog/src/unity/vended.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs` |
| C: Operator documentation | 3.1-3.2 | A, B (documents both resolution paths) | § Impact and § Migration of this plan; `docs/catalogs.md`, `docs/install.md`, `deploy/scripts/install.sh` |

The three groups run in sequence. One field's type changes in a crate both other groups depend on, so
the workspace does not compile until group A finishes. Splitting group A's literal ripple into its own
group was rejected: it shares `creds.rs` and `creds_tests.rs` with the type change, which is the
consolidation signal rather than a parallelism opportunity.

Group B is tagged expert because task 2.2 composes three precedence sources against an existing
plaintext-transport gate that reads the RESOLVED endpoint, and against the `register_side_store`
coupling that makes `path_style` the gate on whether an endpoint reaches the S3 builder at all.
Group A is deliberately untagged: its guard follows the existing `validate_creds` guard patterns in
the same file, and its literal ripple is mechanical.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Comment | `crates/lakehouse-engine/src/adapter/connection_tests.rs:56` | "path_style defaults to true (MinIO behaviour preserved)" states the removed default. Issue #130 cites this text at `connection.rs:346`, where it no longer exists |

`default_true` (`crates/lakehouse-catalog/src/creds.rs`) is deliberately RETAINED. It serves
`StorageProps`, the resolved wire type this plan does not change, and § Design records why.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| One storage-credential projection and one selector serve both readers (amended clauses) | Unit | `crates/lakehouse-catalog/src/creds_tests.rs` | `from_json_preserves_an_absent_path_style_as_unstated` |
| One storage-credential projection and one selector serve both readers (amended clauses) | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `both_readers_derive_an_equal_backend_from_a_password_omitting_path_style` |
| Optional credential fields default sensibly (amended clauses) | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `absent_optional_fields_default_and_still_select_s3` |
| Optional credential fields default sensibly (amended clauses) | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `optional_fields_default` |
| Optional credential fields default sensibly (stated value applied) | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `optional_fields_set_when_supplied` |
| A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `endpoint_without_a_stated_path_style_is_rejected_naming_the_field` |
| A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected (explicit value accepted) | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `an_explicit_path_style_beside_an_endpoint_is_accepted_under_either_value` |
| A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected (guard scope) | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `the_path_style_guard_does_not_fire_without_an_endpoint_or_under_vending` |
| A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected (no leak) | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `the_path_style_rejection_names_no_credential_value` |
| Static storage credentials are ignored, not rejected, when vending is requested (amended clauses) | Unit | `crates/lakehouse-catalog/src/storage_tests.rs` | `a_stated_connection_path_style_wins_over_the_vended_value` |
| Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set (amended clauses) | Unit | `crates/lakehouse-catalog/src/storage_tests.rs` | `path_style_resolves_the_connection_then_the_vended_value_then_the_endpoint_derivation` |
| Vended-credentials request advertises access delegation (independence) | Unit | `crates/lakehouse-catalog/src/storage_tests.rs` | `a_stated_path_style_resolves_independently_of_the_resolved_endpoint` |
| Vended-credentials request advertises access delegation (explicit false honoured) | Unit | `crates/lakehouse-catalog/src/storage_tests.rs` | `a_stated_false_path_style_beside_a_resolved_endpoint_is_honoured` |
| Vended-credentials request advertises access delegation (Unity arm) | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `a_stated_connection_path_style_wins_on_the_unity_vended_arm` |
| The vended selectors take a store address that cannot carry a credential (amended clauses) | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `static_store_address_is_reachable_and_declares_no_credential_field` |
| The vended selectors take a store address that cannot carry a credential (field privacy) | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `static_store_address_fields_are_not_public` |
| The vended store-address type extends the crate's public surface through an explicit reviewed edit (amended clauses, Iceberg arm) | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend` |
| The vended store-address type extends the crate's public surface through an explicit reviewed edit (amended clauses, Unity arm) | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `resolve_uc_vended_storage_signature_takes_only_a_credential_free_store_address` |
| Non-vended MinIO CONNECTION stating path_style still queries end to end | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | existing suite, unchanged, run against the Docker Exasol container |

### Manual Testing

Run against the local Docker Exasol container with the bundled MinIO and Iceberg REST stack.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/connection-credentials (guard) | `CREATE OR REPLACE CONNECTION C1 TO 'http://iceberg-rest:8181' USER '' IDENTIFIED BY '{"warehouse":"s3://warehouse/","endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin"}';` then `CREATE VIRTUAL SCHEMA V1 USING ... WITH CATALOG_CONNECTION = 'C1' ALLOW_HTTP = 'true';` | The `CREATE VIRTUAL SCHEMA` fails with an error naming `path_style` and stating what `true` and `false` each reach. The error contains no credential value |
| vs-adapter/connection-credentials (stated value) | Recreate `C1` with `"path_style": true` added, then `CREATE VIRTUAL SCHEMA V1 ...` and `SELECT COUNT(*) FROM V1.LINEITEM;` | The virtual schema is created and the query returns the row count from MinIO |
| vs-adapter/connection-credentials (AWS shape) | Create a CONNECTION supplying `region` and no `endpoint` and no `path_style`, then `EXPLAIN VIRTUAL SELECT * FROM V2.T;` | The pushed scan SQL carries `"path_style":false` in its storage block |
| vs-adapter/pushdown-planning-cloud-credentials | Against the Lakekeeper vended stack, recreate its CONNECTION with `"path_style": false` added, then `EXPLAIN VIRTUAL SELECT * FROM V3.T;` | The pushed scan SQL carries `"path_style":false` even though the Lakekeeper response vends `s3.path-style-access: true` |
| vs-adapter/pushdown-planning-cloud-credentials (characterization) | Against the same stack with the CONNECTION's `path_style` removed, run the same `EXPLAIN VIRTUAL` | The pushed scan SQL carries `"path_style":true`, the vended value, unchanged from before this plan |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
