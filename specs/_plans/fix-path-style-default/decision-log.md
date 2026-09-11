# Decision Log: fix-path-style-default

## Interview

**Q:** Full scope of #130 (flip default + `Option<bool>` widening + vended admission, superseding a recorded rejection), narrower default-only fix, or see tradeoffs first?
**A:** See tradeoffs first.

**Q:** Given the tradeoffs, which option?
**A:** Option A, the full scope. Root cause fix, bounded mechanical ripple, avoids reopening the question later.

Established facts: the live defaulting site is `StorageCreds::from_json` (`creds.rs:214-217`), not `connection.rs:346`. The "MinIO behaviour preserved" comment survives only in `connection_tests.rs:56`. `StorageCreds` is the one projection both readers use.

## Design Decisions

### [1] `path_style` becomes `Option<bool>`, resolved at the one selector

- **Decision:** `ConnectionCreds.path_style` and `StorageCreds.path_style` become `Option<bool>`. `from_json` preserves the tri-state; `backend()` resolves it.
- **Alternatives:** Resolve in `parse_creds` (destroys the distinction the vended path reads). Keep `bool` and flip only the default (rejected by interview).
- **Rationale:** The recorded one-projection/one-selector contract already guarantees both readers agree. Placing the resolution there makes that hold for absent values too.
- **Promotes to ADR:** no

### [2] Unstated resolves to `false`, not to an endpoint-presence derivation

- **Decision:** On the non-vended path, unstated `path_style` resolves to `false`.
- **Alternatives:** Mirror the vended derivation (`!endpoint.is_empty()`). Rejected: on this path the CONNECTION is the only source, so a derivation would couple one field's meaning to another.
- **Rationale:** #130 and the interview both specify `false`. Matches AWS SDK convention.
- **Promotes to ADR:** no

### [3] Endpoint without stated `path_style` is rejected on the non-vended path

- **Decision:** `validate_creds` rejects a non-vended CONNECTION with a non-empty `endpoint` and no stated `path_style`. Explicit `false` beside an endpoint stays accepted.
- **Alternatives:** Accept the silent breakage. Rejected: `build_undecorated_store` (`object_store.rs:226-231`) passes `endpoint` to `AmazonS3Builder` only inside `if storage.path_style`, so a resolved `false` discards the endpoint entirely (wrong host, not just wrong addressing style).
- **Rationale:** Fires on exactly the combination the default flip newly breaks. No in-repo fixture trips it.
- **Promotes to ADR:** no

### [4] CONNECTION's stated `path_style` admitted into the vended CONNECTION-wins rule

- **Decision:** `s3_backend` resolves `path_style` in three steps: CONNECTION stated value > vended `s3.path-style-access` > endpoint-presence derivation.
- **Alternatives:** Keep the field excluded (type limitation no longer exists). Delete the endpoint-presence derivation (rejected: `register_side_store` needs it when no source states a style).
- **Supersedes:** "path_style does not read the CONNECTION, because the field cannot express 'unstated'" — both premises are now false.
- **Rationale:** Discharges the type limitation that kept `path_style` out. Demoting the derivation to last resort preserves current behavior for CONNECTIONs that omit the field.
- **Promotes to ADR:** yes

### [5] `StorageProps` keeps `bool` with its `true` serde default

- **Decision:** `StorageProps` unchanged. `default_true` retained.
- **Alternatives:** Flip serde default to `false`. Rejected: the adapter always serializes the field, so the default is unreachable from adapter-produced payloads. Flipping it also breaks ~30 scan fixtures using `..Default::default()`.
- **Promotes to ADR:** no

### [6] Store address widens by a field, not by a parameter

- **Decision:** `StaticStoreAddress` gains `path_style: Option<bool>` and one accessor. `s3_backend`'s signature unchanged.
- **Alternatives:** Pass a fourth bare parameter. Rejected: the type is the recorded one-construction home, and a bare parameter puts that decision back at each call site.
- **Promotes to ADR:** no

### [7] Version bump is MINOR

- **Decision:** `lakehouse-engine` 0.45.0 → 0.46.0, `lakehouse-catalog` 0.2.0 → 0.3.0.
- **Rationale:** An operator may need to edit a CONNECTION. `lakehouse-catalog` also changes `StaticStoreAddress`'s published shape.
- **Promotes to ADR:** no

### [8] Four features receive deltas

- **Decision:** Deltas cover `connection-credentials`, `pushdown-planning-cloud-credentials`, `storage-backend-enum`, and `catalog-crate-public-surface-extensions`.
- **Rationale:** Two additional recorded clauses state the store-address type declares EXACTLY two fields. Leaving them standing beside a three-field type is a spec contradiction.
- **Promotes to ADR:** no

### [9] Iceberg/Delta spec-compliance checked, neither triggered

- **Decision:** Both deltas record that `s3.path-style-access` appears nowhere in the Iceberg REST spec (readable only under `additionalProperties: {type: string}`) and Delta's `PROTOCOL.md` contains no `path-style`/`path_style`/`virtual-hosted` occurrence.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Test Disposition cited wrong test name and omitted UC variant

- **Finding:** Plan named `resolve_vended_storage_signature_takes_only_a_credential_free_store_address` at `catalog_public_surface.rs:331-363`; the actual test there is `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend`. The UC variant at `:377` was omitted. Count mismatch: "fourteen sites" vs sixteen enumerated references.
- **Direction change:** Corrected the name, added the UC variant row, fixed count to "sixteen sites (in fourteen files)".
- **Promotes to ADR:** no
