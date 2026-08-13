# Code Review Findings: add-unity-e2e-ci-job

## Summary
- Files reviewed: 2 (`.github/workflows/ci.yml`, `.github/actions/e2e-setup/action.yml`)
- Total findings: 6 (standard: 5, expert: 1)

Verified clean, no finding raised: YAML parses and the wiring is correct
(`e2e-unity needs: ['build-so']`, `release needs: [e2e, e2e-lakekeeper, e2e-unity, lint,
unit-tests, licenses, install-script, install-script-e2e]`);
`cargo clippy --workspace --all-targets --features unity-e2e -- -D warnings` exits 0;
the test step at `ci.yml:731` is byte-identical to `Makefile:250`; the bring-up mirrors
`e2e-lakekeeper`'s one-shot-then-`--wait` sequencing, exit-code check, `|| true`-guarded
diagnostics, artifact upload and `if: always()` teardown; `unitycatalog` has a healthcheck
so `--wait` is meaningful; `seed.sh`'s `LH_NETWORK` default matches the pinned compose
network `name: lakehouse-engine` (`docker-compose.yml:175`) and `LH_UNITY_PORT` default
matches `docker-compose.unity.yml:64`; the coverage command is untouched;
`.github/actions/e2e-setup/action.yml` is a correct one-line description update with no
other change. The inlined `cargo test` (rather than `make test-e2e-unity`), the hardcoded
`lakehouse-engine-rs-minio-init-1` container name, and the omission of the other four e2e
features from the lint step are all settled by `plan.md` § Consequences and are not raised.

## Standard fixes

### .github/workflows/ci.yml

#### [OUTDATED_COMMENT] Two jobs are both labelled "third viability gate"
- Location: line 770
- Issue: the file numbers its e2e gates in an ordinal sequence used as navigation —
  `# ── viability gate:` (461), `# ── second viability gate:` (569),
  `# ── third viability gate:` (677, the new `e2e-unity`). Inserting `e2e-unity` before
  `e2e-azure` made the pre-existing `e2e-azure` banner at line 770 wrong: it still reads
  `# ── third viability gate: REAL Azure ADLS Gen2 storage, local catalog ────`, so the
  file now contains two "third" gates and no "fourth". `e2e-azure` is the fourth gate.
  The defect was created by this change even though line 770 is not in the diff.
- Fix: In `.github/workflows/ci.yml` line 770, change `# ── third viability gate: REAL
  Azure ADLS Gen2 storage, local catalog ────` to `# ── fourth viability gate: REAL Azure
  ADLS Gen2 storage, local catalog ───`, keeping the trailing box-drawing rule the same
  total width as the neighbouring banners.

#### [OUTDATED_COMMENT] `build-so`'s cascade-skip enumeration omits `e2e-unity`
- Location: line 38
- Issue: line 38 reads
  `# e2e / e2e-lakekeeper / e2e-azure cascade-skip via `needs: [build-so]`.`
  and enumerates every job that inherits `build-so`'s draft-PR skip. `e2e-unity` has
  `needs: [build-so]` (verified: `e2e-unity needs: ['build-so']`) and cascade-skips
  identically, but is not named — so the comment under-reports the set it exists to
  enumerate. This is the same stale-enumeration defect task 1.2 corrected at lines
  281-283 for the coverage step; the change fixed one instance of it and left this one.
- Fix: In `.github/workflows/ci.yml` line 38, change
  `# e2e / e2e-lakekeeper / e2e-azure cascade-skip via `needs: [build-so]`.` to
  `# e2e / e2e-lakekeeper / e2e-unity / e2e-azure cascade-skip via `needs: [build-so]`.`

#### [OUTDATED_COMMENT] Clippy comment claims "a tracked follow-up" that nothing tracks
- Location: line 220
- Issue: line 220 asserts the other four e2e features `remain unchecked here — a tracked
  follow-up, not fixed by this change`. No tracker exists: the only issue this plan
  opened is #336 (`Add `E2E (Unity)` to main's required status checks`), and a search of
  all open repository issues returns nothing covering the lint-features widening. Per
  `decision-log.md:133` the deferral is recorded only in `plan.md § Dependencies`, which
  `/speq:record` archives — so the comment's claim will be false in the tree with no
  surviving reference. It also contradicts the file's own convention four lines below,
  where the comparable deferral does cite an issue (`tracked in #336`, mirroring
  `tracked in #252` at line 398), and the project convention that a tracked exception is
  a GitHub issue cited inline.
- Fix: In `.github/workflows/ci.yml`, open a GitHub issue titled `Type-check the remaining
  e2e test suites in the lint job` whose body states that
  `cargo clippy --workspace --all-targets` enables only `unity-e2e`, that the other e2e
  features gate 14 further test files that therefore compile to empty crates, and that
  `--features exasol-e2e,cloud-e2e,lakekeeper-e2e,azure-e2e,unity-e2e` was verified
  clippy-clean; then change line 220 from
  `# unchecked here — a tracked follow-up, not fixed by this change.` to
  `# unchecked here — tracked in #<n>.`, substituting the new issue number.

#### [REDUNDANT_COMMENT] Clippy comment restates the e2e-feature list already maintained 64 lines below
- Location: line 219
- Issue: line 219 spells out `(exasol-e2e, cloud-e2e, lakekeeper-e2e, azure-e2e)`, the
  same feature list the coverage-step comment maintains at lines 281-283 — a list this
  very change had to correct there for having gone stale after #327. The diff therefore
  adds a second hand-maintained copy of the identical fact, so the next feature addition
  must edit both or reintroduce exactly the staleness task 1.2 just fixed. The comment's
  plan-mandated content (task 1.1: "the other four e2e features remain unchecked as a
  tracked follow-up") is fully carried by the count alone; naming them is not required.
- Fix: In `.github/workflows/ci.yml`, delete the parenthetical enumeration on line 219 so
  lines 218-219 read `# Docker stack, no dependencies (`unity-e2e = []`). The other four
  e2e` / `# features remain`, leaving the coverage-step comment at lines 281-283 as the
  file's single enumeration of the e2e features. Reflow the block to the surrounding
  ~76-column comment width and do not touch the `- name: Clippy` step or its `run:` line.

#### [WORK_TRACKING_COMMENT] Orphaned `# #336.` continuation line
- Location: lines 683-684
- Issue: the issue reference is split across two lines as
  `# Not yet in `main`'s ruleset required checks — tracked in` / `# #336.`, leaving
  `# #336.` alone on line 684. The full sentence is 62 characters and fits inside the
  file's ~76-column comment width, so the break is a placeholder-substitution artifact,
  not a wrap. Every comparable reference in the file sits inline
  (`tracked in #252`, line 398). The reference itself is mandated by plan task 5.1 and
  must stay.
- Fix: In `.github/workflows/ci.yml`, replace lines 683-684 with the single line
  `  # Not yet in `main`'s ruleset required checks — tracked in #336.` Keep the blank
  comment separator line 682 and the `e2e-unity:` job key on the following line unchanged.

## Expert fixes

### .github/workflows/ci.yml

#### [INFORMATION_LEAKAGE] The flag-identity contract for the unity cargo line is declared in only one direction
- Location: lines 726-731
- Issue: the step comment declares `MUST stay flag-identical to the Makefile's
  `test-e2e-unity` target's cargo line; this step is the authority`, duplicating the
  invocation at `Makefile:250` with nothing but that one-sided comment holding the two
  copies in agreement. `plan.md` § Consequences justifies the duplication by citing the
  coverage idiom as precedent, but that idiom is bidirectional: `ci.yml:283` says "Keep
  flag-identical to the Makefile's `coverage` target" **and** `Makefile:192-194` carries
  the reciprocal "MUST stay flag-identical to the `cargo llvm-cov` step in ci.yml's
  unit-tests job, which is the authority". Only the ci.yml half was reproduced here, so a
  developer editing `Makefile:250` — adding a `--features` flag or changing
  `--test-threads` — gets no signal that CI carries a second copy, and CI silently keeps
  running the old flags. The failure mode is a green CI run over a command the Makefile no
  longer describes, which is precisely what the precedent's second half prevents.
- Fix: Add the reciprocal notice to `Makefile` immediately above `test-e2e-unity:` (line
  248), inside the existing comment block, stating that the `cargo test` line MUST stay
  flag-identical to the `Run Unity Catalog E2E suite` step in `ci.yml`'s `e2e-unity` job,
  which is the authority — mirroring the wording of the `coverage` target's comment at
  `Makefile:192-194`. Add no new target, change no recipe line, and leave `unity-up` and
  the `test-e2e-unity` recipe bodies byte-identical (`plan.md` § Non-Goals forbids
  behavioral changes to both targets; this is a comment-only addition).
