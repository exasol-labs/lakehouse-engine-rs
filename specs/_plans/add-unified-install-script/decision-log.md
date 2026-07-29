# Decision Log: add-unified-install-script

## Interview

No live interview. This plan was authored **after** the implementation, from the shipped
`deploy/scripts/install.sh` and `deploy/scripts/tests/install.test.sh`, so every decision below was
read out of working code and its tests rather than proposed for future work. Where a decision was
inherited from the never-merged PR #141 (`origin/feat/add-saas-install-script`), the inherited ADR
is named and its re-scoping is stated explicitly.

**Q:** One script or two — SaaS and BucketFS are different upload channels?
**A:** One. The channels differ; nothing else does. See decision [1].

**Q:** How does the script know which target it is installing to?
**A:** From the flags. Both SaaS ids ⇒ SaaS; neither ⇒ BucketFS; exactly one ⇒ error. `--target` is
an assertion, never a selector. See decision [1].

**Q:** How does BucketFS I/O happen, given the SaaS side is raw `curl`?
**A:** Only through `exapump bucketfs cp|ls`. Never a raw HTTP PUT. See decision [7].

**Q:** How is the installer itself distributed, given this repo is private?
**A:** Through the authenticated GitHub contents API on `main`, with `?ref=<tag>` for pinning — not
as a release asset. See decision [4].

**Q:** Which credentials does the installer derive, and which must the operator supply?
**A:** On SaaS it derives the REST PAT from the connectivity credential (they are the same secret).
It never derives, and never reads the value of, the BucketFS write password. See decisions [5] and
[6].

**Q:** Is the Rust SLC installed by default?
**A:** Yes; `--skip-slc` opts out. See decision [9].

## Design Decisions

### [1] One script, target mode auto-detected from the SaaS id pair; `--target` asserts only

- **Context:** SaaS reaches its bucket through a control-plane REST API and a presigned-URL
  exchange. Exasol AsApp, Docker, and on-premise all reach theirs through the raw BucketFS HTTP interface
  that `exapump bucketfs` already speaks. Those two upload channels share no verb, no path grammar,
  and no credential — but prerequisites, connectivity, version resolution, `SCRIPT_LANGUAGES`, the
  DDL, the smoke test and the next-step template are identical across them.
- **Decision:** Ship one `deploy/scripts/install.sh`. `resolve_target_mode` detects the mode from
  the flags: **both** `--account-id` and `--database-id` ⇒ `saas`; **neither** ⇒ `bucketfs` (the
  default target); **exactly one** ⇒ a hard error naming both flags. `resolve_target_layout` then
  seeds four `TARGET_*` globals that every later step reads, and exactly one function —
  `upload_artifact` — branches on `TARGET_MODE` for I/O. The optional `--target <saas|bucketfs>`
  flag never selects a mode; it only fails the run when the caller's stated intent disagrees with
  what the flags actually describe.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | One script, mode auto-detected from the SaaS ids, `--target` as an assertion | ✓ Chosen — the ids are the one input a SaaS install needs that a BucketFS install cannot use, so they already carry the signal; the assertion catches a mistaken intent without introducing a second source of truth |
  | Two scripts (`install-saas.sh` + `install-bucketfs.sh`) | ✗ Rejected — ~85% of the body would be duplicated, and the shared half (SCRIPT_LANGUAGES, DDL, smoke test, template) would drift apart silently |
  | One script with a **required** `--target` | ✗ Rejected — the flag's value is already implied by the presence or absence of the SaaS ids, so requiring it adds a step whose only failure mode is disagreeing with the ids |
  | `--target` selects the mode when given | ✗ Rejected — two sources of truth that can disagree silently; the failure would surface as a wrong `%udf_object` at first query, not at install time |

- **Consequences:** Adding a third target is a new `case` arm in `resolve_target_mode` plus one in
  `resolve_target_layout`, not a new `if` in ten places. The cost is that a caller who forgets one
  SaaS id gets a targeting error rather than a SaaS-specific one — mitigated by naming both flags
  and the SaaS web console in the message. A caller who passes a cross-mode flag that its resolved
  mode does not use (for example `--staging` on a BucketFS install, or any `--bfs-*` flag on a SaaS
  install) gets a targeting error, same as `--target` disagreement and the partial-id pair. See
  `packaging/install-script-targeting/spec.md`.
- **Promotes to ADR:** yes

### [2] Idempotent `SCRIPT_LANGUAGES` read-modify-write with a **mode-resolved** segment

- **Context:** Carries forward ADR `008-add-saas-install-script.md` from the never-merged PR #141,
  generalized. `ALTER SYSTEM SET SCRIPT_LANGUAGES` replaces the entire persisted value, and a real
  database already registers other languages (`PYTHON3`, `JAVA`, …) before the installer runs. The
  original ADR could describe the RUST segment as "a fixed literal", because that installer only
  ever targeted SaaS. That is no longer true: the SaaS segment addresses `uploads/default/rustslc`
  and the BucketFS segment addresses `bfsdefault/<bucket>/slc/lakehouse-rustslc`, so the segment
  is a **mode-resolved value** (`TARGET_RUST_LANG_SEGMENT`), not a constant. What stays constant is
  that the segment carries no SLC version: the version lives in the uploaded tarball's content, not
  in the alias string.
- **Decision:** Read the persisted `SCRIPT_LANGUAGES` from `EXA_PARAMETERS` first, then compute the
  new value with `compute_script_languages(current, segment)` — append `segment` if no `RUST=` entry
  exists, or replace the single existing `RUST=` entry **in place**, preserving every other entry
  and its original order — and only then issue `ALTER SYSTEM SET`. A read that succeeds but yields
  an empty or unparseable value is treated as an anomaly and hard-fails; it never proceeds, because
  computing from an empty current value would wipe every pre-existing language.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | Read-modify-write, appending or replacing the single RUST segment in place | ✓ Chosen — a real cluster already registers other languages; preserving them and de-duplicating RUST makes a re-run safe and an upgrade a no-op on everything else |
  | Set a fixed `SCRIPT_LANGUAGES` string | ✗ Rejected — `ALTER SYSTEM SET` replaces the entire value, so a blind write drops `PYTHON3`/`JAVA`/`R` and breaks every other UDF on the database |
  | Append unconditionally, without the replace branch | ✗ Rejected — a second run would leave two `RUST=` entries; Exasol resolves one of them and the other is dead configuration |
  | Treat an empty read as "no languages yet" and proceed | ✗ Rejected — a live Exasol always has at least one language registered, so an empty read is an unexpected output shape, and proceeding would silently wipe the real value |

- **Consequences:** Every registration costs one extra `exapump sql` round-trip. Re-running the
  installer against a database with other languages, or against one from a prior installer run,
  never drops or duplicates an entry. Because the segment is now mode-resolved, a database
  installed once through the SaaS path and once through the BucketFS path would see the RUST entry
  **replaced**, not duplicated — which is correct: only one of the two paths' `.so` locations is
  live at a time.
- **Promotes to ADR:** yes

### [3] `GITHUB_TOKEN` + plain `curl` for every GitHub access, replacing the `gh` CLI

- **Context:** Carries forward ADR `009-change-saas-install-github-token.md` from PR #141
  unchanged in substance, and extends its reach. The original decision replaced `gh` for release
  resolution and asset download, because requiring `gh auth login` before a `curl | bash` one-liner
  defeats the point of a one-liner. It is now also load-bearing for **script distribution**:
  decision [4] fetches `install.sh` itself through the same authenticated contents API with the
  same token.
- **Decision:** Every GitHub access — the installer fetch, `releases/latest` resolution,
  release-by-tag JSON for the asset-id lookup, and the asset download — goes through `curl` against
  the documented GitHub REST API, authenticated with a bearer token from `--github-token` or
  `GITHUB_TOKEN`. The same token is sent to the private engine repo and to the public
  `language-container-rs`, keeping exactly one authenticated code path. Asset downloads use plain
  `-L` and never `--location-trusted`, so `curl` strips the `Authorization` header across the
  redirect to signed storage — which is required, since that host authenticates the signed URL and
  rejects a second auth mechanism.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | `GITHUB_TOKEN` + plain `curl` for all GitHub access | ✓ Chosen — a provisioned-token user expects a portable `curl \| bash`; one code path avoids dual-path drift, and the same token then also carries the script fetch |
  | Keep `gh` (PR #141's own predecessor choice) | ✗ Superseded — forces every user, CI runner, and agent to install and authenticate a second CLI before the one-liner works |
  | Keep `gh`, add `GITHUB_TOKEN`/`curl` as an optional fallback | ✗ Rejected — a dual path doubles the test surface and drifts |
  | `--location-trusted` on the asset download | ✗ Rejected — forces the `Authorization` header through the cross-host redirect, which breaks the signed storage URL |

- **Consequences:** The prerequisite list is `exapump` + `curl` (+ `tar` on the BucketFS target).
  Resolving a release asset without `jq` needs a bounded bash-regex scan of GitHub's stably
  pretty-printed JSON (`extract_asset_id_by_name`, bounded to the `assets` array and to the
  6-space field depth so a nested `uploader.id` can never be mistaken for the asset id). Every
  install now depends on a supplied token rather than on an ambient `gh` session.
- **Promotes to ADR:** yes

### [4] Distribute the installer through the GitHub contents API on `main`, `?ref=<tag>` to pin

- **Context:** The repository is private, so no plain `raw.githubusercontent.com` or
  `releases/download/…` URL works — the one-liner must authenticate. That leaves two authenticated
  homes for the script: a file on a branch, read through the contents API, or a release asset
  attached to each release.
- **Decision:** Publish the one-liner as a contents-API fetch of `deploy/scripts/install.sh` on
  `main`:
  `curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github.raw" https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/contents/deploy/scripts/install.sh | bash -s -- …`.
  Reproducibility is served by appending `?ref=<tag>` to that same URL, which pins the *script*, and
  by `--lakehouse-version` / `--slc-version`, which pin the *artifacts*. Because there is no release
  gate between merging to `main` and a user's next fetch, both install-script CI jobs are required
  checks **and** block the `release` job.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | Contents API on `main`, `?ref=<tag>` for pinning | ✓ Chosen — an installer bug is fixed for everyone the moment it merges, while a user who needs a byte-identical install can still pin the script and the artifacts independently |
  | Attach `install.sh` as a release asset on every release | ✗ Rejected — freezes the installer at its release-time state, so an installer bug ships forever inside every past release; a user installing an old engine version would be forced through the old, broken installer |
  | A separate public distribution repo holding only the installer | ✗ Rejected — a second repo to keep in sync, and the token is needed anyway for the private engine's release assets |
  | Both: contents API **and** a release asset | ✗ Rejected — two artifacts that can disagree, and no rule for which one a user should trust |

- **Consequences:** `main` is a live distribution channel: any merge to `deploy/scripts/install.sh`
  reaches the next user immediately, with no review-by-release in between. That is precisely why
  the two CI jobs gate `release` (a comment in `.github/workflows/ci.yml` records the reason at the
  job). The `?ref=<tag>` form and the two version flags are documented together in `docs/install.md`,
  because pinning only the script or only the artifacts gives a mismatched pair.
- **Promotes to ADR:** yes

### [5] Derive the SaaS REST PAT from the connectivity credential — **SaaS mode only**

- **Context:** Carries forward ADR `010-fix-saas-install-pat-password-redundancy.md` from PR #141
  verbatim in substance, but its scope must now be stated explicitly rather than implied. That ADR
  was written for a SaaS-only installer, so "the PAT *is* the SQL password — there is exactly one
  secret" needed no qualification. In a two-target installer it does: the reasoning holds **only**
  for the SaaS control-plane PAT, and must not be read as a general "derive credentials from the
  connectivity mode" rule. Decision [6] is its deliberate counterpart.
- **Decision:** There is no `--pat`/`EXASOL_PAT` flag. In SaaS mode only, `resolve_saas_pat` sets
  `RESOLVED_PAT` from whichever connectivity mode was chosen: `--password` directly (host mode); the
  DSN's password segment, percent-decoded (dsn mode); or the named profile's `password` key read out
  of `exapump`'s own config at `${EXAPUMP_CONFIG:-$HOME/.exapump/config.toml}` (profile mode). A
  derivation that fails errors immediately and names the mode; it never falls through to another
  mode and never proceeds with an empty value. The resolved value is never printed.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | Derive `RESOLVED_PAT` per connectivity mode, parsing `exapump`'s config for profile mode | ✓ Chosen — on Exasol SaaS the PAT and the SQL password are the identical secret, so asking for both is pure duplication and a second place to leak it |
  | Keep `--pat`/`EXASOL_PAT` as a separate required input | ✗ Superseded — its stated rationale ("REST and SQL are distinct auth surfaces") is false for every connectivity mode this installer supports on SaaS |
  | Add a `jq` or TOML-parsing dependency to read the profile | ✗ Rejected — the prerequisite list is bash + curl + exapump; one bounded-regex section scan (`read_profile_key`) needs no new dependency |
  | Fall back silently through DSN → profile → empty when derivation fails | ✗ Rejected — masks a real misconfiguration as a confusing downstream 401/403 from the SaaS REST API instead of a named installer-side error |
  | Generalize the derivation to every credential the installer needs | ✗ Rejected — see decision [6]; the BucketFS write password is a genuinely different secret |

- **Consequences:** One credential per connectivity mode instead of two, and one fewer place to
  leak it. The trade-off is a coupling to `exapump`'s config location and TOML shape
  (`EXAPUMP_CONFIG` override, flat `[profile]` sections, a `password` key) — confirmed to be the
  exact override `exapump` itself honors; if `exapump` ever changes either, `resolve_saas_pat`'s
  profile branch must change with it. This decision supersedes PR #141's own predecessor row that
  chose a separate `--pat` input over config-file derivation.
- **Promotes to ADR:** yes

### [6] The BucketFS write password is never derived and never read for its value — presence only

- **Context:** The deliberate counterpart to decision [5]. It would be superficially consistent to
  extend PAT derivation to the BucketFS write password: read it out of the profile the same way and
  hand it to `exapump`. That consistency would be wrong. Unlike the SaaS PAT, the BucketFS write
  password is not the SQL password — it is a separate secret whose home is `exapump`'s own config
  (`bfs_write_password`), and `exapump bucketfs` already resolves it from there without the
  installer's help.
- **Decision:** `validate_bucketfs_required` checks only that the write password is **obtainable**,
  never what it is. In profile connectivity mode it calls `read_profile_key <profile>
  bfs_write_password` and discards the value, keeping only the success/failure; in dsn or host
  connectivity mode — where `exapump bucketfs` has no profile to fall back on, because it accepts no
  DSN or user/password flags of its own — it requires `--bfs-write-password` and `--bfs-host` to be
  supplied explicitly. When the value is supplied on the command line it is forwarded verbatim to
  `exapump` by `exapump_bfs_flags` and never re-read, re-derived, or logged.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | Presence check only; let `exapump` own the value | ✓ Chosen — one owner for the secret, and the installer still fails early and by name when it is missing, which is the only thing the installer actually needs to know |
  | Read `bfs_write_password` out of the profile and pass it explicitly to `exapump` | ✗ Rejected — the installer would hold and forward a secret that `exapump` already resolves for itself, putting it into an argv the installer constructs and into every error path the installer prints |
  | Skip the check and let `exapump bucketfs cp` fail | ✗ Rejected — the failure would land after the release download and after the reachability preflight, and would surface as an HTTP 403 rather than as "you did not configure a write password" |
  | Derive it from the SQL password, mirroring the SaaS PAT | ✗ Rejected — they are different secrets on every BucketFS-reachable deployment; a coincidental match on one test system would enshrine a wrong rule |

- **Consequences:** The installer's own error message can name the missing key, the flag, and the
  exact `[profile]` section in the exact config file, before spending a byte. Its knowledge of the
  secret stops at "present or not". The one supported case it cannot express is a write password
  containing whitespace, because `exapump_bfs_flags` is deliberately word-split by its caller —
  documented at the function, with the profile's `bfs_write_password` as the workaround.
- **Promotes to ADR:** yes

### [7] All BucketFS I/O goes through `exapump bucketfs`, never a raw HTTP PUT

- **Context:** BucketFS is reachable over plain HTTP with basic auth
  (`curl -X PUT -T f https://w:<pass>@<host>:<port>/<bucket>/<path>`), and `docs/install.md`'s
  manual appendix documents exactly that as a hand-operated channel. The installer already uses raw
  `curl` for the SaaS side, so reusing it here would need no new dependency.
- **Decision:** Every BucketFS read and write goes through `exapump bucketfs cp|ls`, funnelled
  through one wrapper (`exapump_bucketfs`) that appends the run's connectivity flag and only the
  `--bfs-*` overrides the caller actually supplied. No raw HTTP PUT and no raw `curl` ever touches
  BucketFS. Paths passed to `exapump` are **bucket-relative** with no leading slash and no bucket
  segment (`udf/liblakehouse_engine.so`, `slc/lakehouse-rustslc.tar.gz`), because `exapump` builds
  its URL as `<scheme>://<bfs-host>:<bfs-port>/<bucket>/<path>` and takes the bucket from
  `--bfs-bucket` or the profile.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | `exapump bucketfs cp|ls` only | ✓ Chosen — reuses the workspace's owned CLI and its credential, TLS and bucket resolution; keeps the write password out of a URL the installer builds and out of its error output; matches the workspace rule that `exapump` is the go-to for all Exasol and BucketFS interaction |
  | Raw `curl -X PUT` with the write password in the URL userinfo | ✗ Rejected — puts the secret in a process argv and in every diagnostic; re-implements TLS handling, bucket resolution and self-signed-certificate handling that `exapump` already owns |
  | Both, with `exapump` preferred and `curl` as a fallback | ✗ Rejected — a fallback path that only runs when the primary is broken is a path that is never tested |
  | Bucket-qualified paths (`/default/udf/…`) passed to `exapump` | ✗ Rejected — verified against a live container: `exapump bucketfs cp f /default/x/f` with bucket `default` creates `default/x/f` *inside* the default bucket, and `exapump bucketfs ls /default` fails with "Path not found" |

- **Consequences:** The BucketFS target inherits `exapump`'s prerequisite that at least one profile
  exist in `~/.exapump/config.toml` — **even in `--dsn` or `--host` connectivity mode with every
  `--bfs-*` flag given explicitly** (verified live: `exapump bucketfs ls` fails with
  `Error: No profiles found in config`). That is a real, documented prerequisite of the BucketFS
  target, not an edge case. The bucket name still appears in the `%udf_object` and RUST-alias
  strings, because those are read by the Exasol engine rather than by `exapump` — which is exactly
  the asymmetry decision [10] exists to keep consistent.
- **Promotes to ADR:** yes

### [8] SaaS uploads the engine **tarball**; BucketFS uploads the **bare `.so`**; the SLC is a tarball in both

- **Context:** BucketFS auto-extracts an uploaded `X.tar.gz` into a sibling directory `X`, adding
  one path level. The engine release asset is `lakehouse-engine.tar.gz`, containing
  `udf/liblakehouse_engine.so`. Two artifact shapes are therefore possible per target, and the
  `%udf_object` string must match whichever is chosen.
- **Decision:** Asymmetric by target, and only for the engine:
  * **SaaS** uploads the tarball as-is to the files API and lets the SaaS bucket auto-extract it;
    `%udf_object` is
    `/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so`.
  * **BucketFS** extracts the archive locally with `tar -xzf` (`extract_engine_so`, which fails by
    name if `udf/liblakehouse_engine.so` is absent or empty) and uploads the **bare `.so`** to
    `udf/liblakehouse_engine.so`; `%udf_object` is
    `buckets/bfsdefault/<bucket>/udf/liblakehouse_engine.so`.
  * The **SLC** goes up as a tarball in **both** modes, because the RUST alias points at the
    extracted `rustslc` / `lakehouse-rustslc` directory rather than at the archive — so BucketFS
    auto-extraction is load-bearing for the SLC and only for the SLC.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | Tarball on SaaS, bare `.so` on BucketFS, tarball for the SLC in both | ✓ Chosen — `udf/liblakehouse_engine.so` is the exact path `make bucketfs-upload-so` and every E2E `%udf_object` already use, so the installed layout is the layout the test suite has always exercised |
  | Upload the tarball in both modes and rely on auto-extraction for the engine too | ✗ Rejected — makes the engine path depend on the auto-extract-into-a-sibling-directory rule, adds a directory level (`udf/lakehouse-engine/udf/liblakehouse_engine.so`), and diverges from the path every existing test and Makefile target uses |
  | Extract locally and upload the bare `.so` on SaaS too | ✗ Rejected — the SaaS files API addresses uploads by a flat file key and applies its own extraction layout; the existing SaaS path is already proven by PR #141's hand-testing |
  | Extract the SLC locally and upload its tree file by file | ✗ Rejected — hundreds of round-trips to reproduce what one upload plus auto-extraction already does |

- **Consequences:** `tar` becomes a prerequisite of the BucketFS target only (`check_prereqs`
  branches on `TARGET_MODE`; a SaaS install on a `tar`-less machine still succeeds). The two
  `%udf_object` layouts are the reason `TARGET_SO_UDF_OBJECT` exists rather than a constant. Because
  only the SLC relies on BucketFS auto-extraction, the engine path sidesteps the async-unpack
  question entirely — the bounded `bucketfs_wait_for_path` retry still guards both uploads.
- **Promotes to ADR:** yes

### [9] SLC registration on by default, `--skip-slc` to opt out

- **Decision:** The installer downloads, uploads and registers the Rust SLC on every run unless
  `--skip-slc` is given. With the flag it logs why it skipped, performs no SLC download, no SLC
  upload, no `SCRIPT_LANGUAGES` read and no `ALTER SYSTEM`, and still resolves and reports the SLC
  version so the operator can see what the database must already have. Everything downstream —
  engine upload, DDL, smoke test, template — runs unchanged.
- **Alternatives:** Off by default with an `--install-slc` opt-in (rejected: the default invocation
  on a bare database would then fail its own smoke test, defeating "one command"). Auto-detect an
  already-registered RUST alias and skip (rejected: the alias says nothing about the SLC *version*
  behind it, and a stale SLC is exactly what the fingerprint smoke test exists to catch — silently
  skipping would hide the mismatch the installer is supposed to surface).
- **Rationale:** The two real reasons to skip are "the SLC is already registered at the right
  version" and "this account has no `ALTER SYSTEM` privilege on a restrictive tenant". Both are
  operator knowledge, not something the script can infer; an explicit flag records the operator's
  claim, and the fingerprint smoke test still checks it.
- **Promotes to ADR:** no

### [10] Adopt the profile's `bfs_bucket` before building the target paths

- **Context:** A real bug, found and fixed on this branch (commit `13ef42a`). `ARG_BFS_BUCKET`
  defaults to `default`. `exapump_bfs_flags` deliberately emits `--bfs-bucket` only when the user
  supplied it explicitly, leaving `exapump` to resolve the profile's own `bfs_bucket` otherwise. But
  `resolve_target_layout` builds `TARGET_SO_UDF_OBJECT` and `TARGET_RUST_LANG_SEGMENT` from
  `ARG_BFS_BUCKET`. With a profile naming a non-default bucket and no explicit `--bfs-bucket`, the
  two disagreed: `exapump` uploaded into the profile's bucket while the DDL pointed at `default`.
  Every step reported success — the upload, the listing verification, the DDL — and the install was
  broken, discoverable only at first query as a `.so` load failure.
- **Decision:** Add `resolve_bfs_bucket_from_profile`, called from `main` **before**
  `resolve_target_layout`. When the target mode is `bucketfs`, the connectivity mode is `profile`,
  and `--bfs-bucket` was not given explicitly, it reads the profile's `bfs_bucket` and adopts it
  into `ARG_BFS_BUCKET`. It is a no-op in SaaS mode, in dsn or host connectivity mode (no profile to
  read), when the profile has no `bfs_bucket` key, and whenever `--bfs-bucket` was given explicitly
  — an explicit flag always wins.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | Adopt the profile's bucket into `ARG_BFS_BUCKET` before layout resolution | ✓ Chosen — makes one variable the single source of truth for "the bucket this install uses", so the upload target and the DDL target cannot diverge |
  | Always pass `--bfs-bucket $ARG_BFS_BUCKET` to `exapump` | ✗ Rejected — forces the installer's `default` guess onto `exapump` and overrides a correctly-configured profile, converting a wrong-DDL bug into a wrong-upload bug |
  | Read the bucket back from `exapump` after the upload and rebuild the DDL paths | ✗ Rejected — `exapump bucketfs` has no "which bucket did you use" output; it would mean parsing a log line |
  | Document it and require `--bfs-bucket` whenever the profile sets one | ✗ Rejected — the failure is silent, so a documentation-only fix relies on the operator already knowing about a trap they cannot observe |

- **Consequences:** The bucket is resolved once, early, from the same source `exapump` will use, and
  every downstream string is built from it. The ordering constraint is real and load-bearing:
  `resolve_bfs_bucket_from_profile` must run before `resolve_target_layout`, and both are documented
  at the function to say so. Covered by `test_resolve_bfs_bucket_from_profile`, including the
  end-to-end assertion that `TARGET_SO_UDF_OBJECT` names the adopted bucket.
- **Promotes to ADR:** yes

### [11] Every subprocess reads stdin from `/dev/null`

- **Decision:** Every `curl` and every `exapump` invocation in `install.sh` ends in `</dev/null`.
- **Alternatives:** Rely on the subprocesses not reading stdin (rejected: a behavioral assumption
  about two external programs across all their versions and all their flag combinations).
  Re-exec the script from a temporary file (rejected: needs a writable temp directory before any
  validation, and changes the documented one-liner).
- **Rationale:** The documented install path pipes the script into `bash` over stdin, so the
  not-yet-parsed remainder of the script body *is* the shell's stdin. A single subprocess that reads
  it truncates execution mid-script — silently, and at a different point depending on buffer sizes.
  Proven per-subprocess rather than by inspection: `test_stdin_piped_invocation_no_body_consumption`
  runs the installer with a sentinel payload on its own stdin while every stub reports any stdin it
  managed to read, and asserts the sentinel reached nothing.
- **Promotes to ADR:** no

### [12] BucketFS-target E2E in CI; SaaS mode has no CI job — a named tracked exception

- **Context:** Decision [4] makes `main` a live distribution channel with no release gate in front
  of a user's `curl | bash`. That raises the bar for automated verification: a stubbed test suite
  proves argv shapes and control flow, not that a real Exasol accepts the DDL, the
  `SCRIPT_LANGUAGES` value, or the uploaded `.so`.
- **Decision:** Two CI jobs, both required and both blocking `release`. `install-script` runs
  ShellCheck over both files plus `make test-install` (the stubbed suite; no network).
  `install-script-e2e` runs the **real** BucketFS flow against a live `exasol` compose service: it
  creates an `exapump` profile from the container's own `EXAConf` write password, then runs
  `bash deploy/scripts/install.sh --profile ci-bucketfs` with **no** version pin, so it resolves and
  installs a real prior GitHub release exactly as a user's default invocation would. It deliberately
  does not consume the PR's own `.so`: it tests the installer's mechanics, not query correctness.
  SaaS mode gets **no** CI job.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | Real BucketFS E2E in CI; SaaS as a named tracked exception | ✓ Chosen — the BucketFS target covers Exasol AsApp, Docker and on-premise and is fully testable against a compose Exasol; naming the SaaS gap follows this repo's convention that a known deviation is an explicit tracked exception, never a silent gap |
  | Also mock the SaaS control plane in CI | ✗ Rejected — a mock re-asserts the same argv shapes the stubbed suite already covers, while proving nothing about the real API; it would read as SaaS coverage without being any |
  | Provision a real SaaS tenant and PAT as CI secrets | ✗ Rejected for now — needs a persistent tenant, a rotating PAT in repository secrets, and a cleanup story for every run's uploaded files; disproportionate to the current risk |
  | Ship with the stubbed suite only | ✗ Rejected — with no release gate between merge and a user's fetch, a DDL or path regression would reach users before any human ran the installer |

- **Consequences:** A BucketFS regression is caught on every push. A SaaS regression is not: the
  SaaS path is proven only by the stubbed integration tests and by hand-testing against a live
  tenant. This is recorded as a named tracked exception in
  `packaging/install-script-deploy/spec.md` and in the plan's Verification section, citing issue
  [#252](https://github.com/exasol-labs/lakehouse-engine-rs/issues/252); a dedicated follow-up issue
  for "SaaS-mode integration test for `install.sh`" should be filed if one does not already exist.
  One further limitation is named alongside it, untested rather than known-broken: whether
  `ALTER SYSTEM SET SCRIPT_LANGUAGES` succeeds under a SaaS tenant's default privileges was never
  confirmed by this branch or by PR #141 — only that a privilege-denied failure aborts with a named
  error (see `packaging/install-script-slc-registration/spec.md`). Note also which run proves what:
  the BucketFS `.tar.gz` auto-extract-into-a-sibling-directory rule is load-bearing for the SLC and
  only for the SLC (decision [8]), and it is the unpinned `install-script-e2e` job that exercises
  it — the hand-driven local verification ran against a container that already had a matching SLC
  registered, so it drove the engine path and the DDL, not `register_slc`.
- **Promotes to ADR:** yes

### [13] `GITHUB_TOKEN` reversed from required to optional — both repos went public

- **Context:** Decision [3] made `GITHUB_TOKEN`/`--github-token` required in both targets, because
  at the time `lakehouse-engine-rs` was private and no unauthenticated GitHub call worked for the
  script fetch, `releases/latest` resolution, or the asset download. Both `lakehouse-engine-rs` and
  `language-container-rs` have since been made public repositories. The requirement outlived its
  reason: it also had a sharper bug than "required" implied — every GitHub `curl` call sent
  `Authorization: Bearer $ARG_GITHUB_TOKEN` unconditionally, so even bypassing the `validate_required`
  check by exporting an empty `GITHUB_TOKEN` would still send a malformed empty bearer header, which
  GitHub rejects with 401 rather than treating as anonymous.
- **Decision:** Drop the `validate_required` hard-required check entirely (it had no other
  content). `--github-token`/`GITHUB_TOKEN` is now optional everywhere: a new `set_github_auth_args`
  populates a `GITHUB_AUTH_ARGS` array with the `-H "Authorization: Bearer <token>"` pair only when
  a token was actually given, and every GitHub `curl` call (`resolve_versions`,
  `download_release_asset`) splices in `"${GITHUB_AUTH_ARGS[@]}"` instead of the header literal. An
  unset/empty token now means "send no `Authorization` header," never "send an empty one." A token
  is still accepted and still useful — it raises the unauthenticated 60-requests/hour GitHub REST
  API rate limit.
- **Options Considered:**

  | Option | Verdict |
  |--------|---------|
  | Token optional, header omitted when absent | ✓ Chosen — matches the repos' actual (public) visibility, fixes the empty-bearer-header 401 bug, and keeps the rate-limit escape hatch for heavy/CI use |
  | Remove `--github-token`/`GITHUB_TOKEN` entirely | ✗ Rejected — a shared-egress-IP CI runner can still hit the 60/hour unauthenticated cap; the flag is cheap to keep and removes a real escape hatch if dropped |
  | Leave the requirement in place | ✗ Rejected — asks every operator for a token a public repo does not need, and still carries the empty-bearer-header bug for anyone who worked around the check |

- **Consequences:** The one-liners in `docs/install.md` and the header comment in `install.sh` no
  longer `export GITHUB_TOKEN=<token>` before the `curl | bash`. The "manual install on a restricted
  network" appendix's release-tarball step now uses the plain, unauthenticated
  `releases/download/<tag>/<asset>` URL instead of the two-step authenticated release-by-tag +
  asset-by-id API dance, which was only ever needed for a private repo. `install.sh`'s own
  `download_release_asset` (used by the one-line installer, not the manual appendix) keeps the
  asset-by-id API mechanism unchanged, now called unauthenticated by default, since rewriting it to
  the plain download URL is a separate, larger change not required to fix the token requirement.
- **Promotes to ADR:** no — a narrow reversal of [3] tracked here is sufficient; nothing here
  changes the architecture [3] and [4] established.
