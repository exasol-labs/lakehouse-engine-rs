# Unity Catalog (native) + Delta E2E harness

Minimal fixture harness from spike **#325**, gating the per-step E2E in #318–#322.
It stands up a native **Unity Catalog OSS** server over the base stack's **MinIO**
and serves **Delta Lake** tables (the milestone's second catalog kind + second
table format).

## Bring up + seed

```bash
make unity-up        # compose up minio+exasol+unitycatalog (overlay) + seed
# ...work...
make unity-down      # tear down + wipe volumes
```

`make unity-up` is equivalent to:

```bash
docker compose -f docker-compose.yml -f docker-compose.unity.yml up -d --wait \
  minio exasol unitycatalog
docker compose -f docker-compose.yml up -d minio-init      # create warehouse bucket
./scripts/unity/seed.sh                                     # upload fixtures, mint the
                                                            # vended session, register in UC
```

## What the seed produces

- Delta fixtures uploaded to `s3://warehouse/delta/` on MinIO and registered in
  Unity Catalog under `unity.delta_e2e` (see `fixtures/PROVENANCE.md` for the full
  per-issue map). Summary of which downstream issue each serves:

  | UC table | Exercises | Serves |
  |---|---|---|
  | `table_with_dv` | deletion vector (10→8) | #320 |
  | `cm_name_mode` / `cm_id_mode` | column mapping (name / id) | #320 |
  | `basic_partitioned` | partition values / partition pruning | #319, #321 |
  | `multi_part_stats` | multi-file stats-based pruning | #321 |
  | `stats_all_types` | broad types: `array` maps to text VARCHAR, `map`/`struct`/`binary` refused per column; `timestampNtz` maps to TIMESTAMP | #322, #350 |
  | `unshredded_variant` | reader feature outside the plan-time gate's seven-feature allow-list (`variantType`, concretely) → fail-loud | #322 |
  | `type_widening` | type widening read across the boundary; 11 of 13 columns queryable at their widened types, `byte_decimal`/`short_decimal` refused per column (outside the Delta protocol's supported-pair list) | #349 |

- The credential Unity Catalog vends for `s3://warehouse`: `seed.sh` mints a real
  MinIO STS session (AssumeRole at MinIO's S3 endpoint, 7-day maximum) and injects
  it into the `unitycatalog` container as its preset per-bucket credential,
  recreating the container so UC picks it up. It must be a genuine session — a
  vended `session_token` is contractually real, the client sends it as
  `x-amz-security-token`, and MinIO rejects anything that is not a live session
  with `403 InvalidTokenId`. UC OSS 0.5.0 cannot vend a credential with no token
  at all, and its own STS generator ignores `AWS_ENDPOINT_URL[_STS]` and so would
  call the real `sts.amazonaws.com`; `server.properties` carries the full
  reasoning. The broad cloud matrix and Databricks' dynamic vending remain
  **#323** (live Databricks).

## Endpoints

| From | Unity Catalog | MinIO |
|---|---|---|
| host | `http://localhost:${LH_UNITY_PORT:-18080}` | `http://localhost:${LH_MINIO_PORT:-19000}` |
| UDF (docker net) | `http://unitycatalog:8080` | `http://minio:9000` |

Auth is **disabled** (UC OSS default) — no token needed locally.

## The one thing #318/#319/#320 must remember

UC OSS does **not** support S3-compatible storage (MinIO) *server-side* — its AWS
SDK client has no endpoint override, and the contribution to add one was declined
(upstream [#43](https://github.com/unitycatalog/unitycatalog/issues/43),
[#1140](https://github.com/unitycatalog/unitycatalog/issues/1140),
[#1532](https://github.com/unitycatalog/unitycatalog/issues/1532);
[#1636](https://github.com/unitycatalog/unitycatalog/issues/1636) closed
"Won't be merged"). The harness sidesteps this entirely: UC is only a metadata
registry + a per-bucket credential lookup (it makes no S3 call of its own), and
**the client (our engine's `ObjectStore`) does all MinIO access with its own
endpoint** — the per-side storage-routing seam already built for #294. Never route
storage through a UC server-side S3 path. The same missing endpoint override is
why UC cannot mint its own session against MinIO, hence the minted-and-injected
session above. Full rationale + evidence in `SPIKE_UC_DELTA_HARNESS.md` (§Q2).

## Read path (proven by the spike)

`UC GET /tables/{full_name}` → `storage_location` + `table_id` →
`POST /temporary-table-credentials` → a vended STS session (access key, secret
key, session token) →
delta-kernel-rs reads from MinIO with a client-side endpoint override
(deletion vector applied: 10→8 rows; column mapping resolved: `[id, name, value]`).
