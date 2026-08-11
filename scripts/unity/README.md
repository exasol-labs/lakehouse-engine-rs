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
./scripts/unity/seed.sh                                     # upload fixtures + register in UC
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
  | `stats_all_types` | broad types incl. array/map/struct → JSON `VARCHAR` | #322 |
  | `unshredded_variant` / `type_widening` | unsupported reader feature → fail-loud | #322 |

  Real STS credential vending and the broad cloud matrix are **#323** (live
  Databricks) — the local static-key harness cannot exercise them.

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
registry + **static-key** credential lookup (it makes no S3 call), and **the
client (our engine's `ObjectStore`) does all MinIO access with its own endpoint**
— the per-side storage-routing seam already built for #294. Never route storage
through a UC server-side S3 path. Full rationale + evidence in
`SPIKE_UC_DELTA_HARNESS.md` (§Q2).

## Read path (proven by the spike)

`UC GET /tables/{full_name}` → `storage_location` + `table_id` →
`POST /temporary-table-credentials` → vended keys →
delta-kernel-rs reads from MinIO with a client-side endpoint override
(deletion vector applied: 10→8 rows; column mapping resolved: `[id, name, value]`).
