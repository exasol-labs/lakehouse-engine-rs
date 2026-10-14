#!/usr/bin/env bash
# Re-running replaces every table registration, so a manifest/fixture change
# never leaves a stale one behind.
#
# Fixtures are prebuilt delta-kernel-rs test tables (fixtures/PROVENANCE.md)
# because delta-rs cannot write deletion vectors or column mapping.
#
# The engine declares Delta columns from the Unity Catalog entry, so each fixture is registered from its log's latest metaData;
# delta_catalog_copy holds deliberately wrong copies for the declaration-check and refusal tests.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NETWORK="${LH_NETWORK:-lakehouse-engine}"
AWS_CLI_IMAGE="amazon/aws-cli:2.27.50"
S3_ENDPOINT="http://seaweedfs:8333"
export UC_BASE="http://localhost:${LH_UNITY_PORT:-18080}/api/2.1/unity-catalog"
export UC_CATALOG="unity"
export UC_SCHEMA="delta_e2e"
export UC_COPY_SCHEMA="delta_catalog_copy"
export FIXTURES_DIR
export UC_PREFIX="delta"

echo "=== unity-seed: uploading Delta fixtures to SeaweedFS (bucket warehouse) ==="
docker run --rm --network "$NETWORK" -v "$FIXTURES_DIR":/fx:ro \
  -e AWS_ACCESS_KEY_ID=lhadmin -e AWS_SECRET_ACCESS_KEY=lhadminsecret123 \
  -e AWS_DEFAULT_REGION=us-east-1 \
  --entrypoint /bin/sh "$AWS_CLI_IMAGE" -c '
    set -e
    for t in /fx/*/; do
      name=$(basename "$t")
      aws --endpoint-url '"$S3_ENDPOINT"' s3 sync --only-show-errors "$t" "s3://warehouse/'"$UC_PREFIX"'/$name/"
      echo "  uploaded $name"
    done
  '

echo "=== unity-seed: minting the SeaweedFS STS session Unity Catalog vends ==="
# UC OSS 0.5.0 vends for `s3://warehouse` only via its static generator, which
# is selected by a non-empty `s3.sessionToken.0` and returns it verbatim, and its
# own STS generator ignores AWS_ENDPOINT_URL. So a real session is minted here by
# the AWS CLI: the base user assumes the read-only LakehouseReader role
# (seaweedfs-iam.json), for SeaweedFS's 12-hour maximum.
STS_TRIPLE=$(
  docker run --rm --network "$NETWORK" \
    -e AWS_ACCESS_KEY_ID=lhassumebase -e AWS_SECRET_ACCESS_KEY=lhassumebasesecret123 \
    -e AWS_DEFAULT_REGION=us-east-1 "$AWS_CLI_IMAGE" \
    --endpoint-url "$S3_ENDPOINT" sts assume-role \
    --role-arn arn:aws:iam::000000000000:role/LakehouseReader \
    --role-session-name lakehouse-unity-e2e --duration-seconds 43200 \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text
)
read -r STS_ACCESS_KEY STS_SECRET_KEY STS_SESSION_TOKEN <<<"$STS_TRIPLE"

echo "=== unity-seed: restarting Unity Catalog with the vended credential ==="
# UC reads credentials only at boot, so it must be recreated; this runs before
# registration because `server.env=test` keeps the catalog in in-memory H2.
# `env` because `s3.accessKey.0` is not a valid shell identifier.
env "s3.accessKey.0=$STS_ACCESS_KEY" \
    "s3.secretKey.0=$STS_SECRET_KEY" \
    "s3.sessionToken.0=$STS_SESSION_TOKEN" \
  docker compose -f "$REPO_ROOT/docker-compose.yml" -f "$REPO_ROOT/docker-compose.unity.yml" \
  up -d --wait unitycatalog

echo "=== unity-seed: registering catalog/schema/tables in Unity Catalog ==="
python3 - <<'PY'
import json, os, urllib.request, urllib.error

BASE=os.environ["UC_BASE"]; CAT=os.environ["UC_CATALOG"]; SCH=os.environ["UC_SCHEMA"]; PFX=os.environ["UC_PREFIX"]
COPY_SCH=os.environ["UC_COPY_SCHEMA"]; FIXTURES=os.environ["FIXTURES_DIR"]

# (uc_table_name, fixture_dir)
TABLES = [
    ("table_with_dv", "table-with-dv-small"),
    ("cm_name_mode", "cdf-column-mapping-name-mode"),
    ("cm_id_mode", "cdf-column-mapping-id-mode"),
    # #319 partition values in ScanSpec + #321 partition pruning
    ("basic_partitioned", "basic_partitioned"),
    # #321 stats-based file pruning (multiple data files with per-file stats)
    ("multi_part_stats", "multi-part-stats"),
    # #322 broad type mapping incl. incompatible->JSON VARCHAR
    ("stats_all_types", "stats-all-types"),
    # #322 fail-loud: the engine reads the log, sees the reader feature, and refuses
    ("unshredded_variant", "unshredded-variant"),
    # #349 type widening: the latest schema carries every column's delta.typeChanges
    ("type_widening", "type-widening"),
]

def latest_metadata(fixture_dir):
    log = os.path.join(FIXTURES, fixture_dir, "_delta_log")
    meta = None
    for name in sorted(n for n in os.listdir(log) if n.endswith(".json")):
        with open(os.path.join(log, name)) as commit:
            for line in commit:
                meta = json.loads(line).get("metaData", meta)
    return meta

PRIMITIVE_TYPE_NAMES = {
    "byte":"BYTE","short":"SHORT","integer":"INT","long":"LONG","float":"FLOAT",
    "double":"DOUBLE","string":"STRING","boolean":"BOOLEAN","date":"DATE",
    "timestamp":"TIMESTAMP","timestamp_ntz":"TIMESTAMP_NTZ","binary":"BINARY","variant":"VARIANT",
}
def type_name(delta_type):
    if isinstance(delta_type, dict):
        return delta_type["type"].upper()
    if delta_type.startswith("decimal("):
        return "DECIMAL"
    return PRIMITIVE_TYPE_NAMES[delta_type]

def registration(fixture_dir):
    meta = latest_metadata(fixture_dir)
    partitions = meta["partitionColumns"]
    columns = []
    for pos, field in enumerate(json.loads(meta["schemaString"])["fields"]):
        delta_type = field["type"]
        c = {"name": field["name"], "type_name": type_name(delta_type),
             "type_text": delta_type if isinstance(delta_type, str) else delta_type["type"],
             "type_json": json.dumps(field), "position": pos, "nullable": field["nullable"]}
        if field["name"] in partitions:
            c["partition_index"] = partitions.index(field["name"])
        columns.append(c)
    return {"table_type":"EXTERNAL", "data_source_format":"DELTA",
            "storage_location":f"s3://warehouse/{PFX}/{fixture_dir}",
            "columns":columns, "properties":dict(meta.get("configuration") or {})}

def column(entry, name):
    return next(c for c in entry["columns"] if c["name"] == name)

def edit_type_json(entry, name, edit):
    c = column(entry, name)
    field = json.loads(c["type_json"])
    edit(field)
    c["type_json"] = json.dumps(field)
    c["nullable"] = field["nullable"]

# (uc_table_name, fixture_dir, edit of the faithful registration)
CATALOG_COPIES = [
    ("no_mode", "cdf-column-mapping-name-mode",
        lambda e: e["properties"].pop("delta.columnMapping.mode")),
    ("no_partition_index", "basic_partitioned",
        lambda e: column(e, "letter").pop("partition_index")),
    ("value_without_physical_name", "cdf-column-mapping-name-mode",
        lambda e: edit_type_json(e, "value",
                                 lambda f: f["metadata"].pop("delta.columnMapping.physicalName"))),
    ("label_mode", "cdf-column-mapping-name-mode",
        lambda e: e["properties"].update({"delta.columnMapping.mode": "label"})),
    ("id_not_null", "cdf-column-mapping-name-mode",
        lambda e: edit_type_json(e, "id", lambda f: f.update({"nullable": False}))),
    ("no_log", "basic_partitioned",
        lambda e: e.update({"storage_location": f"s3://warehouse/{PFX}/no-log-here"})),
]

def post(path, body):
    req=urllib.request.Request(BASE+path, data=json.dumps(body).encode(),
                               headers={"Content-Type":"application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req) as r: return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

def delete(path):
    req=urllib.request.Request(BASE+path, method="DELETE")
    try:
        with urllib.request.urlopen(req) as r: return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

def error_code(txt):
    try: return json.loads(txt).get("error_code","")
    except (ValueError, AttributeError): return ""

# Catalog and schema carry no per-fixture payload, so an existing one is a correct
# end state. Accept the typed already-exists error_code, not a substring of an
# arbitrary body. Unity Catalog OSS v0.5.0 returns HTTP 400 with error_code
# CATALOG_ALREADY_EXISTS / SCHEMA_ALREADY_EXISTS on re-create (verified live against
# the pinned image — it is 400, not 409, so match the code, not the status).
def ensure(label, path, body):
    code, txt = post(path, body)
    if code==200 or error_code(txt).endswith("_ALREADY_EXISTS"):
        print(f"  ok: {label} ({code})")
    else:
        raise SystemExit(f"ERROR seeding {label} -> HTTP {code}: {txt}")

# A table registration carries the full per-fixture payload (table_type,
# data_source_format, storage_location, columns[], properties), so an existing one can be stale
# after a manifest/fixture change. Delete then create: the registration is metadata
# only (the fixture files in the bucket are untouched), so a re-create is cheap and
# always converges. DELETE returns 200 (removed) or 404 TABLE_NOT_FOUND (absent);
# both are acceptable, anything else aborts.
def replace_table(label, schema, name, body):
    dcode, dtxt = delete(f"/tables/{CAT}.{schema}.{name}")
    if dcode not in (200, 404):
        raise SystemExit(f"ERROR clearing {label} -> HTTP {dcode}: {dtxt}")
    code, txt = post("/tables", body)
    if code!=200:
        raise SystemExit(f"ERROR seeding {label} -> HTTP {code}: {txt}")
    print(f"  ok: {label} ({code})")

# UC does not persist its default `unity` sample catalog across a container
# recreate, so create it explicitly rather than assuming it exists (spike).
ensure(f"catalog {CAT}", "/catalogs", {"name":CAT})
for schema in (SCH, COPY_SCH):
    ensure(f"schema {schema}", "/schemas", {"name":schema,"catalog_name":CAT})
for name, fixture_dir in TABLES:
    body = {"name":name,"catalog_name":CAT,"schema_name":SCH, **registration(fixture_dir)}
    replace_table(f"table {name}", SCH, name, body)
for name, fixture_dir, edit in CATALOG_COPIES:
    entry = registration(fixture_dir)
    edit(entry)
    body = {"name":name,"catalog_name":CAT,"schema_name":COPY_SCH, **entry}
    replace_table(f"table {COPY_SCH}.{name}", COPY_SCH, name, body)

# List what we registered.
with urllib.request.urlopen(f"{BASE}/tables?catalog_name={CAT}&schema_name={SCH}") as r:
    for t in json.load(r).get("tables", []):
        print(f"   {t['name']:<20} -> {t['storage_location']}")
PY
echo "=== unity-seed: done. Tables registered under $UC_CATALOG.$UC_SCHEMA ==="
