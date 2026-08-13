#!/usr/bin/env bash
# Seed the Unity Catalog + Delta E2E stack (spike #325):
#   1. upload every vendored Delta fixture table to MinIO (bucket `warehouse`)
#   2. mint the MinIO STS session Unity Catalog vends for `s3://warehouse` and
#      restart the server with it (see server.properties for why it must be real)
#   3. register the fixtures in Unity Catalog as EXTERNAL Delta tables
#
# Convergent: re-running replaces every table registration from the manifest, so a
# manifest/fixture change never leaves a stale registration behind. Fail-loud: any
# step that fails aborts non-zero (the E2E contract is FAIL, not skip, when the
# fixture cannot be provisioned).
#
# Fixtures are prebuilt delta-kernel-rs test tables (see fixtures/PROVENANCE.md)
# because delta-rs cannot WRITE deletion vectors or column mapping — the two
# correctness features this milestone delivers (SPIKE_UC_DELTA_HARNESS.md §Q3).
#
# NOTE on UC columns: for an EXTERNAL Delta table the engine reads the real
# schema + protocol from the Delta log; the UC column list is an advisory
# discovery hint. Nested/incompatible Delta types (array/map/struct/variant/
# binary) are registered here as STRING — mirroring how the engine surfaces them
# to Exasol (JSON VARCHAR). Each downstream issue refines expectations as needed.
#
# Prereqs: the stack is up (docker compose ... up -d minio exasol unitycatalog)
# and MinIO's `warehouse` bucket exists (base `minio-init`). Needs docker + python3.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NETWORK="${LH_NETWORK:-lakehouse-engine}"        # base compose sets name: lakehouse-engine
MC_IMAGE="minio/mc:RELEASE.2025-08-13T08-35-41Z"
export UC_BASE="http://localhost:${LH_UNITY_PORT:-18080}/api/2.1/unity-catalog"
export UC_CATALOG="unity"
export UC_SCHEMA="delta_e2e"
export UC_PREFIX="delta"                          # tables land at s3://warehouse/delta/<dir>

echo "=== unity-seed: uploading Delta fixtures to MinIO (bucket warehouse) ==="
# Bind-mount the vendored fixtures (repo path is shareable with Docker Desktop)
# and mirror each table dir into the bucket. Idempotent; preserves the
# _delta_log/ + parquet + deletion-vector .bin layout (relative paths only).
docker run --rm --network "$NETWORK" -v "$FIXTURES_DIR":/fx:ro \
  --entrypoint /bin/sh "$MC_IMAGE" -c '
    set -e
    mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null
    for t in /fx/*/; do
      name=$(basename "$t")
      mc mirror --overwrite --quiet "$t" "local/warehouse/'"$UC_PREFIX"'/$name/" >/dev/null
      echo "  uploaded $name"
    done
  '

echo "=== unity-seed: minting the MinIO STS session Unity Catalog vends ==="
# Unity Catalog OSS 0.5.0 can only vend a credential for `s3://warehouse` through
# its per-bucket static generator, and that generator is selected ONLY by a
# non-empty `s3.sessionToken.0` — which it then hands back verbatim. A vended
# session token is contractually real (the client must send it as
# `x-amz-security-token`), and MinIO rejects any token that is not a live STS
# session with 403 InvalidTokenId, so a placeholder there makes every vended read
# fail. UC's own STS generator cannot stand in: its bundled SDK ignores
# AWS_ENDPOINT_URL[_STS], so it would call the real sts.amazonaws.com.
#
# The harness therefore mints a genuine, expiring MinIO STS session here and
# injects the resulting triple as UC's preset credential. MinIO serves STS
# AssumeRole at its S3 endpoint (the same mechanism the Lakekeeper overlay's
# vended warehouse uses); the session inherits the parent's permissions and lasts
# MinIO's 7-day maximum, so it outlives the stack it is minted for.
STS_TRIPLE=$(
  MINIO_STS_ENDPOINT="http://localhost:${LH_MINIO_PORT:-19000}" python3 - <<'PY'
import datetime, hashlib, hmac, os, sys, urllib.error, urllib.request
import xml.etree.ElementTree as ET

ENDPOINT = os.environ["MINIO_STS_ENDPOINT"]
KEY = SECRET = "minioadmin"          # base compose's MinIO root credentials
REGION, SERVICE = "us-east-1", "sts"
DURATION = "604800"                  # MinIO's AssumeRole maximum: 7 days

host = ENDPOINT.split("://", 1)[1]
body = ("Action=AssumeRole&Version=2011-06-15"
        f"&DurationSeconds={DURATION}&RoleSessionName=lakehouse-unity-e2e")
now = datetime.datetime.now(datetime.timezone.utc)
stamp, datestamp = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
payload_hash = hashlib.sha256(body.encode()).hexdigest()

signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
canonical = (
    "POST\n/\n\n"
    f"content-type:application/x-www-form-urlencoded\nhost:{host}\n"
    f"x-amz-content-sha256:{payload_hash}\nx-amz-date:{stamp}\n"
    f"\n{signed_headers}\n{payload_hash}"
)
scope = f"{datestamp}/{REGION}/{SERVICE}/aws4_request"
to_sign = (f"AWS4-HMAC-SHA256\n{stamp}\n{scope}\n"
           f"{hashlib.sha256(canonical.encode()).hexdigest()}")

def sign(k, m):
    return hmac.new(k, m.encode(), hashlib.sha256).digest()

signing_key = sign(sign(sign(sign(f"AWS4{SECRET}".encode(), datestamp), REGION),
                        SERVICE), "aws4_request")
signature = hmac.new(signing_key, to_sign.encode(), hashlib.sha256).hexdigest()

req = urllib.request.Request(
    ENDPOINT + "/", data=body.encode(), method="POST",
    headers={"Content-Type": "application/x-www-form-urlencoded", "Host": host,
             "X-Amz-Content-Sha256": payload_hash, "X-Amz-Date": stamp,
             "Authorization": (f"AWS4-HMAC-SHA256 Credential={KEY}/{scope}, "
                               f"SignedHeaders={signed_headers}, Signature={signature}")})
try:
    raw = urllib.request.urlopen(req, timeout=30).read()
except (urllib.error.URLError, OSError) as e:
    detail = e.read().decode(errors="replace")[:500] if hasattr(e, "read") else e
    raise SystemExit(f"ERROR minting the MinIO STS session at {ENDPOINT}: {detail}")

ns = {"s": "https://sts.amazonaws.com/doc/2011-06-15/"}
creds = ET.fromstring(raw).find(".//s:Credentials", ns)
if creds is None:
    raise SystemExit("ERROR minting the MinIO STS session: response carried no "
                     f"Credentials element: {raw.decode(errors='replace')[:500]}")
triple = [creds.findtext(f"s:{f}", namespaces=ns)
          for f in ("AccessKeyId", "SecretAccessKey", "SessionToken")]
if not all(triple):
    raise SystemExit("ERROR minting the MinIO STS session: incomplete credential triple")
print(" ".join(triple))
print(f"  session expires {creds.findtext('s:Expiration', namespaces=ns)}", file=sys.stderr)
PY
)
read -r STS_ACCESS_KEY STS_SECRET_KEY STS_SESSION_TOKEN <<<"$STS_TRIPLE"

echo "=== unity-seed: restarting Unity Catalog with the vended credential ==="
# UC reads server.properties and its environment once, at boot, so the freshly
# minted credential can only reach it through a container recreate. This runs
# BEFORE registration on purpose: `server.env=test` keeps UC's catalog in an
# in-memory H2 database, so a recreate discards every registration.
#
# `env` rather than a shell assignment because UC's property names are also the
# environment-variable names it looks them up under, and `s3.accessKey.0` is not a
# valid shell identifier. The compose service passes these three through by name.
env "s3.accessKey.0=$STS_ACCESS_KEY" \
    "s3.secretKey.0=$STS_SECRET_KEY" \
    "s3.sessionToken.0=$STS_SESSION_TOKEN" \
  docker compose -f "$REPO_ROOT/docker-compose.yml" -f "$REPO_ROOT/docker-compose.unity.yml" \
  up -d --wait unitycatalog

echo "=== unity-seed: registering catalog/schema/tables in Unity Catalog ==="
# Registration is data-driven (a manifest) — Python keeps the multi-column and
# nested-type cases readable, which the bash string-concat approach could not.
python3 - <<'PY'
import json, os, urllib.request, urllib.error

BASE=os.environ["UC_BASE"]; CAT=os.environ["UC_CATALOG"]; SCH=os.environ["UC_SCHEMA"]; PFX=os.environ["UC_PREFIX"]

# (uc_table_name, fixture_dir, [(col, delta_type, is_partition)])
# delta_type: primitive spark name, "decimal(p,s)", or a nested type
# (array/map/struct/variant) which is registered as STRING (advisory — see header).
TABLES = [
    ("table_with_dv", "table-with-dv-small", [("value","int",False)]),
    ("cm_name_mode", "cdf-column-mapping-name-mode",
        [("id","long",False),("name","string",False),("value","double",False)]),
    ("cm_id_mode", "cdf-column-mapping-id-mode",
        [("id","long",False),("name","string",False),("value","double",False)]),
    # #319 partition values in ScanSpec + #321 partition pruning
    ("basic_partitioned", "basic_partitioned",
        [("letter","string",True),("number","long",False),("a_float","double",False)]),
    # #321 stats-based file pruning (multiple data files with per-file stats)
    ("multi_part_stats", "multi-part-stats",
        [("id","long",False),("value","string",False)]),
    # #322 broad type mapping incl. incompatible->JSON VARCHAR; also forces the
    # timestampNtz gate-or-map decision (declares timestampNtz + columnMapping)
    ("stats_all_types", "stats-all-types", [
        ("byte_col","byte",False),("short_col","short",False),("int_col","int",False),
        ("long_col","long",False),("float_col","float",False),("double_col","double",False),
        ("date_col","date",False),("timestamp_col","timestamp",False),
        ("timestamp_ntz_col","timestamp_ntz",False),("string_col","string",False),
        ("decimal_col","decimal(10,2)",False),("boolean_col","boolean",False),
        ("binary_col","binary",False),("array_col","array",False),
        ("map_col","map",False),("nested_struct","struct",False)]),
    # #322 fail-loud: unsupported reader features (variantType / typeWidening).
    # Columns are advisory; the engine reads the log, sees the reader feature,
    # and must refuse (kernel itself reads these fine — gating is engine-side).
    ("unshredded_variant", "unshredded-variant",
        [("id","long",False),("v","variant",False),("array_of_variants","array",False),
         ("struct_of_variants","struct",False),("map_of_variants","map",False)]),
    ("type_widening", "type-widening",
        [("int_long","long",False),("float_double","double",False),
         ("decimal_decimal_same_scale","decimal(20,2)",False)]),
]

# Delta/Spark type -> (UC type_name, type_json spark type, precision, scale)
NESTED = {"array","map","struct","variant"}
PRIM = {
    "byte":"BYTE","short":"SHORT","int":"INT","integer":"INT","long":"LONG",
    "float":"FLOAT","double":"DOUBLE","string":"STRING","boolean":"BOOLEAN",
    "date":"DATE","timestamp":"TIMESTAMP","timestamp_ntz":"TIMESTAMP_NTZ","binary":"BINARY",
}
def map_type(dt):
    if dt.startswith("decimal("):
        p,s = dt[len("decimal("):-1].split(","); return ("DECIMAL", dt, int(p), int(s))
    if dt in NESTED:  # advisory: surfaced to Exasol as JSON VARCHAR
        return ("STRING","string",None,None)
    tn = PRIM.get(dt)
    if not tn: raise SystemExit(f"unmapped delta type: {dt}")
    js = "integer" if dt=="int" else dt
    return (tn, js, None, None)

def col(pos, name, dt, is_part):
    tn, js, prec, scale = map_type(dt)
    tj = {"name":name,"type":js,"nullable":True,"metadata":{}}
    c = {"name":name,"type_text":dt,"type_name":tn,"type_json":json.dumps(tj),
         "position":pos,"nullable":True}
    if prec is not None: c["type_precision"]=prec; c["type_scale"]=scale
    if is_part: c["partition_index"]=0
    return c

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
# data_source_format, storage_location, columns[]), so an existing one can be stale
# after a manifest/fixture change. Delete then create: the registration is metadata
# only (the fixture files in the bucket are untouched), so a re-create is cheap and
# always converges. DELETE returns 200 (removed) or 404 TABLE_NOT_FOUND (absent);
# both are acceptable, anything else aborts.
def replace_table(label, name, body):
    dcode, dtxt = delete(f"/tables/{CAT}.{SCH}.{name}")
    if dcode not in (200, 404):
        raise SystemExit(f"ERROR clearing {label} -> HTTP {dcode}: {dtxt}")
    code, txt = post("/tables", body)
    if code!=200:
        raise SystemExit(f"ERROR seeding {label} -> HTTP {code}: {txt}")
    print(f"  ok: {label} ({code})")

# UC does not persist its default `unity` sample catalog across a container
# recreate, so create it explicitly rather than assuming it exists (spike).
ensure(f"catalog {CAT}", "/catalogs", {"name":CAT})
ensure(f"schema {SCH}",  "/schemas",  {"name":SCH,"catalog_name":CAT})
for name, fixture_dir, cols in TABLES:
    body = {"name":name,"catalog_name":CAT,"schema_name":SCH,"table_type":"EXTERNAL",
            "data_source_format":"DELTA",
            "storage_location":f"s3://warehouse/{PFX}/{fixture_dir}",
            "columns":[col(i,c,t,p) for i,(c,t,p) in enumerate(cols)]}
    replace_table(f"table {name}", name, body)

# List what we registered.
with urllib.request.urlopen(f"{BASE}/tables?catalog_name={CAT}&schema_name={SCH}") as r:
    for t in json.load(r).get("tables", []):
        print(f"   {t['name']:<20} -> {t['storage_location']}")
PY
echo "=== unity-seed: done. Tables registered under $UC_CATALOG.$UC_SCHEMA ==="
