#!/usr/bin/env bash
#
# Manually-invoked live smoke test for the lakehouse-engine VS. NOT part of CI
# (`make test-e2e` is the pipeline path). Two modes (config from a gitignored
# .env — see .env.example):
#
#   docker (default) — SELF-CONTAINED: brings up the local Docker stack (MinIO +
#       Iceberg REST + Exasol), loads TPC-H into the local catalog, and verifies
#       wiring + first perf indicators. No AWS needed; .env is optional.
#   remote — runs against a real AWS Glue catalog + an external Exasol cluster
#       (the later cluster perf phase). Requires AWS_*/EXASOL_* in .env.
#
#   make live-smoke
#   ./scripts/live-smoke.sh selftest   # offline self-check of the string logic
#
set -euo pipefail
cd "$(dirname "$0")/.."

# Object names — match the e2e harness so the same .so entry points resolve.
SCHEMA=LHVS
ADAPTER=LAKEHOUSE_ADAPTER
SCAN=LAKEHOUSE_SCAN
CONN=LAKEHOUSE_CATALOG_CREDS
VS=TPCH
SO_UDF_OBJECT=buckets/bfsdefault/default/udf/liblakehouse_engine.so
SLC_VERSION=0.16.0  # matches the .so ABI fingerprint; do not "upgrade" blindly

require() {
  local v
  for v in "$@"; do
    [ -n "${!v:-}" ] || { echo "ERROR: required env var '$v' is empty (set it in .env)"; exit 1; }
  done
}

# Remote (AWS Glue) catalog-connection password JSON: SigV4 + static creds,
# region-driven endpoint. Mirrors cloud_e2e_test.rs::catalog_connection_password.
build_conn_password_cloud() {
  local token_field=""
  [ -n "${AWS_SESSION_TOKEN:-}" ] && token_field=",\"session_token\":\"${AWS_SESSION_TOKEN}\""
  local json="{\"warehouse\":\"${GLUE_WAREHOUSE}\",\"endpoint\":\"\",\"region\":\"${AWS_REGION}\",\"access_key\":\"${AWS_ACCESS_KEY_ID}\",\"secret_key\":\"${AWS_SECRET_ACCESS_KEY}\",\"path_style\":false,\"use_sigv4\":true,\"use_vended_credentials\":false${token_field}}"
  printf '%s' "${json//\'/\'\'}"  # SQL string-literal escaping: ' -> ''
}

# Docker (local MinIO) catalog-connection password JSON: path-style, no SigV4,
# internal MinIO endpoint. Mirrors stack.rs::local_stack_connection_password.
build_conn_password_local() {
  printf '%s' '{"warehouse":"s3://warehouse/","endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","path_style":true,"use_sigv4":false,"use_vended_credentials":false}'
}

# ---- offline self-check: the only non-trivial string logic (ponytail) --------
if [ "${1:-}" = "selftest" ]; then
  GLUE_WAREHOUSE="s3://b/w/" AWS_REGION=r AWS_ACCESS_KEY_ID=k \
  AWS_SECRET_ACCESS_KEY="se'cret" AWS_SESSION_TOKEN="" \
    out="$(build_conn_password_cloud)"
  case "$out" in *"se''cret"*) ;; *) echo "FAIL: single-quote not escaped: $out"; exit 1;; esac
  case "$out" in *'"use_sigv4":true'*) ;; *) echo "FAIL: cloud missing use_sigv4"; exit 1;; esac
  case "$(build_conn_password_local)" in
    *'"path_style":true'*'"use_sigv4":false'*) ;; *) echo "FAIL: local password shape"; exit 1;; esac
  if (require __DEFINITELY_UNSET_VAR__ >/dev/null 2>&1); then echo "FAIL: require passed on unset"; exit 1; fi
  echo "selftest OK"; exit 0
fi

# ---- config ------------------------------------------------------------------
[ -f .env ] && { set -a; . ./.env; set +a; }   # .env optional (required vars validated per mode)

TARGET="${LIVE_SMOKE_TARGET:-docker}"
EXA_PORT="${LH_EXASOL_PORT:-28563}"
BFS_PORT="${LH_BUCKETFS_PORT:-22581}"
TPCH_SCALE="${TPCH_SCALE:-0.3}"

case "$TARGET" in
  docker)
    HOST=localhost
    SYS_PASS="${EXASOL_SYS_PASSWORD:-exasol}"
    export EXASOL_SYS_PASSWORD="$SYS_PASS"
    NAMESPACE="${ICEBERG_NAMESPACE:-tpch}"
    CATALOG_URI="http://iceberg-rest:8181"          # internal: reachable from the UDF
    CONN_PW="$(build_conn_password_local)"
    VS_EXTRA_PROPS=$'\n  ALLOW_HTTP          = '\''true'\'''   # http catalog + http S3
    PROFILE_ON=0
    echo "== docker: bringing up local stack (minio, iceberg-rest, exasol) =="
    docker compose up -d
    ;;
  remote)
    require AWS_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY GLUE_CATALOG_URI GLUE_WAREHOUSE \
            ICEBERG_NAMESPACE EXASOL_HOST EXASOL_SYS_PASSWORD BUCKETFS_WRITE_PASS
    HOST="$EXASOL_HOST"
    SYS_PASS="$EXASOL_SYS_PASSWORD"
    export BUCKETFS_WRITE_PASS                      # make's $(shell) reads it from the environment
    NAMESPACE="$ICEBERG_NAMESPACE"
    CATALOG_URI="$GLUE_CATALOG_URI"
    CONN_PW="$(build_conn_password_cloud)"
    VS_EXTRA_PROPS=""
    PROFILE_ON="${LIVE_SMOKE_PROFILE:-1}"
    ;;
  *) echo "ERROR: LIVE_SMOKE_TARGET must be 'docker' or 'remote' (got '$TARGET')"; exit 1;;
esac

DSN="exasol://sys:${SYS_PASS}@${HOST}:${EXA_PORT}?validateservercertificate=0"
REPORT="live-smoke-report-$(date +%Y%m%d-%H%M%S).txt"
FAILED=0

# exapump reads SQL from stdin (keeps secrets out of argv); -f sets output format.
sql()  { printf '%s' "$1" | exapump sql -d "$DSN"; }
sqlf() { printf '%s' "$1" | exapump sql -d "$DSN" -f "${2:-csv}"; }
# First data cell of a single-value query (skip CSV header, strip quotes/space).
query_scalar() { printf '%s' "$1" | exapump sql -d "$DSN" -f csv | tail -n +2 | head -1 | tr -d '"[:space:]'; }

wait_http() {  # url name
  local url="$1" name="$2" i
  echo "== waiting for ${name} (${url}) =="
  for i in $(seq 1 60); do
    curl -fsS -o /dev/null "$url" 2>/dev/null && return 0
    sleep 1
  done
  echo "ERROR: ${name} not ready at ${url}"; exit 1
}
wait_exasol() {
  echo "== waiting for Exasol ${HOST}:${EXA_PORT} =="
  local i
  for i in $(seq 1 180); do
    if (exec 3<>"/dev/tcp/${HOST}/${EXA_PORT}") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 1
  done
  echo "ERROR: Exasol not reachable at ${HOST}:${EXA_PORT}"; exit 1
}

# ---- build .so ---------------------------------------------------------------
echo "== building working-tree .so (no-op if fresh) =="
make cross-musl-udf-build

# ---- wait for services + load data (docker only) -----------------------------
if [ "$TARGET" = "docker" ]; then
  wait_http "http://localhost:${LH_MINIO_PORT:-19000}/minio/health/live" "MinIO"
  wait_http "http://localhost:${LH_REST_PORT:-18181}/v1/config" "Iceberg REST"
  wait_exasol
  echo "== loading TPC-H (SF=${TPCH_SCALE}) into local catalog namespace '${NAMESPACE}' =="
  TPCH_SCALE="$TPCH_SCALE" ICEBERG_NAMESPACE="$NAMESPACE" \
    cargo test --features exasol-e2e --test tpch_loader -- --nocapture
else
  wait_exasol
fi

# ---- install SLC + upload .so ------------------------------------------------
echo "== installing SLC ${SLC_VERSION} + uploading .so =="
make install-slc bucketfs-upload-so \
  EXASOL_HOST="$HOST" LH_EXASOL_PORT="$EXA_PORT" LH_BUCKETFS_PORT="$BFS_PORT" \
  SLC_VERSION="$SLC_VERSION" EXASOL_SYS_PASSWORD="$SYS_PASS"

# ---- create schema, scripts, connection, VS (all idempotent) -----------------
echo "== creating schema, scripts, connection, VS '${VS}' =="
sql "CREATE SCHEMA IF NOT EXISTS ${SCHEMA}"
sql "CREATE OR REPLACE RUST ADAPTER SCRIPT ${SCHEMA}.${ADAPTER} AS
%udf_object ${SO_UDF_OBJECT}
/"
sql "CREATE OR REPLACE RUST SET SCRIPT ${SCHEMA}.${SCAN}(spec VARCHAR(2000000))
EMITS (...) AS
%udf_object ${SO_UDF_OBJECT}
/"
sql "CREATE OR REPLACE CONNECTION ${CONN} TO '${CATALOG_URI//\'/\'\'}' USER '' IDENTIFIED BY '${CONN_PW}'"
sql "DROP VIRTUAL SCHEMA IF EXISTS ${VS} CASCADE" || true
sql "CREATE VIRTUAL SCHEMA ${VS}
USING ${SCHEMA}.${ADAPTER} WITH
  CATALOG_CONNECTION  = '${CONN}'
  ICEBERG_NAMESPACE   = '${NAMESPACE}'
  SCAN_SCHEMA         = '${SCHEMA}'${VS_EXTRA_PROPS}"

{
  echo "lakehouse-engine live smoke test — ${TARGET} @ ${HOST}:${EXA_PORT} — $(date)"
  echo "namespace=${NAMESPACE}"
  echo
  echo "== tables exposed by ${VS} =="
} | tee "$REPORT"
sqlf "SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA='${VS}' ORDER BY TABLE_NAME" | tee -a "$REPORT"

# ---- wiring correctness: per-table row counts (docker = known TPC-H sizes) ----
check_count() {  # table expected(optional, empty = just assert > 0)
  local t="$1" exp="${2:-}" n
  n="$(query_scalar "SELECT COUNT(*) FROM ${VS}.${t}")"
  if [ -n "$exp" ]; then
    if [ "$n" = "$exp" ]; then echo "  OK    ${t}: ${n}"; else echo "  FAIL  ${t}: got '${n}', expected ${exp}"; FAILED=1; fi
  elif [ "${n:-0}" -gt 0 ] 2>/dev/null; then
    echo "  OK    ${t}: ${n}"
  else
    echo "  FAIL  ${t}: got '${n:-<none>}', expected > 0"; FAILED=1
  fi
}
if [ "$TARGET" = "docker" ]; then
  { echo; echo "== row counts (REGION/NATION are scale-independent) =="; } | tee -a "$REPORT"
  { check_count REGION 5
    check_count NATION 25
    check_count SUPPLIER
    check_count CUSTOMER
    check_count PART
    check_count PARTSUPP
    check_count ORDERS
    check_count LINEITEM
  } | tee -a "$REPORT"
fi

# ---- TPC-H JOIN query set: plain SELECTs with wall-clock (first perf signal) --
# ponytail: assumes a FLAT namespace -> table names = uppercased Iceberg names
# (LINEITEM, ORDERS, ...). A nested namespace flattens to NS__TABLE
# (flatten_table_name, adapter/tables.rs) — adjust these names if so.
run_query() {
  local name="$1" q="$2" t0 t1
  { echo; echo "### ${name}"; } | tee -a "$REPORT"
  t0=$(date +%s.%N)
  sqlf "$q" | tee -a "$REPORT"
  t1=$(date +%s.%N)
  printf 'elapsed: %ss\n' "$(awk "BEGIN{printf \"%.2f\", ${t1}-${t0}}")" | tee -a "$REPORT"
}

run_query "Q1 supplier x nation x region (wiring check)" \
"SELECT n.N_NAME, r.R_NAME, COUNT(*) AS suppliers
 FROM ${VS}.SUPPLIER s
 JOIN ${VS}.NATION n ON s.S_NATIONKEY = n.N_NATIONKEY
 JOIN ${VS}.REGION r ON n.N_REGIONKEY = r.R_REGIONKEY
 GROUP BY n.N_NAME, r.R_NAME
 ORDER BY n.N_NAME"

run_query "Q2 customer x orders x lineitem (big 3-way scan)" \
"SELECT COUNT(*) AS rows_joined
 FROM ${VS}.CUSTOMER c
 JOIN ${VS}.ORDERS o   ON c.C_CUSTKEY  = o.O_CUSTKEY
 JOIN ${VS}.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY"

run_query "Q3 orders x lineitem + filter + GROUP BY" \
"SELECT o.O_ORDERPRIORITY, COUNT(*) AS cnt, SUM(l.L_EXTENDEDPRICE) AS revenue
 FROM ${VS}.ORDERS o
 JOIN ${VS}.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY
 WHERE o.O_ORDERDATE >= DATE '1994-01-01' AND o.O_ORDERDATE < DATE '1995-01-01'
 GROUP BY o.O_ORDERPRIORITY
 ORDER BY o.O_ORDERPRIORITY"

# ---- remote-only best-effort PROFILE dump ------------------------------------
if [ "$TARGET" = "remote" ] && [ "$PROFILE_ON" = "1" ]; then
  sql "ALTER SYSTEM SET PROFILE = 'ON'" || true
  echo | tee -a "$REPORT"
  echo "== PROFILE (most recent statements, best-effort) ==" | tee -a "$REPORT"
  sqlf "SELECT STMT_ID, PART_ID, PART_NAME, OBJECT_NAME, OBJECT_ROWS, DURATION, CPU
        FROM EXA_USER_PROFILE_LAST_DAY
        ORDER BY STMT_ID DESC, PART_ID
        LIMIT 200" >>"$REPORT" 2>&1 || echo "(profile unavailable)" >>"$REPORT"
fi

echo
if [ "$FAILED" -ne 0 ]; then
  echo "SMOKE TEST FAILED (see counts above). Report: ${REPORT}"; exit 1
fi
echo "Done. Full report: ${REPORT}"
