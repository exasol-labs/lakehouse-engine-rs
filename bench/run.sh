#!/usr/bin/env bash
#
# Manually-invoked live benchmark for the lakehouse-engine VS: exercises the full
# query path against a live system and times it. NOT part of CI (`make test-e2e`
# is the pipeline path). Two modes (config from a gitignored bench/.env — see
# bench/.env.example):
#
#   docker (default) — SELF-CONTAINED: brings up the local Docker stack (MinIO +
#       Iceberg REST + Exasol), loads TPC-H into the local catalog, and verifies
#       wiring + first perf indicators. No AWS needed; .env is optional.
#   remote — runs against a real AWS Glue catalog + an external Exasol cluster
#       (the cluster perf phase, with PROFILE). Requires AWS_*/EXASOL_* in .env.
#
#   make bench
#   ./bench/run.sh selftest   # offline self-check of the string logic
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# Object names — match the e2e harness so the same .so entry points resolve.
SCHEMA=LHVS
ADAPTER=LAKEHOUSE_ADAPTER
SCAN=LAKEHOUSE_SCAN
MERGE=LAKEHOUSE_DISTINCT_MERGE_COUNT
DISTRIBUTOR=LAKEHOUSE_DISTRIBUTE_FILES
CONN=LAKEHOUSE_CATALOG_CREDS
VS=TPCH
# NB: BucketFS-path / SLC / skip-upload settings are env-overridable, so they are
# resolved AFTER .env is sourced (see the config section below), not here.

require() {
  local v
  for v in "$@"; do
    [ -n "${!v:-}" ] || { echo "ERROR: required env var '$v' is empty (set it in .env)"; exit 1; }
  done
}

# Build the VS_EXTRA_PROPS string: NR_OF_CORES + PARALLELISM_FACTOR, and optionally
# ALLOW_HTTP for targets that use plain-HTTP catalogs/storage (docker only).
# Args: allow_http (true|false) nr_of_cores parallelism_factor
build_vs_extra_props() {
  local allow_http="$1" nr_of_cores="$2" parallelism_factor="$3"
  local props
  props="$(printf "\n  NR_OF_CORES         = '%s'\n  PARALLELISM_FACTOR  = '%s'" \
    "${nr_of_cores}" "${parallelism_factor}")"
  [ "$allow_http" = "true" ] && \
    props="$(printf "\n  ALLOW_HTTP          = 'true'")${props}"
  # Sweep knobs (Task 7.1): append DataFusion threading props only when set in env,
  # so the default run is unchanged and the offline selftest still passes.
  [ -n "${BENCH_DF_THREADING_MODE:-}" ] && \
    props="${props}$(printf "\n  DATAFUSION_THREADING_MODE   = '%s'" "${BENCH_DF_THREADING_MODE}")"
  [ -n "${BENCH_DF_THREADS_PER_UDF:-}" ] && \
    props="${props}$(printf "\n  DATAFUSION_THREADS_PER_UDF  = '%s'" "${BENCH_DF_THREADS_PER_UDF}")"
  [ -n "${BENCH_DF_TARGET_PARTITIONS:-}" ] && \
    props="${props}$(printf "\n  DATAFUSION_TARGET_PARTITIONS = '%s'" "${BENCH_DF_TARGET_PARTITIONS}")"
  # Batch-size knob (raw-emit round-trip sweep): append DATAFUSION_BATCH_SIZE only
  # when set, so the default run AUTO-uses 8192 and the offline selftest stays unchanged.
  [ -n "${BENCH_DF_BATCH_SIZE:-}" ] && \
    props="${props}$(printf "\n  DATAFUSION_BATCH_SIZE       = '%s'" "${BENCH_DF_BATCH_SIZE}")"
  # Connection-concurrency knob (Task 9): append S3_MAX_CONNECTIONS only when set,
  # so the default run AUTO-derives it and the offline selftest stays unchanged.
  [ -n "${BENCH_S3_MAX_CONNECTIONS:-}" ] && \
    props="${props}$(printf "\n  S3_MAX_CONNECTIONS  = '%s'" "${BENCH_S3_MAX_CONNECTIONS}")"
  printf '%s' "${props}"
}

# Remote (AWS Glue) catalog-connection password JSON: SigV4 + static creds.
# The adapter requires a non-empty S3 `endpoint`; for real AWS S3 that's the
# regional endpoint (override with AWS_S3_ENDPOINT, e.g. for an S3-compat store).
build_conn_password_cloud() {
  local token_field="" s3_endpoint
  s3_endpoint="${AWS_S3_ENDPOINT:-https://s3.${AWS_REGION}.amazonaws.com}"
  [ -n "${AWS_SESSION_TOKEN:-}" ] && token_field=",\"session_token\":\"${AWS_SESSION_TOKEN}\""
  local json="{\"warehouse\":\"${GLUE_WAREHOUSE}\",\"endpoint\":\"${s3_endpoint}\",\"region\":\"${AWS_REGION}\",\"access_key\":\"${AWS_ACCESS_KEY_ID}\",\"secret_key\":\"${AWS_SECRET_ACCESS_KEY}\",\"path_style\":false,\"use_sigv4\":true,\"use_vended_credentials\":false${token_field}}"
  printf '%s' "${json//\'/\'\'}"  # SQL string-literal escaping: ' -> ''
}

# Docker (local MinIO) catalog-connection password JSON: path-style, no SigV4,
# internal MinIO endpoint. Mirrors stack.rs::local_stack_connection_password.
build_conn_password_local() {
  printf '%s' '{"warehouse":"s3://warehouse/","endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","path_style":true,"use_sigv4":false,"use_vended_credentials":false}'
}

# ---- delete-bearing benchmark (BENCH_WITH_DELETES) pure helpers --------------
# Kept above the selftest block (like build_conn_password_* / build_vs_extra_props)
# so `run.sh selftest` can exercise them offline with no DB.

# Report-header annotation: empty when deletes are OFF, a "\ndeletes=on ns=<ns>"
# line when ON (appended to the namespace= header line).
delete_header_suffix() {  # with_deletes ns -> "" | "\ndeletes=on ns=<ns>"
  [ "$1" = "1" ] && printf '\ndeletes=on ns=%s' "$2" || printf ''
}

# Delete-count sanity: exit 0 iff delete_count/baseline_count is in [0.90, 0.98]
# (the deterministic 5% modulo deletes applied on read -> ~0.95; guards against
# deletes being ignored (ratio ~1.0) or over-applied (ratio < 0.90)). Pure/offline
# (awk only) so the selftest can call it with literal numbers, no DB.
delete_ratio_ok() {  # delete_count baseline_count -> 0 if ratio in [0.90,0.98]
  awk -v d="$1" -v b="$2" 'BEGIN{ if (b<=0){exit 1} r=d/b; exit (r>=0.90 && r<=0.98) ? 0 : 1 }'
}

# Delete-namespace default resolution: an explicit override always wins; otherwise
# docker derives "<baseline>_deletes" (mirrors the local stack's namespace naming),
# other targets default to the fixed "tpch_deletes" (remote Glue catalog namespace).
resolve_delete_ns() {  # target baseline_ns override -> the effective delete namespace
  local target="$1" baseline="$2" override="$3"
  if [ -n "$override" ]; then printf '%s' "$override"; return; fi
  case "$target" in
    docker) printf '%s_deletes' "$baseline";;
    *)      printf 'tpch_deletes';;
  esac
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
  # build_vs_extra_props: docker path includes ALLOW_HTTP; remote path does not.
  docker_props="$(build_vs_extra_props true 8 16)"
  case "$docker_props" in *"ALLOW_HTTP"*"NR_OF_CORES"*"'8'"*"PARALLELISM_FACTOR"*"'16'"*) ;; \
    *) echo "FAIL: docker vs_extra_props shape: $docker_props"; exit 1;; esac
  remote_props="$(build_vs_extra_props false 8 8)"
  case "$remote_props" in *"ALLOW_HTTP"*) echo "FAIL: remote vs_extra_props must not contain ALLOW_HTTP: $remote_props"; exit 1;; esac
  case "$remote_props" in *"NR_OF_CORES"*"'8'"*"PARALLELISM_FACTOR"*"'8'"*) ;; \
    *) echo "FAIL: remote vs_extra_props shape: $remote_props"; exit 1;; esac
  # S3_MAX_CONNECTIONS is appended only when the env knob is set (default run omits it).
  case "$remote_props" in *"S3_MAX_CONNECTIONS"*) echo "FAIL: S3_MAX_CONNECTIONS must be absent when unset: $remote_props"; exit 1;; esac
  s3_props="$(BENCH_S3_MAX_CONNECTIONS=64 build_vs_extra_props false 8 1)"
  case "$s3_props" in *"PARALLELISM_FACTOR"*"'1'"*"S3_MAX_CONNECTIONS"*"'64'"*) ;; \
    *) echo "FAIL: S3_MAX_CONNECTIONS append shape: $s3_props"; exit 1;; esac
  # DATAFUSION_BATCH_SIZE is appended only when the env knob is set (default run omits it).
  case "$remote_props" in *"DATAFUSION_BATCH_SIZE"*) echo "FAIL: DATAFUSION_BATCH_SIZE must be absent when unset: $remote_props"; exit 1;; esac
  bs_props="$(BENCH_DF_BATCH_SIZE=131072 build_vs_extra_props false 8 8)"
  case "$bs_props" in *"PARALLELISM_FACTOR"*"'8'"*"DATAFUSION_BATCH_SIZE"*"'131072'"*) ;; \
    *) echo "FAIL: DATAFUSION_BATCH_SIZE append shape: $bs_props"; exit 1;; esac
  # delete_header_suffix: empty when OFF, "deletes=on ns=<ns>" when ON.
  off_suffix="$(delete_header_suffix 0 anything)"
  case "$off_suffix" in "") ;; *) echo "FAIL: delete_header_suffix OFF must be empty: $off_suffix"; exit 1;; esac
  on_suffix="$(delete_header_suffix 1 mydeletens)"
  case "$on_suffix" in *"deletes=on"*"mydeletens"*) ;; \
    *) echo "FAIL: delete_header_suffix ON shape: $on_suffix"; exit 1;; esac
  # delete_ratio_ok: accepts the [0.90,0.98] band (inclusive), rejects below/above it.
  if ! delete_ratio_ok 95 100; then echo "FAIL: delete_ratio_ok should accept 0.95 ratio"; exit 1; fi
  if delete_ratio_ok 80 100; then echo "FAIL: delete_ratio_ok should reject 0.80 ratio"; exit 1; fi
  if delete_ratio_ok 100 100; then echo "FAIL: delete_ratio_ok should reject 1.00 ratio (deletes not applied)"; exit 1; fi
  if ! delete_ratio_ok 90 100; then echo "FAIL: delete_ratio_ok should accept boundary 0.90"; exit 1; fi
  if ! delete_ratio_ok 98 100; then echo "FAIL: delete_ratio_ok should accept boundary 0.98"; exit 1; fi
  # resolve_delete_ns: docker default derives "<baseline>_deletes", other targets
  # default to "tpch_deletes", and an explicit override always wins.
  case "$(resolve_delete_ns docker tpch "")" in tpch_deletes) ;; \
    *) echo "FAIL: resolve_delete_ns docker default: $(resolve_delete_ns docker tpch "")"; exit 1;; esac
  case "$(resolve_delete_ns remote tpch "")" in tpch_deletes) ;; \
    *) echo "FAIL: resolve_delete_ns remote default: $(resolve_delete_ns remote tpch "")"; exit 1;; esac
  case "$(resolve_delete_ns docker tpch mycustomns)" in mycustomns) ;; \
    *) echo "FAIL: resolve_delete_ns override: $(resolve_delete_ns docker tpch mycustomns)"; exit 1;; esac
  echo "selftest OK"; exit 0
fi

# ---- config ------------------------------------------------------------------
# .env is optional (required vars validated per mode). It supplies DEFAULTS: a
# caller-exported BENCH_*/LAKEHOUSE_* value must WIN over .env, otherwise sweep.sh
# cannot override a knob that .env also sets (e.g. BENCH_PARALLELISM_FACTOR). So
# snapshot the caller's sweep overrides, source .env, then re-apply the snapshot.
if [ -f "$SCRIPT_DIR/.env" ]; then
  _env_overrides="$(export -p | grep -E ' (BENCH_|LAKEHOUSE_)[A-Za-z0-9_]+=' || true)"
  set -a; . "$SCRIPT_DIR/.env"; set +a
  [ -n "$_env_overrides" ] && eval "$_env_overrides"
fi

TARGET="${BENCH_TARGET:-docker}"
EXA_PORT="${LH_EXASOL_PORT:-28563}"
BFS_PORT="${LH_BUCKETFS_PORT:-22581}"
TPCH_SCALE="${TPCH_SCALE:-0.3}"

SLC_VERSION="${BENCH_SLC_VERSION:-0.21.0}"  # matches the .so ABI fingerprint; do not "upgrade" blindly
# BucketFS object path for the .so, as referenced by %udf_object in CREATE SCRIPT.
SO_UDF_OBJECT="${BENCH_SO_UDF_OBJECT:-buckets/bfsdefault/default/udf/liblakehouse_engine.so}"
# Debug level forwarded to the UDF via %udf_debug_level (0.19.0+ live debug surface).
# Valid values: debug|info|warn|error. Default: info (low-noise; set to debug for traces).
UDF_DEBUG_LEVEL="${LAKEHOUSE_UDF_DEBUG_LEVEL:-info}"
# BucketFS path of the EXTRACTED SLC dir (the RUST language alias points inside it,
# at <SLC_BUCKET_PATH>/exaudf/exaudfclient). A foo.tar.gz upload extracts to foo, so
# set this to bfsdefault/<bucket>/<archive-name-without-.tar.gz>.
SLC_BUCKET_PATH="${BENCH_SLC_BUCKET_PATH:-bfsdefault/default/slc/lakehouse-rustslc}"
# Skip BucketFS uploads (SLC tarball + .so) — use when both are already staged in
# BucketFS (e.g. uploaded via AdminUI). Still registers the RUST alias + builds the VS.
SKIP_UPLOAD="${BENCH_SKIP_UPLOAD:-0}"

case "$TARGET" in
  docker)
    HOST=localhost
    SYS_PASS="${EXASOL_SYS_PASSWORD:-exasol}"
    export EXASOL_SYS_PASSWORD="$SYS_PASS"
    NAMESPACE="${ICEBERG_NAMESPACE:-tpch}"
    CATALOG_URI="http://iceberg-rest:8181"          # internal: reachable from the UDF
    CONN_PW="$(build_conn_password_local)"
    # http catalog/S3 + parallelism knobs. NR_OF_CORES (new VS property) drives the
    # DataFusion target-partitions / threads-per-UDF defaults so scans use the cores;
    # multi-file tables (loader) + PARALLELISM_FACTOR drive the GROUP BY shard_key fan-out.
    VS_EXTRA_PROPS="$(build_vs_extra_props true "${BENCH_NR_OF_CORES:-4}" "${BENCH_PARALLELISM_FACTOR:-8}")"
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
    VS_EXTRA_PROPS="$(build_vs_extra_props false "${BENCH_NR_OF_CORES:-4}" "${BENCH_PARALLELISM_FACTOR:-8}")"
    PROFILE_ON="${BENCH_PROFILE:-1}"
    ;;
  *) echo "ERROR: BENCH_TARGET must be 'docker' or 'remote' (got '$TARGET')"; exit 1;;
esac

# ---- BENCH_WITH_DELETES: run the suite against a delete-bearing namespace -----
# OFF (default 0): inert. NAMESPACE stays the baseline resolved in the case block
# above, and every block gated on WITH_DELETES=1 below is skipped -> the OFF path
# is byte-for-byte today's behavior. ON: capture the baseline ns and resolve the
# delete ns (default per mode, override via BENCH_DELETE_NAMESPACE); the actual
# NAMESPACE swap happens AFTER data load so tpch_loader still populates BASELINE_NS.
WITH_DELETES="${BENCH_WITH_DELETES:-0}"
if [ "$WITH_DELETES" = "1" ]; then
  BASELINE_NS="$NAMESPACE"
  DELETE_NS="$(resolve_delete_ns "$TARGET" "$BASELINE_NS" "${BENCH_DELETE_NAMESPACE:-}")"
fi

DSN="exasol://sys:${SYS_PASS}@${HOST}:${EXA_PORT}?validateservercertificate=0"
mkdir -p "$SCRIPT_DIR/reports"
REPORT="$SCRIPT_DIR/reports/bench-report-$(date +%Y%m%d-%H%M%S).txt"
FAILED=0

# exapump reads SQL from stdin (keeps secrets out of argv); -f sets output format.
sql()  { printf '%s' "$1" | exapump sql -d "$DSN"; }
sqlf() { printf '%s' "$1" | exapump sql -d "$DSN" -f "${2:-csv}"; }
# First data cell of a single-value query (skip CSV header, strip quotes/space).
query_scalar() { printf '%s' "$1" | exapump sql -d "$DSN" -f csv | tail -n +2 | head -1 | tr -d '"[:space:]'; }

# Register/replace the RUST language alias to point at an already-staged SLC in
# BucketFS (skip-upload path). Mirrors Makefile install-slc's merge logic, but
# path-parameterized via SLC_BUCKET_PATH. Preserves all non-RUST language defs.
register_rust_alias() {
  local rust_def current new
  rust_def="RUST=localzmq+protobuf:///${SLC_BUCKET_PATH}?lang=rust#buckets/${SLC_BUCKET_PATH}/exaudf/exaudfclient"
  # Raw value (keeps internal spaces; only strip surrounding quotes) — query_scalar would mangle it.
  current="$(printf '%s' "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'" \
    | exapump sql -d "$DSN" -f csv | tail -n +2 | head -1 | sed 's/^"//;s/"$//')"
  new="$(echo "$current $rust_def" | awk '{sep=""; for(i=1;i<=NF;i++){if($i ~ /^RUST=/ && i<NF) continue; printf "%s%s",sep,$i; sep=" "}}')"
  echo "  SCRIPT_LANGUAGES <- ${new}"
  sql "ALTER SYSTEM SET SCRIPT_LANGUAGES = '${new}'"
}

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
  echo "== loading TPC-H (SF=${TPCH_SCALE}, big tables in ${TPCH_FILES:-4} files) into namespace '${NAMESPACE}' =="
  TPCH_SCALE="$TPCH_SCALE" ICEBERG_NAMESPACE="$NAMESPACE" TPCH_FILES="${TPCH_FILES:-4}" \
    cargo test --features exasol-e2e --test tpch_loader -- --nocapture
  if [ "$WITH_DELETES" = "1" ]; then
    echo "== authoring delete-bearing namespace '${DELETE_NS}' from baseline '${BASELINE_NS}' (docker, idempotent) =="
    "$SCRIPT_DIR/make_deletes_docker.sh" "$BASELINE_NS" "$DELETE_NS"
  fi
else
  wait_exasol
fi

# With deletes ON, everything below (VS build, timed queries, pushdown checks, row
# counts) targets the delete-bearing namespace; the baseline was just loaded
# (docker) / pre-exists (remote) and stays reachable via BASELINE_NS for the
# delete-count sanity check.
if [ "$WITH_DELETES" = "1" ]; then
  NAMESPACE="$DELETE_NS"
fi

# ---- install SLC + upload .so (or skip if already staged in BucketFS) --------
if [ "$SKIP_UPLOAD" = "1" ]; then
  echo "== SKIP_UPLOAD=1: assuming SLC + .so already in BucketFS =="
  echo "   SLC dir: ${SLC_BUCKET_PATH}    .so: ${SO_UDF_OBJECT}"
  register_rust_alias
else
  echo "== installing SLC ${SLC_VERSION} + uploading .so =="
  make install-slc bucketfs-upload-so \
    EXASOL_HOST="$HOST" LH_EXASOL_PORT="$EXA_PORT" LH_BUCKETFS_PORT="$BFS_PORT" \
    SLC_VERSION="$SLC_VERSION" EXASOL_SYS_PASSWORD="$SYS_PASS"
fi

# ---- create schema, scripts, connection, VS (all idempotent) -----------------
echo "== creating schema, scripts, connection, VS '${VS}' =="
sql "CREATE SCHEMA IF NOT EXISTS ${SCHEMA}"
sql "CREATE OR REPLACE RUST ADAPTER SCRIPT ${SCHEMA}.${ADAPTER} AS
%udf_object ${SO_UDF_OBJECT}
%udf_debug_level ${UDF_DEBUG_LEVEL}
/"
sql "CREATE OR REPLACE RUST SCALAR SCRIPT ${SCHEMA}.${SCAN}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object ${SO_UDF_OBJECT}
%udf_debug_level ${UDF_DEBUG_LEVEL}
/"
# Scalar distinct-merge script — third entry point in the same .so, created in
# ${SCHEMA} so the pushdown wrapper SQL resolves it schema-qualified like the
# scan script. Unions per-shard COUNT(DISTINCT) local sets and returns the cardinality.
sql "CREATE OR REPLACE RUST SCALAR SCRIPT ${SCHEMA}.${MERGE}(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
%udf_object ${SO_UDF_OBJECT}
%udf_debug_level ${UDF_DEBUG_LEVEL}
/"
# File distributor — LUA SET SCRIPT, pure passthrough. Not a Rust entry point:
# does the cross-node GROUP BY shard_key fan-out for the shard-invariant files
# list only, carrying no row data.
sql "CREATE OR REPLACE LUA SET SCRIPT ${SCHEMA}.${DISTRIBUTOR}(files VARCHAR(2000000))
EMITS (files VARCHAR(2000000)) AS
function run(ctx)
    repeat
        ctx.emit(ctx.files)
    until not ctx.next()
end
/"
sql "CREATE OR REPLACE CONNECTION ${CONN} TO '${CATALOG_URI//\'/\'\'}' USER '' IDENTIFIED BY '${CONN_PW}'"
# Build a virtual schema against a given namespace (idempotent DROP+CREATE). Called
# once normally; when BENCH_WITH_DELETES=1 it is called a SECOND time to build a
# lightweight ${VS}_BASELINE against the untouched baseline ns so the delete-count
# sanity check can compare LINEITEM row counts. Reuses the same adapter/scan/merge
# scripts + CATALOG connection + VS_EXTRA_PROPS; only the name + namespace differ.
build_vs() {  # vs_name namespace
  sql "DROP VIRTUAL SCHEMA IF EXISTS $1 CASCADE" || true
  sql "CREATE VIRTUAL SCHEMA $1
USING ${SCHEMA}.${ADAPTER} WITH
  CATALOG_CONNECTION  = '${CONN}'
  ICEBERG_NAMESPACE   = '$2'${VS_EXTRA_PROPS}"
}
build_vs "${VS}" "${NAMESPACE}"
if [ "$WITH_DELETES" = "1" ]; then
  echo "== building baseline VS '${VS}_BASELINE' (ns '${BASELINE_NS}') for delete-count sanity =="
  build_vs "${VS}_BASELINE" "${BASELINE_NS}"
fi

# Telemetry harness hook (Task 6.2): build scripts+VS then stop, so a separate
# single-leg session can drive queries under a SCRIPT_OUTPUT_ADDRESS redirect
# without the bench's multi-leg joins (which can crash under debug tracing).
if [ "${BENCH_DDL_ONLY:-0}" = "1" ]; then
  echo "== BENCH_DDL_ONLY=1: scripts + VS '${VS}' created (debug_level=${UDF_DEBUG_LEVEL}); skipping queries =="
  exit 0
fi

# PROFILE must be ON *before* the timed queries run — EXA_USER_PROFILE_LAST_DAY
# only has data for statements executed while profiling was enabled, so turning
# it on after the fact (the previous location of this line) captured nothing.
if [ "$TARGET" = "remote" ] && [ "$PROFILE_ON" = "1" ]; then
  sql "ALTER SYSTEM SET PROFILE = 'ON'" || true
fi

{
  echo "lakehouse-engine benchmark — ${TARGET} @ ${HOST}:${EXA_PORT} — $(date)"
  printf 'namespace=%s%s\n' "${NAMESPACE}" "$(delete_header_suffix "$WITH_DELETES" "${DELETE_NS:-}")"
  echo
  echo "== tables exposed by ${VS} =="
} | tee "$REPORT"
sqlf "SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA='${VS}' ORDER BY TABLE_NAME" | tee -a "$REPORT"

# Remote mode never loads data: when BENCH_WITH_DELETES=1 the delete namespace must
# have been pre-authored (deploy/scripts/make-deletes-remote.sh). Hard-stop if the
# VS exposes no tables — unlike run_query's soft FAILED, nothing downstream can
# produce meaningful results against an empty virtual schema.
if [ "$TARGET" = "remote" ] && [ "$WITH_DELETES" = "1" ]; then
  ntab="$(query_scalar "SELECT COUNT(*) FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA='${VS}'")"
  if [ "${ntab:-0}" -gt 0 ] 2>/dev/null; then
    echo "  OK    delete namespace '${NAMESPACE}' resolves ${ntab} table(s) via ${VS}" | tee -a "$REPORT"
  else
    echo "ERROR: BENCH_WITH_DELETES=1 but delete namespace '${NAMESPACE}' exposes no tables via ${VS}." | tee -a "$REPORT"
    echo "       Author it once first: deploy/scripts/make-deletes-remote.sh (see its header for required env vars)" | tee -a "$REPORT"
    exit 1
  fi
fi

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
# Delete-count sanity (BENCH_WITH_DELETES): compare LINEITEM in the delete ns
# (${VS}, now the delete ns) against the untouched baseline (${VS}_BASELINE). Mirrors
# check_count's OK/FAIL + FAILED=1 convention, but runs in the MAIN shell (tee inside,
# like run_query — NOT a `{ } | tee` subshell) so FAILED actually propagates. The pure
# bounds test is delete_ratio_ok (offline-selftested).
check_delete_ratio() {
  local del base pct
  { echo; echo "== delete-count sanity (LINEITEM 90-98% of baseline) =="; } | tee -a "$REPORT"
  del="$(query_scalar "SELECT COUNT(*) FROM ${VS}.LINEITEM")"
  base="$(query_scalar "SELECT COUNT(*) FROM ${VS}_BASELINE.LINEITEM")"
  pct="$(awk -v d="${del:-0}" -v b="${base:-0}" 'BEGIN{ if (b>0) printf "%.1f", d/b*100 }')"
  if delete_ratio_ok "${del:-0}" "${base:-0}"; then
    echo "  OK    delete-count LINEITEM: ${del} (~${pct}% of baseline ${base})" | tee -a "$REPORT"
  else
    echo "  FAIL  delete-count LINEITEM: ${del:-<none>} (${pct:-?}% of baseline ${base:-<none>}); expected 90-98%" | tee -a "$REPORT"
    FAILED=1
  fi
}
if [ "$TARGET" = "docker" ]; then
  { echo; echo "== row counts (REGION/NATION are scale-independent) =="; } | tee -a "$REPORT"
  # With deletes ON the small dims lose 0-1 rows (R_REGIONKEY 0, N_NATIONKEY 0/20
  # satisfy key%20=0), so their exact baseline counts no longer hold — assert only
  # > 0 for them when ON. The cost-dominant tables already assert only > 0.
  region_exp=5; nation_exp=25
  if [ "$WITH_DELETES" = "1" ]; then region_exp=""; nation_exp=""; fi
  { check_count REGION "$region_exp"
    check_count NATION "$nation_exp"
    check_count SUPPLIER
    check_count CUSTOMER
    check_count PART
    check_count PARTSUPP
    check_count ORDERS
    check_count LINEITEM
  } | tee -a "$REPORT"
fi

# Delete-count sanity (flag-gated, both modes): LINEITEM in the delete ns must be
# 90-98% of the baseline ns — proves the 5% position deletes are applied on read.
if [ "$WITH_DELETES" = "1" ]; then
  check_delete_ratio
fi

# ---- TPC-H JOIN query set: plain SELECTs with wall-clock (first perf signal) --
# ponytail: assumes a FLAT namespace -> table names = uppercased Iceberg names
# (LINEITEM, ORDERS, ...). A nested namespace flattens to NS__TABLE
# (flatten_table_name, adapter/tables.rs) — adjust these names if so.
run_query() {
  local name="$1" q="$2" t0 t1
  { echo; echo "### ${name}"; } | tee -a "$REPORT"
  t0=$(date +%s.%N)
  # `if !` guards the pipeline from set -e (same pattern as check_count/pushdown_check below) —
  # a genuinely failing query (e.g. an engine limitation the query is designed to probe) must not
  # abort the rest of the queries/pushdown checks. FAILED still fails the overall run at the end.
  if ! sqlf "$q" | tee -a "$REPORT"; then
    t1=$(date +%s.%N)
    echo "  FAILED" | tee -a "$REPORT"
    printf 'elapsed: %ss (FAILED)\n' "$(awk "BEGIN{printf \"%.2f\", ${t1}-${t0}}")" | tee -a "$REPORT"
    FAILED=1
    return
  fi
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

run_query "Q4 lineitem pricing summary (TPC-H Q1 shape; multi-file -> parallel scan)" \
"SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS sum_qty, SUM(L_EXTENDEDPRICE) AS sum_base_price,
        AVG(L_DISCOUNT) AS avg_disc, COUNT(*) AS count_order
 FROM ${VS}.LINEITEM
 WHERE L_SHIPDATE <= DATE '1998-09-01'
 GROUP BY L_RETURNFLAG, L_LINESTATUS
 ORDER BY L_RETURNFLAG, L_LINESTATUS"

# ---- Q5-Q9b: added to probe specific pushdown strengths/weaknesses beyond Q1-Q4 -------------
# (competitive-comparison follow-up). Identical SQL (dialect-adjusted) in bench/athena_compare.sh,
# bench/trino_compare.sh, deploy/scripts/spark_queries.py — keep all four in sync if you edit one.

run_query "Q5 orders x lineitem GROUP BY, no filter (Q3 minus WHERE)" \
"SELECT o.O_ORDERPRIORITY, COUNT(*) AS cnt, SUM(l.L_EXTENDEDPRICE) AS revenue
 FROM ${VS}.ORDERS o
 JOIN ${VS}.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY
 GROUP BY o.O_ORDERPRIORITY
 ORDER BY o.O_ORDERPRIORITY"

run_query "Q6 lineitem pricing summary, no filter (Q4 minus WHERE)" \
"SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS sum_qty, SUM(L_EXTENDEDPRICE) AS sum_base_price,
        AVG(L_DISCOUNT) AS avg_disc, COUNT(*) AS count_order
 FROM ${VS}.LINEITEM
 GROUP BY L_RETURNFLAG, L_LINESTATUS
 ORDER BY L_RETURNFLAG, L_LINESTATUS"

run_query "Q7 high-cardinality GROUP BY (~45M distinct L_ORDERKEY groups)" \
"SELECT COUNT(*) FROM (SELECT L_ORDERKEY, COUNT(*) AS cnt FROM ${VS}.LINEITEM GROUP BY L_ORDERKEY) t"

run_query "Q8 highly selective filter (single ship-date, <0.05% of rows)" \
"SELECT COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPDATE = DATE '1995-06-15'"

run_query "Q9a narrow projection (single-column full scan)" \
"SELECT SUM(L_QUANTITY) FROM ${VS}.LINEITEM"

run_query "Q9b wide projection (all 16 lineitem columns, full scan)" \
"SELECT COUNT(*),
        SUM(L_ORDERKEY), SUM(L_PARTKEY), SUM(L_SUPPKEY), SUM(L_LINENUMBER),
        SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), SUM(L_DISCOUNT), SUM(L_TAX),
        COUNT(DISTINCT L_RETURNFLAG), COUNT(DISTINCT L_LINESTATUS),
        MIN(L_SHIPDATE), MAX(L_COMMITDATE), MIN(L_RECEIPTDATE),
        COUNT(DISTINCT L_SHIPINSTRUCT), COUNT(DISTINCT L_SHIPMODE),
        SUM(LENGTH(L_COMMENT))
 FROM ${VS}.LINEITEM"

# ---- NQ1-NQ5: added to close the arithmetic-aggregate-pushdown gap + probe LIKE/IN filters,
# ORDER BY+LIMIT, a 4-way join, and GROUP BY+HAVING (add-arithmetic-aggregate-pushdown-and-
# benchmark-suite). Identical SQL (dialect-adjusted) in bench/athena_compare.sh,
# bench/trino_compare.sh, deploy/scripts/spark_queries.py — keep all four in sync if you edit one.

run_query "NQ1 revenue query (TPC-H Q6 shape; arithmetic aggregate pushdown target)" \
"SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue
 FROM ${VS}.LINEITEM
 WHERE L_SHIPDATE >= DATE '1994-01-01' AND L_SHIPDATE < DATE '1995-01-01'
   AND L_DISCOUNT BETWEEN 0.05 AND 0.07 AND L_QUANTITY < 24"

run_query "NQ2 LIKE + IN filter (comment pattern match)" \
"SELECT COUNT(*) FROM ${VS}.LINEITEM
 WHERE L_SHIPMODE IN ('AIR','REG AIR') AND L_COMMENT LIKE '%late%'"

run_query "NQ3 part x partsupp x supplier x nation (4-way join + filter)" \
"SELECT COUNT(*) AS cnt, SUM(ps.PS_SUPPLYCOST) AS total_cost
 FROM ${VS}.PART p
 JOIN ${VS}.PARTSUPP ps ON p.P_PARTKEY = ps.PS_PARTKEY
 JOIN ${VS}.SUPPLIER s ON ps.PS_SUPPKEY = s.S_SUPPKEY
 JOIN ${VS}.NATION n ON s.S_NATIONKEY = n.N_NATIONKEY
 WHERE p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS%' AND n.N_NAME = 'GERMANY'"

run_query "NQ4 top-N by price (ORDER BY + LIMIT)" \
"SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM ${VS}.LINEITEM
 ORDER BY L_EXTENDEDPRICE DESC LIMIT 20"

run_query "NQ5 orders GROUP BY + HAVING (high-cardinality group filter)" \
"SELECT O_ORDERPRIORITY, O_ORDERSTATUS, COUNT(*) AS cnt, AVG(O_TOTALPRICE) AS avg_price
 FROM ${VS}.ORDERS
 GROUP BY O_ORDERPRIORITY, O_ORDERSTATUS
 HAVING COUNT(*) > 1000000
 ORDER BY O_ORDERPRIORITY, O_ORDERSTATUS"

# ---- pushdown analysis: confirm projection/filter/limit + shard fan-out ------
# EXPLAIN VIRTUAL is introspection (not a timed query): it prints the scan spec the
# adapter generates. Assert the expected elements are actually pushed into the scan.
pushdown_check() {
  local name="$1" q="$2"; shift 2
  local out needle
  { echo; echo "### PUSHDOWN: ${name}"; } | tee -a "$REPORT"
  if ! out="$(sqlf "EXPLAIN VIRTUAL ${q}" 2>&1)"; then
    echo "  FAIL  EXPLAIN VIRTUAL errored" | tee -a "$REPORT"; echo "$out" >>"$REPORT"; FAILED=1; return
  fi
  echo "$out" >>"$REPORT"
  for needle in "$@"; do
    if printf '%s' "$out" | grep -qiF -- "$needle"; then
      echo "  OK    pushed: ${needle}" | tee -a "$REPORT"
    else
      echo "  FAIL  not pushed: ${needle}" | tee -a "$REPORT"; FAILED=1
    fi
  done
}

# Shard fan-out active for the multi-file LINEITEM (one shard per file).
pushdown_check "shard fan-out (multi-file LINEITEM)" \
  "SELECT COUNT(*) FROM ${VS}.LINEITEM" "shard_key"
# LIMIT pushdown.
pushdown_check "LIMIT" \
  "SELECT * FROM ${VS}.LINEITEM LIMIT 10" "limit"
# Projection + filter (BETWEEN) + single-group aggregate.
pushdown_check "filter (BETWEEN) + projection" \
  "SELECT COUNT(*), MIN(L_SHIPDATE), MAX(L_SHIPDATE), AVG(L_EXTENDEDPRICE) FROM ${VS}.LINEITEM WHERE L_DISCOUNT BETWEEN 0.05 AND 0.07" \
  "filter" "L_DISCOUNT"
# Filter with GROUP BY aggregate (TPC-H Q1 shape).
pushdown_check "filter + GROUP BY agg" \
  "SELECT L_RETURNFLAG, L_LINESTATUS, COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPDATE <= DATE '1998-09-01' GROUP BY L_RETURNFLAG, L_LINESTATUS" \
  "filter" "L_RETURNFLAG"
# Complex predicate: IN list + OR + comparison.
pushdown_check "filter (IN / OR / comparison)" \
  "SELECT COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPMODE IN ('AIR','RAIL') AND (L_RETURNFLAG = 'R' OR L_QUANTITY > 45)" \
  "filter" "AIR"
# Q9b: mixed expression-argument aggregate (SUM(LENGTH(...))) + single-group COUNT(DISTINCT)
# (add-count-distinct-and-expression-aggregate-pushdown, issue #56). Before this feature, either
# shape alone collapsed detect_aggregates to a full 16-column raw row-scan fallback — no
# "aggregates" field at all, just a raw LAKEHOUSE_SCAN(...) EMITS (16 columns). Both shapes must
# now decompose into a real partial-aggregate scan: "aggregates" present, a "countdistinct" kind
# for the COUNT(DISTINCT ...) columns, and "arg_expr" for the SUM(LENGTH(L_COMMENT)) argument.
pushdown_check "Q9b mixed expression + COUNT(DISTINCT) aggregate pushdown" \
  "SELECT COUNT(*),
          SUM(L_ORDERKEY), SUM(L_PARTKEY), SUM(L_SUPPKEY), SUM(L_LINENUMBER),
          SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), SUM(L_DISCOUNT), SUM(L_TAX),
          COUNT(DISTINCT L_RETURNFLAG), COUNT(DISTINCT L_LINESTATUS),
          MIN(L_SHIPDATE), MAX(L_COMMITDATE), MIN(L_RECEIPTDATE),
          COUNT(DISTINCT L_SHIPINSTRUCT), COUNT(DISTINCT L_SHIPMODE),
          SUM(LENGTH(L_COMMENT))
   FROM ${VS}.LINEITEM" \
  "aggregates" "countdistinct" "arg_expr"
# NQ1: SUM over a two-column binary-arithmetic argument (add-arithmetic-aggregate-pushdown-and-
# benchmark-suite, Group A). Before that feature lands this collapses to a raw 2-column row-scan
# fallback (no "aggregates"/"arg_expr" in the scan spec) — this check documents the TARGET
# behavior and is expected to FAIL until Group A (capability advertisement + operator-name
# reconciliation + expression-argument SUM type derivation) merges. Do not weaken it to pass early.
pushdown_check "NQ1 arithmetic aggregate pushdown (SUM(L_EXTENDEDPRICE * L_DISCOUNT))" \
  "SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue FROM ${VS}.LINEITEM
   WHERE L_SHIPDATE >= DATE '1994-01-01' AND L_SHIPDATE < DATE '1995-01-01'
     AND L_DISCOUNT BETWEEN 0.05 AND 0.07 AND L_QUANTITY < 24" \
  "aggregates" "arg_expr"
# NQ2: LIKE + IN predicates reach the scan spec's filter.
pushdown_check "NQ2 LIKE + IN filter pushdown" \
  "SELECT COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPMODE IN ('AIR','REG AIR') AND L_COMMENT LIKE '%late%'" \
  "filter" "LIKE" "REG AIR"
# NQ4: ORDER BY + LIMIT pushed as a per-shard bounded top-N + Exasol-side merge
# (add-topn-pushdown). Before that feature this collapses to a bare, unlimited
# 2-column raw scan (no "order_by" key, no "limit" in the common spec) — Exasol
# sorts/limits the whole table itself. After the feature the pushed common spec
# carries an "order_by" key AND a "limit", and the outer merge SQL also renders
# its own final "ORDER BY ... LIMIT" (self-contained per decision [5]).
pushdown_check "NQ4 top-N (ORDER BY + LIMIT) pushdown" \
  "SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM ${VS}.LINEITEM ORDER BY L_EXTENDEDPRICE DESC LIMIT 20" \
  "order_by" "LIMIT"

# ---- remote-only best-effort PROFILE dump ------------------------------------
if [ "$TARGET" = "remote" ] && [ "$PROFILE_ON" = "1" ]; then
  echo | tee -a "$REPORT"
  echo "== PROFILE (most recent statements, best-effort) ==" | tee -a "$REPORT"
  sqlf "SELECT STMT_ID, PART_ID, PART_NAME, OBJECT_NAME, OBJECT_ROWS, DURATION, CPU
        FROM EXA_USER_PROFILE_LAST_DAY
        ORDER BY STMT_ID DESC, PART_ID
        LIMIT 200" >>"$REPORT" 2>&1 || echo "(profile unavailable)" >>"$REPORT"
fi

echo
if [ "$FAILED" -ne 0 ]; then
  echo "BENCHMARK FAILED (see counts above). Report: ${REPORT}"; exit 1
fi
echo "Done. Full report: ${REPORT}"
