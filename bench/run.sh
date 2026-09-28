#!/usr/bin/env bash
# Manually invoked live benchmark of the VS (config: gitignored bench/.env, see bench/.env.example).
#   docker (default) — brings up the local stack, loads TPC-H, checks wiring; no AWS needed.
#   remote — real catalog + external Exasol cluster, with PROFILE.
#
#   make bench
#   ./bench/run.sh selftest   # offline self-check of the string logic
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# Match the e2e harness so the same .so entry points resolve.
SCHEMA=LHVS
ADAPTER=LAKEHOUSE_ADAPTER
SCAN=LAKEHOUSE_SCAN
DISTRIBUTOR=LAKEHOUSE_DISTRIBUTE_FILES
CONN=LAKEHOUSE_CATALOG_CREDS
VS=TPCH

require() {
  local v
  for v in "$@"; do
    [ -n "${!v:-}" ] || { echo "ERROR: required env var '$v' is empty (set it in .env)"; exit 1; }
  done
}

# Optional knobs are appended only when their env var is set, so the default run keeps AUTO values.
build_vs_extra_props() {  # allow_http(true|false) parallelism_factor
  local allow_http="$1" parallelism_factor="$2"
  local props
  props="$(printf "\n  PARALLELISM_FACTOR  = '%s'" \
    "${parallelism_factor}")"
  [ "$allow_http" = "true" ] && \
    props="$(printf "\n  ALLOW_HTTP          = 'true'")${props}"
  [ -n "${BENCH_DF_THREADING_MODE:-}" ] && \
    props="${props}$(printf "\n  DATAFUSION_THREADING_MODE   = '%s'" "${BENCH_DF_THREADING_MODE}")"
  [ -n "${BENCH_DF_THREADS_PER_UDF:-}" ] && \
    props="${props}$(printf "\n  DATAFUSION_THREADS_PER_UDF  = '%s'" "${BENCH_DF_THREADS_PER_UDF}")"
  [ -n "${BENCH_DF_TARGET_PARTITIONS:-}" ] && \
    props="${props}$(printf "\n  DATAFUSION_TARGET_PARTITIONS = '%s'" "${BENCH_DF_TARGET_PARTITIONS}")"
  [ -n "${BENCH_DF_BATCH_SIZE:-}" ] && \
    props="${props}$(printf "\n  DATAFUSION_BATCH_SIZE       = '%s'" "${BENCH_DF_BATCH_SIZE}")"
  [ -n "${BENCH_S3_MAX_CONNECTIONS:-}" ] && \
    props="${props}$(printf "\n  S3_MAX_CONNECTIONS  = '%s'" "${BENCH_S3_MAX_CONNECTIONS}")"
  printf '%s' "${props}"
}

# The adapter requires a non-empty S3 `endpoint`; for real AWS S3 that is the regional endpoint.
build_conn_password_cloud() {
  local token_field="" s3_endpoint
  s3_endpoint="${AWS_S3_ENDPOINT:-https://s3.${AWS_REGION}.amazonaws.com}"
  [ -n "${AWS_SESSION_TOKEN:-}" ] && token_field=",\"session_token\":\"${AWS_SESSION_TOKEN}\""
  local json="{\"warehouse\":\"${GLUE_WAREHOUSE}\",\"endpoint\":\"${s3_endpoint}\",\"region\":\"${AWS_REGION}\",\"access_key\":\"${AWS_ACCESS_KEY_ID}\",\"secret_key\":\"${AWS_SECRET_ACCESS_KEY}\",\"path_style\":false,\"use_sigv4\":true,\"use_vended_credentials\":false${token_field}}"
  printf '%s' "${json//\'/\'\'}"  # SQL string-literal escaping: ' -> ''
}

# Separate payload: the adapter rejects a CONNECTION combining use_sigv4 with OAuth2 client
# credentials. S3 access uses the same engine-reader key pair as the Glue payload.
build_conn_password_lakekeeper() {
  local s3_endpoint
  s3_endpoint="${AWS_S3_ENDPOINT:-https://s3.${AWS_REGION}.amazonaws.com}"
  local json="{\"warehouse\":\"${LAKEKEEPER_WAREHOUSE}\",\"client_id\":\"${LAKEKEEPER_CLIENT_ID}\",\"client_secret\":\"${LAKEKEEPER_CLIENT_SECRET}\",\"oauth2_server_uri\":\"${LAKEKEEPER_TOKEN_URI}\",\"endpoint\":\"${s3_endpoint}\",\"region\":\"${AWS_REGION}\",\"access_key\":\"${AWS_ACCESS_KEY_ID}\",\"secret_key\":\"${AWS_SECRET_ACCESS_KEY}\",\"path_style\":false,\"use_vended_credentials\":false}"
  printf '%s' "${json//\'/\'\'}"  # SQL string-literal escaping: ' -> ''
}

# Mirrors stack.rs::local_stack_connection_password.
build_conn_password_local() {
  printf '%s' '{"warehouse":"s3://warehouse/","endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","path_style":true,"use_sigv4":false,"use_vended_credentials":false}'
}

# Remote target only: the docker catalog is neither Glue nor Lakekeeper, so a field there would
# mislabel the run.
catalog_header_field() {  # target catalog_name -> "" | "catalog=<name>\n"
  [ "$1" = "remote" ] && printf 'catalog=%s\n' "$2" || printf ''
}

# Pure helpers stay above the selftest block so it can exercise them offline.
delete_header_suffix() {  # with_deletes ns -> "" | "\ndeletes=on ns=<ns>"
  [ "$1" = "1" ] && printf '\ndeletes=on ns=%s' "$2" || printf ''
}

# The deterministic 5% deletes give ~0.95; ~1.0 means deletes were ignored, < 0.90 over-applied.
delete_ratio_ok() {  # delete_count baseline_count -> 0 if ratio in [0.90,0.98]
  awk -v d="$1" -v b="$2" 'BEGIN{ if (b<=0){exit 1} r=d/b; exit (r>=0.90 && r<=0.98) ? 0 : 1 }'
}

resolve_delete_ns() {  # target baseline_ns override -> the effective delete namespace
  local target="$1" baseline="$2" override="$3"
  if [ -n "$override" ]; then printf '%s' "$override"; return; fi
  case "$target" in
    docker) printf '%s_deletes' "$baseline";;
    *)      printf 'tpch_deletes';;
  esac
}

if [ "${1:-}" = "selftest" ]; then
  GLUE_WAREHOUSE="s3://b/w/" AWS_REGION=r AWS_ACCESS_KEY_ID=k \
  AWS_SECRET_ACCESS_KEY="se'cret" AWS_SESSION_TOKEN="" \
    out="$(build_conn_password_cloud)"
  case "$out" in *"se''cret"*) ;; *) echo "FAIL: single-quote not escaped: $out"; exit 1;; esac
  case "$out" in *'"use_sigv4":true'*) ;; *) echo "FAIL: cloud missing use_sigv4"; exit 1;; esac
  case "$(build_conn_password_local)" in
    *'"path_style":true'*'"use_sigv4":false'*) ;; *) echo "FAIL: local password shape"; exit 1;; esac
  if (require __DEFINITELY_UNSET_VAR__ >/dev/null 2>&1); then echo "FAIL: require passed on unset"; exit 1; fi
  docker_props="$(build_vs_extra_props true 16)"
  case "$docker_props" in *"ALLOW_HTTP"*"PARALLELISM_FACTOR"*"'16'"*) ;; \
    *) echo "FAIL: docker vs_extra_props shape: $docker_props"; exit 1;; esac
  remote_props="$(build_vs_extra_props false 8)"
  case "$remote_props" in *"ALLOW_HTTP"*) echo "FAIL: remote vs_extra_props must not contain ALLOW_HTTP: $remote_props"; exit 1;; esac
  case "$remote_props" in *"PARALLELISM_FACTOR"*"'8'"*) ;; \
    *) echo "FAIL: remote vs_extra_props shape: $remote_props"; exit 1;; esac
  case "$remote_props" in *"S3_MAX_CONNECTIONS"*) echo "FAIL: S3_MAX_CONNECTIONS must be absent when unset: $remote_props"; exit 1;; esac
  s3_props="$(BENCH_S3_MAX_CONNECTIONS=64 build_vs_extra_props false 1)"
  case "$s3_props" in *"PARALLELISM_FACTOR"*"'1'"*"S3_MAX_CONNECTIONS"*"'64'"*) ;; \
    *) echo "FAIL: S3_MAX_CONNECTIONS append shape: $s3_props"; exit 1;; esac
  case "$remote_props" in *"DATAFUSION_BATCH_SIZE"*) echo "FAIL: DATAFUSION_BATCH_SIZE must be absent when unset: $remote_props"; exit 1;; esac
  bs_props="$(BENCH_DF_BATCH_SIZE=131072 build_vs_extra_props false 8)"
  case "$bs_props" in *"PARALLELISM_FACTOR"*"'8'"*"DATAFUSION_BATCH_SIZE"*"'131072'"*) ;; \
    *) echo "FAIL: DATAFUSION_BATCH_SIZE append shape: $bs_props"; exit 1;; esac
  off_suffix="$(delete_header_suffix 0 anything)"
  case "$off_suffix" in "") ;; *) echo "FAIL: delete_header_suffix OFF must be empty: $off_suffix"; exit 1;; esac
  on_suffix="$(delete_header_suffix 1 mydeletens)"
  case "$on_suffix" in *"deletes=on"*"mydeletens"*) ;; \
    *) echo "FAIL: delete_header_suffix ON shape: $on_suffix"; exit 1;; esac
  if ! delete_ratio_ok 95 100; then echo "FAIL: delete_ratio_ok should accept 0.95 ratio"; exit 1; fi
  if delete_ratio_ok 80 100; then echo "FAIL: delete_ratio_ok should reject 0.80 ratio"; exit 1; fi
  if delete_ratio_ok 100 100; then echo "FAIL: delete_ratio_ok should reject 1.00 ratio (deletes not applied)"; exit 1; fi
  if ! delete_ratio_ok 90 100; then echo "FAIL: delete_ratio_ok should accept boundary 0.90"; exit 1; fi
  if ! delete_ratio_ok 98 100; then echo "FAIL: delete_ratio_ok should accept boundary 0.98"; exit 1; fi
  case "$(resolve_delete_ns docker tpch "")" in tpch_deletes) ;; \
    *) echo "FAIL: resolve_delete_ns docker default: $(resolve_delete_ns docker tpch "")"; exit 1;; esac
  case "$(resolve_delete_ns remote tpch "")" in tpch_deletes) ;; \
    *) echo "FAIL: resolve_delete_ns remote default: $(resolve_delete_ns remote tpch "")"; exit 1;; esac
  case "$(resolve_delete_ns docker tpch mycustomns)" in mycustomns) ;; \
    *) echo "FAIL: resolve_delete_ns override: $(resolve_delete_ns docker tpch mycustomns)"; exit 1;; esac

  # The catalog dispatch is inline in the remote) arm, so run the real script as a subprocess that
  # fails on the catalog-specific var before any AWS/Exasol call. Isolation needs both halves: an
  # inherited bench/.env or exported vars would satisfy require() and run the benchmark against a
  # live cluster. BENCH_ENV_FILE=/dev/null blocks the file, env -u the exported vars.
  run_sh_isolated() {  # env_file NAME=VALUE... -> the child's combined output
    local env_file="$1"; shift
    env -u GLUE_CATALOG_URI -u LAKEKEEPER_CATALOG_URI -u NAMESPACE -u EXASOL_HOST \
        -u EXASOL_SYS_PASSWORD -u BUCKETFS_WRITE_PASS \
        BENCH_ENV_FILE="$env_file" "$@" bash "$SCRIPT_DIR/run.sh" 2>&1
  }
  if out="$(run_sh_isolated /dev/null BENCH_TARGET=remote \
    AWS_REGION=r AWS_ACCESS_KEY_ID=k AWS_SECRET_ACCESS_KEY=s)"; then
    echo "FAIL: bench_catalog_selection: default-catalog run with missing vars should exit non-zero"; exit 1
  fi
  case "$out" in *"GLUE_CATALOG_URI"*) ;; \
    *) echo "FAIL: bench_catalog_selection: default BENCH_CATALOG did not require GLUE_CATALOG_URI: $out"; exit 1;; esac
  if out="$(run_sh_isolated /dev/null BENCH_TARGET=remote BENCH_CATALOG=lakekeeper \
    AWS_REGION=r AWS_ACCESS_KEY_ID=k AWS_SECRET_ACCESS_KEY=s)"; then
    echo "FAIL: bench_catalog_selection: lakekeeper run with missing vars should exit non-zero"; exit 1
  fi
  case "$out" in *"LAKEKEEPER_CATALOG_URI"*) ;; \
    *) echo "FAIL: bench_catalog_selection: BENCH_CATALOG=lakekeeper did not require LAKEKEEPER_CATALOG_URI: $out"; exit 1;; esac
  if out="$(run_sh_isolated /dev/null BENCH_TARGET=remote BENCH_CATALOG=bogus)"; then
    echo "FAIL: bench_catalog_selection: unknown BENCH_CATALOG should exit non-zero"; exit 1
  fi
  case "$out" in *"BENCH_CATALOG must be 'glue' or 'lakekeeper'"*) ;; \
    *) echo "FAIL: bench_catalog_selection: unknown value did not hard-error: $out"; exit 1;; esac

  # The probe file stops one variable short of satisfying require(): clearing it entirely would
  # run the build and live-cluster DDL.
  env_file_probe="$(mktemp)"
  trap 'rm -f "$env_file_probe"' EXIT
  cat >"$env_file_probe" <<'PROBEEOF'
GLUE_CATALOG_URI=https://glue.probe.invalid/iceberg
GLUE_WAREHOUSE=000000000000
NAMESPACE=probe_ns
EXASOL_HOST=probe.invalid
EXASOL_SYS_PASSWORD=probe-pw
PROBEEOF
  if out="$(run_sh_isolated "$env_file_probe" BENCH_TARGET=remote \
    AWS_REGION=r AWS_ACCESS_KEY_ID=k AWS_SECRET_ACCESS_KEY=s)"; then
    echo "FAIL: bench_env_file_is_the_only_config_source: probe run must still stop at the last unset var"; exit 1
  fi
  case "$out" in *"BUCKETFS_WRITE_PASS"*) ;; \
    *) echo "FAIL: bench_env_file_is_the_only_config_source: BENCH_ENV_FILE was not sourced: $out"; exit 1;; esac
  for probed in GLUE_CATALOG_URI GLUE_WAREHOUSE NAMESPACE EXASOL_HOST EXASOL_SYS_PASSWORD; do
    case "$out" in *"'$probed'"*) \
      echo "FAIL: bench_env_file_is_the_only_config_source: '$probed' should have come from BENCH_ENV_FILE: $out"; exit 1;; esac
  done
  rm -f "$env_file_probe"
  trap - EXIT

  lk_out="$(LAKEKEEPER_WAREHOUSE=w LAKEKEEPER_CLIENT_ID=cid LAKEKEEPER_CLIENT_SECRET="se'cret" \
    LAKEKEEPER_TOKEN_URI=http://token AWS_REGION=r AWS_ACCESS_KEY_ID=k AWS_SECRET_ACCESS_KEY=s \
    build_conn_password_lakekeeper)"
  case "$lk_out" in *"se''cret"*) ;; *) echo "FAIL: lakekeeper single-quote not escaped: $lk_out"; exit 1;; esac
  case "$lk_out" in *'"client_id":"cid"'*'"oauth2_server_uri":"http://token"'*) ;; \
    *) echo "FAIL: lakekeeper_conn_password_shape missing OAuth2 fields: $lk_out"; exit 1;; esac
  case "$lk_out" in *'"use_sigv4"'*) echo "FAIL: lakekeeper_conn_password_shape must never carry use_sigv4: $lk_out"; exit 1;; esac

  # bench/import_ceiling.sh greps the whole report for s3://.../lineitem.
  for cat in glue lakekeeper; do
    hdr="$(catalog_header_field remote "$cat")"
    case "$hdr" in *"s3://"*) echo "FAIL: catalog header field must never contain s3://: $hdr"; exit 1;; esac
    case "$hdr" in "catalog=$cat") ;; *) echo "FAIL: catalog header field shape for '$cat': $hdr"; exit 1;; esac
  done

  for cat in glue lakekeeper ""; do
    hdr="$(catalog_header_field docker "$cat")"
    case "$hdr" in "") ;; *) echo "FAIL: docker target header must carry no catalog= field (BENCH_CATALOG='$cat'): $hdr"; exit 1;; esac
  done

  # catalog_header_field ends in a newline; capturing it via $(...) would swallow the blank
  # separator line before "== tables exposed by".
  for target in remote docker; do
    hdr_block="$( { printf 'namespace=%s\n' ns; catalog_header_field "$target" glue; echo; echo "== tables exposed by X =="; } )"
    case "$hdr_block" in *$'\n\n== tables exposed by'*) ;; \
      *) echo "FAIL: report header block for target '$target' is missing the blank line before '== tables exposed by': $(printf '%s' "$hdr_block" | cat -A)"; exit 1;; esac
  done

  # Source-text guard over the production text (from the config heading on): a completed run must
  # leave the CONNECTION and virtual schema in place for the live demo, so every schema drop is
  # immediately followed by its recreate and the connection is never dropped.
  config_line="$(grep -n '^# ---- config ' "$SCRIPT_DIR/run.sh" | head -1 | cut -d: -f1)"
  if [ -z "$config_line" ]; then
    echo "FAIL: vs_teardown_is_recreate_only: could not locate the config-section heading"; exit 1
  fi
  prod_src="$(tail -n "+$config_line" "$SCRIPT_DIR/run.sh")"
  drop_vs_count="$(printf '%s\n' "$prod_src" | grep -c 'DROP VIRTUAL SCHEMA')"
  [ "$drop_vs_count" -eq 1 ] || \
    { echo "FAIL: vs_teardown_is_recreate_only: expected exactly one DROP VIRTUAL SCHEMA, found $drop_vs_count"; exit 1; }
  drop_offset="$(printf '%s\n' "$prod_src" | grep -n 'DROP VIRTUAL SCHEMA' | head -1 | cut -d: -f1)"
  next_line="$(printf '%s\n' "$prod_src" | sed -n "$((drop_offset + 1))p")"
  case "$next_line" in *"CREATE VIRTUAL SCHEMA"*) ;; \
    *) echo "FAIL: vs_teardown_is_recreate_only: DROP VIRTUAL SCHEMA not immediately followed by CREATE VIRTUAL SCHEMA: $next_line"; exit 1;; esac
  case "$prod_src" in *"DROP CONNECTION"*) echo "FAIL: vs_teardown_is_recreate_only: DROP CONNECTION must not appear in bench/run.sh"; exit 1;; esac

  echo "selftest OK"; exit 0
fi

# ---- config ------------------------------------------------------------------
# The selftest greps for the heading above. BENCH_ENV_FILE=/dev/null reads no config at all.
# The file supplies defaults only: caller-exported BENCH_*/LAKEHOUSE_* values are re-applied after
# sourcing so sweeps can override knobs the file also sets.
BENCH_ENV_FILE="${BENCH_ENV_FILE:-$SCRIPT_DIR/.env}"
if [ -f "$BENCH_ENV_FILE" ]; then
  _env_overrides="$(export -p | grep -E ' (BENCH_|LAKEHOUSE_)[A-Za-z0-9_]+=' || true)"
  set -a; . "$BENCH_ENV_FILE"; set +a
  [ -n "$_env_overrides" ] && eval "$_env_overrides"
fi

TARGET="${BENCH_TARGET:-docker}"
EXA_PORT="${LH_EXASOL_PORT:-28563}"
BFS_PORT="${LH_BUCKETFS_PORT:-22581}"
TPCH_SCALE="${TPCH_SCALE:-0.3}"

# Must match the workspace `exasol-udf-sdk` pin (.so fingerprint); the Makefile owns the derivation.
SLC_VERSION="${BENCH_SLC_VERSION:-$(make -s print-slc-version)}"
if [ -z "$SLC_VERSION" ]; then
  echo "bench/run.sh: could not read the exasol-udf-sdk version pin from Cargo.toml; set BENCH_SLC_VERSION explicitly" >&2
  exit 1
fi
SO_UDF_OBJECT="${BENCH_SO_UDF_OBJECT:-buckets/bfsdefault/default/udf/liblakehouse_engine.so}"
# debug|info|warn|error
UDF_DEBUG_LEVEL="${LAKEHOUSE_UDF_DEBUG_LEVEL:-info}"
# The extracted SLC dir: a foo.tar.gz upload extracts to foo.
SLC_BUCKET_PATH="${BENCH_SLC_BUCKET_PATH:-bfsdefault/default/slc/lakehouse-rustslc}"
# 1 = SLC + .so already staged in BucketFS; still registers the RUST alias.
SKIP_UPLOAD="${BENCH_SKIP_UPLOAD:-0}"

case "$TARGET" in
  docker)
    HOST=localhost
    SYS_PASS="${EXASOL_SYS_PASSWORD:-exasol}"
    export EXASOL_SYS_PASSWORD="$SYS_PASS"
    NAMESPACE="${NAMESPACE:-tpch}"
    CATALOG_URI="http://iceberg-rest:8181"          # internal: reachable from the UDF
    CONN_PW="$(build_conn_password_local)"
    VS_EXTRA_PROPS="$(build_vs_extra_props true "${BENCH_PARALLELISM_FACTOR:-8}")"
    PROFILE_ON=0
    echo "== docker: bringing up local stack (minio, iceberg-rest, exasol) =="
    docker compose up -d
    ;;
  remote)
    # Explicit selection: bench/.env carries both catalogs' variables, so presence cannot decide.
    # Lakekeeper/Keycloak are reached over plain HTTP.
    BENCH_CATALOG="${BENCH_CATALOG:-glue}"
    case "$BENCH_CATALOG" in
      glue)
        require AWS_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY GLUE_CATALOG_URI GLUE_WAREHOUSE \
                NAMESPACE EXASOL_HOST EXASOL_SYS_PASSWORD BUCKETFS_WRITE_PASS
        CATALOG_URI="$GLUE_CATALOG_URI"
        CONN_PW="$(build_conn_password_cloud)"
        CATALOG_ALLOW_HTTP=false
        ;;
      lakekeeper)
        require AWS_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY \
                LAKEKEEPER_CATALOG_URI LAKEKEEPER_WAREHOUSE LAKEKEEPER_CLIENT_ID \
                LAKEKEEPER_CLIENT_SECRET LAKEKEEPER_TOKEN_URI \
                NAMESPACE EXASOL_HOST EXASOL_SYS_PASSWORD BUCKETFS_WRITE_PASS
        CATALOG_URI="$LAKEKEEPER_CATALOG_URI"
        CONN_PW="$(build_conn_password_lakekeeper)"
        CATALOG_ALLOW_HTTP=true
        ;;
      *) echo "ERROR: BENCH_CATALOG must be 'glue' or 'lakekeeper' (got '$BENCH_CATALOG')"; exit 1;;
    esac
    HOST="$EXASOL_HOST"
    SYS_PASS="$EXASOL_SYS_PASSWORD"
    export BUCKETFS_WRITE_PASS                      # make's $(shell) reads it from the environment
    VS_EXTRA_PROPS="$(build_vs_extra_props "$CATALOG_ALLOW_HTTP" "${BENCH_PARALLELISM_FACTOR:-8}")"
    PROFILE_ON="${BENCH_PROFILE:-1}"
    ;;
  *) echo "ERROR: BENCH_TARGET must be 'docker' or 'remote' (got '$TARGET')"; exit 1;;
esac

# The NAMESPACE swap happens after data load so tpch_loader still populates BASELINE_NS.
WITH_DELETES="${BENCH_WITH_DELETES:-0}"
if [ "$WITH_DELETES" = "1" ]; then
  BASELINE_NS="$NAMESPACE"
  DELETE_NS="$(resolve_delete_ns "$TARGET" "$BASELINE_NS" "${BENCH_DELETE_NAMESPACE:-}")"
fi

DSN="exasol://sys:${SYS_PASS}@${HOST}:${EXA_PORT}?validateservercertificate=0"
mkdir -p "$SCRIPT_DIR/reports"
REPORT="$SCRIPT_DIR/reports/bench-report-$(date +%Y%m%d-%H%M%S).txt"
FAILED=0

# SQL via stdin keeps secrets out of argv.
sql()  { printf '%s' "$1" | exapump sql -d "$DSN"; }
sqlf() { printf '%s' "$1" | exapump sql -d "$DSN" -f "${2:-csv}"; }
query_scalar() { printf '%s' "$1" | exapump sql -d "$DSN" -f csv | tail -n +2 | head -1 | tr -d '"[:space:]'; }

# Mirrors Makefile install-slc's merge logic; preserves all non-RUST language defs.
register_rust_alias() {
  local rust_def current new
  rust_def="RUST=localzmq+protobuf:///${SLC_BUCKET_PATH}?lang=rust#buckets/${SLC_BUCKET_PATH}/exaudf/exaudfclient"
  # Not query_scalar: that would strip the value's internal spaces.
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

echo "== building working-tree .so (no-op if fresh) =="
make cross-udf-build

if [ "$TARGET" = "docker" ]; then
  wait_http "http://localhost:${LH_MINIO_PORT:-19000}/minio/health/live" "MinIO"
  wait_http "http://localhost:${LH_REST_PORT:-18181}/v1/config" "Iceberg REST"
  wait_exasol
  echo "== loading TPC-H (SF=${TPCH_SCALE}, big tables in ${TPCH_FILES:-4} files) into namespace '${NAMESPACE}' =="
  TPCH_SCALE="$TPCH_SCALE" NAMESPACE="$NAMESPACE" TPCH_FILES="${TPCH_FILES:-4}" \
    cargo test --features exasol-e2e --test tpch_loader -- --nocapture
  if [ "$WITH_DELETES" = "1" ]; then
    echo "== authoring delete-bearing namespace '${DELETE_NS}' from baseline '${BASELINE_NS}' (docker, idempotent) =="
    "$SCRIPT_DIR/make_deletes_docker.sh" "$BASELINE_NS" "$DELETE_NS"
  fi
else
  wait_exasol
fi

if [ "$WITH_DELETES" = "1" ]; then
  NAMESPACE="$DELETE_NS"
fi

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
# Passthrough SET script: fans out only the files list across shard groups, no row data.
sql "CREATE OR REPLACE LUA SET SCRIPT ${SCHEMA}.${DISTRIBUTOR}(files VARCHAR(2000000))
EMITS (files VARCHAR(2000000)) AS
function run(ctx)
    repeat
        ctx.emit(ctx.files)
    until not ctx.next()
end
/"
sql "CREATE OR REPLACE CONNECTION ${CONN} TO '${CATALOG_URI//\'/\'\'}' USER '' IDENTIFIED BY '${CONN_PW}'"
build_vs() {  # vs_name namespace
  sql "DROP VIRTUAL SCHEMA IF EXISTS $1 CASCADE" || true
  sql "CREATE VIRTUAL SCHEMA $1
USING ${SCHEMA}.${ADAPTER} WITH
  CATALOG_CONNECTION  = '${CONN}'
  NAMESPACE           = '$2'${VS_EXTRA_PROPS}"
}
build_vs "${VS}" "${NAMESPACE}"
if [ "$WITH_DELETES" = "1" ]; then
  echo "== building baseline VS '${VS}_BASELINE' (ns '${BASELINE_NS}') for delete-count sanity =="
  build_vs "${VS}_BASELINE" "${BASELINE_NS}"
fi

# DDL only, for single-leg sessions under a SCRIPT_OUTPUT_ADDRESS redirect: multi-leg joins can
# crash under debug tracing.
if [ "${BENCH_DDL_ONLY:-0}" = "1" ]; then
  echo "== BENCH_DDL_ONLY=1: scripts + VS '${VS}' created (debug_level=${UDF_DEBUG_LEVEL}); skipping queries =="
  exit 0
fi

# EXA_USER_PROFILE_LAST_DAY only covers statements run while PROFILE was on.
if [ "$TARGET" = "remote" ] && [ "$PROFILE_ON" = "1" ]; then
  sql "ALTER SYSTEM SET PROFILE = 'ON'" || true
fi

{
  echo "lakehouse-engine benchmark — ${TARGET} @ ${HOST}:${EXA_PORT} — $(date)"
  printf 'namespace=%s%s\n' "${NAMESPACE}" "$(delete_header_suffix "$WITH_DELETES" "${DELETE_NS:-}")"
  catalog_header_field "$TARGET" "${BENCH_CATALOG:-glue}"
  echo
  echo "== tables exposed by ${VS} =="
} | tee "$REPORT"
sqlf "SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA='${VS}' ORDER BY TABLE_NAME" | tee -a "$REPORT"

# Remote mode never loads data, so the delete namespace must be pre-authored.
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
# Must run in the main shell (not a `{ } | tee` subshell) so FAILED propagates.
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
  # With deletes, key % 20 = 0 also removes rows from REGION and NATION.
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

if [ "$WITH_DELETES" = "1" ]; then
  check_delete_ratio
fi

# Query set; keep in sync with bench/athena_compare.sh, bench/trino_compare.sh and
# deploy/scripts/spark_queries.py. Assumes a flat namespace: a nested one flattens table names
# to NS__TABLE.
run_query() {
  local name="$1" q="$2" t0 t1
  { echo; echo "### ${name}"; } | tee -a "$REPORT"
  t0=$(date +%s.%N)
  # `if !` keeps set -e from aborting the remaining queries on a failing one.
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

# Asserts that EXPLAIN VIRTUAL's scan spec contains every expected needle.
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

pushdown_check "shard fan-out (multi-file LINEITEM)" \
  "SELECT COUNT(*) FROM ${VS}.LINEITEM" "shard_key"
pushdown_check "LIMIT" \
  "SELECT * FROM ${VS}.LINEITEM LIMIT 10" "limit"
pushdown_check "filter (BETWEEN) + projection" \
  "SELECT COUNT(*), MIN(L_SHIPDATE), MAX(L_SHIPDATE), AVG(L_EXTENDEDPRICE) FROM ${VS}.LINEITEM WHERE L_DISCOUNT BETWEEN 0.05 AND 0.07" \
  "filter" "L_DISCOUNT"
pushdown_check "filter + GROUP BY agg" \
  "SELECT L_RETURNFLAG, L_LINESTATUS, COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPDATE <= DATE '1998-09-01' GROUP BY L_RETURNFLAG, L_LINESTATUS" \
  "filter" "L_RETURNFLAG"
pushdown_check "filter (IN / OR / comparison)" \
  "SELECT COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPMODE IN ('AIR','RAIL') AND (L_RETURNFLAG = 'R' OR L_QUANTITY > 45)" \
  "filter" "AIR"
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
pushdown_check "NQ1 arithmetic aggregate pushdown (SUM(L_EXTENDEDPRICE * L_DISCOUNT))" \
  "SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue FROM ${VS}.LINEITEM
   WHERE L_SHIPDATE >= DATE '1994-01-01' AND L_SHIPDATE < DATE '1995-01-01'
     AND L_DISCOUNT BETWEEN 0.05 AND 0.07 AND L_QUANTITY < 24" \
  "aggregates" "arg_expr"
pushdown_check "NQ2 LIKE + IN filter pushdown" \
  "SELECT COUNT(*) FROM ${VS}.LINEITEM WHERE L_SHIPMODE IN ('AIR','REG AIR') AND L_COMMENT LIKE '%late%'" \
  "filter" "LIKE" "REG AIR"
pushdown_check "NQ4 top-N (ORDER BY + LIMIT) pushdown" \
  "SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM ${VS}.LINEITEM ORDER BY L_EXTENDEDPRICE DESC LIMIT 20" \
  "order_by" "LIMIT"

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
