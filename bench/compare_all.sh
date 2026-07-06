#!/usr/bin/env bash
# Runs the full competitive comparison: lakehouse-engine (make bench) + native IMPORT ceiling +
# Athena + (if opted in) Trino native + Trino via IMPORT FROM JDBC + (if provisioned) Spark,
# against the same TPC-H data. NOT a spec feature, NOT CI — a manual runbook step, same as the
# rest of bench/. Never provisions Spark itself (spark_compare.sh SKIPs cleanly if
# EMR_SERVERLESS_APP_ID is unset).
#
# Trino is the one exception to "never auto-provision": native (trino_compare.sh) and IMPORT FROM
# JDBC (import_jdbc_trino.sh) each get their OWN freshly-booted, cold Trino cluster — provisioned
# and torn down by THIS script, one at a time — so neither measurement pre-warms the other (a
# shared cluster would let whichever script ran first JIT-warm Trino and cache Iceberg manifest/
# split listings for the second script, skewing the comparison — see bench/trino_compare.sh's own
# header comment for the full audit that found this). This roughly doubles Trino provisioning
# time/cost for a full run, which is why it requires an explicit opt-in: set
# RUN_TRINO_COMPARISON=1. Requires the Trino EC2 key
# pair's private key locally for trino_compare.sh's SSH-launched session (see bench/README.md).
#
# Aggregates every produced report's "TIMING <engine> <query> <seconds>" lines into one
# bench/reports/compare-<ts>.txt with an aligned summary table — no new reporting framework, same
# "hand-curated afterward" convention as the rest of bench/.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bench/reports

TS=$(date +%Y%m%d-%H%M%S)
OUT="bench/reports/compare-${TS}.txt"
: > "$OUT"

echo "== make bench (lakehouse-engine-rs) ==" | tee -a "$OUT"
make bench
LH_REPORT="$(ls -t bench/reports/bench-report-*.txt | head -1)"
grep -E '^### |^elapsed: ' "$LH_REPORT" | awk '
  /^### / { name=$0; sub(/^### /,"",name); gsub(/ /,"_",name); next }
  /^elapsed: / { sec=$2; gsub(/s$/,"",sec); print "TIMING lakehouse-engine " name " " sec }
' >> "$OUT"

echo "== import_ceiling.sh (native IMPORT) ==" | tee -a "$OUT"
IC_REPORT="bench/reports/import-ceiling-${TS}.txt"
bench/import_ceiling.sh "$IC_REPORT"
grep -E '^  (import_|vs_)' "$IC_REPORT" | awk -F': ' '{
  split($2, a, " "); sec=a[1]; gsub(/s$/,"",sec)
  label=$1; gsub(/^  /,"",label)
  print "TIMING import-ceiling " label " " sec
}' >> "$OUT"

echo "== athena_compare.sh ==" | tee -a "$OUT"
A_REPORT="bench/reports/athena-compare-${TS}.txt"
bench/athena_compare.sh "$A_REPORT"
grep '^TIMING ' "$A_REPORT" >> "$OUT" || true

TRINO_ENV="${TRINO_ENV:-test1}"

trino_worker_host() {  # reads the just-applied trino-stack's own tofu state
  (cd deploy/trino-stack && tofu output -json trino_worker_hosts | jq -r '.[0]')
}
trino_coordinator_host() {
  (cd deploy/trino-stack && tofu output -raw trino_coordinator_host)
}

if [ "${RUN_TRINO_COMPARISON:-0}" = "1" ]; then
  echo "== provisioning Trino (native comparison, env=$TRINO_ENV) ==" | tee -a "$OUT"
  deploy/scripts/trino-up.sh "$TRINO_ENV"
  TRINO_HOST="$(trino_coordinator_host)" TRINO_WORKER_HOST="$(trino_worker_host)"
  export TRINO_HOST TRINO_WORKER_HOST
  echo "== trino_compare.sh ==" | tee -a "$OUT"
  T_REPORT="bench/reports/trino-compare-${TS}.txt"
  bench/trino_compare.sh "$T_REPORT"
  grep '^TIMING ' "$T_REPORT" >> "$OUT" || true
  echo "== tearing down Trino (native comparison) ==" | tee -a "$OUT"
  deploy/scripts/trino-down.sh "$TRINO_ENV"

  echo "== provisioning Trino (IMPORT FROM JDBC comparison, env=$TRINO_ENV) ==" | tee -a "$OUT"
  deploy/scripts/trino-up.sh "$TRINO_ENV"
  export TRINO_HOST="$(trino_coordinator_host)"
  echo "== import_jdbc_trino.sh ==" | tee -a "$OUT"
  J_REPORT="bench/reports/import-jdbc-trino-${TS}.txt"
  bench/import_jdbc_trino.sh "$J_REPORT"
  grep '^TIMING ' "$J_REPORT" >> "$OUT" || true
  echo "== tearing down Trino (IMPORT FROM JDBC comparison) ==" | tee -a "$OUT"
  deploy/scripts/trino-down.sh "$TRINO_ENV"
else
  echo "SKIP trino_compare.sh / import_jdbc_trino.sh (set RUN_TRINO_COMPARISON=1 — provisions an ephemeral Trino cluster TWICE, real AWS spend, see the header comment)" | tee -a "$OUT"
fi

if [ -n "${EMR_SERVERLESS_APP_ID:-}" ]; then
  echo "== spark_compare.sh ==" | tee -a "$OUT"
  S_REPORT="bench/reports/spark-compare-${TS}.txt"
  bench/spark_compare.sh "$S_REPORT"
  grep '^TIMING ' "$S_REPORT" >> "$OUT" || true
else
  echo "SKIP spark_compare.sh (EMR_SERVERLESS_APP_ID unset — apply data-stack with enable_emr_serverless=true first)" | tee -a "$OUT"
fi

echo | tee -a "$OUT"
echo "== SUMMARY ==" | tee -a "$OUT"
{ echo "ENGINE QUERY SECONDS"; grep -h '^TIMING ' "$OUT" | cut -d' ' -f2-; } | column -t | tee -a "$OUT"

echo
echo "Done. Full comparison report: $OUT"
