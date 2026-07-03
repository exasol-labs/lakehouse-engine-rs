#!/usr/bin/env bash
# Runs the full competitive comparison: lakehouse-engine (make bench) + native IMPORT ceiling +
# Athena + (if provisioned) Trino + (if provisioned) Spark, against the same TPC-H data. NOT a spec
# feature, NOT CI — a manual runbook step, same as the rest of bench/. Never provisions Trino/Spark
# itself: their compare scripts SKIP cleanly if TRINO_HOST / EMR_SERVERLESS_APP_ID are unset.
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

if [ -n "${TRINO_HOST:-}" ]; then
  echo "== trino_compare.sh ==" | tee -a "$OUT"
  T_REPORT="bench/reports/trino-compare-${TS}.txt"
  bench/trino_compare.sh "$T_REPORT"
  grep '^TIMING ' "$T_REPORT" >> "$OUT" || true
else
  echo "SKIP trino_compare.sh (TRINO_HOST unset — run deploy/scripts/trino-up.sh <env> first)" | tee -a "$OUT"
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
