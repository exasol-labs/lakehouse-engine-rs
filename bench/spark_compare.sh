#!/usr/bin/env bash
# Competitive engine comparison: Spark (EMR Serverless) vs the lakehouse engine, over the SAME
# Glue Iceberg TPC-H tables. NOT a spec feature — manually invoked, like the rest of bench/.
# Requires deploy/data-stack applied with -var enable_emr_serverless=true first — this script
# NEVER creates the application (cost-safety: nothing shall be started unless used).
#
# Query text lives in deploy/scripts/spark_queries.py (translated from bench/run.sh's Q1-Q4,
# lines ~321-349; identical SQL to bench/athena_compare.sh / trino_compare.sh). The submitted job
# prints "elapsed: <name> <secs>s" per query to its driver stdout log, which this script scrapes.
#
#   EMR_SERVERLESS_APP_ID=... EMR_SERVERLESS_ROLE_ARN=... SPARK_SCRIPT_S3_URI=... \
#   SPARK_LOG_S3_URI=... AWS_REGION=... ./spark_compare.sh
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }

if [ -z "${EMR_SERVERLESS_APP_ID:-}" ]; then
  echo "SKIP: EMR_SERVERLESS_APP_ID not set (apply data-stack with -var enable_emr_serverless=true first)"
  exit 0
fi
: "${EMR_SERVERLESS_ROLE_ARN:?set EMR_SERVERLESS_ROLE_ARN (tofu output emr_serverless_job_role_arn)}"
: "${SPARK_SCRIPT_S3_URI:?set SPARK_SCRIPT_S3_URI (tofu output spark_script_s3_uri)}"
: "${SPARK_LOG_S3_URI:?set SPARK_LOG_S3_URI (tofu output emr_serverless_log_uri)}"
: "${GLUE_CATALOG_URI:?set GLUE_CATALOG_URI}"
: "${GLUE_WAREHOUSE:?set GLUE_WAREHOUSE}"
: "${AWS_REGION:?set AWS_REGION}"

REPORT="${1:-bench/reports/spark-compare-$(date +%Y%m%d-%H%M%S).txt}"
mkdir -p "$(dirname "$REPORT")"
: > "$REPORT"

JOB_DRIVER=$(cat <<EOF
{"sparkSubmit":{"entryPoint":"${SPARK_SCRIPT_S3_URI}","entryPointArguments":["${GLUE_CATALOG_URI}","${GLUE_WAREHOUSE}","${AWS_REGION}"],"sparkSubmitParameters":"--conf spark.executor.cores=2 --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"}}
EOF
)
CONFIG_OVERRIDES=$(cat <<EOF
{"monitoringConfiguration":{"s3MonitoringConfiguration":{"logUri":"${SPARK_LOG_S3_URI}"}}}
EOF
)

echo "spark benchmark (EMR Serverless) — app=${EMR_SERVERLESS_APP_ID} — $(date)" | tee -a "$REPORT"

JOB_ID="$(aws emr-serverless start-job-run \
  --application-id "$EMR_SERVERLESS_APP_ID" \
  --execution-role-arn "$EMR_SERVERLESS_ROLE_ARN" \
  --job-driver "$JOB_DRIVER" \
  --configuration-overrides "$CONFIG_OVERRIDES" \
  --query 'jobRunId' --output text)"
echo "job run: $JOB_ID" | tee -a "$REPORT"

STATE=""
for _ in $(seq 1 180); do
  STATE="$(aws emr-serverless get-job-run --application-id "$EMR_SERVERLESS_APP_ID" --job-run-id "$JOB_ID" \
    --query 'jobRun.state' --output text)"
  case "$STATE" in SUCCESS|FAILED|CANCELLED) break ;; esac
  sleep 10
done
echo "final state: $STATE" | tee -a "$REPORT"

LOG_PREFIX="${SPARK_LOG_S3_URI}/applications/${EMR_SERVERLESS_APP_ID}/jobs/${JOB_ID}/SPARK_DRIVER"
if [ "$STATE" != "SUCCESS" ]; then
  echo "FAILED — see ${LOG_PREFIX}/stdout.gz" | tee -a "$REPORT"; exit 1
fi

aws s3 cp "${LOG_PREFIX}/stdout.gz" - | gunzip -c > /tmp/lh-spark-driver-stdout.log
grep -E '^elapsed: ' /tmp/lh-spark-driver-stdout.log | tee -a "$REPORT"
# Normalize "elapsed: q1 3.21s" -> "TIMING spark q1 3.21"
grep -E '^elapsed: ' /tmp/lh-spark-driver-stdout.log \
  | awk '{name=$2; sec=$3; gsub(/s$/,"",sec); print "TIMING spark " name " " sec}' >> "$REPORT"

echo "Done. Report: $REPORT"
