#!/usr/bin/env bash
# Spark (EMR Serverless) vs the lakehouse engine over the same Glue Iceberg TPC-H tables.
# Never creates the EMR application; apply deploy/data-stack with enable_emr_serverless=true.
# Queries live in deploy/scripts/spark_queries.py; the job's driver stdout is scraped for
# "elapsed: <name> <secs>s".
#   EMR_SERVERLESS_APP_ID=... EMR_SERVERLESS_ROLE_ARN=... SPARK_SCRIPT_S3_URI=... \
#   SPARK_LOG_S3_URI=... ./spark_compare.sh
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }
# bench/.env's creds are the scoped engine reader without emr-serverless:* permissions; fall back
# to the operator's own credential chain.
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

if [ -z "${EMR_SERVERLESS_APP_ID:-}" ]; then
  echo "SKIP: EMR_SERVERLESS_APP_ID not set (apply data-stack with -var enable_emr_serverless=true first)"
  exit 0
fi
: "${EMR_SERVERLESS_ROLE_ARN:?set EMR_SERVERLESS_ROLE_ARN (tofu output emr_serverless_job_role_arn)}"
: "${SPARK_SCRIPT_S3_URI:?set SPARK_SCRIPT_S3_URI (tofu output spark_script_s3_uri)}"
: "${SPARK_LOG_S3_URI:?set SPARK_LOG_S3_URI (tofu output emr_serverless_log_uri)}"
WAREHOUSE_S3_URI="s3://$(printf '%s' "$SPARK_SCRIPT_S3_URI" | sed -E 's#^s3://([^/]+)/.*#\1#')/"

WITH_DELETES="${BENCH_WITH_DELETES:-0}"
if [ -z "${SPARK_NAMESPACE:-}" ]; then
  SPARK_NAMESPACE="tpch"
  [ "$WITH_DELETES" = "1" ] && SPARK_NAMESPACE="tpch_deletes"
fi
ENGINE_LABEL="spark"
[ "$WITH_DELETES" = "1" ] && ENGINE_LABEL="spark-deletes"

REPORT="${1:-bench/reports/spark-compare-$(date +%Y%m%d-%H%M%S).txt}"
mkdir -p "$(dirname "$REPORT")"
: > "$REPORT"

# EMR Serverless has no internet egress by default, so use the release's bundled Iceberg jar
# instead of spark.jars.packages.
JOB_DRIVER=$(cat <<EOF
{"sparkSubmit":{"entryPoint":"${SPARK_SCRIPT_S3_URI}","entryPointArguments":["${WAREHOUSE_S3_URI}","${SPARK_NAMESPACE}"],"sparkSubmitParameters":"--conf spark.executor.cores=2 --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"}}
EOF
)
CONFIG_OVERRIDES=$(cat <<EOF
{"monitoringConfiguration":{"s3MonitoringConfiguration":{"logUri":"${SPARK_LOG_S3_URI}"}}}
EOF
)

echo "spark benchmark (EMR Serverless) — app=${EMR_SERVERLESS_APP_ID} namespace=${SPARK_NAMESPACE} with_deletes=${WITH_DELETES} — $(date)" | tee -a "$REPORT"

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
grep -E '^elapsed: ' /tmp/lh-spark-driver-stdout.log \
  | awk -v engine="$ENGINE_LABEL" '{name=$2; sec=$3; gsub(/s$/,"",sec); print "TIMING " engine " " name " " sec}' >> "$REPORT"

echo "Done. Report: $REPORT"
