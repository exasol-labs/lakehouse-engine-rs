#!/usr/bin/env bash
# One-time prep: an EMR Serverless job (make_deletes_remote.py) authors the `tpch_deletes` Glue
# database (Iceberg v2 merge-on-read, 5% position-deleted) from `tpch`. Never creates the EMR
# application; apply deploy/data-stack with enable_emr_serverless=true.
#   EMR_SERVERLESS_APP_ID=... EMR_SERVERLESS_ROLE_ARN=... SPARK_DELETES_SCRIPT_S3_URI=... \
#   SPARK_LOG_S3_URI=... [SOURCE_NS=tpch] [TARGET_NS=tpch_deletes] ./make-deletes-remote.sh
set -uo pipefail
cd "$(dirname "$0")"

# Scoped engine-reader creds lack emr-serverless:* permissions; use the operator's credential chain.
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

: "${EMR_SERVERLESS_APP_ID:?set EMR_SERVERLESS_APP_ID (apply data-stack with -var enable_emr_serverless=true first)}"
: "${EMR_SERVERLESS_ROLE_ARN:?set EMR_SERVERLESS_ROLE_ARN (tofu output emr_serverless_job_role_arn)}"
: "${SPARK_DELETES_SCRIPT_S3_URI:?set SPARK_DELETES_SCRIPT_S3_URI (tofu output spark_deletes_script_s3_uri)}"
: "${SPARK_LOG_S3_URI:?set SPARK_LOG_S3_URI (tofu output emr_serverless_log_uri)}"
SOURCE_NS="${SOURCE_NS:-tpch}"
TARGET_NS="${TARGET_NS:-tpch_deletes}"

WAREHOUSE_S3_URI="s3://$(printf '%s' "$SPARK_DELETES_SCRIPT_S3_URI" | sed -E 's#^s3://([^/]+)/.*#\1#')/"

# EMR Serverless has no internet egress, so use the release's bundled Iceberg jar instead of
# spark.jars.packages.
JOB_DRIVER=$(cat <<EOF
{"sparkSubmit":{"entryPoint":"${SPARK_DELETES_SCRIPT_S3_URI}","entryPointArguments":["${WAREHOUSE_S3_URI}","${SOURCE_NS}","${TARGET_NS}"],"sparkSubmitParameters":"--conf spark.executor.cores=2 --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"}}
EOF
)
CONFIG_OVERRIDES=$(cat <<EOF
{"monitoringConfiguration":{"s3MonitoringConfiguration":{"logUri":"${SPARK_LOG_S3_URI}"}}}
EOF
)

echo "make-deletes (EMR Serverless) — app=${EMR_SERVERLESS_APP_ID} — ${SOURCE_NS} -> ${TARGET_NS} — $(date)"

JOB_ID="$(aws emr-serverless start-job-run \
  --application-id "$EMR_SERVERLESS_APP_ID" \
  --execution-role-arn "$EMR_SERVERLESS_ROLE_ARN" \
  --job-driver "$JOB_DRIVER" \
  --configuration-overrides "$CONFIG_OVERRIDES" \
  --query 'jobRunId' --output text)"
echo "job run: $JOB_ID"

STATE=""
for _ in $(seq 1 180); do
  STATE="$(aws emr-serverless get-job-run --application-id "$EMR_SERVERLESS_APP_ID" --job-run-id "$JOB_ID" \
    --query 'jobRun.state' --output text)"
  case "$STATE" in SUCCESS|FAILED|CANCELLED) break ;; esac
  sleep 10
done
echo "final state: $STATE"

LOG_PREFIX="${SPARK_LOG_S3_URI}/applications/${EMR_SERVERLESS_APP_ID}/jobs/${JOB_ID}/SPARK_DRIVER"
if [ "$STATE" != "SUCCESS" ]; then
  echo "FAILED — see ${LOG_PREFIX}/stdout.gz"; exit 1
fi

aws s3 cp "${LOG_PREFIX}/stdout.gz" - | gunzip -c > /tmp/lh-make-deletes-driver-stdout.log
grep -E '^(authoring|  |SKIP:|DONE)' /tmp/lh-make-deletes-driver-stdout.log
if ! grep -qE '^DONE$' /tmp/lh-make-deletes-driver-stdout.log; then
  echo "WARNING: job succeeded but no DONE line found — check ${LOG_PREFIX}/stdout.gz"; exit 1
fi
echo "Done. ${TARGET_NS} authored (or already present)."
