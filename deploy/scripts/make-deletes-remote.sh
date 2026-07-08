#!/usr/bin/env bash
# ONE-TIME remote delete-prep: submits deploy/scripts/make_deletes_remote.py as an EMR Serverless job
# to author the `tpch_deletes` Glue database (Iceberg v2 merge-on-read, 5% position-deleted) from the
# pristine `tpch` Glue tables. Run ONCE before a remote delete-bench; NOT invoked by bench-remote.sh
# automatically. This is a DIFFERENT job from bench/spark_compare.sh (that one is the competitive
# comparison over the clean tables) — it only mirrors that script's EMR submit/poll/log-scrape shape.
#
# Requires deploy/data-stack applied with -var enable_emr_serverless=true first — this script NEVER
# creates the application (cost-safety: nothing shall be started unless used).
#
#   EMR_SERVERLESS_APP_ID=... EMR_SERVERLESS_ROLE_ARN=... SPARK_DELETES_SCRIPT_S3_URI=... \
#   SPARK_LOG_S3_URI=... [SOURCE_NS=tpch] [TARGET_NS=tpch_deletes] ./make-deletes-remote.sh
#
# No -e for the same reason as spark_compare.sh: a failing AWS CLI call must be caught by its own
# error handling below, not abort the script — required vars are still guarded by the `:` checks,
# which DO hard-stop, so keep pipefail but drop -e.
set -uo pipefail
cd "$(dirname "$0")"

# Fall back to AWS_PROFILE / the default credential chain: any scoped engine-reader creds in the env
# (Glue+S3 read only, for the Exasol CONNECTION) have no emr-serverless:* permissions. Unset them so
# the `aws` CLI uses the operator's broader identity (same handling as spark_compare.sh).
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

# Unlike spark_compare.sh (which SKIPs when the app is off, because it runs inside an automated
# sweep), this is a deliberate manual prep step — a missing app id is an operator error, so hard-stop.
: "${EMR_SERVERLESS_APP_ID:?set EMR_SERVERLESS_APP_ID (apply data-stack with -var enable_emr_serverless=true first)}"
: "${EMR_SERVERLESS_ROLE_ARN:?set EMR_SERVERLESS_ROLE_ARN (tofu output emr_serverless_job_role_arn)}"
: "${SPARK_DELETES_SCRIPT_S3_URI:?set SPARK_DELETES_SCRIPT_S3_URI (tofu output spark_deletes_script_s3_uri)}"
: "${SPARK_LOG_S3_URI:?set SPARK_LOG_S3_URI (tofu output emr_serverless_log_uri)}"
SOURCE_NS="${SOURCE_NS:-tpch}"
TARGET_NS="${TARGET_NS:-tpch_deletes}"

# The Iceberg GlueCatalog impl just needs the S3 root Glue tables live under — derived from the
# script's own bucket, never hardcoded (same "derive don't hardcode" convention as spark_compare.sh).
WAREHOUSE_S3_URI="s3://$(printf '%s' "$SPARK_DELETES_SCRIPT_S3_URI" | sed -E 's#^s3://([^/]+)/.*#\1#')/"

# EMR Serverless has no internet egress, so use the release's LOCALLY bundled Iceberg jar instead of
# spark.jars.packages (Maven-central fetch via Ivy, which times out).
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

# Scrape the driver stdout for the per-table progress + final DONE (or the idempotent SKIP line).
aws s3 cp "${LOG_PREFIX}/stdout.gz" - | gunzip -c > /tmp/lh-make-deletes-driver-stdout.log
grep -E '^(authoring|  |SKIP:|DONE)' /tmp/lh-make-deletes-driver-stdout.log
if ! grep -qE '^DONE$' /tmp/lh-make-deletes-driver-stdout.log; then
  echo "WARNING: job succeeded but no DONE line found — check ${LOG_PREFIX}/stdout.gz"; exit 1
fi
echo "Done. ${TARGET_NS} authored (or already present)."
