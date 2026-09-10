#!/usr/bin/env bash
# shellcheck disable=SC2030,SC2031
# Offline, stubbed-PATH test harness for the AWS Lakekeeper stack (deploy/lakekeeper-stack/*) and
# its provisioning scripts (deploy/scripts/lakekeeper-provision.sh, secrets.sh). Follows
# deploy/scripts/tests/install.test.sh's structure and pass/fail/assert_* helpers.
#
# Runs with NO Docker and NO network. `tofu`, `aws`, `ssh`, `curl`, and `jq` are all stubbed on a
# temporary PATH, recording every argv (and, for curl's request bodies, the body itself) to a log.
# The `jq` stub is a RECORDING WRAPPER ONLY: it logs its own argv, then execs the REAL jq by
# absolute path, so body construction inside the scripts under test still works while jq's own
# command line becomes assertable. The harness's OWN assertions call the real jq by absolute path
# ($REAL_JQ) so they are never served by the stub.
#
# This is the offline complement to lakekeeper-local.test.sh (live Docker integration, task 5.2) --
# it never touches Docker or a real Lakekeeper/Keycloak server. Where lakekeeper-local.test.sh
# drives the real HTTP flow, this harness stubs every external call and asserts the SHAPE of what
# the scripts would have sent -- the request bodies, the credential-hygiene invariants, and the
# static .tf declarations -- which is what replaces compile-time checking for bash (plan.md
# Consequences: "no compile-time type or JSON-shape checking").
#
# Run: bash deploy/scripts/tests/lakekeeper.test.sh

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DS="$(cd "$HERE/.." && pwd)"
LKS="$(cd "$DS/../lakekeeper-stack" && pwd)"
ORIG_PATH="$PATH"
BASH_BIN="$(command -v bash)"
# Exported so the tofu stub — written from a quoted heredoc, so nothing is interpolated into it —
# can reach the REAL jq at run time instead of the recording wrapper on the stubbed PATH.
export REAL_JQ
REAL_JQ="$(command -v jq)"

PROVISION="$DS/lakekeeper-provision.sh"
SECRETS="$DS/secrets.sh"
USERDATA_TPL="$LKS/lakekeeper-userdata.sh.tftpl"
MAIN_TF="$LKS/main.tf"

for _prereq in jq; do
  command -v "$_prereq" >/dev/null 2>&1 || {
    printf 'FATAL: %s is required to run these tests; install it and re-run.\n' "$_prereq" >&2
    exit 1
  }
done

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); printf '  ok   %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); printf '  FAIL %s\n' "$1"; [[ -n "${2:-}" ]] && printf '       %s\n' "$2"; }

assert_eq()          { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1" "expected [$2] got [$3]"; fi; }
assert_contains()    { if [[ "$2" == *"$3"* ]]; then pass "$1"; else fail "$1" "missing [$3]"; fi; }
assert_not_contains(){ if [[ "$2" != *"$3"* ]]; then pass "$1"; else fail "$1" "found [$3] (should be absent)"; fi; }
assert_rc_zero()     { if [[ "$2" -eq 0 ]]; then pass "$1"; else fail "$1" "expected rc 0 got $2"; fi; }
assert_rc_nonzero()  { if [[ "$2" -ne 0 ]]; then pass "$1"; else fail "$1" "expected nonzero rc got $2"; fi; }
assert_gt_zero()     { if [[ "$2" -gt 0 ]]; then pass "$1"; else fail "$1" "expected a count above zero, got $2"; fi; }

# Asserts the first $STUB_LOG line matching `before` really precedes the first matching `after`.
# Order is itself the claim wherever one recorded call is only correct because another already
# happened -- a provisioning call issued before the box answers its health endpoint, or a workspace
# deleted before the destroy that empties it, both still leave every individual line in the log.
assert_log_order() { # desc before_regex after_regex
  local desc="$1" first second
  first="$(grep -nE "$2" "$STUB_LOG" | head -1 | cut -d: -f1)"
  second="$(grep -nE "$3" "$STUB_LOG" | head -1 | cut -d: -f1)"
  if [[ -n "$first" && -n "$second" && "$first" -lt "$second" ]]; then
    pass "$desc"
  else
    fail "$desc" "[$2] first at line ${first:-<none>}, [$3] first at line ${second:-<none>}"
  fi
}

# Runs the REAL jq (never the recording stub) as the ground truth for a JSON-shape assertion.
assert_jq() { # desc json filter
  local desc="$1" json="$2" filter="$3"
  if printf '%s' "$json" | "$REAL_JQ" -e "$filter" >/dev/null 2>&1; then
    pass "$desc"
  else
    fail "$desc" "jq -e '$filter' failed against: $json"
  fi
}

# --- sandbox + stubs ---------------------------------------------------------

SANDBOX="$(mktemp -d)"
trap 'rm -rf "$SANDBOX"' EXIT

STUBDIR="$SANDBOX/stubs"
mkdir -p "$STUBDIR"

STUB_LOG="$SANDBOX/stub.log"
BODY_LOG="$SANDBOX/bodies.log"
export STUB_LOG BODY_LOG
: > "$STUB_LOG"
: > "$BODY_LOG"

REGISTER_STATE_DIR="$SANDBOX/register-state"
mkdir -p "$REGISTER_STATE_DIR"
export STUB_REGISTER_STATE="$REGISTER_STATE_DIR"

# --- jq: recording wrapper, delegates to the REAL jq by absolute path -------------------------
write_jq_stub() {
  cat > "$1/jq" <<STUBEOF
#!/usr/bin/env bash
printf 'jq %s\n' "\$*" >> "\${STUB_LOG:-/dev/null}"
exec "$REAL_JQ" "\$@"
STUBEOF
  chmod +x "$1/jq"
}

# --- aws: glue get-tables / s3 cp / ssm get-parameter -----------------------------------------
# Generic enough for all three call sites this plan touches: lakekeeper-provision.sh's glue
# source producer, lakekeeper-userdata.sh.tftpl's realm-export fetch + SSM secret reads, and
# secrets.sh's SSM reads.
write_aws_stub() {
  cat > "$1/aws" <<'STUBEOF'
#!/usr/bin/env bash
printf 'aws %s\n' "$*" >> "${STUB_LOG:-/dev/null}"

DEFAULT_GLUE_TABLES='[{"name":"lineitem","metadata_location":"s3://stub-bucket/tpch.db/lineitem/metadata/001.json"},{"name":"orders","metadata_location":"s3://stub-bucket/tpch.db/orders/metadata/001.json"}]'

case "${1:-}" in
  glue)
    if [[ "${2:-}" == "get-tables" ]]; then
      printf '%s\n' "${STUB_GLUE_TABLES_JSON:-$DEFAULT_GLUE_TABLES}"
      exit 0
    fi
    ;;
  s3)
    if [[ "${2:-}" == "cp" ]]; then
      src="${3:-}"
      dst="${4:-}"
      if [[ "$dst" == "-" ]]; then
        # Source-side metadata fetch (glue producer): print {"location": <table_location>} by
        # stripping the trailing /metadata/<file>.json segment off the metadata document's own key.
        loc="${src%/metadata/*}"
        printf '{"location":"%s"}\n' "$loc"
      else
        # userdata's realm-export fetch: write dummy bytes to the destination file.
        printf 'stub-realm-bytes\n' > "$dst"
      fi
      exit 0
    fi
    ;;
  ssm)
    if [[ "${2:-}" == "get-parameter" ]]; then
      name=""
      prev=""
      for a in "$@"; do
        [[ "$prev" == "--name" ]] && name="$a"
        prev="$a"
      done
      printf 'STUB-SECRET-%s' "${name##*/}"
      exit 0
    fi
    ;;
esac
exit 0
STUBEOF
  chmod +x "$1/aws"
}

# --- curl: IMDSv2 + Lakekeeper/Keycloak REST + management API ---------------------------------
# `-o <file> -w '%{http_code}'` calls (every curl_bearer_status call in lakekeeper-provision.sh)
# write the canned body to the file and print only the status; a call with no `-o` (oauth2_token,
# the IMDSv2 probes) gets the body printed straight to stdout, matching real curl's own behavior
# for each shape. Any `--data @<path>` argument's file content is captured to $BODY_LOG, tagged by
# URL, before this stub answers -- that is how the harness's own assertions see the exact REQUEST
# BODIES the scripts under test constructed.
write_curl_stub() {
  cat > "$1/curl" <<STUBEOF
#!/usr/bin/env bash
printf 'curl %s\n' "\$*" >> "\${STUB_LOG:-/dev/null}"

method="GET"
outfile=""
datafile=""
url=""
prev=""
for a in "\$@"; do
  case "\$prev" in
    -o) outfile="\$a" ;;
    --request) method="\$a" ;;
  esac
  case "\$a" in
    @*) datafile="\${a#@}" ;;
    http*) url="\$a" ;;
  esac
  prev="\$a"
done

# A process-substitution @<path> is a FIFO, not a regular file (-f is always false on it), and it
# can only be READ ONCE -- a second cat/open on the same path after the first has drained it
# returns empty. So the content is captured exactly once here and reused everywhere below.
request_body=""
if [[ -n "\$datafile" ]]; then
  request_body="\$(cat "\$datafile" 2>/dev/null)"
  [[ -n "\$request_body" ]] && printf '%s\t%s\n' "\$url" "\$request_body" >> "\${BODY_LOG:-/dev/null}"
fi

register_state="\${STUB_REGISTER_STATE:-/tmp}"
body=""
status="200"

# Default bodies are plain variables, never a raw {...} literal directly inside a \${VAR:-...}
# expansion: bash's default-value parser ends the expansion at the FIRST unescaped '}' it meets,
# which breaks on any JSON literal that itself contains a '}'.
DEFAULT_EMPTY_OBJECT='{}'
DEFAULT_LIST_WAREHOUSE_BODY='{"warehouses":[]}'
DEFAULT_CONFIG_BODY='{"defaults":{"prefix":""}}'

case "\$url" in
  *169.254.169.254*api/token*) body="IMDS-TOKEN-STUB" ;;
  *169.254.169.254*local-ipv4) body="\${STUB_PRIVATE_IPV4:-10.0.5.20}" ;;
  *169.254.169.254*public-ipv4) body="\${STUB_PUBLIC_IPV4:-34.201.1.2}" ;;
  *"/protocol/openid-connect/token")
    token="\${STUB_ACCESS_TOKEN:-stub-access-token}"
    body="\$("$REAL_JQ" -n -c --arg t "\$token" '{access_token: \$t}')"
    ;;
  *"/management/v1/info")
    if [[ "\${STUB_ALREADY_BOOTSTRAPPED:-0}" == "1" ]]; then
      body='{"bootstrapped": true}'
    else
      body='{"bootstrapped": false}'
    fi
    ;;
  *"/management/v1/bootstrap")
    status="\${STUB_BOOTSTRAP_STATUS:-200}"
    body="\${STUB_BOOTSTRAP_BODY:-\$DEFAULT_EMPTY_OBJECT}"
    ;;
  *"/management/v1/warehouse")
    if [[ "\$method" == "POST" ]]; then
      status="\${STUB_CREATE_WAREHOUSE_STATUS:-200}"
      body="\${STUB_CREATE_WAREHOUSE_BODY:-\$DEFAULT_EMPTY_OBJECT}"
    else
      body="\${STUB_LIST_WAREHOUSE_BODY:-\$DEFAULT_LIST_WAREHOUSE_BODY}"
    fi
    ;;
  *"/v1/config?warehouse="*)
    body="\${STUB_CONFIG_BODY:-\$DEFAULT_CONFIG_BODY}"
    ;;
  *"/register")
    status="\${STUB_REGISTER_STATUS:-200}"
    if [[ -n "\$request_body" ]]; then
      rname="\$("$REAL_JQ" -r '.name' <<<"\$request_body" 2>/dev/null)"
      rloc="\$("$REAL_JQ" -r '."metadata-location"' <<<"\$request_body" 2>/dev/null)"
      [[ -n "\$rname" ]] && printf '%s' "\$rloc" > "\$register_state/\$rname"
    fi
    if [[ "\${STUB_REGISTER_REPORTS_LOCATION_TAKEN:-0}" == "1" ]]; then
      body='{"error":{"type":"AlreadyExistsException","code":409,"message":"LocationAlreadyTaken: the requested location is already taken"}}'
    else
      body="\${STUB_REGISTER_BODY:-\$DEFAULT_EMPTY_OBJECT}"
    fi
    ;;
  *"/namespaces")
    status="\${STUB_CREATE_NAMESPACE_STATUS:-200}"
    body="{}"
    ;;
  *"/tables/"*)
    tname="\${url##*/tables/}"
    status="\${STUB_READBACK_STATUS:-200}"
    if [[ -n "\${STUB_READBACK_OVERRIDE:-}" ]]; then
      body="\$STUB_READBACK_OVERRIDE"
    else
      loc=""
      [[ -f "\$register_state/\$tname" ]] && loc="\$(cat "\$register_state/\$tname")"
      body="\$("$REAL_JQ" -n -c --arg loc "\$loc" '{"metadata-location": \$loc}')"
    fi
    ;;
  *) body="{}" ;;
esac

if [[ -n "\$outfile" ]]; then
  printf '%s' "\$body" > "\$outfile"
  printf '%s' "\$status"
else
  printf '%s\n' "\$body"
fi
exit 0
STUBEOF
  chmod +x "$1/curl"
}

# --- ssh: secrets.sh's BucketFS write-password read -----------------------------------------
write_ssh_stub() {
  cat > "$1/ssh" <<'STUBEOF'
#!/usr/bin/env bash
printf 'ssh %s\n' "$*" >> "${STUB_LOG:-/dev/null}"
printf '%s\n' "${STUB_BFS_ENC:-c3R1Yi1iZnMtcGxhaW4tcHc=}"
STUBEOF
  chmod +x "$1/ssh"
}

# --- tofu: workspace select/new/delete, output -json / -raw, apply, destroy --------------------
# Every invocation is recorded as `tofu[<cwd basename>] <argv>`. The cwd is part of the record
# because which STACK DIRECTORY an apply or a destroy ran in is the assertion that keeps
# lakekeeper-up.sh / lakekeeper-down.sh from touching data-stack, cluster-stack or trino-stack;
# the argv alone never carries it. `output -raw <name>` is served from the same fixture JSON as
# `output -json` and FAILS on an unpublished name, so a test that asks for an output the stack
# does not declare breaks loudly instead of handing the script under test an empty string.
write_tofu_stub() {
  cat > "$1/tofu" <<'STUBEOF'
#!/usr/bin/env bash
printf 'tofu[%s] %s\n' "$(basename "$PWD")" "$*" >> "${STUB_LOG:-/dev/null}"
case "${1:-}" in
  workspace)
    case "${2:-}" in
      select)
        base="$(basename "$PWD")"
        if [[ "$base" == "lakekeeper-stack" && "${STUB_LK_WORKSPACE_MISSING:-0}" == "1" ]]; then
          exit 1
        fi
        exit 0
        ;;
      *) exit 0 ;;
    esac
    ;;
  output)
    base="$(basename "$PWD")"
    src=""
    [[ "$base" == "cluster-stack" ]] && src="${STUB_CLUSTER_OUTPUT_JSON:-}"
    [[ "$base" == "lakekeeper-stack" ]] && src="${STUB_LAKEKEEPER_OUTPUT_JSON:-}"
    if [[ "${2:-}" == "-raw" ]]; then
      value=""
      [[ -n "$src" ]] && value="$("${REAL_JQ:-jq}" -r --arg n "${3:-}" '.[$n].value // empty' "$src")"
      if [[ -z "$value" ]]; then
        printf 'stub tofu: no output named "%s" published by %s\n' "${3:-}" "$base" >&2
        exit 1
      fi
      printf '%s' "$value"
      exit 0
    fi
    [[ -n "$src" ]] && cat "$src"
    exit 0
    ;;
  *) exit 0 ;;
esac
STUBEOF
  chmod +x "$1/tofu"
}

# --- no-op stubs for the userdata script's OS-level calls (apt-get, systemctl, docker) --------
write_noop_stub() { # dir name
  cat > "$1/$2" <<STUBEOF
#!/usr/bin/env bash
printf '$2 %s\n' "\$*" >> "\${STUB_LOG:-/dev/null}"
exit 0
STUBEOF
  chmod +x "$1/$2"
}

write_jq_stub "$STUBDIR"
write_aws_stub "$STUBDIR"
write_curl_stub "$STUBDIR"
write_ssh_stub "$STUBDIR"
write_tofu_stub "$STUBDIR"
write_noop_stub "$STUBDIR" apt-get
write_noop_stub "$STUBDIR" systemctl
write_noop_stub "$STUBDIR" docker

RUN_PATH="$STUBDIR:$ORIG_PATH"

reset_env() {
  : > "$STUB_LOG"
  : > "$BODY_LOG"
  rm -rf "${REGISTER_STATE_DIR:?}"/* 2>/dev/null || true
  unset LK_SOURCE_KIND LK_SOURCE_REGION LK_SOURCE_DATABASE LK_SOURCE_CATALOG_URI LK_SOURCE_TOKEN_URI \
        LK_SOURCE_CLIENT_ID LK_SOURCE_CLIENT_SECRET LK_SOURCE_NAMESPACE LK_SOURCE_WAREHOUSE \
        LK_TARGET_CATALOG_URI LK_TARGET_TOKEN_URI LK_TARGET_CLIENT_ID LK_TARGET_CLIENT_SECRET \
        LK_TARGET_WAREHOUSE LK_TARGET_NAMESPACE LK_TARGET_REGION LK_TARGET_ACCESS_KEY_ID \
        LK_TARGET_SECRET_ACCESS_KEY LK_TARGET_S3_ENDPOINT 2>/dev/null || true
  unset STUB_GLUE_TABLES_JSON STUB_LIST_WAREHOUSE_BODY STUB_CREATE_WAREHOUSE_STATUS \
        STUB_CREATE_WAREHOUSE_BODY STUB_REGISTER_STATUS STUB_REGISTER_BODY \
        STUB_REGISTER_REPORTS_LOCATION_TAKEN STUB_READBACK_OVERRIDE STUB_READBACK_STATUS \
        STUB_ALREADY_BOOTSTRAPPED STUB_BOOTSTRAP_STATUS STUB_CREATE_NAMESPACE_STATUS \
        STUB_CONFIG_BODY STUB_ACCESS_TOKEN STUB_LK_WORKSPACE_MISSING STUB_CLUSTER_OUTPUT_JSON \
        STUB_LAKEKEEPER_OUTPUT_JSON STUB_BFS_ENC 2>/dev/null || true
  RUN_PATH="$STUBDIR:$ORIG_PATH"
}

# ============================================================================
# Group 1: static .tf source assertions (ingress, IAM, OIDC, SSM types)
# ============================================================================

ssm_param_type() { # name -> the type value ("String"/"SecureString") of that resource block
  local pname="$1"
  awk -v want="\"aws_ssm_parameter\" \"$pname\"" '
    index($0, want) { found=1 }
    found && /type[ ]*=/ { line=$0; sub(/.*= *"/, "", line); sub(/".*/, "", line); print line; exit }
    found && /^}/ { exit }
  ' "$MAIN_TF"
}

test_stack_declares_a_distinct_iam_user_with_an_attached_managed_policy() {
  echo "== test_stack_declares_a_distinct_iam_user_with_an_attached_managed_policy =="
  local tf; tf="$(cat "$MAIN_TF")"
  assert_contains "iam: storage user has a managed-policy attachment" "$tf" \
    'resource "aws_iam_user_policy_attachment" "lakekeeper_storage"'
  assert_not_contains "iam: no inline aws_iam_user_policy resource anywhere in the stack" "$tf" \
    'resource "aws_iam_user_policy" "'
}

test_ssm_string_vs_securestring_params() {
  echo "== test_ssm_string_vs_securestring_params =="
  local p
  for p in warehouse_name oauth2_client_id catalog_uri_public catalog_uri_private \
           token_uri_public token_uri_private; do
    assert_eq "ssm: $p is declared as a plain String parameter" "String" "$(ssm_param_type "$p")"
  done
  for p in db_password metadata_encryption_key keycloak_admin_password \
           storage_access_key_id storage_secret_access_key oauth2_client_secret; do
    assert_eq "ssm: $p is declared as a SecureString parameter" "SecureString" "$(ssm_param_type "$p")"
  done
}

test_realm_s3_key_outside_tpch_prefix() {
  echo "== test_realm_s3_key_outside_tpch_prefix =="
  local tf; tf="$(cat "$MAIN_TF")"
  assert_contains "realm object: key sits under the dedicated lakekeeper/ prefix" "$tf" \
    '"lakekeeper/keycloak-realm-iceberg.json"'
  assert_not_contains "realm object: key never sits under the tpch.db/ data prefix" "$tf" \
    'tpch.db/keycloak-realm-iceberg.json'
}

test_ingress_rules_scope() {
  echo "== test_ingress_rules_scope =="
  local tf; tf="$(cat "$MAIN_TF")"
  assert_contains "ingress: SSH is restricted to the allowlist CIDRs only" "$tf" \
    'cidr_blocks       = local.effective_cidrs'
  assert_contains "ingress: lakekeeper/keycloak ports are additionally open to the VPC CIDR" "$tf" \
    'cidr_blocks       = concat(local.effective_cidrs, [data.aws_vpc.this.cidr_block])'
  assert_contains "ingress: the catalog-ports rule covers exactly 8181 and 8080" "$tf" \
    'for_each          = toset(["8181", "8080"])'
}

test_oidc_two_vantages_declared() {
  echo "== test_oidc_two_vantages_declared =="
  local tf; tf="$(cat "$MAIN_TF")"
  assert_contains "oidc: the public catalog URI uses the instance's public_ip" "$tf" \
    'catalog_uri_public  = "http://${aws_instance.lakekeeper.public_ip}:8181/catalog"'
  assert_contains "oidc: the private catalog URI uses the instance's private_ip" "$tf" \
    'catalog_uri_private = "http://${aws_instance.lakekeeper.private_ip}:8181/catalog"'
}

# ============================================================================
# Group 2: rendered user-data (lakekeeper-userdata.sh.tftpl actually executed)
# ============================================================================

# Renders the tftpl the way OpenTofu's templatefile() would (single-$ interpolation of the named
# vars, $${...} unescaped to a literal ${...}), then sandboxes its two hardcoded absolute paths
# (the tee log target and WORKDIR) so running it never touches the real filesystem outside $SANDBOX.
render_userdata() {
  local out="$1"
  sed \
    -e "s|\${region}|$TPL_REGION|g" \
    -e "s|\${bucket}|$TPL_BUCKET|g" \
    -e "s|\${realm_s3_key}|$TPL_REALM_S3_KEY|g" \
    -e "s|\${ssm_root}|$TPL_SSM_ROOT|g" \
    -e "s|\${oidc_realm}|$TPL_OIDC_REALM|g" \
    -e "s|\${oidc_audience}|$TPL_OIDC_AUDIENCE|g" \
    -e "s|\${postgres_image}|$TPL_POSTGRES_IMAGE|g" \
    -e "s|\${keycloak_image}|$TPL_KEYCLOAK_IMAGE|g" \
    -e "s|\${lakekeeper_image}|$TPL_LAKEKEEPER_IMAGE|g" \
    "$USERDATA_TPL" | sed -e 's/\$\${/${/g' > "$out"
  sed -i \
    -e "s|/var/log/lakekeeper-userdata.log|$SANDBOX/userdata.log|g" \
    -e "s|WORKDIR=/opt/lakekeeper|WORKDIR=$SANDBOX/lakekeeper-workdir|g" \
    "$out"
  chmod +x "$out"
}

test_rendered_userdata_declares_both_issuer_uris_and_ssm_sourced_admin_password() {
  echo "== test_rendered_userdata_declares_both_issuer_uris_and_ssm_sourced_admin_password =="
  reset_env
  # shellcheck disable=SC2034
  local TPL_REGION="eu-west-1"
  local TPL_BUCKET="stub-bucket"
  local TPL_REALM_S3_KEY="lakekeeper/keycloak-realm-iceberg.json"
  local TPL_SSM_ROOT="/spot-strata/lakekeeper/testenv"
  local TPL_OIDC_REALM="iceberg"
  local TPL_OIDC_AUDIENCE="lakekeeper"
  local TPL_POSTGRES_IMAGE="postgres:17"
  local TPL_KEYCLOAK_IMAGE="quay.io/keycloak/keycloak:26.0.7"
  local TPL_LAKEKEEPER_IMAGE="quay.io/lakekeeper/catalog:v0.13.1"
  export STUB_PRIVATE_IPV4="10.0.5.20"
  export STUB_PUBLIC_IPV4="34.201.1.2"

  local rendered="$SANDBOX/rendered-userdata.sh"
  rm -rf "$SANDBOX/lakekeeper-workdir"
  render_userdata "$rendered"

  local out rc
  out="$( ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$rendered" ) 2>&1 )"
  rc=$?
  assert_rc_zero "userdata: the rendered boot script runs to completion" "$rc"

  local compose; compose="$(cat "$SANDBOX/lakekeeper-workdir/docker-compose.yml" 2>/dev/null || true)"
  assert_contains "userdata: private-IP issuer is the primary OPENID_PROVIDER_URI" "$compose" \
    "LAKEKEEPER__OPENID_PROVIDER_URI=http://10.0.5.20:8080/realms/iceberg"
  assert_contains "userdata: public-IP issuer is declared as an ADDITIONAL issuer" "$compose" \
    "LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS=http://34.201.1.2:8080/realms/iceberg"

  assert_contains "userdata: Keycloak bootstrap admin password comes from SSM" "$compose" \
    "KC_BOOTSTRAP_ADMIN_PASSWORD=STUB-SECRET-keycloak_admin_password"
  assert_not_contains "userdata: the local compose file's insecure literal never appears" "$compose" \
    "This-is-NOT-Secure!"
  assert_not_contains "userdata: the literal 'admin' password never appears as the admin password value" \
    "$compose" "ADMIN_PASSWORD=admin"
  assert_contains "userdata: Postgres password also comes from SSM" "$compose" \
    "POSTGRES_PASSWORD=STUB-SECRET-db_password"
  assert_contains "userdata: metadata-encryption key also comes from SSM" "$compose" \
    "LAKEKEEPER__PG_ENCRYPTION_KEY=STUB-SECRET-metadata_encryption_key"

  local log; log="$(cat "$STUB_LOG")"
  assert_contains "userdata: fetches the realm export from the exact bucket/key" "$log" \
    "s3 cp s3://stub-bucket/lakekeeper/keycloak-realm-iceberg.json"
  assert_contains "userdata: reads db_password from this stack's own SSM root" "$log" \
    "--name /spot-strata/lakekeeper/testenv/db_password"
  assert_contains "userdata: reads metadata_encryption_key from this stack's own SSM root" "$log" \
    "--name /spot-strata/lakekeeper/testenv/metadata_encryption_key"
  assert_contains "userdata: reads keycloak_admin_password from this stack's own SSM root" "$log" \
    "--name /spot-strata/lakekeeper/testenv/keycloak_admin_password"
}

# ============================================================================
# Group 3: lakekeeper-provision.sh -- request bodies, register outcomes, hygiene
# ============================================================================

setup_provision_env() {
  export LK_SOURCE_KIND=glue
  export LK_SOURCE_REGION=eu-west-1
  export LK_SOURCE_DATABASE=tpch
  export LK_TARGET_CATALOG_URI="http://lakekeeper.example:8181/catalog"
  export LK_TARGET_TOKEN_URI="http://keycloak.example:8080/realms/iceberg/protocol/openid-connect/token"
  export LK_TARGET_CLIENT_ID="lakehouse"
  export LK_TARGET_CLIENT_SECRET="CANARY-CLIENT-SECRET-VALUE"
  export LK_TARGET_WAREHOUSE="spot-strata-testenv-lakekeeper-warehouse"
  export LK_TARGET_NAMESPACE="tpch"
  export LK_TARGET_REGION="eu-west-1"
  export LK_TARGET_ACCESS_KEY_ID="CANARY-ACCESS-KEY-ID"
  export LK_TARGET_SECRET_ACCESS_KEY="CANARY-SECRET-ACCESS-KEY"
  unset LK_TARGET_S3_ENDPOINT 2>/dev/null || true
  export STUB_LIST_WAREHOUSE_BODY
  STUB_LIST_WAREHOUSE_BODY="$("$REAL_JQ" -n -c --arg name "$LK_TARGET_WAREHOUSE" \
    '{warehouses:[{name: $name, "storage-profile": {bucket: "stub-bucket", "key-prefix": "tpch.db"}}]}')"
}

ONE_TABLE_JSON='[{"name":"orders","metadata_location":"s3://stub-bucket/tpch.db/orders/metadata/001.json"}]'

run_provision() {
  LAST_OUT="$( ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$PROVISION" "$@" ) 2>&1 )"
  LAST_RC=$?
}

test_provision_source_only_mode_makes_no_target_call() {
  echo "== test_provision_source_only_mode_makes_no_target_call =="
  reset_env
  setup_provision_env
  run_provision --source-only
  assert_rc_zero "source-only: exits zero" "$LAST_RC"
  assert_jq "source-only: reports the derived bucket" "$LAST_OUT" '.bucket == "stub-bucket"'
  assert_jq "source-only: reports the derived key prefix" "$LAST_OUT" '.key_prefix == "tpch.db"'
  assert_jq "source-only: reports both source table names" "$LAST_OUT" \
    '([.tables[].name] | sort) == ["lineitem", "orders"]'
  local curl_calls; curl_calls="$(grep -c '^curl ' "$STUB_LOG" 2>/dev/null || true)"
  assert_eq "source-only: no target-catalog call is made at all (no curl invocation)" "0" "${curl_calls:-0}"
}

test_provision_happy_path_registers_both_tables() {
  echo "== test_provision_happy_path_registers_both_tables =="
  reset_env
  setup_provision_env
  run_provision
  assert_rc_zero "happy: provisioning succeeds end to end" "$LAST_RC"
  assert_contains "happy: summary reports both tables registered" "$LAST_OUT" \
    "Summary: 2 registered, 0 already present, 0 failed"
}

test_provision_request_bodies_shape() {
  echo "== test_provision_request_bodies_shape =="
  reset_env
  setup_provision_env
  run_provision
  assert_rc_zero "bodies: the happy-path run succeeded so every call was made" "$LAST_RC"

  local bootstrap_body warehouse_body namespace_body register_body
  bootstrap_body="$(grep -F '/management/v1/bootstrap' "$BODY_LOG" | tail -1 | cut -f2-)"
  warehouse_body="$(grep -F '/management/v1/warehouse' "$BODY_LOG" | tail -1 | cut -f2-)"
  namespace_body="$(grep -F '/namespaces' "$BODY_LOG" | grep -vF '/register' | tail -1 | cut -f2-)"
  register_body="$(grep -F '/register' "$BODY_LOG" | tail -1 | cut -f2-)"

  assert_jq "bootstrap body: accept-terms-of-use is true" "$bootstrap_body" '.["accept-terms-of-use"] == true'
  assert_jq "bootstrap body: is-operator is true" "$bootstrap_body" '.["is-operator"] == true'

  assert_jq "warehouse body: warehouse-name matches LK_TARGET_WAREHOUSE" "$warehouse_body" \
    '.["warehouse-name"] == "spot-strata-testenv-lakekeeper-warehouse"'
  assert_jq "warehouse body: storage-profile bucket is the derived bucket" "$warehouse_body" \
    '.["storage-profile"].bucket == "stub-bucket"'
  assert_jq "warehouse body: storage-profile key-prefix is the derived prefix" "$warehouse_body" \
    '.["storage-profile"]["key-prefix"] == "tpch.db"'
  assert_jq "warehouse body: storage-profile flavor is aws when no S3 endpoint is set" "$warehouse_body" \
    '.["storage-profile"].flavor == "aws"'
  assert_jq "warehouse body: storage-profile sts-enabled is false" "$warehouse_body" \
    '.["storage-profile"]["sts-enabled"] == false'
  assert_jq "warehouse body: storage-profile carries no endpoint in the aws flavor" "$warehouse_body" \
    '(.["storage-profile"] | has("endpoint")) == false'
  assert_jq "warehouse body: storage-profile carries no path-style-access in the aws flavor" "$warehouse_body" \
    '(.["storage-profile"] | has("path-style-access")) == false'
  assert_jq "warehouse body: storage-credential type is s3" "$warehouse_body" \
    '.["storage-credential"].type == "s3"'
  assert_jq "warehouse body: storage-credential credential-type is access-key" "$warehouse_body" \
    '.["storage-credential"]["credential-type"] == "access-key"'
  assert_jq "warehouse body: canonical access-key-id field carries the target access key" "$warehouse_body" \
    '.["storage-credential"]["access-key-id"] == "CANARY-ACCESS-KEY-ID"'
  assert_jq "warehouse body: canonical secret-access-key field carries the target secret" "$warehouse_body" \
    '.["storage-credential"]["secret-access-key"] == "CANARY-SECRET-ACCESS-KEY"'
  assert_jq "warehouse body: delete-profile type is the soft variant" "$warehouse_body" \
    '.["delete-profile"].type == "soft"'
  assert_jq "warehouse body: delete-profile expiration-seconds is the one-week value" "$warehouse_body" \
    '.["delete-profile"]["expiration-seconds"] == 604800'

  assert_jq "namespace body: namespace array holds exactly the target namespace" "$namespace_body" \
    '.namespace == ["tpch"]'

  assert_jq "register body: overwrite is an explicit false" "$register_body" '.overwrite == false'
  assert_jq "register body: metadata-location is a non-empty string" "$register_body" \
    '(.["metadata-location"] | type) == "string" and (.["metadata-location"] | length) > 0'
}

test_no_secret_in_recorded_argv_or_output_and_no_set_x() {
  echo "== test_no_secret_in_recorded_argv_or_output_and_no_set_x =="
  reset_env
  setup_provision_env
  run_provision
  assert_rc_zero "hygiene: the happy-path run still succeeds" "$LAST_RC"

  local log; log="$(cat "$STUB_LOG")"
  assert_not_contains "hygiene: the client-secret canary never appears in any stubbed command's argv" \
    "$log" "CANARY-CLIENT-SECRET-VALUE"
  assert_not_contains "hygiene: the access-key-id canary never appears in any stubbed command's argv" \
    "$log" "CANARY-ACCESS-KEY-ID"
  assert_not_contains "hygiene: the secret-access-key canary never appears in any stubbed command's argv" \
    "$log" "CANARY-SECRET-ACCESS-KEY"

  # argv is only half the exposure the scenario names: a credential the script never puts on a
  # command line can still reach an operator's terminal, a CI log, or a pasted error report through
  # the script's OWN stdout/stderr, so the same canaries are checked against the captured run output.
  assert_not_contains "hygiene: the client-secret canary never reaches the run's own output" \
    "$LAST_OUT" "CANARY-CLIENT-SECRET-VALUE"
  assert_not_contains "hygiene: the access-key-id canary never reaches the run's own output" \
    "$LAST_OUT" "CANARY-ACCESS-KEY-ID"
  assert_not_contains "hygiene: the secret-access-key canary never reaches the run's own output" \
    "$LAST_OUT" "CANARY-SECRET-ACCESS-KEY"

  # A recorded jq argv entry can itself span multiple lines (the warehouse-body filter is a
  # multi-line program text), so these checks scan the WHOLE log rather than a '^jq '-anchored
  # line filter, which would silently miss anything past a jq entry's first line.
  assert_contains "hygiene: the warehouse-body jq call reads the access key id from jq's environment" \
    "$log" "env.LK_TARGET_ACCESS_KEY_ID"
  assert_contains "hygiene: the warehouse-body jq call reads the secret access key from jq's environment" \
    "$log" "env.LK_TARGET_SECRET_ACCESS_KEY"
  assert_not_contains "hygiene: no --arg token names the access key id field" "$log" "--arg access_key_id"
  assert_not_contains "hygiene: no --argjson token names the access key id field" "$log" \
    "--argjson access_key_id"
  assert_not_contains "hygiene: no --arg token names the secret access key field" "$log" \
    "--arg secret_access_key"
  assert_not_contains "hygiene: no --argjson token names the secret access key field" "$log" \
    "--argjson secret_access_key"

  # Shell tracing would defeat every assertion above at once by echoing each expanded command,
  # credential-bearing ones included, so the source-text ban belongs with the canaries it protects.
  local src; src="$(cat "$PROVISION")"
  assert_not_contains "hygiene: no set -x anywhere in the provisioning script" "$src" "set -x"
}

# The run-site-agnostic half of the same credential story (plan.md § Scenario Coverage,
# "Provisioning runs unchanged from an operator's laptop and from an EC2 box"): naming no profile is
# what lets the AWS CLI resolve a laptop's environment and an EC2 instance profile through the same
# chain, and passing --region explicitly is what stops the EC2 box -- which has no ~/.aws/config --
# from resolving a different region than the laptop did.
test_provision_uses_only_the_aws_credential_chain_and_an_explicit_region() {
  echo "== test_provision_uses_only_the_aws_credential_chain_and_an_explicit_region =="
  reset_env
  setup_provision_env
  run_provision
  assert_rc_zero "credential chain: the happy-path run still succeeds" "$LAST_RC"

  local aws_lines aws_count
  aws_lines="$(grep '^aws ' "$STUB_LOG" || true)"
  # Without this the two checks below pass vacuously: a run that never reached the AWS CLI at all
  # records no line carrying --profile and no line missing --region.
  aws_count="$(grep -c '^aws ' "$STUB_LOG" || true)"
  assert_gt_zero "credential chain: the run actually invoked the AWS CLI" "${aws_count:-0}"
  assert_not_contains "credential chain: no aws call passes --profile" "$aws_lines" "--profile"
  local missing_region
  missing_region="$(printf '%s\n' "$aws_lines" | grep -v -- '--region' | grep -v '^$' || true)"
  assert_eq "credential chain: every aws call passes --region" "" "$missing_region"
}

test_provision_source_text_declares_no_destructive_path() {
  echo "== test_provision_source_text_declares_no_destructive_path =="
  local src; src="$(cat "$PROVISION")"
  assert_not_contains "source: no -X DELETE" "$src" "-X DELETE"
  assert_not_contains "source: no --request DELETE" "$src" "--request DELETE"
  assert_not_contains "source: no purgeRequested" "$src" "purgeRequested"
  assert_not_contains "source: no aws s3 rm" "$src" "aws s3 rm"
  assert_not_contains "source: no aws s3api delete-object(s)" "$src" "aws s3api delete-object"
}

test_provision_outcome_already_registered() {
  echo "== test_provision_outcome_already_registered =="
  reset_env
  setup_provision_env
  export STUB_GLUE_TABLES_JSON="$ONE_TABLE_JSON"
  export STUB_REGISTER_STATUS=409
  run_provision
  assert_rc_zero "already-registered: an idempotent re-run still succeeds" "$LAST_RC"
  assert_contains "already-registered: table reported as already registered" "$LAST_OUT" \
    "orders: already registered"
  assert_contains "already-registered: summary counts it as already present" "$LAST_OUT" \
    "Summary: 0 registered, 1 already present, 0 failed"
}

test_provision_outcome_location_mismatch() {
  echo "== test_provision_outcome_location_mismatch =="
  reset_env
  setup_provision_env
  export STUB_GLUE_TABLES_JSON="$ONE_TABLE_JSON"
  export STUB_REGISTER_STATUS=409
  export STUB_READBACK_OVERRIDE='{"metadata-location":"s3://stub-bucket/tpch.db/orders/metadata/DIFFERENT.json"}'
  run_provision
  assert_rc_nonzero "location-mismatch: the run fails" "$LAST_RC"
  assert_contains "location-mismatch: names the mismatch explicitly" "$LAST_OUT" \
    "orders: FAILED, registered table's metadata-location does not match the one this run submitted"
  assert_contains "location-mismatch: summary counts it as failed" "$LAST_OUT" \
    "Summary: 0 registered, 0 already present, 1 failed"
}

test_provision_outcome_readback_http_failure() {
  echo "== test_provision_outcome_readback_http_failure =="
  reset_env
  setup_provision_env
  export STUB_GLUE_TABLES_JSON="$ONE_TABLE_JSON"
  export STUB_REGISTER_STATUS=409
  export STUB_READBACK_STATUS=404
  run_provision
  assert_rc_nonzero "readback-404: the run fails" "$LAST_RC"
  assert_contains "readback-404: names the confirming GET that could not confirm" "$LAST_OUT" \
    "orders: FAILED, confirming loadTable GET"
  assert_contains "readback-404: names the HTTP 404 the read-back actually returned" "$LAST_OUT" \
    "returned HTTP 404"
  assert_contains "readback-404: summary counts it as failed" "$LAST_OUT" \
    "Summary: 0 registered, 0 already present, 1 failed"
}

test_provision_outcome_location_already_taken() {
  echo "== test_provision_outcome_location_already_taken =="
  reset_env
  setup_provision_env
  export STUB_GLUE_TABLES_JSON="$ONE_TABLE_JSON"
  export STUB_REGISTER_STATUS=400
  export STUB_REGISTER_REPORTS_LOCATION_TAKEN=1
  run_provision
  assert_rc_nonzero "location-taken: the run fails" "$LAST_RC"
  assert_contains "location-taken: names the rejected-location outcome" "$LAST_OUT" \
    "orders: FAILED, register POST"
  assert_contains "location-taken: the wording names the location as already taken" "$LAST_OUT" \
    "rejected the location as already taken"
  assert_contains "location-taken: summary counts it as failed" "$LAST_OUT" \
    "Summary: 0 registered, 0 already present, 1 failed"
  assert_not_contains "location-taken: no confirming read-back GET was ever attempted" \
    "$(cat "$STUB_LOG")" "/tables/orders"
}

test_provision_rejects_a_missing_required_environment_variable() {
  echo "== test_provision_rejects_a_missing_required_environment_variable =="
  reset_env
  setup_provision_env
  unset LK_SOURCE_REGION
  run_provision --source-only
  assert_rc_nonzero "missing env var: the run fails" "$LAST_RC"
  assert_contains "missing env var: names the missing variable" "$LAST_OUT" "LK_SOURCE_REGION"
}

test_provision_rejects_an_unknown_command_line_argument() {
  echo "== test_provision_rejects_an_unknown_command_line_argument =="
  reset_env
  setup_provision_env
  run_provision --bogus
  assert_rc_nonzero "unknown arg: the run fails" "$LAST_RC"
  assert_contains "unknown arg: prints usage" "$LAST_OUT" "usage:"
}

test_provision_rejects_a_reserved_target_namespace() {
  echo "== test_provision_rejects_a_reserved_target_namespace =="
  local ns
  for ns in system examples information_schema; do
    reset_env
    setup_provision_env
    export LK_TARGET_NAMESPACE="$ns"
    run_provision
    assert_rc_nonzero "reserved namespace '$ns': the run fails" "$LAST_RC"
    assert_contains "reserved namespace '$ns': names it as reserved by Lakekeeper" "$LAST_OUT" \
      "reserved by Lakekeeper"
  done
}

test_provision_rejects_table_locations_spanning_two_buckets() {
  echo "== test_provision_rejects_table_locations_spanning_two_buckets =="
  reset_env
  setup_provision_env
  export STUB_GLUE_TABLES_JSON='[{"name":"lineitem","metadata_location":"s3://stub-bucket-a/tpch.db/lineitem/metadata/001.json"},{"name":"orders","metadata_location":"s3://stub-bucket-b/tpch.db/orders/metadata/001.json"}]'
  run_provision --source-only
  assert_rc_nonzero "mixed bucket: the run fails" "$LAST_RC"
  assert_contains "mixed bucket: names the mixed-bucket constraint" "$LAST_OUT" \
    "span more than one S3 bucket"
}

test_provision_rejects_an_empty_derived_key_prefix() {
  echo "== test_provision_rejects_an_empty_derived_key_prefix =="
  reset_env
  setup_provision_env
  export STUB_GLUE_TABLES_JSON='[{"name":"a_table","metadata_location":"s3://stub-bucket/a/metadata/001.json"},{"name":"b_table","metadata_location":"s3://stub-bucket/b/metadata/001.json"}]'
  run_provision --source-only
  assert_rc_nonzero "empty prefix: the run fails" "$LAST_RC"
  assert_contains "empty prefix: names the no-common-prefix constraint" "$LAST_OUT" \
    "share no common key prefix"
}

test_provision_fails_when_the_warehouse_read_back_disagrees_with_the_derived_prefix() {
  echo "== test_provision_fails_when_the_warehouse_read_back_disagrees_with_the_derived_prefix =="
  reset_env
  setup_provision_env
  export STUB_LIST_WAREHOUSE_BODY
  STUB_LIST_WAREHOUSE_BODY="$("$REAL_JQ" -n -c --arg name "$LK_TARGET_WAREHOUSE" \
    '{warehouses:[{name: $name, "storage-profile": {bucket: "stub-bucket", "key-prefix": "other.db"}}]}')"
  run_provision
  assert_rc_nonzero "prefix disagreement: the run fails" "$LAST_RC"
  assert_contains "prefix disagreement: names the expected bucket and key prefix" "$LAST_OUT" \
    "expected bucket 'stub-bucket' and key prefix 'tpch.db'"
  assert_contains "prefix disagreement: names the bucket and key prefix the read-back returned" "$LAST_OUT" \
    "got bucket 'stub-bucket' and key prefix 'other.db'"
}

test_provision_storage_profile_uses_the_s3_compatible_flavor_when_an_endpoint_is_set() {
  echo "== test_provision_storage_profile_uses_the_s3_compatible_flavor_when_an_endpoint_is_set =="
  reset_env
  setup_provision_env
  export LK_TARGET_S3_ENDPOINT="http://minio.example:9000"
  run_provision
  assert_rc_zero "s3-compat: the run still succeeds" "$LAST_RC"
  local warehouse_body
  warehouse_body="$(grep -F '/management/v1/warehouse' "$BODY_LOG" | tail -1 | cut -f2-)"
  assert_jq "s3-compat: flavor is s3-compat" "$warehouse_body" '.["storage-profile"].flavor == "s3-compat"'
  assert_jq "s3-compat: endpoint carries the configured value" "$warehouse_body" \
    '.["storage-profile"].endpoint == "http://minio.example:9000"'
  assert_jq "s3-compat: path-style-access is true" "$warehouse_body" \
    '.["storage-profile"]["path-style-access"] == true'
}

test_provision_derives_the_parent_prefix_from_table_locations_at_different_depths() {
  echo "== test_provision_derives_the_parent_prefix_from_table_locations_at_different_depths =="
  reset_env
  setup_provision_env
  # partsupp (deeper) listed FIRST so the assertion cannot pass merely because the first entry's
  # own ancestor already happens to equal the final answer -- it forces the shorten-to-parent walk
  # in derive_bucket_and_prefix to actually shorten "tpch.db/nested" down to "tpch.db" once the
  # shallower "part" entry is read.
  export STUB_GLUE_TABLES_JSON='[{"name":"partsupp","metadata_location":"s3://stub-bucket/tpch.db/nested/partsupp/metadata/001.json"},{"name":"part","metadata_location":"s3://stub-bucket/tpch.db/part/metadata/001.json"}]'
  run_provision --source-only
  assert_rc_zero "different depths: source-only still succeeds" "$LAST_RC"
  assert_jq "different depths: key_prefix shortens to the shared ancestor" "$LAST_OUT" \
    '.key_prefix == "tpch.db"'
}

test_provision_derives_the_key_prefix_from_a_single_source_table() {
  echo "== test_provision_derives_the_key_prefix_from_a_single_source_table =="
  reset_env
  setup_provision_env
  export STUB_GLUE_TABLES_JSON="$ONE_TABLE_JSON"
  run_provision --source-only
  assert_rc_zero "single table: source-only still succeeds" "$LAST_RC"
  assert_jq "single table: key_prefix is the table's own parent directory" "$LAST_OUT" \
    '.key_prefix == "tpch.db"'
}

# ============================================================================
# Group 4: secrets.sh -- generated bench/.env, credential hygiene
# ============================================================================

SECRETS_SANDBOX="$SANDBOX/secrets-sandbox"
mkdir -p "$SECRETS_SANDBOX/deploy/scripts" "$SECRETS_SANDBOX/deploy/cluster-stack" \
  "$SECRETS_SANDBOX/deploy/lakekeeper-stack" "$SECRETS_SANDBOX/bench"
cp "$SECRETS" "$SECRETS_SANDBOX/deploy/scripts/secrets.sh"
chmod +x "$SECRETS_SANDBOX/deploy/scripts/secrets.sh"
SECRETS_ENV_FILE="$SECRETS_SANDBOX/bench/.env"

CLUSTER_OUTPUT_JSON_FILE="$SANDBOX/cluster-output.json"
cat > "$CLUSTER_OUTPUT_JSON_FILE" <<'JSON'
{
  "first_node_ip": {"value": "10.0.0.5"},
  "ssm_root": {"value": "/spot-strata/testenv-cluster"},
  "data_ssm_root": {"value": "/spot-strata/testenv"},
  "exasol_db_port": {"value": 8563},
  "bucketfs_port": {"value": 2580},
  "key_pair_name": {"value": "spot-strata-key"}
}
JSON

# data_ssm_root deliberately does NOT contain the env name: the data-stack defaults env_name to
# "data" and deploy/README.md applies it with no override, so a run for Lakekeeper env "testenv"
# still reads /spot-strata/data/*. Composing the root from the Lakekeeper env name instead of
# reading this output is the defect test_up_reads_the_data_stack_ssm_root_from_the_stack_output
# guards, and it can only fail after the billable EC2 box already exists.
LK_OUTPUT_JSON_FILE="$SANDBOX/lk-output.json"
cat > "$LK_OUTPUT_JSON_FILE" <<'JSON'
{
  "ssm_root": {"value": "/spot-strata/lakekeeper/testenv"},
  "data_ssm_root": {"value": "/spot-strata/data"},
  "public_host": {"value": "34.201.1.2"},
  "lakekeeper_port": {"value": 8181},
  "catalog_uri_public": {"value": "http://34.201.1.2:8181/catalog"},
  "catalog_uri_private": {"value": "http://10.0.0.9:8181/catalog"},
  "token_uri_public": {"value": "http://34.201.1.2:8080/realms/iceberg/protocol/openid-connect/token"},
  "token_uri_private": {"value": "http://10.0.0.9:8080/realms/iceberg/protocol/openid-connect/token"},
  "oidc_client_id": {"value": "lakehouse"},
  "warehouse_name": {"value": "spot-strata-testenv-lakekeeper-warehouse"}
}
JSON

STUB_BFS_PLAIN="STUB-BFS-PLAIN-PW"

run_secrets() {
  LAST_OUT="$( ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$SECRETS_SANDBOX/deploy/scripts/secrets.sh" "$@" ) 2>&1 )"
  LAST_RC=$?
}

test_secrets_emits_lakekeeper_block_beside_untouched_glue_block() {
  echo "== test_secrets_emits_lakekeeper_block_beside_untouched_glue_block =="
  reset_env
  rm -f "$SECRETS_ENV_FILE"
  export STUB_CLUSTER_OUTPUT_JSON="$CLUSTER_OUTPUT_JSON_FILE"
  export STUB_LAKEKEEPER_OUTPUT_JSON="$LK_OUTPUT_JSON_FILE"
  export STUB_LK_WORKSPACE_MISSING=0
  export STUB_BFS_ENC; STUB_BFS_ENC="$(printf '%s' "$STUB_BFS_PLAIN" | base64)"
  run_secrets testenv
  assert_rc_zero "secrets: the run succeeds" "$LAST_RC"

  local env_text; env_text="$(cat "$SECRETS_ENV_FILE" 2>/dev/null || true)"
  assert_contains "secrets: BENCH_TARGET is remote" "$env_text" "BENCH_TARGET=remote"
  assert_contains "secrets: Glue block carries the region read from SSM" "$env_text" "AWS_REGION=STUB-SECRET-region"
  assert_contains "secrets: EXASOL_HOST is the cluster's first-node IP" "$env_text" "EXASOL_HOST=10.0.0.5"
  assert_contains "secrets: BucketFS write password is decoded from its double-base64 form" "$env_text" \
    "BUCKETFS_WRITE_PASS=$STUB_BFS_PLAIN"
  assert_not_contains "secrets: BENCH_CATALOG is never written" "$env_text" "BENCH_CATALOG"

  assert_contains "secrets: Lakekeeper block carries the private catalog URI" "$env_text" \
    "LAKEKEEPER_CATALOG_URI=http://10.0.0.9:8181/catalog"
  assert_contains "secrets: Lakekeeper block carries the private token URI" "$env_text" \
    "LAKEKEEPER_TOKEN_URI=http://10.0.0.9:8080/realms/iceberg/protocol/openid-connect/token"
  assert_contains "secrets: Lakekeeper block carries the warehouse name" "$env_text" \
    "LAKEKEEPER_WAREHOUSE=spot-strata-testenv-lakekeeper-warehouse"
  assert_contains "secrets: Lakekeeper client secret is read from SSM" "$env_text" \
    "LAKEKEEPER_CLIENT_SECRET=STUB-SECRET-client_secret"
}

test_secrets_omits_lakekeeper_block_without_workspace() {
  echo "== test_secrets_omits_lakekeeper_block_without_workspace =="
  reset_env
  rm -f "$SECRETS_ENV_FILE"
  export STUB_CLUSTER_OUTPUT_JSON="$CLUSTER_OUTPUT_JSON_FILE"
  export STUB_LAKEKEEPER_OUTPUT_JSON="$LK_OUTPUT_JSON_FILE"
  export STUB_LK_WORKSPACE_MISSING=1
  export STUB_BFS_ENC; STUB_BFS_ENC="$(printf '%s' "$STUB_BFS_PLAIN" | base64)"
  run_secrets testenv
  assert_rc_zero "secrets: the run still succeeds with no lakekeeper-stack workspace" "$LAST_RC"
  assert_contains "secrets: prints the no-workspace note" "$LAST_OUT" "No lakekeeper-stack workspace"

  local env_text; env_text="$(cat "$SECRETS_ENV_FILE" 2>/dev/null || true)"
  assert_not_contains "secrets: no Lakekeeper block is written without an applied workspace" \
    "$env_text" "LAKEKEEPER_"
  assert_not_contains "secrets: BENCH_CATALOG is still never written" "$env_text" "BENCH_CATALOG"
}

test_secrets_credential_hygiene() {
  echo "== test_secrets_credential_hygiene =="
  reset_env
  rm -f "$SECRETS_ENV_FILE"
  export STUB_CLUSTER_OUTPUT_JSON="$CLUSTER_OUTPUT_JSON_FILE"
  export STUB_LAKEKEEPER_OUTPUT_JSON="$LK_OUTPUT_JSON_FILE"
  export STUB_LK_WORKSPACE_MISSING=0
  export STUB_BFS_ENC; STUB_BFS_ENC="$(printf '%s' "$STUB_BFS_PLAIN" | base64)"
  run_secrets testenv
  assert_rc_zero "secrets: the run succeeds" "$LAST_RC"

  local log; log="$(cat "$STUB_LOG")"
  assert_not_contains "secrets: the BucketFS plaintext password never appears in any command's argv" \
    "$log" "$STUB_BFS_PLAIN"
}

# ============================================================================
# Group 5: lakekeeper-up.sh / lakekeeper-down.sh -- stack scope, ordering, SSM roots
# ============================================================================

UP="$DS/lakekeeper-up.sh"
DOWN="$DS/lakekeeper-down.sh"

# Everything lakekeeper-up.sh needs from beyond its own argv: this stack's OpenTofu outputs, and a
# list-warehouse read-back agreeing with the bucket/prefix the stubbed Glue source derives. No LK_*
# variable is exported here on purpose -- that lakekeeper-up.sh derives the whole
# lakekeeper-provision.sh contract from stack outputs and SSM alone, with nothing operator-set, is
# part of what these tests assert.
setup_up_env() {
  export STUB_LAKEKEEPER_OUTPUT_JSON="$LK_OUTPUT_JSON_FILE"
  local warehouse
  warehouse="$("$REAL_JQ" -r '.warehouse_name.value' "$LK_OUTPUT_JSON_FILE")"
  export STUB_LIST_WAREHOUSE_BODY
  STUB_LIST_WAREHOUSE_BODY="$("$REAL_JQ" -n -c --arg name "$warehouse" \
    '{warehouses:[{name: $name, "storage-profile": {bucket: "stub-bucket", "key-prefix": "tpch.db"}}]}')"
}

run_up() {
  LAST_OUT="$( ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$UP" "$@" ) 2>&1 )"
  LAST_RC=$?
}

run_down() {
  LAST_OUT="$( ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$DOWN" "$@" ) 2>&1 )"
  LAST_RC=$?
}

test_up_reads_the_data_stack_ssm_root_from_the_stack_output() {
  echo "== test_up_reads_the_data_stack_ssm_root_from_the_stack_output =="
  reset_env
  setup_up_env
  run_up testenv
  assert_rc_zero "up: the run completes against the stubbed stack" "$LAST_RC"

  local log; log="$(cat "$STUB_LOG")"
  assert_contains "up: the source region is read under the published data_ssm_root" "$log" \
    "--name /spot-strata/data/region"
  assert_contains "up: the source bucket is read under the published data_ssm_root" "$log" \
    "--name /spot-strata/data/bucket"
  assert_contains "up: the source tpch namespace is read under the published data_ssm_root" "$log" \
    "--name /spot-strata/data/namespace/tpch"
  assert_not_contains "up: no SSM read composes the data-stack root from the Lakekeeper env name" \
    "$log" "--name /spot-strata/testenv/"
  assert_contains "up: this box's own secrets still come from this stack's own ssm_root output" \
    "$log" "--name /spot-strata/lakekeeper/testenv/oauth2/client_secret"
}

test_up_applies_only_this_stack_and_waits_for_health() {
  echo "== test_up_applies_only_this_stack_and_waits_for_health =="
  reset_env
  setup_up_env
  run_up testenv
  assert_rc_zero "up: the run completes against the stubbed stack" "$LAST_RC"

  local apply_lines apply_count foreign_applies
  apply_lines="$(grep -E '^tofu\[[^]]*\] apply( |$)' "$STUB_LOG" || true)"
  apply_count="$(printf '%s\n' "$apply_lines" | grep -c . || true)"
  assert_eq "up: exactly one tofu apply is recorded for the whole run" "1" "${apply_count:-0}"
  # Scoping is the cost-relevant claim: data-stack, cluster-stack and trino-stack all carry
  # long-lived or far more expensive resources, and an apply in any of them would be recorded here
  # under that directory's own name.
  foreign_applies="$(printf '%s\n' "$apply_lines" | grep -v '^tofu\[lakekeeper-stack\]' | grep -v '^$' || true)"
  assert_eq "up: no tofu apply runs in any stack directory but lakekeeper-stack" "" "$foreign_applies"
  assert_contains "up: the apply pins the env name given on argv" "$apply_lines" "-var env_name=testenv"
  assert_contains "up: the apply stamps a created_date so the TTL/expiry tags are populated" \
    "$apply_lines" "-var created_date="

  assert_log_order "up: the health endpoint is polled before the first Keycloak token request" \
    '^curl .*:8181/health' '^curl .*openid-connect/token'

  assert_contains "up: provisioning ran to its per-table summary" "$LAST_OUT" "==> Summary:"
  assert_contains "up: the closing banner names the teardown command for this env" "$LAST_OUT" \
    "./lakekeeper-down.sh testenv"
}

test_down_destroys_only_the_lakekeeper_workspace() {
  echo "== test_down_destroys_only_the_lakekeeper_workspace =="
  reset_env
  run_down testenv
  assert_rc_zero "down: the teardown completes" "$LAST_RC"

  local destroy_lines destroy_count foreign_destroys
  destroy_lines="$(grep -E '^tofu\[[^]]*\] destroy( |$)' "$STUB_LOG" || true)"
  destroy_count="$(printf '%s\n' "$destroy_lines" | grep -c . || true)"
  assert_eq "down: exactly one tofu destroy is recorded" "1" "${destroy_count:-0}"
  foreign_destroys="$(printf '%s\n' "$destroy_lines" | grep -v '^tofu\[lakekeeper-stack\]' | grep -v '^$' || true)"
  assert_eq "down: no tofu destroy runs in any stack directory but lakekeeper-stack" "" "$foreign_destroys"
  assert_contains "down: the destroy targets the env given on argv" "$destroy_lines" "-var env_name=testenv"
  assert_not_contains "down: teardown never applies anything" "$(cat "$STUB_LOG")" "] apply"

  # The workspace is released only AFTER the destroy: deleting it first would strand every resource
  # the state file names, leaving a billing box no script can reach any more.
  assert_log_order "down: the workspace is left before it is deleted" \
    '^tofu\[lakekeeper-stack\] destroy' '^tofu\[lakekeeper-stack\] workspace select default'
  assert_log_order "down: the named workspace is deleted only after the destroy" \
    '^tofu\[lakekeeper-stack\] destroy' '^tofu\[lakekeeper-stack\] workspace delete testenv'
}

# ============================================================================
# Run
# ============================================================================

main() {
  test_stack_declares_a_distinct_iam_user_with_an_attached_managed_policy
  test_ssm_string_vs_securestring_params
  test_realm_s3_key_outside_tpch_prefix
  test_ingress_rules_scope
  test_oidc_two_vantages_declared

  test_rendered_userdata_declares_both_issuer_uris_and_ssm_sourced_admin_password

  test_provision_source_only_mode_makes_no_target_call
  test_provision_happy_path_registers_both_tables
  test_provision_request_bodies_shape
  test_no_secret_in_recorded_argv_or_output_and_no_set_x
  test_provision_uses_only_the_aws_credential_chain_and_an_explicit_region
  test_provision_source_text_declares_no_destructive_path
  test_provision_outcome_already_registered
  test_provision_outcome_location_mismatch
  test_provision_outcome_readback_http_failure
  test_provision_outcome_location_already_taken
  test_provision_rejects_a_missing_required_environment_variable
  test_provision_rejects_an_unknown_command_line_argument
  test_provision_rejects_a_reserved_target_namespace
  test_provision_rejects_table_locations_spanning_two_buckets
  test_provision_rejects_an_empty_derived_key_prefix
  test_provision_fails_when_the_warehouse_read_back_disagrees_with_the_derived_prefix
  test_provision_storage_profile_uses_the_s3_compatible_flavor_when_an_endpoint_is_set
  test_provision_derives_the_parent_prefix_from_table_locations_at_different_depths
  test_provision_derives_the_key_prefix_from_a_single_source_table

  test_secrets_emits_lakekeeper_block_beside_untouched_glue_block
  test_secrets_omits_lakekeeper_block_without_workspace
  test_secrets_credential_hygiene

  test_up_applies_only_this_stack_and_waits_for_health
  test_up_reads_the_data_stack_ssm_root_from_the_stack_output
  test_down_destroys_only_the_lakekeeper_workspace

  echo ""
  echo "=================================================="
  printf 'RESULT: %d passed, %d failed\n' "$PASS" "$FAIL"
  echo "=================================================="
  [[ "$FAIL" -eq 0 ]]
}

main "$@"
