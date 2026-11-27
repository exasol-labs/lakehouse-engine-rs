#!/usr/bin/env bash
# `start` does not wait for Exasol, so CI compiles the suite while Exasol boots
# and runs `wait` afterwards.
set -euo pipefail

usage() {
  echo "usage: $0 <e2e|lakekeeper|azure|unity|glue> <start|wait|up|logs|down>" >&2
  exit 2
}
[[ $# -eq 2 ]] || usage
stack=$1
action=$2

cd "$(dirname "$0")/.."

case "$stack" in
  e2e | glue) files=(-f docker-compose.yml) ;;
  lakekeeper) files=(-f docker-compose.yml -f docker-compose.lakekeeper.yml) ;;
  azure) files=(-f docker-compose.yml -f docker-compose.lakekeeper.yml -f docker-compose.lakekeeper.azure.yml) ;;
  unity) files=(-f docker-compose.yml -f docker-compose.unity.yml) ;;
  *) usage ;;
esac

# docker-db is amd64-only, so arm64 hosts default to Nano. The Makefile exports the same choice.
if [[ -z "${EXASOL_FLAVOR:-}" ]]; then
  if [[ "${EXASOL_IMAGE:-}" == *exasol/nano* ]]; then
    EXASOL_FLAVOR=nano
  elif [[ -n "${EXASOL_IMAGE:-}" ]]; then
    EXASOL_FLAVOR=docker-db
  else
    case "$(uname -m)" in
      arm64 | aarch64) EXASOL_FLAVOR=nano ;;
      *) EXASOL_FLAVOR=docker-db ;;
    esac
  fi
fi
case "$EXASOL_FLAVOR" in
  docker-db) ;;
  nano) files+=(-f docker-compose.nano.yml) ;;
  *)
    echo "EXASOL_FLAVOR must be 'docker-db' or 'nano', got '$EXASOL_FLAVOR'" >&2
    exit 2
    ;;
esac

compose() {
  docker compose "${files[@]}" "$@"
}

# Pulls the images of exactly the services it starts.
up() {
  compose up -d --quiet-pull "$@"
}

# `up --wait` fails on a one-shot container even after a clean exit, so
# one-shots are `docker wait`-ed.
await_exit() {
  local service=$1 code
  code=$(docker wait "$(compose ps -aq "$service")")
  if [[ "$code" != "0" ]]; then
    echo "::error::$service exited $code" >&2
    compose logs "$service"
    exit 1
  fi
}

lakekeeper_start() {
  up exasol
  up openfga-migrate
  await_exit openfga-migrate
  up lakekeeper-migrate
  await_exit lakekeeper-migrate
  up "$@"
}

start() {
  case "$stack" in
    e2e)
      up exasol proxy
      up --wait seaweedfs iceberg-rest
      up spark-iceberg-fixtures
      ;;
    # iceberg-rest holds the source tables of `make test-lakekeeper-local`.
    lakekeeper) lakekeeper_start seaweedfs iceberg-rest keycloak lakekeeper-db openfga-db openfga lakekeeper proxy ;;
    azure) lakekeeper_start keycloak lakekeeper-db openfga-db openfga lakekeeper proxy ;;
    unity)
      up exasol
      up --wait seaweedfs unitycatalog proxy
      ./scripts/unity/seed.sh
      ;;
    glue) up exasol proxy ;;
    *) usage ;;
  esac
}

# Some Compose releases reject `up --wait` on a service without a healthcheck, which Nano
# lacks, so Nano's exasol starts without waiting and wait_nano_sql probes it instead.
up_wait() {
  local services=() service
  for service in "$@"; do
    if [[ "$EXASOL_FLAVOR" == nano && "$service" == exasol ]]; then
      up exasol
    else
      services+=("$service")
    fi
  done
  if ((${#services[@]})); then
    up --wait "${services[@]}"
  fi
}

# A sidecar in Nano's network namespace sees the SQL port itself, where a published port
# would accept before the DB does.
wait_nano_sql() {
  local container
  container=$(compose ps -q exasol)
  if ! docker run --rm --network "container:$container" debian:trixie-slim bash -c \
    'for _ in $(seq 150); do (</dev/tcp/127.0.0.1/8563) 2>/dev/null && exit 0; sleep 2; done; exit 1'; then
    echo "::error::Exasol Nano did not accept SQL connections within 300 s" >&2
    compose logs exasol | tail -50
    exit 1
  fi
}

wait_healthy() {
  wait_services
  if [[ "$EXASOL_FLAVOR" == nano ]]; then
    wait_nano_sql
  fi
}

wait_services() {
  case "$stack" in
    e2e)
      up_wait exasol proxy
      await_exit spark-iceberg-fixtures
      ;;
    lakekeeper) up_wait exasol seaweedfs iceberg-rest keycloak lakekeeper-db openfga-db openfga lakekeeper proxy ;;
    azure) up_wait exasol keycloak lakekeeper-db openfga-db openfga lakekeeper proxy ;;
    unity) up_wait exasol ;;
    glue) up_wait exasol proxy ;;
    *) usage ;;
  esac
}

# Failure diagnostics: the state and the log tail of every service the stack ran.
logs() {
  compose ps -a
  compose logs --tail 100
}

case "$action" in
  start) start ;;
  wait) wait_healthy ;;
  up)
    start
    wait_healthy
    ;;
  logs) logs ;;
  down) compose down -v ;;
  *) usage ;;
esac
