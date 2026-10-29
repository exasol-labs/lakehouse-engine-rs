#!/usr/bin/env bash
# Starts, waits for, or stops one E2E stack. `start` does not wait for Exasol,
# so CI compiles the suite while Exasol boots and runs `wait` afterwards.
set -euo pipefail

usage() {
  echo "usage: $0 <e2e|lakekeeper|azure|unity|glue> <start|wait|up|down>" >&2
  exit 2
}
[ $# -eq 2 ] || usage
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

compose() {
  docker compose "${files[@]}" "$@"
}

# `up --wait` fails on a one-shot container even after a clean exit, so
# one-shots are `docker wait`-ed.
await_exit() {
  local service=$1 code
  code=$(docker wait "$(compose ps -aq "$service")")
  if [ "$code" != "0" ]; then
    echo "::error::$service exited $code" >&2
    compose logs "$service"
    exit 1
  fi
}

lakekeeper_start() {
  compose up -d exasol
  compose up -d openfga-migrate
  await_exit openfga-migrate
  compose up -d lakekeeper-migrate
  await_exit lakekeeper-migrate
  compose up -d "$@"
}

start() {
  case "$stack" in
    e2e)
      compose up -d exasol proxy
      compose up -d --wait seaweedfs iceberg-rest
      compose up -d spark-iceberg-fixtures
      ;;
    lakekeeper) lakekeeper_start seaweedfs keycloak lakekeeper-db openfga-db openfga lakekeeper proxy ;;
    azure) lakekeeper_start keycloak lakekeeper-db openfga-db openfga lakekeeper proxy ;;
    unity)
      compose up -d exasol
      compose up -d --wait seaweedfs unitycatalog proxy
      ./scripts/unity/seed.sh
      ;;
    glue) compose up -d exasol proxy ;;
  esac
}

wait_healthy() {
  case "$stack" in
    e2e)
      compose up -d --wait exasol proxy
      await_exit spark-iceberg-fixtures
      ;;
    lakekeeper) compose up -d --wait exasol seaweedfs keycloak lakekeeper-db openfga-db openfga lakekeeper proxy ;;
    azure) compose up -d --wait exasol keycloak lakekeeper-db openfga-db openfga lakekeeper proxy ;;
    unity) compose up -d --wait exasol ;;
    glue) compose up -d --wait exasol proxy ;;
  esac
}

case "$action" in
  start) start ;;
  wait) wait_healthy ;;
  up)
    start
    wait_healthy
    ;;
  down) compose down -v ;;
  *) usage ;;
esac
