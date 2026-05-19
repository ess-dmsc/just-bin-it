#!/bin/sh

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="${CI_PROJECT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"

export JBI_COMPOSE_PROJECT="${JBI_COMPOSE_PROJECT:-just-bin-it-integration-tests}"
export JBI_KAFKA_ADVERTISED_HOST_NAME="${JBI_KAFKA_ADVERTISED_HOST_NAME:-kafka}"
export JBI_KAFKA_BROKERS="${JBI_KAFKA_BROKERS:-kafka:9092}"
export JBI_KAFKA_MANAGED_EXTERNALLY="${JBI_KAFKA_MANAGED_EXTERNALLY:-1}"
export JBI_TEST_WORKDIR="${JBI_TEST_WORKDIR:-/tmp/just-bin-it}"

compose() {
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" -p "$JBI_COMPOSE_PROJECT" "$@"
}

docker info

compose down --volumes --remove-orphans || true
compose up -d --build zookeeper kafka just-bin-it test-runner
compose exec -T test-runner mkdir -p "$JBI_TEST_WORKDIR"

tar \
    -C "$PROJECT_DIR" \
    --exclude=.git \
    --exclude=.mypy_cache \
    --exclude=.nox \
    --exclude=.pytest_cache \
    --exclude=.ruff_cache \
    --exclude=.venv \
    --exclude=dist \
    --exclude='*.egg-info' \
    -cf - . | compose exec -T test-runner tar -C "$JBI_TEST_WORKDIR" -xf -
