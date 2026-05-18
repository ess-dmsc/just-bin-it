#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${CI_PROJECT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"

JBI_COMPOSE_PROJECT="${JBI_COMPOSE_PROJECT:-just-bin-it-integration-tests}"
JBI_TEST_WORKDIR="${JBI_TEST_WORKDIR:-/tmp/just-bin-it}"

COMPOSE=(docker compose -f "$SCRIPT_DIR/docker-compose.yml" -p "$JBI_COMPOSE_PROJECT")

"${COMPOSE[@]}" cp test-runner:"$JBI_TEST_WORKDIR"/integration-tests/IntegrationTestsOutput.xml "$SCRIPT_DIR/IntegrationTestsOutput.xml" || true
"${COMPOSE[@]}" down --volumes --remove-orphans || true

rm -rf "$PROJECT_DIR/test_env"
rm -rf "$SCRIPT_DIR"/output-files/* || true
