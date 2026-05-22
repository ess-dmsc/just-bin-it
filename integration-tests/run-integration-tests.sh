#!/bin/sh

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export JBI_COMPOSE_PROJECT="${JBI_COMPOSE_PROJECT:-just-bin-it-integration-tests}"
export JBI_TEST_WORKDIR="${JBI_TEST_WORKDIR:-/tmp/just-bin-it}"

compose() {
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" -p "$JBI_COMPOSE_PROJECT" "$@"
}

set +e
compose exec -T test-runner sh -lc '
cd "$JBI_TEST_WORKDIR" &&
uv run --no-dev --group integration pytest -s --junitxml=./integration-tests/IntegrationTestsOutput.xml integration-tests
'
test_rc=$?
set -e

compose cp \
    "test-runner:${JBI_TEST_WORKDIR}/integration-tests/IntegrationTestsOutput.xml" \
    "$SCRIPT_DIR/IntegrationTestsOutput.xml" || true

exit "$test_rc"
