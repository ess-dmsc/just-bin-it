#!/bin/sh

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export JBI_COMPOSE_PROJECT="${JBI_COMPOSE_PROJECT:-just-bin-it-integration-tests}"

compose() {
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" -p "$JBI_COMPOSE_PROJECT" "$@"
}

compose exec -T test-runner bash -lc '
cd "$JBI_TEST_WORKDIR" &&
scl enable rh-python38 -- python -m venv test_env &&
source test_env/bin/activate &&
python -m pip install --upgrade pip &&
python -m pip install -r requirements-dev.txt &&
python -m pip install -r integration-tests/requirements.txt &&
python -m pip install "requests<2.30.0" &&
cd integration-tests &&
python -m pytest -s --junitxml=./IntegrationTestsOutput.xml .
'
