# Integration tests and Kafka client tests

## Requirements
* Docker installed and running

## Integration tests
Tests just-bin-it using fake data being streamed through a real instance of Kafka.
These tests take a little while to run because they have to start up Kafka and
just-bin-it.

```
./integration-tests/setup.sh
./integration-tests/run-integration-tests.sh
./integration-tests/teardown.sh
```
Note: The event data topic has two partitions to confirm the just-bin-it can handle multiple data partitions.

## Kafka client tests
Tests that our code that talks directly to Kafka works as expected.
These are quicker than the integration tests.

```
uv run --group integration pytest integration-tests/test_kafka_consumer.py
```

It is also possible to run these tests against a local instance of Kafka, to do this
set `JBI_KAFKA_MANAGED_EXTERNALLY=1` and `JBI_KAFKA_BROKERS` before running pytest.
Set `JBI_JUST_BIN_IT_MANAGED_EXTERNALLY=1` too if just-bin-it is already running.
