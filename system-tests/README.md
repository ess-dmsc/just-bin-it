# System tests and integrated tests

## Requirements
* Docker installed and running

## System tests
Tests just-bin-it using fake data being streamed through a real instance of Kafka.
These tests take a little while to run because they have to start up Kakfa and
just-bin-it.

```
cd system-tests
pytest test_just_bin_it.py
```
Note: The event data topic has two partitions to confirm the just-bin-it can handle multiple data partitions.

## Integrated tests
Tests that our code that talks directly to Kafka works as expected.
These are quicker than the system tests.

```
cd system-tests
pytest test_kafka_consumer.py
```

It is also possible to run these tests against a local instance of Kafka, to do this
rename the conftest.py file to something like conftest.py.old.
This enables the tests to be run significantly faster.
