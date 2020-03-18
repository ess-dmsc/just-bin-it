# System tests

Tests just-bin-it using fake data being streamed through a real instance of Kafka.

## Requirements

* Docker installed and running

## How to run the tests
```
cd system-tests
pytest -s
```

## Notes

* These tests are slow to run - they are not unit tests!
* The event data topic has two partitions to confirm the just-bin-it can handle multiple data partitions.
