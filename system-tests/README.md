# System tests

Tests just-bin-it using fake data being streamed through a real instance of Kafka.

## Requirements

* Docker installed and running

## How to run the tests

* cd to the system-tests directory

* Start Kafka using docker compose:
```
docker-compose up &
```

* Start just-bin-it
```
python ..\bin\just-bin-it.py -b localhost:9092 -t hist_commands &
```

* Run the test files via pytest:
```
pytest
```

## Notes

* These tests are slow to run - they are not unit tests!
* The event data topic has two partitions to confirm the just-bin-it can handle multiple data partitions
