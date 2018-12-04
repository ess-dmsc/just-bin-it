# Just Bin It

A lightweight program for histogramming neutron event data for diagnostic purposes.

## Setup
Python 3.7+ only. Might work on older version of Python 3 but not tested.

```
>>> pip install -r requirements.txt
```

## Usage

```
usage: main.py [-h] -b BROKERS [BROKERS ...] -t TOPIC [-o]

optional arguments:
  -h, --help            show this help message and exit
  -o, --one-shot-plot   runs the program until it gets some data then plots it
                        then exits. Used for testing

required arguments:
  -b BROKERS [BROKERS ...], --brokers BROKERS [BROKERS ...]
                        the broker addresses
  -t TOPIC, --topic TOPIC
                        the configuration topic
```


## How to run the demo
This assumes you have Kafka running on localhost and have the NeXus-Streamer
running in the background using the SANS_test_reduced.hdf5 dataset.

Start the histogrammer:
```
python main.py --brokers localhost:9092 --topic hist_commands --one-shot-plot
```

Send the configuration:

```python
from kafka import KafkaProducer

CONFIG_JSON = b"""
{
  "data_brokers": ["localhost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {"num_dims": 1, "det_range": [0, 100000000], "num_bins": 50}
  ]
}
"""

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send("hist_commands", CONFIG_JSON)
producer.flush()
```

This should plot a 1-D histogram with some data in it.

## Supported schemas

Currently only supports the [ev42](https://github.com/ess-dmsc/streaming-data-types) event schema.

## For developers

### Install the commit hooks (important)
The commit hooks are handled using [pre-commit](https://pre-commit.com).

To install the hooks for this project run:
```
>>> pre-commit install
```

To test the hooks run:
```
>>> pre-commit run --all-files
```

### Running unit tests
From the top directory:
```
>>> py.test --cov .
```

### Formatting
Formatting is handled by [Black](https://black.readthedocs.io/en/stable/).

It is automatically added as a commit hook.

To run manually:
```
>>> pre-commit run --all-files
```

Flake8 is also run as a commit hook.
