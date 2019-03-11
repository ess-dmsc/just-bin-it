# Just Bin It

A lightweight program for histogramming neutron event data for diagnostic purposes.

## Setup
Python 3.7+ only. Might work on older versions of Python 3 but not tested.

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


## How to run
This assumes you have Kafka running somewhere with an incoming stream of event
data (ev42 schema).

For demo/testing purposes this could be Kafka running on localhost with the
NeXus-Streamer running in the background using the SANS_test_reduced.hdf5
dataset.

Start the histogrammer from the command-line:
```
python main.py --brokers localhost:9092 --topic hist_commands
```

Next send a JSON command; a Python example might be:

```python
from kafka import KafkaProducer

CONFIG_JSON = b"""
{
  "data_brokers": ["localhost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {"num_dims": 1, "det_range": [0, 100000000], "num_bins": 50, "topic": "output_topic"}
  ]
}
"""

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send("hist_commands", CONFIG_JSON)
producer.flush()
```

This will start histogramming data from the `TEST_events` topic and publish the
histogrammed data to `output_topic`.

To see what the data looks like run the example client:

```
python client/client.py --brokers localhost:9092 --topic output_topic
```
This will plot a graph of the most recent histogram.

## Configuring the histogramming

A JSON configuration command has the following parameters:

* "data_brokers" (string array): the addresses of the Kafka brokers
* "data_topics" (string array): the topics to listen for event data on
* "histograms" (array of dicts): the histograms to create, contains the following:
    * "num_dims" (int): the number of dimenstions for the histogram (1 or 2)
    * "det_range" (array of ints): the range of detectors to histogram
    * "num_bins" (int): the number of histogram bins
    * "topic" (string): the topic to write histogram data to

Note: sending a new configuration replace the existing configuration meaning that
existing histograms will no longer be updated.

## Supported schemas

Input data: [ev42](https://github.com/ess-dmsc/streaming-data-types) only.
Output data: [hs00](https://github.com/ess-dmsc/streaming-data-types) only.

## For developers

### Install the commit hooks (important)
There are commit hooks for Black and Flake8.

The commit hooks are handled using [pre-commit](https://pre-commit.com).

To install the hooks for this project run:
```
>>> pre-commit install
```

To test the hooks run:
```
>>> pre-commit run --all-files
```
This command can also be used to run the hooks manually.

### Running unit tests
From the top directory:
```
>>> py.test --cov .
```

### Formatting
Formatting is handled by [Black](https://black.readthedocs.io/en/stable/).

It should be added as a commit hook (see above).

### Debugging

When the `one-shot-plot` option is specified then the program with collect a
small amount of data, histogram it and then plot the histogram before stopping.
This can be useful for checking that the data and program are behaving correctly.

Note: no histogram data is written to Kafka in this mode.

```
python main.py --brokers localhost:9092 --topic hist_commands --one-shot-plot
```
