# Just Bin It

A lightweight program for histogramming neutron event data for diagnostic purposes.

## Setup
Python 3.6+ only.

```
>>> pip install -r requirements.txt
```

## Usage

```
usage: main.py [-h] -b BROKERS [BROKERS ...] -t TOPIC [-c CONFIG_FILE]
               [-g GRAPHITE_CONFIG_FILE] [-o] [-s]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config-file CONFIG_FILE
                        configure an initial histogram from a file
  -g GRAPHITE_CONFIG_FILE, --graphite-config-file GRAPHITE_CONFIG_FILE
                        configuration file for publishing to Graphite
  -o, --one-shot-plot   runs the program until it gets some data, plot it and
                        then exit. Used for testing
  -s, --simulation-mode
                        runs the program in simulation mode. 1-D histograms
                        only.

  -l LOG_LEVEL, --log-level LOG_LEVEL
                        sets the logging level: debug=1, info=2, warning=3,
                        error=4, critical=5.

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

Next send a JSON configuration via Kafka (the format of the message is described
in greater detail below).

An example of sending a configuration via Python might look like:

```python
from kafka import KafkaProducer

CONFIG_JSON = b"""
{
  "cmd": "config",
  "data_brokers": ["localhost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {
      "type": "hist1d",
      "tof_range": [0, 100000000],
      "num_bins": 50,
      "topic": "output_topic"
    }
  ]
}
"""

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send("hist_commands", CONFIG_JSON)
producer.flush()
```

This will start histogramming data from the `TEST_events` topic and publish the
histogrammed data to `output_topic` using the hs00 schema.

To see what the data looks like run the example client:

```
python client/client.py --brokers localhost:9092 --topic output_topic
```
This will plot a graph of the most recent histogram.

### Configuring histogramming

A JSON histogramming configuration has the following parameters:

* "data_brokers" (string array): the addresses of the Kafka brokers
* "data_topics" (string array): the topics to listen for event data on
* "start" (seconds since epoch in ms): only histogram data after this UTC time [optional]
* "stop" (seconds since epoch in ms): only histogram data up to this UTC time [optional]
* "interval" (seconds): only histogram for this interval [optional]
* "histograms" (array of dicts): the histograms to create, contains the following:
    * "type" (string): the histogram type (hist1d or hist2d)
    * "tof_range" (array of ints): the time-of-flight range to histogram
    * "det_range" (array of ints): the range of detectors to histogram [2-D only]
    * "num_bins" (int): the number of histogram bins
    * "topic" (string): the topic to write histogram data to
    * "source" (string): the name of the source to accept data from
    * "id" (string): an identifier for the histogram which will be contained in the published histogram data [optional]

If "start" is not defined then counting with start with the next message.
If "stop" is not defined then counting will not stop.

If "interval" is defined in combination with "start" and/or "stop" then the message will be treated as invalid and ignored.

For example:
```json
{
  "cmd": "config",
  "data_brokers": ["localhost:9092"],
  "data_topics": ["TEST_events"],
  "start": 1564727596867,
  "stop":  1564727668779,
  "histograms": [
    {
      "type": "hist1d",
      "tof_range": [0, 100000000],
      "num_bins": 50,
      "topic": "output_topic_for_1d",
      "source": "monitor1",
      "id": "histogram1"
    },
    {
      "type": "hist2d",
      "tof_range": [0, 100000000],
      "det_range": [100, 1000],
      "num_bins": 50,
      "topic": "output_topic_for_2d"
    }
  ]
}
```

Note: sending a new configuration replace the existing configuration meaning that
existing histograms will no longer be updated.

### Restarting the count
To restarting the histograms counting from zero, send the restart command:
```json
{
  "cmd": "restart"
}
```
This will start all the histograms counting from zero but will not change any other
settings, such as bin edges etc.

### One-shot plot
When the `one-shot-plot` option is specified then the program with collect a
small amount of data, histogram it and then plot the histogram before stopping.
This can be useful for checking that the data and program are behaving correctly.

Note: no histogram data is written to the output topic in Kafka with this mode.

```
python main.py --brokers localhost:9092 --topic hist_commands --one-shot-plot
```

### Supplying a configuration file
An initial histogramming configuration can be supplied via the `config-file`
commandline option.
This enables a histogram to be created without the need to send a command from a
Kafka client, this could be useful for debugging as it allows the system to be
set up quickly.

The configuration file should contain the standard JSON for configuring histograms
as described above.

```
python main.py --brokers localhost:9092 --topic hist_commands --config_file example_configs/config1d.json
```

Note: this configuration will be replaced if a new configuration is sent the command
topic.

An example configuration file (config.json) is included in the example_configs
directory.

### The outputted histogram data
The histogram data generated by just-bin-it is written to Kafka using the hs00 FlatBuffers schema.
The info field is used to store extra data about the histogram such as the histogramming state and
the histogram ID.
This data is stored as a string representation of a JSON dictionary, for example:

```json
{
    "state": "COUNTING",
    "id": "histogram1"
}
```

### Sending statistics to Graphite
To enable statistics about the histograms to be send to Graphite it is necessary
to supply a configuration JSON file, for example

```
python main.py --brokers localhost:9092 --topic hist_commands --graphite-config-file graphite_config.json
```

The file must contain the following:

* "address" (string): the server name or address of the Graphite server.
* "port" (int): the port Graphite is listening on.
* "prefix" (string): the overarching name to store all histogram data under.
* "metric" (string): the base name to give individual histograms,
the histogram index will be auto appended.

For example:
```json
{
  "address": "127.0.0.1",
  "port": 2003,
  "prefix": "just-bin-it",
  "metric": "histogram-"
}
```

In this case, the histogram data would be stored in Graphite as
`just-bin-it.histogram-0`, `just-bin-it.histogram-1` etc. depending on the number
of histograms.

An example configuration file (graphite.json) is included in the example_configs
directory.

## Supported schemas

Input data: [ev42](https://github.com/ess-dmsc/streaming-data-types) only.
Output data: [hs00](https://github.com/ess-dmsc/streaming-data-types) only.

## For developers

### Install the commit hooks (important)
There are commit hooks for Black and Flake8.

The commit hooks are handled using [pre-commit](https://pre-commit.com).

To install the hooks for this project run:
```
pre-commit install
```

To test the hooks run:
```
pre-commit run --all-files
```
This command can also be used to run the hooks manually.

### Running unit tests
From the top directory:
```
py.test --cov .
```

For HTML output:
```
pytest --cov --cov-report html .
```

### Formatting
Formatting is handled by [Black](https://black.readthedocs.io/en/stable/).

It should be added as a commit hook (see above).

### mutmut
Occasionally run mutmut to check that mutating the code causes tests to fail.

To run:
```
mutmut run --use-coverage  --paths-to-mutate .
```

To see the ID of the mutations that survived the tests run:
```
mutmut results
```
This produces a list of the mutation numbers that survived, to see the actual
code change for the ID run:
```
mutmut show 56
```
where 56 can be replaced with the appropriate ID.

Note: there will be a number of false-positives.
