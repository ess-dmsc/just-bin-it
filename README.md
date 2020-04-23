# just-bin-it

A lightweight program for histogramming neutron event data.

## Setup
Python 3.6+ only.

```
>>> pip install -r requirements.txt
```

## Usage

```
usage: just-bin-it.py [-h] -b BROKERS [BROKERS ...] -t CONF_TOPIC
                      [-hb HB_TOPIC] [-c CONFIG_FILE]
                      [-g GRAPHITE_CONFIG_FILE] [-s] [-l LOG_LEVEL]

optional arguments:
  -h, --help            show this help message and exit
  -hb HB_TOPIC, --hb-topic HB_TOPIC
                        the topic where the heartbeat is published
  -c CONFIG_FILE, --config-file CONFIG_FILE
                        configure an initial histogram from a file
  -g GRAPHITE_CONFIG_FILE, --graphite-config-file GRAPHITE_CONFIG_FILE
                        configuration file for publishing to Graphite
  -s, --simulation-mode
                        runs the program in simulation mode.
  -l LOG_LEVEL, --log-level LOG_LEVEL
                        sets the logging level: debug=1, info=2, warning=3,
                        error=4, critical=5.

required arguments:
  -b BROKERS [BROKERS ...], --brokers BROKERS [BROKERS ...]
                        the broker addresses
  -t CONF_TOPIC, --config-topic CONF_TOPIC
                        the configuration topic
```


## How to run just-bin-it
This assumes you have Kafka running somewhere with an incoming stream of event
data (ev42 schema).

For demo/testing purposes this could be Kafka running on localhost with
generate_event_data.py running (see Generating fake event data below).

Start the histogrammer from the command-line:
```
python bin/just-bin-it.py --brokers localhost:9092 --config-topic hist_commands
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
  "data_topics": ["fake_events"],
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

To see what the data looks like see Viewing the histogram data below:

### Configuring histogramming
Note: sending a new configuration replace the existing configuration meaning that
existing histograms will no longer be updated.

A JSON histogramming configuration has the following parameters:

* "data_brokers" (string array): the addresses of the Kafka brokers
* "data_topics" (string array): the topics to listen for event data on
* "start" (seconds since epoch in ms): only histogram data after this UTC time (optional)
* "stop" (seconds since epoch in ms): only histogram data up to this UTC time (optional)
* "interval" (seconds): only histogram for this interval (optional)
* "histograms" (array of dicts): the histograms to create, contains the following:
    * "type" (string): the histogram type (hist1d, hist2d or dethist)
    * "tof_range" (array of ints): the time-of-flight range to histogram (hist1d and hist2d only)
    * "det_range" (array of ints): the range of detectors to histogram (optional for hist1d)
    * "width" (int): the width of the detector (dethist only)
    * "height" (int): the height of the detector (dethist only)
    * "num_bins" (int): the number of histogram bins (hist1d and hist2d only)
    * "topic" (string): the topic to write histogram data to
    * "source" (string): the name of the source to accept data from
    * "id" (string): an identifier for the histogram which will be contained in the published histogram data (optional)

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
      "id": "histogram1d"
    },
    {
      "type": "hist2d",
      "tof_range": [0, 100000000],
      "det_range": [100, 1000],
      "num_bins": 50,
      "topic": "output_topic_for_2d",
      "source": "detector1",
      "id": "histogram2d"
    },
    {
      "type": "dethist",
      "tof_range":[0, 100000000],
      "det_range":[1, 6144],
      "width":32,
      "height":192,
      "topic": "output_topic_for_map",
      "source": "freia_detector",
      "id": "histogram-map"
    }
  ]
}
```

#### Counting for a specified time
By default just-bin-it will start counting from when it receives the configuration
command and continue indefinitely.

If `start` is supplied then it will start histogramming from that time regardless
if it is the future or past.
If `stop` is supplied then histogramming will stop at that time.
If both `start` and `stop` are in the past then historic data will be used
(provided it still exists)

`interval` starts counting immediately and stops after the interval time is exceeded.

If "interval" is defined
If `interval`"` is defined in combination with `start` and/or `stop` then the
message will be treated as invalid and ignored.

#### Histogram types

##### hist1d
A simple 1-D histogram of time-of-flight vs counts.

The `tof_range` specifies the time-of-flight range to histogram over and `num_bins`
specifies how many bins to divide the range up into. Note: the bins are equally sized.

The `det_range` is optional but if supplied then data from detectors with IDs outside of that
range are ignored.

##### hist2d
A 2-D histogram of time-of-flight vs detector IDs.

The `tof_range` specifies the time-of-flight range to histogram over and `num_bins`
specifies how many bins to divide the range up into. Note: the bins are equally sized.

The `det_range` specifies the range of detector IDs to histogram over and `num_bins`
specifies how many bins to divide the range up into. Note: the bins are equally sized.

Currently the number of bins are the same for time-of-flight and the detectors.

##### dethist
A 2-D histogram of detector IDs (pixels) where each ID is a bin and the histogram
is arranged to approximate the physical layout of the detector.

The `det_range` specifies the range of detector IDs to histogram over.

The `width` and `height` define the dimensions of the detector for the
conversion of detector IDs into their respective 2-D positions.

### Restarting the count
To restarting the histograms counting from zero, send the `reset_counts` command:
```json
{
  "cmd": "reset_counts"
}
```
This will start all the histograms counting from zero but will not change any other
settings, such as bin edges etc.

### Stoping counting
In order to stop counting send the stop command:
```json
{
  "cmd" : "stop"
}
```
This will cause histogramming to stop and the final histogram to be published.

### Simulation mode
When in simulation mode just-bin-it will try to provide simulated data matching
the requested configuration. For example: if the config specifies a 2-D
histogram then the simulated data will be 2-D.

### Enabling a heartbeat
When a heartbeat topic is supplied via the `hb-topic` option then just-bin-it
will send periodic messages to that topic.

Note: no histogram data is written to the output topic in Kafka with this mode.

```
python bin/just-bin-it.py --brokers localhost:9092 --config-topic hist_commands --hb-topic heartbeat
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
python bin/just-bin-it.py --brokers localhost:9092 --config-topic hist_commands --config_file example_configs/config1d.json
```

Note: this configuration will be replaced if a new configuration is sent to the command
topic.

An example configuration file (config.json) is included in the example_configs
directory.

### The outputted histogram data
The histogram data generated by just-bin-it is written to Kafka using the hs00 FlatBuffers schema.
The info field is used to store extra data about the histogram such as the histogramming state and
the histogram ID.
This data is stored as a JSON representation of a dictionary with the keys `state` and `id`.

### Sending statistics to Graphite
To enable statistics about the histograms to be send to Graphite it is necessary
to supply a configuration JSON file, for example

```
python bin/just-bin-it.py --brokers localhost:9092 --config-topic hist_commands --graphite-config-file graphite_config.json
```

The file must contain the following:

* "address" (string): the server name or address of the Graphite server
* "port" (int): the port Graphite is listening on
* "prefix" (string): the overarching name to store all histogram data under
* "metric" (string): the base name to give individual histograms, the histogram index will be auto appended

For example:
```json
{
  "address": "127.0.0.1",
  "port": 2003,
  "prefix": "just-bin-it",
  "metric": "histogram-"
}
```

An example configuration file (graphite.json) is included in the example_configs
directory.

## Generating fake event data
For testing purposes it is possible to create fake event data that is send to Kafka.

```
python bin/generate_event_data.py --brokers localhost:9092 --config-topic fake_events --num_messages 100 --num_events 10000
```
The command line parameters are:
* brokers (string): the address for the Kafka brokers
* topic (string): the topic to publish the fake events to
* num_messages (int): the number of messages to send
* num_events (int): the number of events to put in each message (optional)

The messages are sent periodically and contain data that is roughly Gaussian.

### Viewing the histogram data
There are two simple ways to view the data being produced by just-bin-it:
bin/viewer.py and bin/view_output_messages.py.

viewer.py will produce a matplotlib plot of the histogram data. Example usage:
```
python bin/viewer.py --brokers localhost:9092 --config-topic output_topic
```
This will plot a graph of the most recent histogram. Note: the plot does not update,
so it will be necessary to re-run it to get fresh data.

view_output_messages.py will continously print a textual representation of the
data being outputted. Example usage:
```
python bin/view_output_messages.py --brokers localhost:9092 --config-topic output_topic
```

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
From the tests directory:
```
pytest --cov .
```

For HTML output:
```
pytest --cov --cov-report html .
```

### Tox
Tox allows the unit tests to be run against multiple versions of Python.
See the tox.ini file for which versions are supported.
From the top directory:
```
tox
```

### System tests
There are system tests that tests the whole system with a real instance of Kafka.
See the system-tests folder for more information on how to run them.

They should be run before modified code is pushed to the code repository.

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
