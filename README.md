# just-bin-it

A lightweight program for histogramming neutron event data.

## Setup
Python 3.6+ only.

```
>>> pip install -r requirements.txt
```

## Usage

```
usage: just-bin-it.py [-h] -b BROKERS [BROKERS ...] -t CONFIG_TOPIC
                      [-hb HB_TOPIC] [-rt RESPONSE_TOPIC] [-c CONFIG_FILE]
                      [-g GRAPHITE_CONFIG_FILE] [-s] [-l LOG_LEVEL]

optional arguments:
  -h, --help            show this help message and exit
  -hb HB_TOPIC, --hb-topic HB_TOPIC
                        the topic where the heartbeat is published
  -rt RESPONSE_TOPIC, --response-topic RESPONSE_TOPIC
                        the topic where the response messages to commands are
                        published
  -c CONFIG_FILE, --config-file CONFIG_FILE
                        configure an initial histogram from a file
  -g GRAPHITE_CONFIG_FILE, --graphite-config-file GRAPHITE_CONFIG_FILE
                        configuration file for publishing to Graphite
  -s, --simulation-mode
                        runs the program in simulation mode
  -l LOG_LEVEL, --log-level LOG_LEVEL
                        sets the logging level: debug=1, info=2, warning=3,
                        error=4, critical=5.

required arguments:
  -b BROKERS [BROKERS ...], --brokers BROKERS [BROKERS ...]
                        the broker addresses
  -t CONFIG_TOPIC, --config-topic CONFIG_TOPIC
                        the configuration topic
```

IMPORTANT NOTE: The Kafka topics for `config-topic`, `hb-topic` and `response-topic`
must be separate and all must have only one partition.


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
  "histograms": [
    {
      "type": "hist1d",
      "data_brokers": ["localhost:9092"],
      "data_topics": ["fake_events"],
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

To see what the data looks like see "Viewing the histogram data" below:

### Configuring histogramming
Note: sending a new configuration replace the existing configuration meaning that
existing histograms will no longer be updated.

A JSON histogramming configuration has the following parameters:

* "cmd" (string): the command type (config, stop, etc.)
* "msg_id" (string): a unique identifier for the message
* "start" (seconds since epoch in ms): only histogram data after this UTC time (optional)
* "stop" (seconds since epoch in ms): only histogram data up to this UTC time (optional)
* "interval" (seconds): only histogram for this interval (optional)
* "histograms" (array of dicts): the histograms to create, contains the following:
    * "type" (string): the histogram type (hist1d, hist2d or dethist)
    * "data_brokers" (string array): the addresses of the Kafka brokers
    * "data_topics" (string array): the topics to listen for event data on
    * "tof_range" (array of ints): the time-of-flight range to histogram (hist1d and hist2d only)
    * "det_range" (array of ints): the range of detectors to histogram (optional for hist1d)
    * "width" (int): the width of the detector (dethist only)
    * "height" (int): the height of the detector (dethist only)
    * "num_bins" (int): the number of histogram bins (hist1d and hist2d only)
    * "topic" (string): the topic to write histogram data to
    * "source" (string): the name of the source to accept data from
    * "id" (string): a unique identifier for the histogram which will be contained in the published histogram data (optional but recommended)

For example:
```json
{
  "cmd": "config",
  "start": 1564727596867,
  "stop":  1564727668779,
  "histograms": [
    {
      "type": "hist1d",
      "data_brokers": ["localhost:9092"],
      "data_topics": ["TEST_events"],
      "tof_range": [0, 100000000],
      "num_bins": 50,
      "topic": "output_topic_for_1d",
      "source": "monitor1",
      "id": "histogram1d"
    },
    {
      "type": "hist2d",
      "data_brokers": ["localhost:9092"],
      "data_topics": ["TEST_events"],
      "tof_range": [0, 100000000],
      "det_range": [100, 1000],
      "num_bins": 50,
      "topic": "output_topic_for_2d",
      "source": "detector1",
      "id": "histogram2d"
    },
    {
      "type": "dethist",
      "data_brokers": ["localhost:9092"],
      "data_topics": ["TEST_events"],
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

##### A note about the old style configuration syntax
The previous configuration style had the `data_brokers` and `data_topics` defined globally rather per histogram.
For the short-term, that style of configuration will continue to work but may disappear without warning.

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

### Stopping counting
In order to stop counting send the stop command:
```json
{
  "cmd" : "stop"
}
```
This will cause histogramming to stop and the final histogram to be published.

### Simulation mode
When in simulation mode just-bin-it will try to provide simulated data matching
the requested configuration. For example: if the configuration specifies a 2-D
histogram then the simulated data will be 2-D.

### Enabling a heartbeat
When a heartbeat topic is supplied via the `hb-topic` option then just-bin-it
will send periodic messages to that topic.

Note: no histogram data is written to the output topic in Kafka with this mode.

```
python bin/just-bin-it.py --brokers localhost:9092 --config-topic hist_commands --hb-topic heartbeat
```

### Enabling a response topic and getting messages from just-bin-it
If a response topic is supplied then just-bin-it will supply a response when it
receives a command via Kafka, if, and only if, the command contains a message ID
(the `msg_id` field).
For example:
```json
{
  "cmd": "config",
  "msg_id": "unique_id_123",
  ...
}
```
It is recommended that the message ID is unique to avoid collisions with other
messages.
If the command is successful then an acknowledgement message like the following
will be sent to the response topic:
```json
{"msg_id": "unique_id_123", "response": "ACK"}
```
The message ID in the response will be the same as in the original command.

If the command is refused for some reason (invalid command name, missing paramters,
etc.) then an error message like the following will be sent to the response topic:
```json
{"msg_id": "unique_id_123", "response": "ERR", "message": "Unknown command type 'conf'"}
```
The `message` field will contain the reason for the error.

If the command is valid but for some reason histogramming is stopped prematurely
, e.g. the start time supplied is older than the data in Kafka, then just-bin-it
will try to send the last known status of the histogram plus information about the
error in the `hs00` schema's `info` field, for example:
```json
{
    "id": "some_id1",
    "start": 1590758218000,
    "state": "ERROR",
    "error_message": "Cannot find start time in the data, either the supplied time is too old or there is no data available"
}
```
The key points are that the `state` will be set to "ERROR" and there is an `error_message`
field.

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
There are two simple diagnostic tools for viewing the data being produced by just-bin-it:
bin/viewer.py and bin/view_output_messages.py.

viewer.py will produce a matplotlib plot of the histogram data.
To use this, some additional requirements must be installed:
```
>>> pip install -r requirements-gui.txt
```

Example usage:
```
python bin/viewer.py --brokers localhost:9092 --config-topic output_topic
```
This will plot a graph of the most recent histogram. Note: the plot does not update,
so it will be necessary to re-run it to get fresh data.

view_output_messages.py will continuously print a textual representation of the
data being outputted. Example usage:
```
python bin/view_output_messages.py --brokers localhost:9092 --config-topic output_topic
```

## Supported schemas

Input data: [ev42](https://github.com/ess-dmsc/streaming-data-types) only.
Output data: [hs00](https://github.com/ess-dmsc/streaming-data-types) only.

## For developers

### Install the developer requirements
```
>>> pip install -r requirements-dev.txt
```

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

### Integrated tests
There are some integrated tests in the system-tests folder that check our code for
talking to Kafka works as expected.
See the system-tests folder for more information on how to run them.

They should be run before modified code is pushed to the code repository.

### Formatting
Formatting is handled by [Black](https://black.readthedocs.io/en/stable/).

It should be added as a commit hook (see above).

### mutmut
Occasionally run mutmut to check that mutating the code causes tests to fail.

Note: can take a long time to run.

To run:
```
mutmut run --paths-to-mutate just_bin_it
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

Note: there will be a significant number of false-positives.

## Architectural overview

### Core

The core of just-bin-it consists of three main areas:
* The main loop - primarily listens for configuration commands from clients (e.g. NICOS)
* Command handling - based on the configuration command type starts or stops the histogram processes
* Histogram processing - converts event data into histograms and publishes them

If configured, responses to command messages will be published on a Kafka topic.
These messages can be used to check whether a command was accepted or rejected.

The histogramming is performed in separate self-contained processes (using Python's multiprocessing module)
as this enables higher throughput of event data. Each histogram process has its own Kafka consumer and producer.
The events are read by the consumer and histogrammed; the resulting histogram is published via the producer.
Communication to and from the "main" program is via queues.

### Additional outputs

just-bin-it can be configured to output other supplemental data:
* A heartbeat on a specified Kafka topic
* Raw statistics to Graphite, if required

These are published via the main loop on a regular interval.

### Message formats

#### Heartbeat

JSON
```
{
    "message": "some message",
    "message_interval": heartbeat_interval_ms,
}
```
As the message, it currently return the message time.

#### Command responses

*Acknowledged (successful)*

JSON
```
{
    "msg_id": some_unique_msg_id,
    "response": "ACK"
}
```

*Error*

JSON
```
{
    "msg_id": some_unique_msg_id,
    "response": "ERR",
    "message": "some error description"}
```
