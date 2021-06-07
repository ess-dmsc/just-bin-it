# 2-D TOF Histogram
A 2-D histogram of time-of-flight vs detector IDs.

## How to configure
The configuration parameters:
- "type": the type of histogram to generate, must be "hist2d".
- "data_brokers": the address of one or more Kafka brokers (list of strings).
- "data_topics": the Kafka topics on which the event data is found (list of strings).
- "num_bins": the number of histogram bins in both directions (tuple of two ints).
- "tof_range": the time-of-flight range to histogram (array of ints).
- "det_range": the range of detectors to histogram over (array of ints).
- "topic": the topic to write the histogram data to.
- "source": if specified only data from that source will be histogrammed [optional].
- "id": a unique identifier for the histogram which will be contained in the published histogram data [optional but recommended]

## Example configuration
```
{
    "type": "hist2d",
    "data_brokers": ["localhost:9092"],
    "data_topics": ["fake_events"],
    "tof_range": [0, 100000000],
    "det_range": [0, 100],
    "num_bins": (50, 100),
    "topic": "hist-topic",
    "source": "detector_1",
    "id": "some_unique_id"
}
```
