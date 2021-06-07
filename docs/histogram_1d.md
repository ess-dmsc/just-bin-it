# 1-D TOF Histogram
A 1-D histogram of time-of-flight vs counts.

## How to configure
The configuration parameters:
- "type": the type of histogram to generate, must be "hist2d".
- "data_brokers": the address of one or more Kafka brokers (list of strings).
- "data_topics": the Kafka topics on which the event data is found (list of strings).
- "num_bins": the number of histogram bins for time-of-flight (ints).
- "tof_range": the time-of-flight range to histogram (array of ints).
- "det_range": the range of detectors to histogram over (array of ints) [optional].
- "topic": the topic to write the histogram data to.
- "source": if specified only data from that source will be histogrammed [optional].
- "id": a unique identifier for the histogram which will be contained in the published histogram data [optional but recommended]

If the `det_range` is not specified then it will default to counting across all detectors.

## Example configuration
```
{
    "type": "hist2d",
    "data_brokers": ["localhost:9092"],
    "data_topics": ["fake_events"],
    "tof_range": [0, 100000000],
    "det_range": [0, 100],
    "num_bins": 50,
    "topic": "hist-topic",
    "source": "detector_1",
    "id": "some_unique_id"
}
```
