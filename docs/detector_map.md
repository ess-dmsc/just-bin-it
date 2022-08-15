# Detector map

A 2-D histogram of detector IDs (pixels) where each ID is a bin and the histogram
is arranged to approximate the physical layout of the detector. It assumes a rectangular
shape.

## How to configure
The configuration parameters:
- "type": the type of histogram to generate, must be "dethist".
- "data_brokers": the address of one or more Kafka brokers (list of strings).
- "data_topics": the Kafka topics on which the event data is found (list of strings).
- "det_range": the range of detectors to histogram over (array of ints).
  - end of the range is calculated using `det_range[1] = width * height + det_range[0]`, so supplied value is ignored.
- "width": the width of the detector (int).
- "height": the height of the detector (int).
- "topic": the topic to write the histogram data to.
- "source": if specified only data from that source will be histogrammed [optional].
- "id": a unique identifier for the histogram which will be contained in the published histogram data [optional but recommended]

## Example configuration
```
{
    "type": "dethist",
    "data_brokers": ["localhost:9092"],
    "data_topics": ["fake_events"],
    "det_range": [1, 6145],
    "width": 32,
    "height": 192,
    "topic": "hist-topic",
    "source": "detector_1",
    "id": "some_unique_id"
}
```

## Information for developers
The actual histogramming is done using a 1-D histogram as that is quicker to update than a 2-D one.
The data is converted to a 2-D detector when read. As the data is only read once per second compared to multiple updates per second,
we gain some performance from doing this.
