# Region-of-interest histogram

Histograms over a selected rectangular range of a detector.

## How to configure
The configuration parameters:
- "type": the type of histogram to generate, must be "roihist".
- "data_brokers": the address of one or more Kafka brokers (list of strings).
- "data_topics": the Kafka topics on which the event data is found (list of strings).
- "left_edges" : the leftmost pixel for each row which we are interested in (list of pixel IDs).
- "width": how many pixels starting from the leftmost pixel to include (integer)
- "topic": the topic to write the histogram data to.
- "source": if specified only data from that source will be histogrammed [optional].
- "id": a unique identifier for the histogram which will be contained in the published histogram data [optional but recommended]

The key parameters for defining the ROI are the left edges and the width.
For example, consider a 5 x 4 detector:

| | | | | |
|:---:| :---: | :---:| :---: |:---: |
|1|2|3|4|5|
|6|**7**|**8**|**9**|10|
|11|**12**|**13**|**14**|15|
|16|17|18|19|20|

We want to histogram over pixels 7, 8, 9, 12, 13, 14 which means the relevant configuration parameters
would be:
```
"left_edges": [7, 12],
"width": 3,          
```
## Example configuration
```
{
    "type": "roihist",
    "data_brokers": ["localhost:9092"],
    "data_topics": ["FREIA_detector"],
    "left_edges": [33, 65, 97, 129, 161, 192, 225],
    "width": 16,
    "topic":"hist-topic",
    "id": "some_id"
},
```

## Information for developers
### The rhomboid detector used for the tests
For some tests we use a 10 x 5 rhomboid detector because it is more challenging to get right that a rectangle ;)
The detector looks something like this:

| | | | | | | | | | | | | | |
|:---:| :---: | :---:| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
|1|2|3|4|5|6|7|8|9|10
| |11|12|13|14|15|**16**|**17**|**18**|**19**|20
| | |21|22|23|24|**25**|**26**|**27**|**28**|29|30
| | | |31|32|33|**34**|**35**|**36**|**37**|38|39|40
| | | | |41|42|43|44|45|46|47|48|49|50

During the tests the ROI is defined by the highlighted pixel IDs.

### How the histogramming is done
The actual histogramming is done using a 1-D histogram as that is quicker to update than a 2-D one.
The data is converted to a 2-D detector when read. As the data is only read once per second compared to multiple updates per second,
we gain some performance from doing this.

For performance reasons we would like to minimise the number of bins in the 1-D histogram, but Numpy 1-D arrays are contiguous.
Our workaround is to use wide bins for the IDs we are not interested in, e.g for the rhomboid detector above we would define the bins as:
```
16, 17, 18, 19, 20, 25, 26, 27, ..
```
Because we are not interested in the IDs between 19 and 25 we create one wide bin to reduce the total number of bins.
When the 1-D data is transformed into 2-D we ignore the data in the bins we don't care about.

#### Why do we add two extra bins at the end?
Numpy includes the right most edge of the last bin as part of that bin,
for example, bins = [1,2,3] gives two buckets 1 to 1.9999999 and 2 to 3.

However, what we want is three buckets, one each for 1, 2, 3, but adding one extra bin isn't sufficient because
bins = [1,2,3,4] means the value 4 will end up in the "3" bin, so we add one more bin.
