# Just Bin It

A lightweight program for histogramming neutron event data for diagnostic purposes.

## How to run
Python 3.7+ only. Might work on older version of Python 3 but not tested.

This assumes you have Kafka running on localhost and have recently run the NeXus-Streamer
using the SANS_test_reduced.hdf5 dataset.

```
>>> pip install -r requirements.txt
>>> python main.py --config config.json
```

The config.json looks like this:
```json
{
  "data_brokers": ["localhost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {"num_dims": 1, "det_range": [0, 100000000], "num_bins": 50}
  ]
}
```

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
