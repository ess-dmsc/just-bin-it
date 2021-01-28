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