import json
import logging
from just_bin_it.exceptions import KafkaException


# TODO: tests!
class HeartbeatPublisher:
    def __init__(self, producer, topic, heartbeat_interval_ms):
        self.producer = producer
        self.topic = topic
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.next_time_to_publish = 0

    def publish(self, current_time):
        if current_time // 1_000_000 > self.next_time_to_publish:
            self._publish(current_time)
            self._update_publish_time(current_time)

    def _update_publish_time(self, current_time):
        self.next_time_to_publish = (
            current_time // 1_000_000 + self.heartbeat_interval_ms
        )
        self.next_time_to_publish -= (
            self.next_time_to_publish % self.heartbeat_interval_ms
        )

    def _publish(self, current_time):
        msg = {
            "message": current_time // 1_000_000,
            "message_interval": self.heartbeat_interval_ms,
        }
        try:
            self.producer.publish_message(self.topic, bytes(json.dumps(msg), "utf-8"))
        except KafkaException as error:
            logging.error("Could not publish heartbeat: %s", error)
