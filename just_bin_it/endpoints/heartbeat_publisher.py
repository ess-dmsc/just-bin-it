import json
import logging

from just_bin_it.exceptions import KafkaException


class HeartbeatPublisher:
    def __init__(self, producer, topic, heartbeat_interval_ms=1000):
        self.producer = producer
        self.topic = topic
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.next_time_to_publish = 0

    def publish(self, current_time_ms):
        """
        Publish the heartbeat if enough time has elapsed.

        :param current_time_ms: milliseconds since UNIX epoch.
        """
        assert current_time_ms >= 0

        if current_time_ms >= self.next_time_to_publish:
            self._publish(current_time_ms)
            self._update_publish_time(current_time_ms)

    def _update_publish_time(self, current_time_ms):
        self.next_time_to_publish = current_time_ms + self.heartbeat_interval_ms
        # Round to nearest whole interval
        self.next_time_to_publish -= (
            self.next_time_to_publish % self.heartbeat_interval_ms
        )

    def _publish(self, current_time_ms):
        msg = {
            "message": current_time_ms,
            "message_interval": self.heartbeat_interval_ms,
        }
        try:
            self.producer.publish_message(self.topic, bytes(json.dumps(msg), "utf-8"))
        except KafkaException as error:
            logging.error("Could not publish heartbeat: %s", error)
