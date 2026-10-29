import os
import json

from confluent_kafka import Producer


class KafkaEventProducer:

    def __init__(self, bootstrap_servers=None):
        bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )

        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers
        })

    def delivery_report(self, err, msg):
        if err is not None:
            print(
                f"KAFKA DELIVERY FAILED: "
                f"topic={msg.topic()} error={err}"
            )

    def send_event(self, topic, key, event):
        event_json = json.dumps(event)

        self.producer.produce(
            topic=topic,
            key=key,
            value=event_json,
            callback=self.delivery_report
        )

        self.producer.poll(0)

    def flush(self):
        self.producer.flush()