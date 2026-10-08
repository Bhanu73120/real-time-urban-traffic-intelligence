from confluent_kafka import Consumer


class KafkaEventConsumer:

    def __init__(self, topics, group_id):
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        })

        self.consumer.subscribe(topics)

    def read_event(self):
        message = self.consumer.poll(1.0)

        if message is None:
            return None

        if message.error():
            raise RuntimeError(message.error())

        return message

    def commit(self):
        self.consumer.commit(asynchronous=False)

    def close(self):
        self.consumer.close()