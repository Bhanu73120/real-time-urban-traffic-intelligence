from streaming.producers.kafka_producer import KafkaEventProducer


producer = KafkaEventProducer()

event = {
    "event_id": "EVT_PYTHON_001",
    "vehicle_id": "VH_1001",
    "road_id": "ROAD_003",
    "speed_kmph": 17.5
}

producer.send_event(
    topic="traffic.vehicle.events",
    key=event["road_id"],
    event=event
)

producer.flush()

print("Test event sent to Kafka.")