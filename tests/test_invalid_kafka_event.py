from datetime import datetime, timezone

from streaming.producers.kafka_producer import KafkaEventProducer
from streaming.producers.topics import VEHICLE_EVENTS_TOPIC


producer = KafkaEventProducer()

invalid_event = {
    "event_id": "EVT_INVALID_250",
    "vehicle_id": "VH_TEST",
    "road_id": "ROAD_003",
    "event_time": datetime.now(timezone.utc).isoformat(),
    "latitude": 17.4551,
    "longitude": 78.3843,
    "speed_kmph": 250.0,
    "direction": "north",
    "vehicle_type": "car"
}

producer.send_event(
    topic=VEHICLE_EVENTS_TOPIC,
    key=invalid_event["road_id"],
    event=invalid_event
)

producer.flush()

print("Invalid test event published to Kafka.")