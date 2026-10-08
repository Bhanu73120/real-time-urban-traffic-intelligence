import json
import time

from streaming.consumers.kafka_consumer import KafkaEventConsumer
from streaming.producers.topics import VEHICLE_EVENTS_TOPIC
from data_quality.vehicle_validator import validate_vehicle_event


consumer = KafkaEventConsumer(
    topics=[VEHICLE_EVENTS_TOPIC],
    group_id="traffic-vehicle-validator"
)

print("Traffic consumer started. Press Ctrl+C to stop.")

try:
    while True:
        message = consumer.read_event()

        if message is None:
            continue

        try:
            event = json.loads(message.value().decode("utf-8"))
            valid, reason = validate_vehicle_event(event)

        except (ValueError, TypeError) as error:
            valid = False
            reason = f"Invalid JSON or event structure: {error}"

        if valid:
            print(
                f"VALID: {event['event_id']} "
                f"road={event['road_id']} "
                f"partition={message.partition()} "
                f"offset={message.offset()}"
            )
        else:
            print(
                f"REJECTED: {reason} "
                f"partition={message.partition()} "
                f"offset={message.offset()}"
            )

        # For this learning version, both valid and rejected
        # records are considered handled after printing.
        consumer.commit()
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping traffic consumer...")

finally:
    consumer.close()
    print("Traffic consumer closed.")