import random
import time

from streaming.producers.kafka_producer import KafkaEventProducer
from streaming.producers.topics import (
    VEHICLE_EVENTS_TOPIC,
    INCIDENT_EVENTS_TOPIC
)

from simulator.generators.vehicle_generator import (
    generate_vehicle_event,
    update_traffic_state
)

from simulator.generators.incident_generator import (
    generate_incident,
    update_incidents
)


# Create one Kafka producer and reuse it
producer = KafkaEventProducer()

print("Traffic simulator started.")
print("Press Ctrl+C to stop.")

# Set initial road traffic conditions
update_traffic_state()

cycle = 0


try:
    while True:
        cycle += 1

        # -----------------------------
        # Generate one vehicle event
        # -----------------------------
        event = generate_vehicle_event()
        event_data = event.to_dict()

        # Send vehicle event to Kafka
        producer.send_event(
            topic=VEHICLE_EVENTS_TOPIC,
            key=event.road_id,
            event=event_data
        )

        print("VEHICLE:", event_data)

        # -----------------------------
        # 10% chance of an incident
        # -----------------------------
        if random.random() < 0.10:
            incident = generate_incident()

            if incident:
                incident_data = incident.to_dict()

                # Send incident event to Kafka
                producer.send_event(
                    topic=INCIDENT_EVENTS_TOPIC,
                    key=incident.road_id,
                    event=incident_data
                )

                print("INCIDENT:", incident_data)

        # -----------------------------
        # Update active incidents
        # -----------------------------
        update_incidents()

        # Refresh traffic conditions
        # every 10 simulation cycles
        if cycle % 10 == 0:
            update_traffic_state()

        # -----------------------------
        # Wait before next cycle
        # -----------------------------
        time.sleep(3)


except KeyboardInterrupt:
    print("\nStopping traffic simulator...")


finally:
    # Wait for queued Kafka messages
    # to finish sending before shutdown
    producer.flush()

    print("Kafka producer flushed.")
    print("Traffic simulator stopped.")