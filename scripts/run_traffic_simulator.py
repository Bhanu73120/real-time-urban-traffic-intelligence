import argparse
import random
import time

from streaming.producers.kafka_producer import KafkaEventProducer
from datetime import datetime, timezone
from streaming.producers.topics import (
    VEHICLE_EVENTS_TOPIC,
    INCIDENT_EVENTS_TOPIC,
)

from simulator.generators.vehicle_generator import (
    generate_vehicle_event,
    update_traffic_state,
)

from simulator.generators.incident_generator import (
    generate_incident,
    update_incidents,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate simulated traffic events and publish to Kafka."
    )

    parser.add_argument(
        "--events",
        type=int,
        default=None,
        help="Number of vehicle events to generate. Default: unlimited.",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=3.0,
        help="Seconds between vehicle events. Default: 3.",
    )

    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional demo run ID supplied by Airflow.",
    )
    args = parser.parse_args()

    if args.events is not None and args.events <= 0:
        parser.error("--events must be greater than zero")

    if args.interval < 0:
        parser.error("--interval cannot be negative")

    return args


def main():
    args = parse_args()

    run_id = args.run_id or datetime.now(timezone.utc).strftime(
        "DEMO_%Y%m%d_%H%M%S_%f"
    )
    print(f"Demo Run ID: {run_id}")

    producer = KafkaEventProducer()

    print("Traffic simulator started.")
    print(f"Event limit: {args.events or 'Unlimited'}")
    print(f"Interval: {args.interval} seconds")

    update_traffic_state()

    cycle = 0

    try:
        while args.events is None or cycle < args.events:
            cycle += 1

            event = generate_vehicle_event()
            event_data = event.to_dict()
            event_data["run_id"] = run_id

            producer.send_event(
                topic=VEHICLE_EVENTS_TOPIC,
                key=event.road_id,
                event=event_data,
            )

            print(f"[{cycle}] VEHICLE:", event_data)

            if random.random() < 0.10:
                incident = generate_incident()

                if incident:
                    incident_data = incident.to_dict()
                    incident_data["run_id"] = run_id

                    producer.send_event(
                        topic=INCIDENT_EVENTS_TOPIC,
                        key=incident.road_id,
                        event=incident_data,
                    )

                    print("INCIDENT:", incident_data)

            update_incidents()

            if cycle % 10 == 0:
                update_traffic_state()

            if args.events is None or cycle < args.events:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nStopping traffic simulator...")

    finally:
        producer.flush()
        print("Kafka producer flushed.")
        print(f"Total vehicle events generated: {cycle}")
        print("Traffic simulator stopped.")


if __name__ == "__main__":
    main()