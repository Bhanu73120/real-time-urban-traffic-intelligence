import random
import time

from simulator.generators.vehicle_generator import (
    generate_vehicle_event,
    update_traffic_state
)

from simulator.generators.incident_generator import (
    generate_incident,
    update_incidents
)


print("Traffic simulator started.")
print("Press Ctrl+C to stop.")


# Set initial road traffic conditions
update_traffic_state()

cycle = 0


try:
    while True:
        cycle += 1

        # Generate one vehicle event
        event = generate_vehicle_event()

        print("VEHICLE:", event.to_dict())

        # 10% chance of generating an incident
        if random.random() < 0.10:

            incident = generate_incident()

            if incident:
                print("INCIDENT:", incident.to_dict())

        # Update active incident countdowns
        update_incidents()

        # Refresh normal traffic every 10 cycles
        if cycle % 10 == 0:
            update_traffic_state()

        # Wait one second
        time.sleep(1)

except KeyboardInterrupt:
    print("\nTraffic simulator stopped.")