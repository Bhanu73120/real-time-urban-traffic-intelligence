from simulator.generators.vehicle_generator import generate_speed
from simulator.models.traffic_state import (
    ROADS,
    ROAD_TRAFFIC_STATE,
    ACTIVE_INCIDENTS
)


road_id = "ROAD_003"

road = next(
    road
    for road in ROADS
    if road["road_id"] == road_id
)

normal_speed = road["normal_speed_kmph"]


print("\n========== CONTROLLED INCIDENT TEST ==========")

print("\nRoad:", road_id)
print("Configured normal speed:", normal_speed, "km/h")


# BEFORE INCIDENT
ROAD_TRAFFIC_STATE[road_id] = "normal"

before_speed = generate_speed(
    normal_speed,
    ROAD_TRAFFIC_STATE[road_id]
)

print("\nBEFORE INCIDENT")
print("Traffic state:", ROAD_TRAFFIC_STATE[road_id])
print("Generated speed:", round(before_speed, 2), "km/h")


# HIGH INCIDENT STARTS
ROAD_TRAFFIC_STATE[road_id] = "heavy"

ACTIVE_INCIDENTS[road_id] = {
    "incident": "TEST_HIGH_INCIDENT",
    "remaining_cycles": 8
}

during_speed = generate_speed(
    normal_speed,
    ROAD_TRAFFIC_STATE[road_id]
)

print("\nDURING HIGH INCIDENT")
print("Traffic state:", ROAD_TRAFFIC_STATE[road_id])
print("Generated speed:", round(during_speed, 2), "km/h")


# INCIDENT CLEARS
del ACTIVE_INCIDENTS[road_id]

ROAD_TRAFFIC_STATE[road_id] = "normal"

after_speed = generate_speed(
    normal_speed,
    ROAD_TRAFFIC_STATE[road_id]
)

print("\nAFTER RECOVERY")
print("Traffic state:", ROAD_TRAFFIC_STATE[road_id])
print("Generated speed:", round(after_speed, 2), "km/h")


print("\n==============================================")