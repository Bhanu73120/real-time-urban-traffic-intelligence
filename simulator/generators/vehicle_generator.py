import random
import uuid
from datetime import datetime, timezone

from simulator.models.traffic_state import (
    ROADS,
    ROAD_TRAFFIC_STATE,
    ACTIVE_INCIDENTS
)
from simulator.models.vehicle_event import VehicleEvent


VEHICLE_TYPES = ["car", "bus", "truck", "bike"]
DIRECTIONS = ["north", "south", "east", "west"]


def update_traffic_state():
    current_hour = datetime.now().hour

    is_rush_hour = (
        7 <= current_hour <= 10
        or
        17 <= current_hour <= 20
    )

    for road in ROADS:
        road_id = road["road_id"]   #ROAD_001

        # If this road has an incident,
        # do not change its traffic condition.suppose road_003 in active_acc the skip rush hour code
        if road_id in ACTIVE_INCIDENTS: #Skip the remaining code for ROAD_003 and move to the next road.
            continue

        if is_rush_hour:
            condition = random.choices(
                ["light", "normal", "heavy"],
                weights=[10, 35, 55],
                k=1
            )[0]

        else:
            condition = random.choices(
                ["light", "normal", "heavy"],
                weights=[40, 45, 15],
                k=1
            )[0]

        ROAD_TRAFFIC_STATE[road_id] = condition

def generate_speed(normal_speed, traffic_condition):

    if traffic_condition == "light":
        return random.uniform(
            normal_speed * 0.9,
            normal_speed * 1.1
        )

    elif traffic_condition == "normal":
        return random.uniform(
            normal_speed * 0.7,
            normal_speed * 0.9
        )

    else:
        return random.uniform(
            normal_speed * 0.3,
            normal_speed * 0.6
        )

def generate_vehicle_event():
    # Select one road
    road = random.choice(ROADS)

    road_id = road["road_id"]
    normal_speed = road["normal_speed_kmph"]

    # Find the current condition of that road
    traffic_condition = ROAD_TRAFFIC_STATE[road_id]

    speed = generate_speed(
    normal_speed,
    traffic_condition
)

    # Create VehicleEvent
    event = VehicleEvent(
        event_id=f"EVT_{uuid.uuid4().hex[:8]}",
        vehicle_id=f"VH_{random.randint(1000, 9999)}",
        road_id=road_id,
        event_time=datetime.now(timezone.utc).isoformat(),
        latitude=road["latitude"],
        longitude=road["longitude"],
        speed_kmph=round(speed, 2),
        direction=random.choice(DIRECTIONS),
        vehicle_type=random.choice(VEHICLE_TYPES)
    )

    return event