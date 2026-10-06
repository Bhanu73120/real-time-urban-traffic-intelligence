import random
import uuid
from datetime import datetime, timezone

from simulator.models.incident_event import IncidentEvent
from simulator.models.traffic_state import (
    ROADS,
    ROAD_TRAFFIC_STATE,
    ACTIVE_INCIDENTS
)


INCIDENT_TYPES = [
    "accident",
    "vehicle_breakdown",
    "road_closure",
    "construction"
]

SEVERITIES = [
    "low",
    "medium",
    "high"
]


def generate_incident():
    # Select only roads that don't already have an incident
    available_roads = [
        road
        for road in ROADS
        if road["road_id"] not in ACTIVE_INCIDENTS
    ]

    # If every road already has an incident
    if not available_roads:
        return None

    road = random.choice(available_roads)

    severity = random.choice(SEVERITIES)

    incident = IncidentEvent(
        incident_id=f"INC_{uuid.uuid4().hex[:8]}",
        road_id=road["road_id"],
        incident_type=random.choice(INCIDENT_TYPES),
        severity=severity,
        event_time=datetime.now(timezone.utc).isoformat()
    )

    # Decide traffic impact and duration
    if severity == "low":
        traffic_condition = "normal"
        duration = 3

    elif severity == "medium":
        traffic_condition = "heavy"
        duration = 5

    else:
        traffic_condition = "heavy"
        duration = 8

    # Change road condition
    ROAD_TRAFFIC_STATE[road["road_id"]] = traffic_condition

    # Remember that this road currently has an incident
    ACTIVE_INCIDENTS[road["road_id"]] = {
        "incident": incident,
        "remaining_cycles": duration
    }

    return incident


def update_incidents():
    recovered_roads = []

    # Reduce remaining duration of active incidents
    for road_id, incident_data in ACTIVE_INCIDENTS.items():

        incident_data["remaining_cycles"] -= 1

        if incident_data["remaining_cycles"] <= 0:
            recovered_roads.append(road_id)

    # Clear finished incidents
    for road_id in recovered_roads:

        del ACTIVE_INCIDENTS[road_id]

        ROAD_TRAFFIC_STATE[road_id] = "normal"

        print(f"RECOVERY: {road_id} incident cleared.")