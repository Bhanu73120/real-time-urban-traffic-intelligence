from simulator.generators.road_loader import load_roads


# Load road reference data once
ROADS = load_roads()


# Store the current traffic condition of every road
ROAD_TRAFFIC_STATE = {}
for road in ROADS:
    ROAD_TRAFFIC_STATE[road["road_id"]] = "normal" #"ROAD_001": "normal"


# Store currently active incidents
ACTIVE_INCIDENTS = {}