from simulator.models.vehicle_event import VehicleEvent


vehicle = VehicleEvent(
    event_id="EVT_001",
    vehicle_id="VH_101",
    road_id="ROAD_001",
    event_time="2026-10-05T08:30:00Z",
    latitude=17.4435,
    longitude=78.3772,
    speed_kmph=35.5,
    direction="north",
    vehicle_type="car"
)

print(vehicle)

print(vehicle.to_dict())