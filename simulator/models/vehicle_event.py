from dataclasses import asdict, dataclass


@dataclass
class VehicleEvent:
    event_id: str
    vehicle_id: str
    road_id: str
    event_time: str
    latitude: float
    longitude: float
    speed_kmph: float
    direction: str
    vehicle_type: str

    def to_dict(self):
        return asdict(self)