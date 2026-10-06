from dataclasses import asdict, dataclass


@dataclass
class IncidentEvent:
    incident_id: str
    road_id: str
    incident_type: str
    severity: str
    event_time: str

    def to_dict(self):
        return asdict(self)