def validate_vehicle_event(event):
    required_fields = [
        "event_id",
        "vehicle_id",
        "road_id",
        "event_time",
        "latitude",
        "longitude",
        "speed_kmph",
        "direction",
        "vehicle_type"
    ]

    for field in required_fields:
        if field not in event or event[field] is None:
            return False, f"Missing field: {field}"

    if not isinstance(event["speed_kmph"], (int, float)):
        return False, "Speed must be numeric"

    if not 0 <= event["speed_kmph"] <= 200:
        return False, "Invalid vehicle speed"

    if not -90 <= event["latitude"] <= 90:
        return False, "Invalid latitude"

    if not -180 <= event["longitude"] <= 180:
        return False, "Invalid longitude"

    return True, "Valid vehicle event"