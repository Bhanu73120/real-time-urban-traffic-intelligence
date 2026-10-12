from data_quality.vehicle_validator import validate_vehicle_event


valid_event = {
    "event_id": "EVT_TEST_100",
    "vehicle_id": "VH_1001",
    "road_id": "ROAD_003",
    "event_time": "2026-10-08T09:00:00+00:00",
    "latitude": 17.4551,
    "longitude": 78.3843,
    "speed_kmph": 35,
    "direction": "north",
    "vehicle_type": "car"
}


# Test 1: Valid event
result, reason = validate_vehicle_event(valid_event)
print("Test 1:", result, reason)
assert result is True


# Test 2: Invalid speed
invalid_speed = valid_event.copy()
invalid_speed["speed_kmph"] = 250

result, reason = validate_vehicle_event(invalid_speed)
print("Test 2:", result, reason)
assert result is False


# Test 3: Missing road_id
missing_road = valid_event.copy()
del missing_road["road_id"]

result, reason = validate_vehicle_event(missing_road)
print("Test 3:", result, reason)
assert result is False


# Test 4: Invalid latitude
invalid_latitude = valid_event.copy()
invalid_latitude["latitude"] = 120

result, reason = validate_vehicle_event(invalid_latitude)
print("Test 4:", result, reason)
assert result is False


print("All vehicle validation tests passed.")