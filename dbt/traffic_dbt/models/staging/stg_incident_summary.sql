SELECT
    road_id,
    total_incidents,
    high_severity_incidents,
    medium_severity_incidents,
    low_severity_incidents,
    accidents,
    vehicle_breakdowns,
    road_closures,
    construction_incidents
FROM {{ source('traffic_source', 'incident_summary') }}
WHERE road_id IS NOT NULL