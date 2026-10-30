SELECT
    road_id,
    total_incidents,
    high_severity_incidents,
    medium_severity_incidents,
    low_severity_incidents,
    accidents,
    vehicle_breakdowns,
    road_closures,
    construction_incidents,
    CASE
        WHEN total_incidents = 0 THEN 0
        ELSE ROUND(
            high_severity_incidents::numeric
            / total_incidents * 100,
            2
        )
    END AS high_severity_percentage
FROM {{ ref('stg_incident_summary') }}