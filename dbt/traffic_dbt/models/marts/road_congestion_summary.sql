
SELECT
    road_id,
    COUNT(*) AS total_windows,
    SUM(event_count) AS total_vehicle_events,
    ROUND(
        (
            SUM(avg_speed_kmph * event_count)
            / NULLIF(SUM(event_count), 0)
        )::numeric,
        2
    ) AS avg_speed_kmph,
    SUM(
        CASE WHEN congestion_status = 'HEAVY'
        THEN 1 ELSE 0 END
    ) AS heavy_windows,
    SUM(
        CASE WHEN congestion_status = 'MODERATE'
        THEN 1 ELSE 0 END
    ) AS moderate_windows,
    SUM(
        CASE WHEN congestion_status = 'FREE_FLOW'
        THEN 1 ELSE 0 END
    ) AS free_flow_windows
FROM {{ ref('stg_traffic_analytics') }}
GROUP BY road_id
