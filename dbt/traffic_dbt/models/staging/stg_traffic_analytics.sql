
SELECT
    window_start,
    window_end,
    road_id,
    avg_speed_kmph,
    event_count,
    congestion_status
FROM {{ source('traffic_source', 'traffic_analytics') }}
WHERE road_id IS NOT NULL
  AND window_start IS NOT NULL
  AND window_end IS NOT NULL
