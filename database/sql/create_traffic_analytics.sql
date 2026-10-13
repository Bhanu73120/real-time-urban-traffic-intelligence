create schema if not exists analytics;

create table if not exists analytics.traffic_analytics (
    window_start timestamp not null,
    window_end timestamp not null,
    road_id varchar(50) not null,
    avg_speed_kmph double precision,
    event_count bigint,
    congestion_status varchar(20),
    primary key (window_start, window_end, road_id)
);