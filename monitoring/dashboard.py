
import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st


st.set_page_config(
    page_title="Urban Traffic Intelligence",
    page_icon="🚦",
    layout="wide",
)

st.title("🚦 Real-Time Urban Traffic Intelligence")
st.caption("Kafka → Spark → MinIO → PostgreSQL → dbt → Streamlit")


@st.cache_data(ttl=15)
def load_data():
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError(
            "POSTGRES_PASSWORD is missing. Load your .env file first."
        )

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname="traffic_analytics",
        user="traffic_user",
        password=password,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    road_id,
                    total_windows,
                    total_vehicle_events,
                    avg_speed_kmph,
                    heavy_windows,
                    moderate_windows,
                    free_flow_windows
                FROM dbt_traffic.road_congestion_summary
                ORDER BY road_id
                """
            )
            road_columns = [column[0] for column in cursor.description]
            road_data = pd.DataFrame(
                cursor.fetchall(), columns=road_columns
            )

            cursor.execute(
                """
                SELECT
                    window_start,
                    window_end,
                    road_id,
                    avg_speed_kmph,
                    event_count,
                    congestion_status
                FROM dbt_traffic.stg_traffic_analytics
                ORDER BY window_start, road_id
                """
            )
            window_columns = [column[0] for column in cursor.description]
            window_data = pd.DataFrame(
                cursor.fetchall(), columns=window_columns
            )

        return road_data, window_data

    finally:
        connection.close()


try:
    road_df, window_df = load_data()
except Exception as exc:
    st.error(f"Unable to load traffic analytics: {exc}")
    st.stop()


if road_df.empty:
    st.warning("No traffic analytics found. Run the pipeline first.")
    st.stop()


# Convert PostgreSQL numeric values to consistent Python types
road_df["avg_speed_kmph"] = pd.to_numeric(
    road_df["avg_speed_kmph"]
).astype(float)

road_df["total_vehicle_events"] = pd.to_numeric(
    road_df["total_vehicle_events"]
).astype(int)

window_df["avg_speed_kmph"] = pd.to_numeric(
    window_df["avg_speed_kmph"]
).astype(float)

window_df["event_count"] = pd.to_numeric(
    window_df["event_count"]
).astype(int)

total_vehicles = int(road_df["total_vehicle_events"].sum())
total_roads = int(road_df["road_id"].nunique())

weighted_speed = (
    (
        road_df["avg_speed_kmph"]
        * road_df["total_vehicle_events"]
    ).sum()
    / total_vehicles
    if total_vehicles
    else 0
)

heavy_windows = int(road_df["heavy_windows"].sum())

st.sidebar.header("Dashboard Controls")

if st.sidebar.button("Refresh analytics"):
    st.cache_data.clear()
    st.rerun()

selected_roads = st.sidebar.multiselect(
    "Select roads",
    options=road_df["road_id"].tolist(),
    default=road_df["road_id"].tolist(),
)

st.subheader("Traffic Overview")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Vehicle Events", f"{total_vehicles:,}")
col2.metric("Roads Monitored", total_roads)
col3.metric("Average Speed", f"{weighted_speed:.2f} km/h")
col4.metric("Heavy Congestion Windows", heavy_windows)

filtered_roads = road_df[
    road_df["road_id"].isin(selected_roads)
].copy()

filtered_windows = window_df[
    window_df["road_id"].isin(selected_roads)
].copy()

if filtered_roads.empty:
    st.info("Select at least one road to display the charts.")
    st.stop()

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Average Speed by Road")

    fig_speed = px.bar(
        filtered_roads,
        x="road_id",
        y="avg_speed_kmph",
        color="road_id",
        labels={
            "road_id": "Road",
            "avg_speed_kmph": "Average Speed (km/h)",
        },
    )
    fig_speed.update_layout(showlegend=False)
    st.plotly_chart(fig_speed, use_container_width=True)

with right:
    st.subheader("Vehicle Events by Road")

    fig_events = px.bar(
        filtered_roads,
        x="road_id",
        y="total_vehicle_events",
        color="road_id",
        labels={
            "road_id": "Road",
            "total_vehicle_events": "Vehicle Events",
        },
    )
    fig_events.update_layout(showlegend=False)
    st.plotly_chart(fig_events, use_container_width=True)

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Congestion Distribution")

    congestion_counts = pd.DataFrame(
        {
            "Status": ["HEAVY", "MODERATE", "FREE_FLOW"],
            "Windows": [
                int(filtered_roads["heavy_windows"].sum()),
                int(filtered_roads["moderate_windows"].sum()),
                int(filtered_roads["free_flow_windows"].sum()),
            ],
        }
    )

    fig_congestion = px.pie(
        congestion_counts,
        names="Status",
        values="Windows",
        hole=0.45,
    )
    st.plotly_chart(fig_congestion, use_container_width=True)

with right:
    st.subheader("Traffic Speed Over Time")

    if not filtered_windows.empty:
        fig_timeline = px.line(
            filtered_windows,
            x="window_start",
            y="avg_speed_kmph",
            color="road_id",
            markers=True,
            labels={
                "window_start": "Time",
                "avg_speed_kmph": "Average Speed (km/h)",
                "road_id": "Road",
            },
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("No time-window data available.")

st.divider()

st.subheader("Detailed Road Analytics")

st.dataframe(
    filtered_roads,
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Data source: PostgreSQL dbt_traffic schema. "
    "Refresh after a successful pipeline run."
)
