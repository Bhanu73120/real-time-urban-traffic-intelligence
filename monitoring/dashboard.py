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

            cursor.execute(
                """
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
                    high_severity_percentage
                FROM dbt_traffic.road_incident_summary
                ORDER BY road_id
                """
            )

            incident_columns = [
                column[0] for column in cursor.description
            ]

            incident_data = pd.DataFrame(
                cursor.fetchall(),
                columns=incident_columns
            )

        return road_data, window_data, incident_data

    finally:
        connection.close()


try:
    road_df, window_df,incident_df = load_data()
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



# -------------------------------------------------------
# Incident Analytics
# -------------------------------------------------------

st.divider()
st.subheader("🚨 Incident Analytics")

if incident_df.empty:
    st.info("No incidents recorded for the latest pipeline run.")

else:
    numeric_columns = [
        "total_incidents",
        "high_severity_incidents",
        "medium_severity_incidents",
        "low_severity_incidents",
        "accidents",
        "vehicle_breakdowns",
        "road_closures",
        "construction_incidents",
    ]

    for column in numeric_columns:
        incident_df[column] = pd.to_numeric(
            incident_df[column]
        ).fillna(0).astype(int)

    total_incidents = int(
        incident_df["total_incidents"].sum()
    )

    high_incidents = int(
        incident_df["high_severity_incidents"].sum()
    )

    medium_incidents = int(
        incident_df["medium_severity_incidents"].sum()
    )

    low_incidents = int(
        incident_df["low_severity_incidents"].sum()
    )

    affected_roads = int(
        incident_df["road_id"].nunique()
    )

    i1, i2, i3, i4 = st.columns(4)

    i1.metric("Total Incidents", total_incidents)
    i2.metric("High Severity", high_incidents)
    i3.metric("Medium Severity", medium_incidents)
    i4.metric("Affected Roads", affected_roads)

    filtered_incidents = incident_df[
        incident_df["road_id"].isin(selected_roads)
    ].copy()

    if filtered_incidents.empty:
        st.info("No incidents on the selected roads.")

    else:
        left, right = st.columns(2)

        with left:
            st.subheader("Incidents by Road")

            fig_incident_roads = px.bar(
                filtered_incidents,
                x="road_id",
                y="total_incidents",
                color="road_id",
                labels={
                    "road_id": "Road",
                    "total_incidents": "Incidents",
                },
            )

            fig_incident_roads.update_layout(
                showlegend=False
            )

            st.plotly_chart(
                fig_incident_roads,
                use_container_width=True
            )

        with right:
            st.subheader("Incident Severity Distribution")

            severity_df = pd.DataFrame(
                {
                    "Severity": ["High", "Medium", "Low"],
                    "Count": [
                        int(filtered_incidents[
                            "high_severity_incidents"
                        ].sum()),
                        int(filtered_incidents[
                            "medium_severity_incidents"
                        ].sum()),
                        int(filtered_incidents[
                            "low_severity_incidents"
                        ].sum()),
                    ],
                }
            )

            fig_severity = px.pie(
                severity_df,
                names="Severity",
                values="Count",
                hole=0.4,
            )

            st.plotly_chart(
                fig_severity,
                use_container_width=True
            )

        st.subheader("Incident Types")

        type_columns = {
            "Accidents": "accidents",
            "Vehicle Breakdowns": "vehicle_breakdowns",
            "Road Closures": "road_closures",
            "Construction": "construction_incidents",
        }

        type_df = pd.DataFrame(
            [
                {
                    "Incident Type": name,
                    "Count": int(
                        filtered_incidents[column].sum()
                    ),
                }
                for name, column in type_columns.items()
            ]
        )

        fig_types = px.bar(
            type_df,
            x="Incident Type",
            y="Count",
            color="Incident Type",
        )

        fig_types.update_layout(showlegend=False)

        st.plotly_chart(
            fig_types,
            use_container_width=True
        )

        st.subheader("Detailed Incident Analytics")

        st.dataframe(
            filtered_incidents,
            use_container_width=True,
            hide_index=True,
        )
