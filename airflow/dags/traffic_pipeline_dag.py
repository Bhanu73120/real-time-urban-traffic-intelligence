import os
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def start_pipeline():
        run_id = datetime.utcnow().strftime("DEMO_%Y%m%d_%H%M%S_%f")

        print("Traffic Intelligence Pipeline started!")
        print(f"Demo Run ID: {run_id}")

        return run_id



def check_traffic_data():
    connection = psycopg2.connect(
        host=os.environ["TRAFFIC_DB_HOST"],
        port=int(os.environ["TRAFFIC_DB_PORT"]),
        dbname=os.environ["TRAFFIC_DB_NAME"],
        user=os.environ["TRAFFIC_DB_USER"],
        password=os.environ["TRAFFIC_DB_PASSWORD"],
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(event_count), 0)
                FROM analytics.traffic_analytics
                """
            )
            row_count, total_events = cursor.fetchone()

        print(f"Gold analytics rows: {row_count}")
        print(f"Total vehicle events: {total_events}")

        if row_count == 0:
            raise ValueError("No traffic analytics data found!")

        if total_events != 50:
            raise ValueError(
                f"PostgreSQL validation failed: "
                f"expected 50 events, found {total_events}."
            )

        print("PostgreSQL validation PASSED: 50 events.")

    finally:
        connection.close()

def validate_pipeline():
    connection = psycopg2.connect(
        host=os.environ["TRAFFIC_DB_HOST"],
        port=int(os.environ["TRAFFIC_DB_PORT"]),
        dbname=os.environ["TRAFFIC_DB_NAME"],
        user=os.environ["TRAFFIC_DB_USER"],
        password=os.environ["TRAFFIC_DB_PASSWORD"],
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(total_vehicle_events), 0)
                FROM dbt_traffic.road_congestion_summary
                """
            )
            road_count, total_events = cursor.fetchone()

        print(f"Roads in dbt mart: {road_count}")
        print(f"Vehicle events in dbt mart: {total_events}")

        if road_count == 0:
            raise ValueError("dbt mart contains no roads!")

        if total_events != 50:
            raise ValueError(
                f"Final validation failed: "
                f"expected 50 events, found {total_events}."
            )

        print("FINAL VALIDATION PASSED!")
        print("All 50 vehicle events are available in the dbt mart.")
        print("Traffic Intelligence Pipeline completed successfully!")

    finally:
        connection.close()



with DAG(
    dag_id="traffic_intelligence_pipeline",
    description="Orchestrates the Urban Traffic Intelligence pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
        default_args={
            "retries": 0,
        },
    tags=["traffic", "data-engineering"],
) as dag:

    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )

    generate_traffic_events = DockerOperator(
        task_id="generate_traffic_events",
        image="traffic-spark-gold-loader:latest",
        command=(
            "python -m scripts.run_traffic_simulator "
            "--events 50 "
            "--interval 0.1 "
            "--run-id {{ ti.xcom_pull(task_ids='start_pipeline') }}"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="real-time-urban-traffic-intelligence_default",
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
        },
        mount_tmp_dir=False,
        auto_remove="success",
    )


    ingest_bronze = DockerOperator(
        task_id="ingest_kafka_to_bronze",
        image="traffic-spark-gold-loader:latest",
        command="python -m scripts.run_bronze_ingestion --available-now",
        docker_url="unix://var/run/docker.sock",
        network_mode="real-time-urban-traffic-intelligence_default",
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
            "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
            "BRONZE_CHECKPOINT_PATH": (
                "s3a://traffic-data-lake/checkpoints/"
                "airflow_vehicle_events_v3/"
            ),
        },
        mount_tmp_dir=False,
        auto_remove="success",
    )


    transform_silver = DockerOperator(
        task_id="transform_bronze_to_silver",
        image="traffic-spark-gold-loader:latest",
        command=(
            "python -m scripts.run_silver_pipeline "
            "--run-id {{ ti.xcom_pull(task_ids='start_pipeline') }}"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="real-time-urban-traffic-intelligence_default",
        environment={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
            "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
        },
        mount_tmp_dir=False,
        auto_remove="success",
    )


    transform_gold = DockerOperator(
        task_id="transform_silver_to_gold",
        image="traffic-spark-gold-loader:latest",
        command="python -m scripts.run_gold_pipeline",
        docker_url="unix://var/run/docker.sock",
        network_mode="real-time-urban-traffic-intelligence_default",
        environment={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
            "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
        },
        mount_tmp_dir=False,
        auto_remove="success",
    )

    check_data = PythonOperator(
        task_id="check_traffic_data",
        python_callable=check_traffic_data,
    )


    load_gold = DockerOperator(
        task_id="load_gold_to_postgres",
        image="traffic-spark-gold-loader:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="real-time-urban-traffic-intelligence_default",
        environment={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
            "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
            "TRAFFIC_JDBC_URL": (
                "jdbc:postgresql://postgres:5432/traffic_analytics"
            ),
            "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
        },
        mount_tmp_dir=False,
        auto_remove="success",
    )


    run_dbt = DockerOperator(
        task_id="run_dbt_models",
        image="real-time-urban-traffic-intelligence-dbt:latest",
        command="build",
        docker_url="unix://var/run/docker.sock",
        network_mode="real-time-urban-traffic-intelligence_default",
        environment={
            "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
            "DBT_PROFILES_DIR": "/app",
        },
        mount_tmp_dir=False,
        auto_remove="success",
    )


    validate = PythonOperator(
        task_id="validate_setup",
        python_callable=validate_pipeline,
    )

    (
        start
        >> generate_traffic_events
        >> ingest_bronze
        >> transform_silver
        >> transform_gold
        >> load_gold
        >> check_data
        >> run_dbt
        >> validate
    )