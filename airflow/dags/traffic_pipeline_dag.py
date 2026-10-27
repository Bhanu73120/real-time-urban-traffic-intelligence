import os
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def start_pipeline():
    print("Traffic Intelligence Pipeline started!")


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
                "SELECT COUNT(*) FROM analytics.traffic_analytics"
            )
            row_count = cursor.fetchone()[0]

        print(f"Gold analytics rows: {row_count}")

        if row_count == 0:
            raise ValueError("No traffic analytics data found!")

        print("PostgreSQL traffic data validation passed!")

    finally:
        connection.close()


def validate_pipeline():
    print("Traffic pipeline validation completed!")


with DAG(
    dag_id="traffic_intelligence_pipeline",
    description="Orchestrates the Urban Traffic Intelligence pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["traffic", "data-engineering"],
) as dag:

    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
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
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin123",
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

    start >> load_gold >> check_data >> run_dbt >> validate