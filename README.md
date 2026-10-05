# Real-Time Urban Traffic Intelligence Platform

An end-to-end data engineering project for processing continuously
generated urban traffic events and identifying congestion in near real time.

## Problem

Urban traffic conditions can change rapidly because of increasing vehicle
volume, accidents, road closures, and other incidents.

Traditional batch processing may identify these patterns only after the
events have occurred. This project builds a streaming data platform capable
of continuously ingesting vehicle movement and road incident events,
processing them in near real time, detecting congestion, and retaining
historical data for analytics.

## Objectives

- Generate realistic continuous vehicle GPS events.
- Generate road incident events.
- Stream events using Apache Kafka.
- Process events using Spark Structured Streaming.
- Validate and deduplicate incoming events.
- Handle late-arriving events using event-time processing.
- Detect road congestion using window-based aggregations.
- Store historical data using MinIO and Parquet.
- Store serving and analytical data in PostgreSQL.
- Build analytical models using dbt.
- Orchestrate scheduled workflows using Apache Airflow.
- Visualize traffic conditions using Grafana.
- Implement automated testing and data-quality checks.
- Run the platform locally without paid cloud infrastructure.

## Architecture

Python → Kafka → Spark Structured Streaming → MinIO / PostgreSQL
→ dbt → Grafana

Apache Airflow will orchestrate scheduled batch workflows,
quality checks, maintenance tasks, and backfills.

## Cost

The project is designed to run locally using open-source tools,
without AWS, Azure, or GCP.