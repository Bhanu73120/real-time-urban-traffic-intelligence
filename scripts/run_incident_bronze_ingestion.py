import argparse
import time

from streaming.spark.incident_bronze_writer import (
    start_incident_bronze_stream,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest Kafka incident events into Bronze."
    )

    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process currently available incident events and exit.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark, query = start_incident_bronze_stream(
        available_now=args.available_now
    )

    try:
        if args.available_now:
            print("Incident Bronze ingestion started.")
            query.awaitTermination()

            if query.exception() is not None:
                raise RuntimeError(
                    f"Incident Bronze ingestion failed: {query.exception()}"
                )

            print("Incident Bronze ingestion completed.")

        else:
            print("Incident continuous ingestion started.")
            print("Press Ctrl+C to stop.")

            while query.isActive:
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping incident Bronze ingestion...")

    finally:
        if query.isActive:
            query.stop()

        spark.stop()
        print("Incident Bronze ingestion stopped.")


if __name__ == "__main__":
    main()