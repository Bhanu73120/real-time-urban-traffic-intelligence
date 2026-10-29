
import argparse
import time

from streaming.spark.bronze_writer import start_bronze_stream


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest Kafka vehicle events into Bronze."
    )

    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process currently available Kafka events and exit.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark, query = start_bronze_stream(
        available_now=args.available_now
    )

    try:
        if args.available_now:
            print("Bronze available-now ingestion started.")
            query.awaitTermination()

            if query.exception() is not None:
                raise RuntimeError(
                    f"Bronze ingestion failed: {query.exception()}"
                )

            print("Bronze available-now ingestion completed.")

        else:
            print("Bronze continuous ingestion started.")
            print("Press Ctrl+C to stop.")

            while query.isActive:
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping Bronze ingestion...")

    finally:
        if query.isActive:
            query.stop()

        spark.stop()
        print("Bronze ingestion stopped.")


if __name__ == "__main__":
    main()
