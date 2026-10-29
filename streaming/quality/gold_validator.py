
from pyspark.sql import functions as F


def validate_gold_data(df):
    """Validate Gold traffic analytics before PostgreSQL loading."""

    print("Starting Gold data quality checks...")

    # Check 1: Dataset must contain records.
    total_rows = df.count()

    if total_rows == 0:
        raise ValueError("Gold dataset is empty.")

    print(f"PASS: Gold contains {total_rows} records")

    # Check 2: Required fields cannot be NULL.
    required_columns = [
        "window_start",
        "window_end",
        "road_id",
        "avg_speed_kmph",
        "event_count",
        "congestion_status",
    ]

    missing_columns = set(required_columns) - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    null_condition = None

    for column in required_columns:
        condition = F.col(column).isNull()
        null_condition = (
            condition if null_condition is None
            else null_condition | condition
        )

    if df.filter(null_condition).limit(1).count() > 0:
        raise ValueError("Gold dataset contains NULL values.")

    print("PASS: No NULL values in required columns")

    # Check 3: Speeds and event counts must be valid.
    invalid_records = df.filter(
        (F.isnan("avg_speed_kmph")) |
        (F.col("avg_speed_kmph") < 0) |
        (F.col("event_count") <= 0) |
        (F.col("window_end") <= F.col("window_start"))
    )

    if invalid_records.limit(1).count() > 0:
        raise ValueError("Gold dataset contains invalid analytics values.")

    print("PASS: Analytics values are valid")

    # Check 4: Each road/window combination must be unique.
    duplicates = (
        df.groupBy("window_start", "window_end", "road_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError("Duplicate road/window analytics detected.")

    print("PASS: No duplicate analytics keys")
    print("All Gold data quality checks passed!")

    return df
