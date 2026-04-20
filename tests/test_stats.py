from datetime import datetime
import pyspark.sql.types as T
from src.stats import compute_stats


schema = T.StructType(
    [
        T.StructField("turbine_id", T.IntegerType(), True)
        ,T.StructField("timestamp", T.TimestampType(), True)
        ,T.StructField("wind_speed", T.DoubleType(), True)
        ,T.StructField("wind_direction", T.DoubleType(), True)
        ,T.StructField("power_output", T.DoubleType(), True)
    ]
)


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=schema)


def test_stats_columns_present(spark):
    """Output must contain desired statistical columns."""

    rows = [(1, datetime(2022, 3, 1, i, 0, 0), 10.0, 180.0, 3.0) for i in range(5)]
    result = compute_stats(make_df(spark, rows))
    expected = {
        "turbine_id"
        ,"date"
        ,"min_power"
        ,"max_power"
        ,"avg_power"
        ,"record_count"
    }

    assert expected.issubset(set(result.columns))


def test_one_row_per_turbine_per_day(spark):
    """Stats step should produce a single row per (turbine_id, date) combination."""

    rows = (
        [(1, datetime(2022, 3, 1, i, 0, 0), 10.0, 180.0, 3.0) for i in range(24)] # turbine 1 
        + [(1, datetime(2022, 3, 2, i, 0, 0), 10.0, 180.0, 3.0) for i in range(24)] # turbine 1
        + [(2, datetime(2022, 3, 1, i, 0, 0), 10.0, 180.0, 3.0) for i in range(24)] # turbine 2
    )
    result = compute_stats(make_df(spark, rows))

    # turbine 1 has 2 days of data, turbine 2 has 1 day of data
    # should result in 3 rows total
    assert result.count() == 3


def test_min_max_avg_correct(spark):
    """Aggregated values should be mathematically correct against known input."""

    # create dataset with these stats:
    # min_power = 1.0
    # max_power = 5.0
    # avg_power = 3.0
    rows = [
        (1, datetime(2022, 3, 1, 0, 0, 0), 10.0, 180.0, 1.0)
        ,(1, datetime(2022, 3, 1, 1, 0, 0), 10.0, 180.0, 3.0)
        ,(1, datetime(2022, 3, 1, 2, 0, 0), 10.0, 180.0, 5.0)
    ]
    result = compute_stats(make_df(spark, rows))
    row = result.first()

    assert row["min_power"] == 1.0 # confirm min
    assert row["max_power"] == 5.0 # confirm max
    assert row["avg_power"] == 3.0 # confirm avg


def test_record_count_correct(spark):
    """
    record_count field should reflect the number of readings that day 
    (sensor dropout identification).
    """
    # create dataset with these stats:
    # record_count = 24
    rows = [(1, datetime(2022, 3, 1, i, 0, 0), 10.0, 180.0, 3.0) for i in range(24)]
    result = compute_stats(make_df(spark, rows))
    
    assert result.first()["record_count"] == 24

