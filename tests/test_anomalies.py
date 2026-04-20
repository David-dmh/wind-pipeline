from datetime import datetime
import pyspark.sql.types as T
from src.anomalies import detect_anomalies


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


def test_high_anomaly_flagged(spark):
    """
    A reading statistically above the daily mean should be flagged as an anomaly.
    Value of 15.0 MW is physically plausible but far above the daily average of 3.0 MW (mock data).
    """

    rows = (
        [(1, datetime(2022, 3, 1, i, 0, 0), 10.0, 180.0, 3.0) for i in range(23)]
        + [(1, datetime(2022, 3, 1, 23, 0, 0), 10.0, 180.0, 15.0)]  # high anomaly should be flagged
    )
    result = detect_anomalies(make_df(spark, rows))

    assert result.count() >= 1
    assert result.filter(result.power_output == 15.0).count() == 1


def test_low_anomaly_flagged(spark):
    """
    A reading statistically below the daily mean should be flagged as an anomaly.
    Value of 0.1 MW is physically plausible but far below the daily average of 3.0 MW (mock data).
    """
    
    rows = (
        [(1, datetime(2022, 3, 1, i, 0, 0), 10.0, 180.0, 3.0) for i in range(23)]
        + [(1, datetime(2022, 3, 1, 23, 0, 0), 10.0, 180.0, 0.1)]  # low anomaly should be flagged
    )
    result = detect_anomalies(make_df(spark, rows))

    assert result.count() >= 1
    assert result.filter(result.power_output == 0.1).count() == 1


def test_stable_readings_not_flagged(spark):
    """All identical readings produce zero variance - nothing should be flagged."""

    rows = [(1, datetime(2022, 3, 1, i, 0, 0), 10.0, 180.0, 3.0) for i in range(24)]
    result = detect_anomalies(make_df(spark, rows))

    assert result.count() == 0
