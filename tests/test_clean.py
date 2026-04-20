from datetime import datetime
import pyspark.sql.types as T
from src.clean import clean_data


schema = T.StructType(
    [
        T.StructField("turbine_id", T.IntegerType(), True)
        ,T.StructField("timestamp", T.TimestampType(), True)
        ,T.StructField("wind_speed", T.DoubleType(), True)
        ,T.StructField("wind_direction", T.DoubleType(), True)
        ,T.StructField("power_output", T.DoubleType(), True)
    ]
)

T1 = datetime(2022, 3, 1, 0, 0, 0)
T2 = datetime(2022, 3, 1, 1, 0, 0)
T3 = datetime(2022, 3, 1, 2, 0, 0)


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=schema)


def test_duplicates_are_dropped(spark):
    """Rows with the same (turbine_id, timestamp) should be deduplicated."""

    rows = [
        (1, T1, 10.0, 180.0, 3.0)
        ,(1, T1, 10.0, 180.0, 3.0)  # duplicate of first row
        ,(1, T2, 10.0, 180.0, 3.0)
    ]
    result = clean_data(make_df(spark, rows))

    assert result.count() == 2


def test_null_turbine_id_dropped(spark):
    """Rows with NULL turbine_id cannot be attributed to a turbine and must be dropped."""

    rows = [
        (None, T1, 10.0, 180.0, 3.0)
        ,(1, T2, 10.0, 180.0, 3.0)
    ]
    result = clean_data(make_df(spark, rows))

    assert result.count() == 1
    assert result.first()["turbine_id"] == 1


def test_null_timestamp_dropped(spark):
    """Rows with NULL timestamp cannot be placed on the timeline and must be dropped."""

    rows = [
        (1, None, 10.0, 180.0, 3.0)
        ,(1, T1, 10.0, 180.0, 3.0)
    ]
    result = clean_data(make_df(spark, rows))

    assert result.count() == 1


def test_null_power_output_imputed(spark):
    """NULL power_output should be filled with the per-turbine median."""

    # dataset with 4 as median
    rows = [
        (1, T1, 10.0, 180.0, 4.0)
        ,(1, T2, 10.0, 180.0, 4.0)
        ,(1, T3, 10.0, 180.0, None) # should be imputed to 4.0
    ]
    result = clean_data(make_df(spark, rows))

    assert result.filter(result.power_output.isNull()).count() == 0


def test_null_power_imputed_per_turbine_not_globally(spark):
    """
    Imputation should use the median for that specific turbine.
    Turbine 1 median = 2.0, turbine 2 median = 8.0 - imputed values should differ.
    """

    rows = [
        (1, T1, 10.0, 180.0, 2.0)
        ,(1, T2, 10.0, 180.0, 2.0)
        ,(1, T3, 10.0, 180.0, None)
        ,(2, T1, 10.0, 180.0, 8.0)
        ,(2, T2, 10.0, 180.0, 8.0)
        ,(2, T3, 10.0, 180.0, None)
    ]
    result = clean_data(make_df(spark, rows))

    t1_imputed = result.filter(
        (result.turbine_id == 1) & (result.timestamp == T3)
    ).first()["power_output"]

    t2_imputed = result.filter(
        (result.turbine_id == 2) & (result.timestamp == T3)
    ).first()["power_output"]

    assert t1_imputed < t2_imputed # 2 < 8


def test_null_wind_speed_imputed(spark):
    """NULL wind_speed should be filled with the median for that specific turbine."""

    rows = [
        (1, T1, 10.0, 180.0, 3.0)
        ,(1, T2, 10.0, 180.0, 3.0)
        ,(1, T3, None, 180.0, 3.0) # wind_speed should be imputed
    ]
    result = clean_data(make_df(spark, rows))

    assert result.filter(result.wind_speed.isNull()).count() == 0


def test_negative_power_output_removed(spark):
    """Negative power output is a sensor error and must be removed."""

    rows = [
        (1, T1, 10.0, 180.0, 3.0)
        ,(1, T2, 10.0, 180.0, -1.0)  # -1 should be removed
    ]
    result = clean_data(make_df(spark, rows))

    assert result.count() == 1
    assert result.filter(result.power_output < 0).count() == 0


def test_power_output_above_ceiling_removed(spark):
    """Power output above 20MW is treated as a sensor error and removed."""

    rows = [
        (1, T1, 10.0, 180.0, 3.0)
        ,(1, T2, 10.0, 180.0, 999.0)  # 999 should be removed
    ]
    result = clean_data(make_df(spark, rows))

    assert result.count() == 1
    assert result.filter(result.power_output > 20).count() == 0


def test_clean_data_passes_through_unchanged(spark):
    """Fully valid data should not lose any rows during cleaning."""

    rows = [
        (1, T1, 10.0, 180.0, 3.0)
        ,(1, T2, 11.0, 190.0, 3.5)
        ,(2, T1, 9.0,  170.0, 2.8)
    ]
    result = clean_data(make_df(spark, rows))

    assert result.count() == 3


def test_invalid_wind_direction_nulled(spark):
    """Wind direction values outside 0-360 should be set to NULL."""

    rows = [
        (1, datetime(2022, 3, 1, 0, 0, 0), 10.0, 365.0, 2.0)
        ,(2, datetime(2022, 3, 1, 1, 0, 0), 10.0, 180.0, 2.0)
        ,(3, datetime(2022, 3, 1, 2, 0, 0), 10.0, -1.0, 2.0)
    ]
    result = clean_data(make_df(spark, rows))

    directions = {
        row["turbine_id"]: row["wind_direction"]
        for row in result.collect()
    }

    assert directions[1] is None  # 365 is invalid
    assert directions[2] == 180.0  # 180 is valid
    assert directions[3] is None  # -1 is invalid
