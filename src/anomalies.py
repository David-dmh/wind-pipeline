import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def detect_anomalies(df: DataFrame) -> DataFrame:
    """
    Flags readings where power output deviates more than 2 standard deviations
    from the turbine's own daily mean. Thresholds are computed per turbine per
    calendar day so that each turbine is compared against its own expected
    behaviour rather than a global fleet average.
    """
    # Derive date to match the same 24-hour window used in stats
    df = df.withColumn("date", F.to_date("timestamp"))

    # Compute per-turbine daily mean and std - used to define the anomaly threshold
    daily_stats = df.groupBy("turbine_id", "date").agg(
        F.avg("power_output").alias("avg_power")
        ,F.stddev("power_output").alias("std_power")
    )

    df = df.join(
        daily_stats
        ,on=["turbine_id", "date"]
        ,how="left"
    )

    # Flag readings outside mean ± 2 standard deviations
    df = df.withColumn(
        "is_anomaly",
        (
            (F.col("power_output") > F.col("avg_power") + 2 * F.col("std_power"))
            | (F.col("power_output") < F.col("avg_power") - 2 * F.col("std_power"))
        )
    )

    anomalies = df.filter(F.col("is_anomaly") == True)

    return anomalies
