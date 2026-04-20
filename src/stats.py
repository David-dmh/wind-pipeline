import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def compute_stats(df: DataFrame) -> DataFrame:
    """
    Computes daily summary statistics per turbine.
    The 24-hour window is defined as a calendar day (midnight to midnight).
    record_count is included to take note of days with missing sensor readings,
    where a full day should contain 24 readings.
    """
    # Derive calendar date to define the 24-hour aggregation window
    df = df.withColumn("date", F.to_date("timestamp"))

    stats = df.groupBy("turbine_id", "date").agg(
        F.min("power_output").alias("min_power")
        ,F.max("power_output").alias("max_power")
        ,F.avg("power_output").alias("avg_power")
        ,F.count("*").alias("record_count")
    )

    logger.info("Computed daily stats for %d turbine-day combinations", stats.count())

    return stats
