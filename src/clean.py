import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def clean_data(df: DataFrame) -> DataFrame:
    """
    Cleans raw turbine data in the following order:
    1. Deduplicate on primary key (turbine_id, timestamp)
    2. Treat invalid wind directions as NULL
    3. Drop rows where primary key fields are NULL
    4. Impute missing numeric values with turbine-median
    5. Remove sensor error readings
    """

    # 1 - Dedupe on primary key - must come first to avoid skewing median imputation
    before = df.count()
    df = df.dropDuplicates(["turbine_id", "timestamp"])
    logger.info("Dropped %d duplicate rows", before - df.count())

    # 2. NULL wind_direction values outside valid compass range 0-360
    df = df.withColumn(
        "wind_direction"
        ,F.when(
            (F.col("wind_direction") < 0) 
            | (F.col("wind_direction") > 360)
            ,None
        ).otherwise(F.col("wind_direction"))
    )

    # 3 - Drop rows where primary key fields are NULL
    # These rows cannot be attributed to a turbine or placed on the timeline
    before = df.count()
    df = df.dropna(subset=["turbine_id", "timestamp"])
    logger.info("Dropped %d rows with NULL turbine_id or timestamp", before - df.count())

    # 4.1 - Impute missing power_output with per-turbine median
    null_count = df.filter(F.col("power_output").isNull()).count()
    logger.info("Imputing %d NULL power_output values with per-turbine median", null_count)

    # Median is preferred over mean as it is robust to the extreme sensor error values 
    # such as negative readings that are removed later
    median_df = df.groupBy("turbine_id").agg(
        F.percentile_approx(
            "power_output"
            ,0.5
        ).alias("median_power")
    )

    # Add median_power column for use in imputation
    df = df.join(
        median_df
        ,on="turbine_id"
        ,how="left"
    )

    df = df.withColumn(
        "power_output"
        ,F.when(
            F.col("power_output").isNull()
            ,F.col("median_power")
        )
        .otherwise(F.col("power_output"))
    ).drop("median_power")

    # 4.2 - Impute missing wind_speed with per-turbine median
    null_count = df.filter(F.col("wind_speed").isNull()).count()
    logger.info("Imputing %d null wind_speed values with per-turbine median", null_count)

    wind_median_df = df.groupBy("turbine_id").agg(
            F.percentile_approx(
                "wind_speed"
                ,0.5
            ).alias("median_wind_speed")
        )

    df = df.join(
        wind_median_df
        ,on="turbine_id"
        ,how="left"
    )
    
    df = df.withColumn(
        "wind_speed",
        F.when(
            F.col("wind_speed").isNull()
            ,F.col("median_wind_speed")
        )
        .otherwise(F.col("wind_speed"))
    ).drop("median_wind_speed")

    # 5 - Remove sensor error values (not statistical anomalies)
    before = df.count()
    df = df.filter(
        (F.col("power_output") >= 0)
        & (F.col("power_output") <= 20)
        & (F.col("wind_speed") >= 0)
    )
    logger.info("Dropped %d rows with sensor error values", before - df.count())

    return df
