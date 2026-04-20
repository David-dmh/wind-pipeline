import logging
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T

logger = logging.getLogger(__name__)


def ingest_data(spark: SparkSession, path: str = "data/") -> DataFrame:
    """
    Reads all CSV files from the given path into a single DataFrame using 
    a defined schema. The schema enforces types at read time rather than 
    relying on Spark's inference, ensuring type safety.
    """
    logger.info("Ingesting CSV data from path: %s", path)

    schema = T.StructType([
        T.StructField("timestamp", T.TimestampType(), nullable=False)
        ,T.StructField("turbine_id", T.IntegerType(), nullable=False)
        ,T.StructField("wind_speed", T.DoubleType(), nullable=True)
        ,T.StructField("wind_direction", T.DoubleType(), nullable=True)
        ,T.StructField("power_output", T.DoubleType(), nullable=True)
    ])

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )

    return df