import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_outputs(
    df_clean: DataFrame
    ,stats: DataFrame
    ,anomalies: DataFrame
    ,base_path: str = "output/"
) -> None:
    logger.info("Writing cleaned data to %scleaned", base_path)
    df_clean.write.mode("overwrite").parquet(f"{base_path}/cleaned")

    logger.info("Writing summary statistics to %sstats", base_path)
    stats.write.mode("overwrite").parquet(f"{base_path}/stats")

    logger.info("Writing anomalies to %sanomalies", base_path)
    anomalies.write.mode("overwrite").parquet(f"{base_path}/anomalies")
