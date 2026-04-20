import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def setup_spark(app_name: str = "wind-pipeline") -> SparkSession:
    logger.info("Initialising SparkSession: %s", app_name)
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
