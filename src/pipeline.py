import logging
from pyspark.sql import SparkSession

from src.ingest import ingest_data
from src.clean import clean_data
from src.stats import compute_stats
from src.anomalies import detect_anomalies
from src.write import write_outputs

logger = logging.getLogger(__name__)


def run_pipeline(spark: SparkSession) -> None:
    logger.info("Starting pipeline...")

    # Ingestion
    df = ingest_data(spark)
    logger.info("Ingested %d rows", df.count())

    # Clean
    df_clean = clean_data(df)

    # Prevent recomputation when passed to stats and anomalies tasks 
    # materialise cache with count here before passing into to stats, anomalies and writes
    df_clean.cache() 
    count_df_clean = df_clean.count()

    logger.info("After cleaning: %d rows", count_df_clean)

    # Stats
    stats = compute_stats(df_clean)
    logger.info("Statistics computed")

    # Anomalies
    anomalies = detect_anomalies(df_clean)
    logger.info("Anomalies analysed")

    # Write out
    write_outputs(df_clean, stats, anomalies)
    logger.info("Pipeline complete")
