import logging

from src.spark import setup_spark
from src.pipeline import run_pipeline


logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    spark = setup_spark()
    run_pipeline(spark)
