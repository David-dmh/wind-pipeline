import pytest
from pyspark.sql import SparkSession

# this function is a fixture - can be injected into any test function that lists spark as a param
@pytest.fixture(scope="session")
def spark():
    """
    Shared SparkSession for all tests. scope="session" means Spark starts once
    for the entire test run rather than per test, avoiding JVM startup overhead.
    """
    return (
        SparkSession.builder
        .master("local[2]") # run locally with 2 threads which is fine for testing
        .appName("wind-pipeline-tests")
        .getOrCreate()
    )
