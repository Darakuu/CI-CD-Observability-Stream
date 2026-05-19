"""Spark session setup for the MLlib scoring stage."""

from pyspark.sql import SparkSession


APP_NAME = "tap-cicd-otel-mllib"
SHUFFLE_PARTITIONS = "2"


def build_spark_session() -> SparkSession:
    """Create the Spark session used by the MLlib streaming scorer."""

    return (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
