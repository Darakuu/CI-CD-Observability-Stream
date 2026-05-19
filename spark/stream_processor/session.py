"""Spark session setup for the local CI/CD observability demo."""

from pyspark.sql import SparkSession


APP_NAME = "tap-cicd-otel-processing"
SHUFFLE_PARTITIONS = "2"


def build_spark_session() -> SparkSession:
    # The job is small, so two shuffle partitions keep the Structured Streaming batches lighter.
    return (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
