import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import get_json_object
from pyspark.sql.functions import json_tuple
from pyspark.sql.functions import lit
from pyspark.sql.functions import sha2
from pyspark.sql.functions import struct
from pyspark.sql.functions import to_json
from pyspark.sql.functions import when


# These values are supplied by docker-compose in normal runs. Keeping defaults
# here also lets the script run in a local Spark shell with the same topic names.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "cicd.otel.raw")
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "cicd.otel.processed")
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "/tmp/spark-checkpoints/cicd-otel-processed",
)


def build_spark_session() -> SparkSession:
    # SparkSession is the main entry point for DataFrame and streaming work.
    # This job is small, so two shuffle partitions keep local execution lighter.
    return (
        SparkSession.builder.appName("tap-cicd-otel-processing")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def read_raw_events(spark: SparkSession):
    # readStream creates a live DataFrame. Spark does not read anything yet;
    # the stream starts only after writeStream.start() is called later.
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def normalize_events(kafka_events):
    # Spark's Kafka source exposes both the Kafka envelope and the message body.
    # We keep topic, partition, offset, and timestamp so downstream stages can
    # trace a processed event back to its exact raw Kafka position.
    raw_events = kafka_events.select(
        col("topic").alias("source_topic"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        col("timestamp").alias("source_kafka_timestamp"),
        col("value").cast("string").alias("raw_event"),
    )

    # Logstash writes dotted JSON field names such as "otel.signal".
    # json_tuple reads those top-level keys without requiring a full schema yet.
    extracted_fields = raw_events.select(
        "*",
        json_tuple(
            col("raw_event"),
            "otel.signal",
            "event.dataset",
            "ingestion.component",
        ).alias(
            "otel_signal_from_ingestion",
            "event_dataset",
            "ingestion_component",
        ),
    )

    # Logstash already marks the signal type, but this fallback keeps the
    # processor useful if a raw OpenTelemetry JSON message reaches Kafka later.
    normalized_events = (
        extracted_fields.withColumn(
            "otel_signal",
            coalesce(
                col("otel_signal_from_ingestion"),
                when(get_json_object(col("raw_event"), "$.resourceSpans").isNotNull(), "traces")
                .when(get_json_object(col("raw_event"), "$.resourceMetrics").isNotNull(), "metrics")
                .when(get_json_object(col("raw_event"), "$.resourceLogs").isNotNull(), "logs")
                .otherwise("unknown"),
            ),
        )
        .withColumn("processing_component", lit("spark-structured-streaming"))
        .withColumn("processed_at", current_timestamp())
        .withColumn("raw_event_sha256", sha2(col("raw_event"), 256))
    )

    # Kafka sinks expect the outgoing DataFrame to contain key/value columns.
    # The value is a compact JSON document, while the key is stable for the
    # original raw event and can later help with grouping or deduplication.
    return normalized_events.select(
        col("raw_event_sha256").alias("key"),
        to_json(
            struct(
                "processing_component",
                "processed_at",
                "otel_signal",
                "event_dataset",
                "ingestion_component",
                "source_topic",
                "source_partition",
                "source_offset",
                "source_kafka_timestamp",
                "raw_event_sha256",
                "raw_event",
            )
        ).alias("value"),
    )


def write_processed_events(processed_events):
    # The checkpoint stores offsets and progress for this streaming query only.
    # It lets Spark resume without replaying already processed raw events.
    return (
        processed_events.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", PROCESSED_TOPIC)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .outputMode("append")
        .start()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # The pipeline is declared as transformations first, then started at the
    # sink. After start(), Spark keeps running micro-batches until stopped.
    raw_events = read_raw_events(spark)
    processed_events = normalize_events(raw_events)
    query = write_processed_events(processed_events)

    query.awaitTermination()


if __name__ == "__main__":
    main()
