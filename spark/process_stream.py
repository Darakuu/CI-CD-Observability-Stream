import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import get_json_object
from pyspark.sql.functions import json_tuple
from pyspark.sql.functions import length
from pyspark.sql.functions import lit
from pyspark.sql.functions import regexp_extract
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
    def non_empty(column_name):
        return when(length(col(column_name)) > 0, col(column_name))

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

    # Jenkins writes compact key=value log lines during the demo pipeline.
    # These fields pull the useful CI/CD facts out of the OpenTelemetry payload
    # so MLlib, Elasticsearch and Kibana do not need to parse the raw JSON blob.
    # Had to write Regex, sadly. :(
    text_fields = [
        ("ci_event_with_status", r"event=([A-Za-z0-9_-]+)[^\"]*status=(?:success|passed|failed)"),
        ("ci_failed_event", r"event=([A-Za-z0-9_-]+)[^\"]*status=failed"),
        ("ci_first_event", r"event=([A-Za-z0-9_-]+)"),
        ("ci_stage_from_log", r"stage=([A-Za-z0-9_-]+)"),
        ("ci_status", r"status=([A-Za-z0-9_-]+)"),
        ("pipeline_status", r"pipeline_status=([A-Za-z0-9_-]+)"),
        ("failure_reason", r"status=failed[^\"]*reason=([A-Za-z0-9_.-]+)"),
        ("failure_detail", r'detail=([^"\s]+)'),
        ("error_code", r"error_code=([A-Za-z0-9_-]+)"),
        ("service_name", r"service=([A-Za-z0-9_.-]+)"),
        ("job_name", r'job_name=([^"\s]+)'),
        ("job_name_from_log_path", r"/jobs/([^/]+)/builds/[0-9]+/log"),
        ("source_branch", r"branch=([A-Za-z0-9_./-]+)"),
        ("service_module", r"module=([A-Za-z0-9_.-]+)"),
        ("target_environment", r"environment=([A-Za-z0-9_.-]+)"),
        ("forced_success", r"forced_success=(true|false)"),
        ("severity_text", r'"severityText":"([A-Z]+)"'),
        ("trace_id", r'"traceId":"([A-Fa-f0-9]+)"'),
        ("span_id", r'"spanId":"([A-Fa-f0-9]+)"'),
        ("span_name", r'"name":"([^"]+)","startTimeUnixNano"'),
        ("http_method", r'"value":\{"stringValue":"([A-Z]+)"\},"key":"http.request.method"'),
        ("http_route", r'"value":\{"stringValue":"([^"]+)"\},"key":"http.route"'),
        ("exception_type", r'"value":\{"stringValue":"([^"]+)"\},"key":"exception.type"'),
        ("exception_message", r'"value":\{"stringValue":"([^"]+)"\},"key":"exception.message"'),
    ]
    number_fields = [
        ("build_number", r"build_number=([0-9]+)"),
        ("random_scenario", r"random_scenario=([0-9]+)"),
        ("disk_free_pct", r"disk_free_pct=([0-9]+)"),
        ("cpu_temp_c", r"cpu_temp_c=([0-9]+)"),
        ("compile_time_ms", r"compile_time_ms=([0-9]+)"),
        ("test_total", r"total=([0-9]+)"),
        ("failing_tests", r"failing_tests=([0-9]+)"),
        ("test_duration_ms", r"duration_ms=([0-9]+)"),
        ("artifact_size_mb", r"artifact_size_mb=([0-9]+)"),
        ("rollout_seconds", r"rollout_seconds=([0-9]+)"),
        ("http_status_code", r'"value":\{"intValue":"?([0-9]+)"?\},"key":"http.response.status_code"'),
        ("build_number_from_log_path", r"/builds/([0-9]+)/log"),
    ]

    enriched_events = normalized_events
    for field_name, pattern in text_fields:
        enriched_events = enriched_events.withColumn(
            field_name,
            regexp_extract(col("raw_event"), pattern, 1),
        )
    for field_name, pattern in number_fields:
        enriched_events = enriched_events.withColumn(
            field_name,
            regexp_extract(col("raw_event"), pattern, 1).cast("int"),
        )

    for field_name, _ in text_fields:
        enriched_events = enriched_events.withColumn(field_name, non_empty(field_name))

    enriched_events = (
        enriched_events.withColumn(
            "ci_event",
            coalesce(
                col("ci_failed_event"),
                col("ci_event_with_status"),
                col("ci_first_event"),
                when(col("exception_type").isNotNull(), lit("jenkins_exception")),
                when(col("http_status_code").isNotNull(), lit("jenkins_http")),
            ),
        )
        .withColumn(
            "ci_stage",
            coalesce(
                col("ci_stage_from_log"),
                col("ci_failed_event"),
                col("ci_event_with_status"),
            ),
        )
        .withColumn("job_name", coalesce(col("job_name"), col("job_name_from_log_path")))
        .withColumn("build_number", coalesce(col("build_number"), col("build_number_from_log_path")))
        .withColumn("forced_success", col("forced_success").cast("boolean"))
    )

    enriched_events = (
        enriched_events.withColumn(
            "is_failure",
            when(col("failure_reason").isNotNull(), lit(True))
            .when(col("exception_type").isNotNull(), lit(True))
            .when(col("http_status_code") >= 500, lit(True))
            .when(col("pipeline_status") == "failure", lit(True))
            .when(col("ci_status") == "failed", lit(True))
            .otherwise(lit(False)),
        )
        .withColumn( # what category of failure was it?
            "failure_category",
            when(col("failure_reason").isin("disk_full", "thermal_throttling"), "infrastructure")
            .when(col("failure_reason") == "scm_timeout", "source_control")
            .when(col("failure_reason") == "dependency_resolution", "build")
            .when(col("failure_reason") == "flaky_test", "test")
            .when(col("failure_reason") == "artifact_checksum_mismatch", "package")
            .when(col("failure_reason") == "rollout_timeout", "deployment")
            .when(col("exception_type").isNotNull(), "jenkins_runtime")
            .when(col("http_status_code") >= 500, "jenkins_http")
            .when(col("failure_reason").isNotNull(), "other")
            .otherwise(lit(None)),
        )
        .withColumn(
            "risk_hint", # how severe was the risk?
            when(col("is_failure"), lit(1.0))
            .when(col("severity_text") == "WARNING", lit(0.6))
            .when(col("http_status_code") >= 400, lit(0.6))
            .when(col("ci_stage").isin("deploy", "package"), lit(0.3))
            .when(col("ci_stage").isNotNull(), lit(0.15))
            .otherwise(lit(0.05)),
        )
    )

    # Kafka sinks expect the outgoing DataFrame to contain key/value columns.
    # The value is a compact JSON document, while the key is stable for the
    # original raw event and can later help with grouping or deduplication.
    return enriched_events.select(
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
                "ci_event",
                "ci_stage",
                "ci_status",
                "pipeline_status",
                "is_failure",
                "failure_category",
                "failure_reason",
                "failure_detail",
                "error_code",
                "risk_hint",
                "service_name",
                "service_module",
                "job_name",
                "build_number",
                "source_branch",
                "target_environment",
                "random_scenario",
                "forced_success",
                "disk_free_pct",
                "cpu_temp_c",
                "compile_time_ms",
                "test_total",
                "failing_tests",
                "test_duration_ms",
                "artifact_size_mb",
                "rollout_seconds",
                "severity_text",
                "trace_id",
                "span_id",
                "span_name",
                "http_method",
                "http_route",
                "http_status_code",
                "exception_type",
                "exception_message",
                "raw_event",
            ),
            {"ignoreNullFields": "false"},
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
