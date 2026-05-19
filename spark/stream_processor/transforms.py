"""DataFrame transformations for the CI/CD telemetry stream.

The classes follow the shape of the pipeline: unwrap the Kafka envelope,
expand OpenTelemetry spans, enrich Jenkins CI fields, and finally project the
compact event that the next pipeline stage will consume.
"""

from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import explode_outer
from pyspark.sql.functions import from_json
from pyspark.sql.functions import get_json_object
from pyspark.sql.functions import json_tuple
from pyspark.sql.functions import length
from pyspark.sql.functions import lit
from pyspark.sql.functions import lower
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import sha2
from pyspark.sql.functions import struct
from pyspark.sql.functions import to_json
from pyspark.sql.functions import when

from stream_processor.rules import KNOWN_STAGE_EVENTS
from stream_processor.rules import NUMBER_FIELDS
from stream_processor.rules import OUTPUT_FIELDS
from stream_processor.rules import TEXT_FIELDS
from stream_processor.schemas import OTEL_TRACE_SCHEMA
from stream_processor.span_attributes import span_attr_int
from stream_processor.span_attributes import span_attr_string
from stream_processor.status import normalize_ci_status
from stream_processor.status import normalize_pipeline_status


class CiCdTelemetryTransformer:
    def __init__(self):
        self.kafka_normalizer = KafkaEnvelopeNormalizer()
        self.span_extractor = OpenTelemetrySpanExtractor()
        self.ci_enricher = JenkinsCiEventEnricher()
        self.projector = ProcessedEventProjector()

    def transform(self, kafka_events):
        # Keep the high-level flow readable; the heavier Spark expressions live
        # in the smaller components below.
        normalized_events = self.kafka_normalizer.normalize(kafka_events)
        expanded_events = self.span_extractor.expand(normalized_events)
        enriched_events = self.ci_enricher.enrich(expanded_events)
        return self.projector.project(enriched_events)


class KafkaEnvelopeNormalizer:
    def normalize(self, kafka_events):
        # Spark exposes Kafka metadata alongside the message body. Keeping that
        # envelope lets later dashboards trace a processed event back to Kafka.
        raw_events = kafka_events.select(
            col("topic").alias("source_topic"),
            col("partition").alias("source_partition"),
            col("offset").alias("source_offset"),
            col("timestamp").alias("source_kafka_timestamp"),
            col("value").cast("string").alias("raw_event"),
        )

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

        return (
            extracted_fields.withColumn("otel_signal", self._otel_signal())
            .withColumn("processing_component", lit("spark-structured-streaming"))
            .withColumn("processed_at", current_timestamp())
            .withColumn("raw_event_sha256", sha2(col("raw_event"), 256))
            .withColumn("message", get_json_object(col("raw_event"), "$.message"))
            .withColumn("log_path", get_json_object(col("raw_event"), "$.path"))
        )

    def _otel_signal(self):
        return coalesce(
            col("otel_signal_from_ingestion"),
            when(get_json_object(col("raw_event"), "$.resourceSpans").isNotNull(), "traces")
            .when(get_json_object(col("raw_event"), "$.resourceMetrics").isNotNull(), "metrics")
            .when(get_json_object(col("raw_event"), "$.resourceLogs").isNotNull(), "logs")
            .otherwise("unknown"),
        )


class OpenTelemetrySpanExtractor:
    def expand(self, normalized_events):
        return (
            normalized_events.withColumn(
                "otel_trace",
                from_json(
                    col("raw_event"),
                    OTEL_TRACE_SCHEMA,
                    {"primitivesAsString": "true"},
                ),
            )
            .withColumn("resource_span", explode_outer(col("otel_trace.resourceSpans")))
            .withColumn("scope_span", explode_outer(col("resource_span.scopeSpans")))
            .withColumn("span", explode_outer(col("scope_span.spans")))
            .withColumn("otel_span_name", col("span.name"))
            .withColumn("otel_trace_id", col("span.traceId"))
            .withColumn("otel_span_id", col("span.spanId"))
            .withColumn("otel_status_code", col("span.status.code").cast("int"))
            .withColumn("otel_status_message", col("span.status.message"))
            .withColumn(
                "span_pipeline_status",
                normalize_pipeline_status(span_attr_string("ci.pipeline.run.result")),
            )
            .withColumn(
                "span_job_name",
                coalesce(
                    span_attr_string("ci.pipeline.name"),
                    span_attr_string("ci.pipeline.id"),
                ),
            )
            .withColumn("span_build_number", span_attr_int("ci.pipeline.run.number"))
            .withColumn("span_exception_type", span_attr_string("exception.type"))
            .withColumn("span_exception_message", span_attr_string("exception.message"))
        )


class JenkinsCiEventEnricher:
    def __init__(self, non_empty=None):
        self.non_empty = non_empty

    def enrich(self, events):
        # Console log lines and OpenTelemetry build spans describe the same
        # pipeline from different angles, so both feed the normalized CI fields.
        events = (
            events.withColumn(
                "is_console_event",
                col("event_dataset") == "jenkins.build.console",
            )
            .withColumn(
                "is_otel_build_span",
                col("span_build_number").isNotNull()
                & (
                    col("otel_span_name").startswith("BUILD ")
                    | col("span_pipeline_status").isNotNull()
                ),
            )
            .filter(col("is_console_event") | col("is_otel_build_span"))
            .withColumn(
                "ci_text",
                when(col("is_console_event"), coalesce(col("message"), col("raw_event"))).otherwise(lit("")),
            )
            .withColumn(
                "job_name_from_log_path",
                regexp_extract(col("log_path"), r"/jobs/([^/]+)/builds/[0-9]+/log", 1),
            )
            .withColumn(
                "build_number_from_log_path",
                regexp_extract(col("log_path"), r"/builds/([0-9]+)/log", 1).cast("int"),
            )
        )

        events = self._extract_regex_fields(events)
        events = self._empty_strings_to_null(events)
        return self._derive_ci_fields(events)

    def _extract_regex_fields(self, events):
        for rule in TEXT_FIELDS:
            events = events.withColumn(
                rule.name,
                regexp_extract(col("ci_text"), rule.pattern, 1),
            )

        for rule in NUMBER_FIELDS:
            events = events.withColumn(
                rule.name,
                regexp_extract(col("ci_text"), rule.pattern, 1).cast("int"),
            )

        return events

    def _empty_strings_to_null(self, events):
        for rule in TEXT_FIELDS:
            events = events.withColumn(rule.name, self._non_empty(rule.name))

        return events.withColumn(
            "job_name_from_log_path",
            self._non_empty("job_name_from_log_path"),
        )

    def _derive_ci_fields(self, events):
        return (
            events.withColumn(
                "ci_event",
                coalesce(
                    col("ci_failed_event"),
                    col("ci_event_with_status"),
                    col("ci_first_event"),
                    when(col("is_otel_build_span"), lit("build_report")),
                    when(col("pipeline_status").isNotNull(), lit("pipeline_result")),
                    when(col("build_url").isNotNull(), lit("build_summary")),
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
                    when(col("ci_first_event").isin(*KNOWN_STAGE_EVENTS), col("ci_first_event")),
                    when(col("is_otel_build_span"), lit("pipeline")),
                    when(col("pipeline_status").isNotNull(), lit("pipeline")),
                    when(col("build_url").isNotNull(), lit("pipeline")),
                ),
            )
            .withColumn(
                "job_name",
                coalesce(col("job_name"), col("job_name_from_log_path"), col("span_job_name")),
            )
            .withColumn(
                "build_number",
                coalesce(
                    col("build_number"),
                    col("build_number_from_log_path"),
                    col("span_build_number"),
                ),
            )
            .withColumn("forced_success", col("forced_success").cast("boolean"))
            .withColumn(
                "pipeline_status",
                coalesce(
                    normalize_pipeline_status(col("pipeline_status")),
                    col("span_pipeline_status"),
                ),
            )
            .withColumn("trace_id", coalesce(col("trace_id"), col("otel_trace_id")))
            .withColumn("span_id", coalesce(col("span_id"), col("otel_span_id")))
            .withColumn("span_name", coalesce(col("span_name"), col("otel_span_name")))
            .withColumn("exception_type", coalesce(col("exception_type"), col("span_exception_type")))
            .withColumn(
                "exception_message",
                coalesce(col("exception_message"), col("span_exception_message")),
            )
            .withColumn("ci_status", normalize_ci_status(col("ci_status")))
            .withColumn(
                "ci_status",
                coalesce(
                    col("ci_status"),
                    when(col("pipeline_status") == "failure", lit("failed")).when(
                        col("pipeline_status") == "success",
                        lit("success"),
                    ),
                ),
            )
            .withColumn(
                "failure_reason",
                coalesce(
                    col("failure_reason"),
                    when(
                        col("is_otel_build_span") & (col("pipeline_status") == "failure"),
                        lower(
                            regexp_replace(
                                coalesce(col("exception_type"), lit("pipeline_failure")),
                                r"[^A-Za-z0-9_.-]+",
                                "_",
                            )
                        ),
                    ),
                ),
            )
        )

    def _non_empty(self, column_name):
        if self.non_empty is not None:
            return self.non_empty(column_name)

        return when(length(col(column_name)) > 0, col(column_name))


class ProcessedEventProjector:
    def project(self, events):
        # Kafka expects key/value columns. The value is the clean JSON document
        # that later ML, Elasticsearch, and Kibana steps can consume directly.
        events = (
            events.withColumn("is_failure", self._is_failure())
            .withColumn("failure_category", self._failure_category())
            .withColumn("risk_hint", self._risk_hint())
        )

        return events.select(
            col("raw_event_sha256").alias("key"),
            to_json(
                struct(*OUTPUT_FIELDS),
                {"ignoreNullFields": "true"},
            ).alias("value"),
        )

    def _is_failure(self):
        return (
            when(col("failure_reason").isNotNull(), lit(True))
            .when(col("exception_type").isNotNull(), lit(True))
            .when(col("http_status_code") >= 500, lit(True))
            .when(col("otel_status_code") >= 2, lit(True))
            .when(col("pipeline_status") == "failure", lit(True))
            .when(col("ci_status") == "failed", lit(True))
            .otherwise(lit(False))
        )

    def _failure_category(self):
        return (
            when(col("failure_reason").isin("disk_full", "thermal_throttling"), "infrastructure")
            .when(col("failure_reason") == "scm_timeout", "source_control")
            .when(col("failure_reason") == "dependency_resolution", "build")
            .when(col("failure_reason") == "flaky_test", "test")
            .when(col("failure_reason") == "artifact_checksum_mismatch", "package")
            .when(col("failure_reason") == "rollout_timeout", "deployment")
            .when(col("failure_reason") == "pipeline_failure", "pipeline")
            .when(col("pipeline_status") == "failure", "pipeline")
            .when(col("exception_type").isNotNull(), "jenkins_runtime")
            .when(col("http_status_code") >= 500, "jenkins_http")
            .when(col("failure_reason").isNotNull(), "other")
            .otherwise(lit(None))
        )

    def _risk_hint(self):
        return (
            when(col("is_failure"), lit(1.0))
            .when(col("severity_text") == "WARNING", lit(0.6))
            .when(col("http_status_code") >= 400, lit(0.6))
            .when(col("ci_stage").isin("deploy", "package"), lit(0.3))
            .when(col("ci_stage").isNotNull(), lit(0.15))
            .otherwise(lit(0.05))
        )
