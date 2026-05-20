"""DataFrame transformations for the CI/CD telemetry stream.

The classes follow the shape of the pipeline: unwrap the Kafka envelope,
expand OpenTelemetry spans, enrich Jenkins CI fields, and finally project the
compact event that the next pipeline stage will consume.
"""

from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import explode_outer
from pyspark.sql.functions import from_json
from pyspark.sql.functions import get_json_object
from pyspark.sql.functions import greatest
from pyspark.sql.functions import json_tuple
from pyspark.sql.functions import least
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
    """Runs the full DataFrame transformation from raw Kafka rows to processed events."""

    def __init__(self):
        """Wire the smaller transformation steps in the order the stream uses them."""

        self.kafka_normalizer = KafkaEnvelopeNormalizer()
        self.span_extractor = OpenTelemetrySpanExtractor()
        self.ci_enricher = JenkinsCiEventEnricher()
        self.projector = ProcessedEventProjector()

    def transform(self, kafka_events):
        """Normalize, enrich, and project raw Kafka events into the processed schema."""

        # Keep the high-level flow readable; the heavier Spark expressions live
        # in the smaller components below.
        normalized_events = self.kafka_normalizer.normalize(kafka_events)
        expanded_events = self.span_extractor.expand(normalized_events)
        enriched_events = self.ci_enricher.enrich(expanded_events)
        return self.projector.project(enriched_events)


class KafkaEnvelopeNormalizer:
    """Turns Spark's Kafka rows into telemetry rows with stable metadata columns."""

    def normalize(self, kafka_events):
        """Keep the Kafka envelope and expose the raw message as a string column."""

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
                "@timestamp",
                "otel.signal",
                "event.dataset",
                "ingestion.component",
            ).alias(
                "observed_at",
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
        """Infer the OpenTelemetry signal when Logstash did not already label it."""

        return coalesce(
            col("otel_signal_from_ingestion"),
            when(get_json_object(col("raw_event"), "$.resourceSpans").isNotNull(), "traces")
            .when(get_json_object(col("raw_event"), "$.resourceMetrics").isNotNull(), "metrics")
            .when(get_json_object(col("raw_event"), "$.resourceLogs").isNotNull(), "logs")
            .otherwise("unknown"),
        )


class OpenTelemetrySpanExtractor:
    """Expands trace payloads enough to reuse span details in CI/CD events."""

    def expand(self, normalized_events):
        """Parse OpenTelemetry traces and attach selected span fields to each row."""

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
    """Builds CI/CD fields from Jenkins console lines and OpenTelemetry spans."""

    def __init__(self, non_empty=None):
        """Allow tests to inject the empty-string handling while production uses Spark."""

        self.non_empty = non_empty

    def enrich(self, events):
        """Filter relevant Jenkins events and derive the normalized CI fields."""

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
            .filter(col("is_console_event") | col("is_otel_build_span")) # This line REDUCES the amount of messages in .processed and further topics!
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
        return self._derive_ci_fields(events).filter(self._kept_observability_event())

    def _extract_regex_fields(self, events):
        """Apply the configured text and numeric regex rules to the CI log text."""

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
        """Convert missing regex matches from empty strings to null-like values."""

        for rule in TEXT_FIELDS:
            events = events.withColumn(rule.name, self._non_empty(rule.name))

        return events.withColumn(
            "job_name_from_log_path",
            self._non_empty("job_name_from_log_path"),
        )

    def _derive_ci_fields(self, events):
        """Combine parsed log fields and span fields into the final CI columns."""

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

    def _kept_observability_event(self):
        """Keep only result-level events useful to ML and dashboards."""

        return (
            (col("ci_status").isNotNull() | col("pipeline_status").isNotNull())
            & ~coalesce(col("ci_event").isin("build_report", "build_summary", "stage_start", "simulation"), lit(False))
        )

    def _non_empty(self, column_name):
        """Return the column only when the extracted text contains a value."""

        if self.non_empty is not None:
            return self.non_empty(column_name)

        return when(length(col(column_name)) > 0, col(column_name))


class ProcessedEventProjector:
    """Selects the final event shape that is written to the processed Kafka topic."""

    def project(self, events):
        """Create Kafka key/value columns from the enriched telemetry rows."""

        # Kafka expects key/value columns. The value is the clean JSON document
        # that later ML, Elasticsearch, and Kibana steps can consume directly.
        # The raw payload stays out of the lower stages; the hash keeps a trace
        # back to the original record without carrying noisy log text forward.
        events = (
            events.withColumn("source_system", lit("jenkins"))
            .withColumn("event_source", self._event_source())
            .withColumn("stage_order", self._stage_order())
            .withColumn("feature_scm_pressure", self._feature_scm_pressure())
            .withColumn("feature_agent_pressure", self._feature_agent_pressure())
            .withColumn("feature_build_pressure", self._feature_build_pressure())
            .withColumn("feature_test_pressure", self._feature_test_pressure())
            .withColumn("feature_artifact_pressure", self._feature_artifact_pressure())
            .withColumn("feature_deploy_pressure", self._feature_deploy_pressure())
            .withColumn("feature_overall_pressure", self._feature_overall_pressure())
            .withColumn("is_failure", self._is_failure())
            .withColumn("failure_category", self._failure_category())
            .withColumn("signal_domain", self._signal_domain())
            .withColumn("signal_name", self._signal_name())
            .withColumn("signal_value", self._signal_value())
            .withColumn("signal_unit", self._signal_unit())
            .withColumn("severity_level", self._severity_level())
            .withColumn("alert_candidate", self._alert_candidate())
            .withColumn("alert_type", self._alert_type())
            .withColumn("alert_reason", self._alert_reason())
            .withColumn("event_kind", self._event_kind())
            .withColumn("event_summary", self._event_summary())
            .withColumn("risk_hint", self._risk_hint())
        )

        return events.select(
            col("raw_event_sha256").alias("key"),
            to_json(
                struct(*OUTPUT_FIELDS),
                {"ignoreNullFields": "true"},
            ).alias("value"),
        )

    def _bounded(self, expression):
        """Clamp a numeric expression to the 0..1 range used by feature columns."""

        return least(greatest(expression, lit(0.0)), lit(1.0))

    def _event_source(self):
        return (
            when(col("is_console_event"), lit("jenkins_console_log"))
            .when(col("is_otel_build_span"), lit("jenkins_otel_span"))
            .otherwise(lit("unknown"))
        )

    def _stage_order(self):
        return (
            when(col("ci_stage") == "checkout", lit(1))
            .when(col("ci_stage") == "preflight", lit(2))
            .when(col("ci_stage") == "build", lit(3))
            .when(col("ci_stage") == "test", lit(4))
            .when(col("ci_stage") == "package", lit(5))
            .when(col("ci_stage") == "deploy", lit(6))
            .when(col("ci_stage") == "pipeline", lit(7))
        )

    def _feature_scm_pressure(self):
        latency_pressure = self._bounded(
            (coalesce(col("scm_latency_ms"), lit(0)).cast("double") - lit(2500.0)) / lit(2700.0)
        )
        retry_pressure = self._bounded(coalesce(col("retry_count"), lit(0)).cast("double") / lit(3.0))
        return greatest(latency_pressure, retry_pressure)

    def _feature_agent_pressure(self):
        disk_pressure = self._bounded((lit(25.0) - coalesce(col("disk_free_pct"), lit(100))) / lit(25.0))
        heat_pressure = self._bounded((coalesce(col("cpu_temp_c"), lit(0)).cast("double") - lit(70.0)) / lit(25.0))
        return greatest(disk_pressure, heat_pressure)

    def _feature_build_pressure(self):
        compile_pressure = self._bounded(
            coalesce(col("compile_time_ms"), lit(0)).cast("double") / lit(12000.0)
        )
        cache_pressure = when(col("dependency_cache") == "miss", lit(1.0)).otherwise(lit(0.0))
        return greatest(compile_pressure, cache_pressure)

    def _feature_test_pressure(self):
        failure_ratio = (
            when(
                coalesce(col("test_total"), lit(0)) > 0,
                coalesce(col("failing_tests"), lit(0)).cast("double") / col("test_total").cast("double"),
            )
            .when(coalesce(col("failing_tests"), lit(0)) > 0, lit(1.0))
            .otherwise(lit(0.0))
        )
        duration_pressure = self._bounded(
            coalesce(col("test_duration_ms"), lit(0)).cast("double") / lit(12000.0)
        )
        return greatest(failure_ratio, duration_pressure)

    def _feature_artifact_pressure(self):
        return self._bounded(
            (coalesce(col("artifact_size_mb"), lit(0)).cast("double") - lit(18.0)) / lit(6.0)
        )

    def _feature_deploy_pressure(self):
        duration_pressure = self._bounded(
            coalesce(col("rollout_seconds"), lit(0)).cast("double") / lit(120.0)
        )
        replica_gap = when(
            coalesce(col("replicas_expected"), lit(0)) > 0,
            self._bounded(
                (
                    col("replicas_expected").cast("double")
                    - coalesce(col("replicas_ready"), lit(0)).cast("double")
                )
                / col("replicas_expected").cast("double")
            ),
        ).otherwise(lit(0.0))
        return greatest(duration_pressure, replica_gap)

    def _feature_overall_pressure(self):
        return greatest(
            col("feature_scm_pressure"),
            col("feature_agent_pressure"),
            col("feature_build_pressure"),
            col("feature_test_pressure"),
            col("feature_artifact_pressure"),
            col("feature_deploy_pressure"),
        )

    def _is_failure(self):
        """Build the boolean expression that marks failed or risky CI/CD events."""

        return (
            when(col("failure_reason").isNotNull(), lit(True))
            .when(col("exception_type").isNotNull(), lit(True))
            .when(col("otel_status_code") >= 2, lit(True))
            .when(col("pipeline_status") == "failure", lit(True))
            .when(col("ci_status") == "failed", lit(True))
            .otherwise(lit(False))
        )

    def _failure_category(self):
        """Classify known failure reasons into broad dashboard-friendly groups."""

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
            .when(col("failure_reason").isNotNull(), "other")
            .otherwise(lit(None))
        )

    def _signal_domain(self):
        return (
            when((col("ci_stage") == "checkout") | (col("failure_reason") == "scm_timeout"), lit("source_control"))
            .when(
                (col("ci_stage") == "preflight")
                | col("failure_reason").isin("disk_full", "thermal_throttling"),
                lit("agent_health"),
            )
            .when((col("ci_stage") == "build") | (col("failure_reason") == "dependency_resolution"), lit("build"))
            .when((col("ci_stage") == "test") | (col("failure_reason") == "flaky_test"), lit("test_quality"))
            .when(
                (col("ci_stage") == "package") | (col("failure_reason") == "artifact_checksum_mismatch"),
                lit("artifact"),
            )
            .when((col("ci_stage") == "deploy") | (col("failure_reason") == "rollout_timeout"), lit("deployment"))
            .when(col("ci_stage") == "pipeline", lit("pipeline"))
            .when(col("exception_type").isNotNull(), lit("jenkins_runtime"))
            .otherwise(lit("unknown"))
        )

    def _signal_name(self):
        disk_pressure = self._bounded((lit(25.0) - coalesce(col("disk_free_pct"), lit(100))) / lit(25.0))
        heat_pressure = self._bounded((coalesce(col("cpu_temp_c"), lit(0)).cast("double") - lit(70.0)) / lit(25.0))

        return (
            when(col("signal_domain") == "source_control", lit("scm_latency"))
            .when((col("failure_reason") == "disk_full"), lit("disk_free_pct"))
            .when((col("failure_reason") == "thermal_throttling"), lit("cpu_temp_c"))
            .when((col("signal_domain") == "agent_health") & (disk_pressure >= heat_pressure), lit("disk_free_pct"))
            .when((col("signal_domain") == "agent_health") & col("cpu_temp_c").isNotNull(), lit("cpu_temp_c"))
            .when((col("signal_domain") == "build") & (col("dependency_cache") == "miss"), lit("dependency_cache"))
            .when(col("signal_domain") == "build", lit("compile_time_ms"))
            .when((col("signal_domain") == "test_quality") & (coalesce(col("failing_tests"), lit(0)) > 0), lit("test_failure_ratio"))
            .when(col("signal_domain") == "test_quality", lit("test_duration_ms"))
            .when(col("signal_domain") == "artifact", lit("artifact_size_mb"))
            .when((col("signal_domain") == "deployment") & (col("replicas_ready") < col("replicas_expected")), lit("replica_readiness_gap"))
            .when(col("signal_domain") == "deployment", lit("rollout_seconds"))
            .when(col("signal_domain") == "pipeline", lit("pipeline_status"))
            .otherwise(lit("unknown"))
        )

    def _signal_value(self):
        test_ratio = when(
            coalesce(col("test_total"), lit(0)) > 0,
            coalesce(col("failing_tests"), lit(0)).cast("double") / col("test_total").cast("double"),
        )
        replica_gap = when(
            coalesce(col("replicas_expected"), lit(0)) > 0,
            col("replicas_expected").cast("double") - coalesce(col("replicas_ready"), lit(0)).cast("double"),
        )

        return (
            when(col("signal_name") == "scm_latency", col("scm_latency_ms").cast("double"))
            .when(col("signal_name") == "disk_free_pct", col("disk_free_pct").cast("double"))
            .when(col("signal_name") == "cpu_temp_c", col("cpu_temp_c").cast("double"))
            .when(col("signal_name") == "dependency_cache", when(col("dependency_cache") == "miss", lit(1.0)).otherwise(lit(0.0)))
            .when(col("signal_name") == "compile_time_ms", col("compile_time_ms").cast("double"))
            .when(col("signal_name") == "test_failure_ratio", test_ratio)
            .when(col("signal_name") == "test_duration_ms", col("test_duration_ms").cast("double"))
            .when(col("signal_name") == "artifact_size_mb", col("artifact_size_mb").cast("double"))
            .when(col("signal_name") == "replica_readiness_gap", replica_gap)
            .when(col("signal_name") == "rollout_seconds", col("rollout_seconds").cast("double"))
        )

    def _signal_unit(self):
        return (
            when(col("signal_name").isin("scm_latency", "compile_time_ms", "test_duration_ms"), lit("ms"))
            .when(col("signal_name") == "disk_free_pct", lit("percent"))
            .when(col("signal_name") == "cpu_temp_c", lit("celsius"))
            .when(col("signal_name") == "test_failure_ratio", lit("ratio"))
            .when(col("signal_name") == "artifact_size_mb", lit("mb"))
            .when(col("signal_name") == "rollout_seconds", lit("seconds"))
            .when(col("signal_name").isin("replica_readiness_gap", "dependency_cache"), lit("count"))
        )

    def _severity_level(self):
        return (
            when(col("is_failure"), lit("critical"))
            .when(col("feature_overall_pressure") >= lit(0.85), lit("critical"))
            .when((col("feature_overall_pressure") >= lit(0.65)) | (col("severity_text") == "WARNING"), lit("warning"))
            .otherwise(lit("normal"))
        )

    def _alert_candidate(self):
        return (
            (~coalesce(col("is_failure"), lit(False)))
            & (col("feature_overall_pressure") >= lit(0.65))
            & col("ci_stage").isNotNull()
        )

    def _alert_type(self):
        return (
            when(col("is_failure"), concat_ws("_", lit("failure"), col("failure_category")))
            .when(col("alert_candidate"), concat_ws("_", lit("predictive"), col("signal_domain"), col("signal_name")))
        )

    def _alert_reason(self):
        return (
            when(col("is_failure"), coalesce(col("failure_reason"), lit("pipeline_failure")))
            .when(col("alert_candidate"), concat_ws("_", col("signal_name"), lit("near_limit")))
        )

    def _event_kind(self):
        return (
            when(col("is_failure"), lit("failure"))
            .when(col("alert_candidate"), lit("predictive_signal"))
            .when(col("ci_event") == "simulation", lit("simulation"))
            .when(col("ci_event") == "stage_start", lit("stage_start"))
            .when(col("pipeline_status").isNotNull(), lit("pipeline_result"))
            .when(col("ci_status").isin("success", "passed"), lit("stage_result"))
            .otherwise(lit("telemetry"))
        )

    def _event_summary(self):
        return concat_ws(
            " ",
            col("event_kind"),
            col("signal_domain"),
            col("signal_name"),
            col("severity_level"),
        )

    def _risk_hint(self):
        """Assign a simple score that later ML and dashboards can treat as a hint."""

        return (
            when(col("is_failure"), lit(1.0))
            .when(col("feature_overall_pressure") >= lit(0.85), lit(0.9))
            .when(col("feature_overall_pressure") >= lit(0.65), lit(0.7))
            .when(col("severity_text") == "WARNING", lit(0.6))
            .when(col("ci_stage").isin("deploy", "package"), lit(0.3))
            .when(col("ci_stage").isNotNull(), lit(0.15))
            .otherwise(lit(0.05))
        )
