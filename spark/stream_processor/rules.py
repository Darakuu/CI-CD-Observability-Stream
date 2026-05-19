"""Parsing and output rules used by the telemetry transformer.

Most Jenkins demo events arrive as compact key=value text inside a larger
OpenTelemetry payload. Keeping the regexes and output fields here makes the
transformation code easier to read and keeps field changes in one place.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RegexField:
    """Names one field that can be extracted from Jenkins text with a regex."""

    name: str
    pattern: str


TEXT_FIELDS = (
    RegexField("ci_event_with_status", r'event=([A-Za-z0-9_-]+)[^"]*\s+status=(?:success|passed|failed)'),
    RegexField("ci_failed_event", r'event=([A-Za-z0-9_-]+)[^"]*\s+status=failed'),
    RegexField("ci_first_event", r"event=([A-Za-z0-9_-]+)"),
    RegexField("ci_stage_from_log", r"stage=([A-Za-z0-9_-]+)"),
    RegexField("ci_status", r"(?:^|\s+)status=([A-Za-z0-9_-]+)"),
    RegexField("pipeline_status", r"pipeline_status=([A-Za-z0-9_-]+)"),
    RegexField("failure_reason", r'\s+status=failed[^"]*\s+reason=([A-Za-z0-9_.-]+)'),
    RegexField("failure_detail", r'detail=([^\s"]+)'),
    RegexField("error_code", r"error_code=([A-Za-z0-9_-]+)"),
    RegexField("service_name", r"service=([A-Za-z0-9_.-]+)"),
    RegexField("job_name", r'job_name=([^\s"]+)'),
    RegexField("build_url", r'build_url=([^\s"]+)'),
    RegexField("source_branch", r"branch=([A-Za-z0-9_./-]+)"),
    RegexField("service_module", r"module=([A-Za-z0-9_.-]+)"),
    RegexField("build_tool", r"tool=([A-Za-z0-9_.-]+)"),
    RegexField("dependency_cache", r"dependency_cache=([A-Za-z0-9_.-]+)"),
    RegexField("test_suite", r"suite=([A-Za-z0-9_.-]+)"),
    RegexField("artifact_name", r'artifact=([^\s"]+)'),
    RegexField("artifact_checksum", r'checksum=([^\s"]+)'),
    RegexField("target_environment", r"environment=([A-Za-z0-9_.-]+)"),
    RegexField("deployment_strategy", r"strategy=([A-Za-z0-9_.-]+)"),
    RegexField("forced_success", r"forced_success=(true|false)"),
    RegexField("severity_text", r'"severityText":"([A-Z]+)"'),
    RegexField("trace_id", r'"traceId":"([A-Fa-f0-9]+)"'),
    RegexField("span_id", r'"spanId":"([A-Fa-f0-9]+)"'),
    RegexField("span_name", r'"name":"([^"]+)","startTimeUnixNano"'),
    RegexField("http_method", r'"value":[{]"stringValue":"([A-Z]+)"[}],"key":"http.request.method"'),
    RegexField("http_route", r'"value":[{]"stringValue":"([^"]+)"[}],"key":"http.route"'),
    RegexField("exception_type", r'"value":[{]"stringValue":"([^"]+)"[}],"key":"exception.type"'),
    RegexField("exception_message", r'"value":[{]"stringValue":"([^"]+)"[}],"key":"exception.message"'),
)

NUMBER_FIELDS = (
    RegexField("build_number", r"build_number=([0-9]+)"),
    RegexField("random_scenario", r"random_scenario=([0-9]+)"),
    RegexField("disk_free_pct", r"disk_free_pct=([0-9]+)"),
    RegexField("cpu_temp_c", r"cpu_temp_c=([0-9]+)"),
    RegexField("compile_time_ms", r"compile_time_ms=([0-9]+)"),
    RegexField("test_total", r"total=([0-9]+)"),
    RegexField("passed_tests", r"passed_tests=([0-9]+)"),
    RegexField("failing_tests", r"failing_tests=([0-9]+)"),
    RegexField("test_duration_ms", r"duration_ms=([0-9]+)"),
    RegexField("artifact_size_mb", r"artifact_size_mb=([0-9]+)"),
    RegexField("replicas_ready", r"replicas_ready=([0-9]+)"),
    RegexField("replicas_expected", r"replicas_expected=([0-9]+)"),
    RegexField("rollout_seconds", r"rollout_seconds=([0-9]+)"),
    RegexField("http_status_code", r'"value":[{]"intValue":"?([0-9]+)"?[}],"key":"http.response.status_code"'),
)

KNOWN_STAGE_EVENTS = ("checkout", "preflight", "build", "test", "package", "deploy")

OUTPUT_FIELDS = (
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
    "build_tool",
    "dependency_cache",
    "job_name",
    "build_number",
    "source_branch",
    "target_environment",
    "deployment_strategy",
    "random_scenario",
    "forced_success",
    "disk_free_pct",
    "cpu_temp_c",
    "compile_time_ms",
    "test_suite",
    "test_total",
    "passed_tests",
    "failing_tests",
    "test_duration_ms",
    "artifact_name",
    "artifact_checksum",
    "artifact_size_mb",
    "replicas_ready",
    "replicas_expected",
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
)
