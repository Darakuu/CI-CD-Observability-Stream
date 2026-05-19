"""Status normalization shared by log-derived and span-derived events."""

from pyspark.sql.functions import lit
from pyspark.sql.functions import lower
from pyspark.sql.functions import when


def normalize_pipeline_status(status_column):
    """Map Jenkins and OpenTelemetry result words to a common pipeline status."""

    normalized = lower(status_column)
    return (
        when(normalized.isin("failure", "failed", "error"), lit("failure"))
        .when(normalized.isin("success", "succeeded", "passed"), lit("success"))
        .otherwise(normalized)
    )


def normalize_ci_status(status_column):
    """Map stage-level result words to the CI status used by processed events."""

    normalized = lower(status_column)
    return (
        when(normalized.isin("failure", "failed", "error"), lit("failed"))
        .when(normalized.isin("success", "succeeded"), lit("success"))
        .when(normalized == "passed", lit("passed"))
        .otherwise(normalized)
    )
