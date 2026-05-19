"""OpenTelemetry schemas used when Spark expands trace payloads.

The schemas cover only the fields this project needs. Spark can then read span
attributes without forcing the rest of the OpenTelemetry document into code.
"""

from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


ATTRIBUTE_VALUE_SCHEMA = StructType(
    [
        StructField("stringValue", StringType()),
        StructField("intValue", StringType()),
        StructField("boolValue", StringType()),
        StructField("doubleValue", StringType()),
    ]
)

ATTRIBUTE_SCHEMA = StructType(
    [
        StructField("key", StringType()),
        StructField("value", ATTRIBUTE_VALUE_SCHEMA),
    ]
)

SPAN_EVENT_SCHEMA = StructType(
    [
        StructField("name", StringType()),
        StructField("attributes", ArrayType(ATTRIBUTE_SCHEMA)),
    ]
)

SPAN_SCHEMA = StructType(
    [
        StructField("traceId", StringType()),
        StructField("spanId", StringType()),
        StructField("parentSpanId", StringType()),
        StructField("name", StringType()),
        StructField(
            "status",
            StructType(
                [
                    StructField("code", StringType()),
                    StructField("message", StringType()),
                ]
            ),
        ),
        StructField("attributes", ArrayType(ATTRIBUTE_SCHEMA)),
        StructField("events", ArrayType(SPAN_EVENT_SCHEMA)),
    ]
)

OTEL_TRACE_SCHEMA = StructType(
    [
        StructField(
            "resourceSpans",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "scopeSpans",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("spans", ArrayType(SPAN_SCHEMA)),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
    ]
)
