"""Helpers for reading typed OpenTelemetry span attributes.

OpenTelemetry stores attributes as an array of key/value objects. These helpers
hide the Spark SQL expression needed to pick one attribute by name.
"""

from pyspark.sql.functions import expr


def span_attr_string(attribute_name):
    """Return a Spark expression that reads one span attribute as text."""

    return expr(
        "element_at("
        f"transform(filter(span.attributes, x -> x.key = '{attribute_name}'), "
        "x -> x.value.stringValue), "
        "1)"
    )


def span_attr_int(attribute_name):
    """Return a Spark expression that reads one span attribute as an integer."""

    return expr(
        "cast(element_at("
        f"transform(filter(span.attributes, x -> x.key = '{attribute_name}'), "
        "x -> coalesce(x.value.intValue, x.value.stringValue)), "
        "1) as int)"
    )
