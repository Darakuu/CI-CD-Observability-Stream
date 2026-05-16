from pyspark.sql.functions import expr


def span_attr_string(attribute_name):
    return expr(
        "element_at("
        f"transform(filter(span.attributes, x -> x.key = '{attribute_name}'), "
        "x -> x.value.stringValue), "
        "1)"
    )


def span_attr_int(attribute_name):
    return expr(
        "cast(element_at("
        f"transform(filter(span.attributes, x -> x.key = '{attribute_name}'), "
        "x -> coalesce(x.value.intValue, x.value.stringValue)), "
        "1) as int)"
    )
