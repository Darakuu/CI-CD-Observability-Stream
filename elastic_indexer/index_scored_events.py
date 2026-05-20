"""Index ML stage-warning telemetry into Elasticsearch."""

from __future__ import annotations

import json
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_INPUT_TOPIC = "cicd.otel.scored"
DEFAULT_KAFKA_GROUP_ID = "elasticsearch-indexer"
DEFAULT_ELASTICSEARCH_URL = "http://elasticsearch:9200"
DEFAULT_ELASTICSEARCH_USERNAME = "elastic"
DEFAULT_ELASTICSEARCH_INDEX = "cicd-observability-events"
DEFAULT_BATCH_SIZE = 200
DEFAULT_FLUSH_INTERVAL_SECONDS = 5.0

INDEXED_DOCUMENT_FIELDS = {
    "processing_component",
    "ml_scored_at",
    "ml_model_name",
    "ml_model_version",
    "ml_model_type",
    "ml_prediction_target",
    "ml_stage_failure_warning",
    "predicted_failure_stage",
    "warning_level",
    "warning_type",
    "warning_title",
    "warning_message",
    "warning_reason",
    "recommended_action",
    "dashboard_category",
    "raw_event_sha256",
    "observed_at",
    "job_name",
    "build_number",
    "service_name",
    "ci_stage",
    "stage_order",
    "ci_status",
    "pipeline_status",
    "event_kind",
    "signal_domain",
    "signal_name",
    "signal_value",
    "signal_unit",
    "severity_level",
    "event_summary",
    "is_failure",
    "alert_candidate",
    "failure_category",
    "failure_reason",
    "target_environment",
}


@dataclass(frozen=True)
class IndexerConfig:
    """Runtime settings for the Elasticsearch indexing stage."""

    kafka_bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    input_topic: str = DEFAULT_INPUT_TOPIC
    kafka_group_id: str = DEFAULT_KAFKA_GROUP_ID
    elasticsearch_url: str = DEFAULT_ELASTICSEARCH_URL
    elasticsearch_username: str = DEFAULT_ELASTICSEARCH_USERNAME
    elasticsearch_password: str = "admine"
    index_name: str = DEFAULT_ELASTICSEARCH_INDEX
    batch_size: int = DEFAULT_BATCH_SIZE
    flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS

    @classmethod
    def from_env(cls) -> "IndexerConfig":
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
            ),
            input_topic=os.getenv("ELASTICSEARCH_INPUT_TOPIC", DEFAULT_INPUT_TOPIC),
            kafka_group_id=os.getenv("ELASTICSEARCH_KAFKA_GROUP_ID", DEFAULT_KAFKA_GROUP_ID),
            elasticsearch_url=os.getenv("ELASTICSEARCH_URL", DEFAULT_ELASTICSEARCH_URL),
            elasticsearch_username=os.getenv(
                "ELASTICSEARCH_USERNAME",
                DEFAULT_ELASTICSEARCH_USERNAME,
            ),
            elasticsearch_password=os.getenv("ELASTICSEARCH_PASSWORD", "admine"),
            index_name=os.getenv("ELASTICSEARCH_INDEX", DEFAULT_ELASTICSEARCH_INDEX),
            batch_size=int(os.getenv("ELASTICSEARCH_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))),
            flush_interval_seconds=float(
                os.getenv(
                    "ELASTICSEARCH_FLUSH_INTERVAL_SECONDS",
                    str(DEFAULT_FLUSH_INTERVAL_SECONDS),
                )
            ),
        )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def kafka_timestamp_to_iso(timestamp_ms: int | None) -> str | None:
    if timestamp_ms is None:
        return None

    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat()


def build_es_client(config: IndexerConfig) -> Elasticsearch:
    auth = None
    if config.elasticsearch_username:
        auth = (config.elasticsearch_username, config.elasticsearch_password)

    return Elasticsearch(
        config.elasticsearch_url,
        basic_auth=auth,
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )


def response_body(response) -> dict[str, Any]:
    return response.body if hasattr(response, "body") else response


def wait_for_elasticsearch(client: Elasticsearch, timeout_seconds: int = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            client.info()
            return
        except Exception as exc:  # Elasticsearch may still be booting.
            last_error = exc
            time.sleep(2)

    raise RuntimeError("Elasticsearch did not become ready in time") from last_error


def create_index_template(client: Elasticsearch, index_name: str) -> None:
    """Install a small mapping tuned for CI/CD observability dashboards."""

    keyword_fields = [
        "processing_component",
        "ml_model_name",
        "ml_model_version",
        "ml_model_type",
        "ml_prediction_target",
        "predicted_failure_stage",
        "warning_level",
        "warning_type",
        "warning_reason",
        "recommended_action",
        "dashboard_category",
        "raw_event_sha256",
        "job_name",
        "service_name",
        "ci_stage",
        "ci_status",
        "pipeline_status",
        "event_kind",
        "signal_domain",
        "signal_name",
        "signal_unit",
        "severity_level",
        "failure_category",
        "failure_reason",
        "target_environment",
        "indexer_source_topic",
    ]

    integer_fields = [
        "build_number",
        "stage_order",
        "indexer_source_partition",
    ]

    long_fields = [
        "indexer_source_offset",
    ]

    float_fields = [
        "signal_value",
    ]

    boolean_fields = [
        "ml_stage_failure_warning",
        "is_failure",
        "alert_candidate",
    ]

    date_fields = [
        "@timestamp",
        "indexed_at",
        "ml_scored_at",
        "observed_at",
        "indexer_source_kafka_timestamp",
    ]

    properties: dict[str, Any] = {
        field_name: {"type": "keyword", "ignore_above": 512} for field_name in keyword_fields
    }
    properties.update(
        {field_name: {"type": "integer", "ignore_malformed": True} for field_name in integer_fields}
    )
    properties.update(
        {field_name: {"type": "long", "ignore_malformed": True} for field_name in long_fields}
    )
    properties.update(
        {field_name: {"type": "float", "ignore_malformed": True} for field_name in float_fields}
    )
    properties.update({field_name: {"type": "boolean"} for field_name in boolean_fields})
    properties.update(
        {field_name: {"type": "date", "ignore_malformed": True} for field_name in date_fields}
    )
    properties.update(
        {
            "event_summary": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
            },
            "warning_title": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
            },
            "warning_message": {"type": "text"},
        }
    )

    client.indices.put_index_template(
        name=f"{index_name}-template",
        index_patterns=[index_name, f"{index_name}-*"],
        priority=500,
        template={
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
            "mappings": {
                "dynamic": False,
                "properties": properties,
            },
        },
    )

    if client.indices.exists(index=index_name):
        # Templates only affect newly-created indices. Updating the live mapping
        # lets newly-added fields appear in Kibana after refreshing the data view.
        current_properties = existing_index_properties(client, index_name)
        missing_properties = {
            name: mapping for name, mapping in properties.items() if name not in current_properties
        }
        if missing_properties:
            client.indices.put_mapping(index=index_name, properties=missing_properties)
    else:
        client.indices.create(index=index_name)


def existing_index_properties(client: Elasticsearch, index_name: str) -> dict[str, Any]:
    mapping = response_body(client.indices.get_mapping(index=index_name))
    index_mapping = mapping.get(index_name) or next(iter(mapping.values()), {})
    return index_mapping.get("mappings", {}).get("properties", {})


def build_consumer(config: IndexerConfig) -> KafkaConsumer:
    servers = [server.strip() for server in config.kafka_bootstrap_servers.split(",")]
    last_error: Exception | None = None

    for _ in range(60):
        try:
            return KafkaConsumer(
                config.input_topic,
                bootstrap_servers=servers,
                group_id=config.kafka_group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            time.sleep(2)

    raise RuntimeError("Kafka did not become ready in time") from last_error


def document_timestamp(document: dict[str, Any], kafka_timestamp_ms: int | None) -> str:
    return (
        document.get("ml_scored_at")
        or document.get("observed_at")
        or kafka_timestamp_to_iso(kafka_timestamp_ms)
        or utc_now()
    )


def document_id(document: dict[str, Any], message) -> str:
    if document.get("raw_event_sha256"):
        return str(document["raw_event_sha256"])

    return f"{message.topic}-{message.partition}-{message.offset}"


def action_from_message(message, index_name: str) -> dict[str, Any] | None:
    try:
        document = json.loads(message.value.decode("utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Skipping non-JSON Kafka message at offset {message.offset}: {exc}", flush=True)
        return None

    if not isinstance(document, dict):
        print(f"Skipping Kafka message at offset {message.offset}: expected JSON object", flush=True)
        return None

    document = {key: value for key, value in document.items() if key in INDEXED_DOCUMENT_FIELDS}
    document["@timestamp"] = document_timestamp(document, message.timestamp)
    document["indexed_at"] = utc_now()
    document["indexer_source_topic"] = message.topic
    document["indexer_source_partition"] = message.partition
    document["indexer_source_offset"] = message.offset
    document["indexer_source_kafka_timestamp"] = kafka_timestamp_to_iso(message.timestamp)

    return {
        "_op_type": "index",
        "_index": index_name,
        "_id": document_id(document, message),
        "_source": document,
    }


def flush_actions(client: Elasticsearch, actions: list[dict[str, Any]]) -> None:
    if not actions:
        return

    bulk_client = client.options(request_timeout=60)
    indexed_count, errors = bulk(
        bulk_client,
        actions,
        raise_on_error=False,
    )

    if errors:
        print(
            f"Indexed {indexed_count} documents; {len(errors)} documents failed bulk indexing.",
            flush=True,
        )
        print(f"First bulk error: {errors[0]}", flush=True)
    else:
        print(f"Indexed {indexed_count} documents into Elasticsearch.", flush=True)


def main() -> None:
    config = IndexerConfig.from_env()
    stop_requested = False

    def request_stop(_signum, _frame) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    es = build_es_client(config)
    wait_for_elasticsearch(es)
    create_index_template(es, config.index_name)

    consumer = build_consumer(config)
    actions: list[dict[str, Any]] = []
    last_flush = time.monotonic()

    print(
        f"Indexing Kafka topic {config.input_topic} into Elasticsearch index {config.index_name}.",
        flush=True,
    )

    while not stop_requested:
        records = consumer.poll(timeout_ms=1000, max_records=config.batch_size)
        for messages in records.values():
            for message in messages:
                action = action_from_message(message, config.index_name)
                if action:
                    actions.append(action)

        due_to_size = len(actions) >= config.batch_size
        due_to_time = actions and (time.monotonic() - last_flush >= config.flush_interval_seconds)

        if due_to_size or due_to_time:
            flush_actions(es, actions)
            actions.clear()
            consumer.commit()
            last_flush = time.monotonic()

    flush_actions(es, actions)
    consumer.commit()
    consumer.close()


if __name__ == "__main__":
    main()
