"""Index ML-scored CI/CD telemetry into Elasticsearch."""

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
        "spark_processing_component",
        "ml_model_name",
        "ml_model_version",
        "ml_model_type",
        "ml_risk_band",
        "ml_anomaly_class",
        "otel_signal",
        "event_dataset",
        "ingestion_component",
        "source_topic",
        "raw_event_sha256",
        "ci_event",
        "ci_stage",
        "ci_status",
        "pipeline_status",
        "failure_category",
        "failure_reason",
        "error_code",
        "service_name",
        "service_module",
        "build_tool",
        "dependency_cache",
        "job_name",
        "source_branch",
        "target_environment",
        "deployment_strategy",
        "severity_text",
        "trace_id",
        "span_id",
        "span_name",
        "http_method",
        "http_route",
        "exception_type",
        "indexer_source_topic",
    ]

    integer_fields = [
        "source_partition",
        "ml_source_partition",
        "build_number",
        "random_scenario",
        "disk_free_pct",
        "cpu_temp_c",
        "compile_time_ms",
        "test_total",
        "passed_tests",
        "failing_tests",
        "test_duration_ms",
        "artifact_size_mb",
        "replicas_ready",
        "replicas_expected",
        "rollout_seconds",
        "http_status_code",
        "indexer_source_partition",
    ]

    long_fields = [
        "source_offset",
        "ml_source_offset",
        "indexer_source_offset",
    ]

    float_fields = [
        "ml_risk_score",
        "ml_model_probability",
        "ml_feature_risk_hint",
        "ml_feature_duration_signal",
        "ml_feature_test_failure_ratio",
        "ml_feature_low_disk_signal",
        "ml_feature_heat_signal",
        "ml_feature_deploy_gap_signal",
        "ml_feature_http_error_signal",
        "ml_feature_cache_miss_signal",
        "risk_hint",
    ]

    boolean_fields = [
        "ml_failure_prediction",
        "is_failure",
        "forced_success",
    ]

    date_fields = [
        "@timestamp",
        "indexed_at",
        "ml_scored_at",
        "processed_at",
        "source_kafka_timestamp",
        "ml_source_kafka_timestamp",
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
            "failure_detail": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
            },
            "exception_message": {"type": "text"},
            "artifact_name": {"type": "keyword", "ignore_above": 512},
            "artifact_checksum": {"type": "keyword", "ignore_above": 512},
            "test_suite": {"type": "keyword", "ignore_above": 512},
            "raw_event": {"type": "text", "index": False},
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
                "dynamic": True,
                "properties": properties,
            },
        },
    )

    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name)


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
        or document.get("processed_at")
        or document.get("source_kafka_timestamp")
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
