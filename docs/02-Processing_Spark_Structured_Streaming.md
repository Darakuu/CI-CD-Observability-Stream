# Processing with Spark Structured Streaming

This step adds the processing stage immediately after Kafka. It reads raw CI/CD telemetry events from `cicd.otel.raw`, extracts useful Jenkins pipeline fields, and writes enriched events to `cicd.otel.processed`.

```mermaid
flowchart TD
    RawKafka[("Kafka topic\ncicd.otel.raw")]
    Spark["Spark Structured Streaming\nspark-processor"]
    Checkpoints[("Spark checkpoints\nspark-checkpoints/")]
    ProcessedKafka[("Kafka topic\ncicd.otel.processed")]

    RawKafka -->|"raw JSON events"| Spark
    Spark -->|"offset and progress state"| Checkpoints
    Spark -->|"processed JSON events"| ProcessedKafka
```

## What this stage uses

- Raw input topic: `cicd.otel.raw`
- Processed output topic: `cicd.otel.processed`
- Spark checkpoint path: `/tmp/spark-checkpoints/cicd-otel-processed`
- Component name added by Spark: `spark-structured-streaming`

The Spark job only consumes from the raw topic and writes to the processed topic.
It does not touch Jenkins, Logstash state files, or the OpenTelemetry output files.

Both Kafka topics are created by `kafka-init` before Logstash and Spark start using them.
This keeps the startup order predictable and avoids Spark subscribing to a topic that does not exist yet.

## What Spark writes

Each message written to Kafka uses `raw_event_sha256` as the key.
The value is a JSON object like this:

```json
{
  "processing_component": "spark-structured-streaming",
  "processed_at": "2026-05-11T00:00:00.000Z",
  "otel_signal": "traces",
  "event_dataset": "jenkins.otel.raw",
  "ingestion_component": "logstash",
  "source_topic": "cicd.otel.raw",
  "source_partition": 0,
  "source_offset": 42,
  "source_kafka_timestamp": "2026-05-11T00:00:00.000Z",
  "raw_event_sha256": "sha256-of-the-original-event",
  "ci_event": "preflight",
  "ci_stage": "preflight",
  "ci_status": "failed",
  "pipeline_status": "failure",
  "is_failure": true,
  "failure_category": "infrastructure",
  "failure_reason": "disk_full",
  "failure_detail": "workspace_volume_available_space_below_1_percent",
  "error_code": null,
  "risk_hint": 1.0,
  "service_name": "demo-service",
  "service_module": "demo-service-api",
  "job_name": "demo-ci-observability",
  "build_number": 42,
  "source_branch": "main",
  "target_environment": "staging",
  "random_scenario": 2,
  "forced_success": false,
  "disk_free_pct": null,
  "cpu_temp_c": null,
  "compile_time_ms": null,
  "test_total": null,
  "failing_tests": null,
  "test_duration_ms": null,
  "artifact_size_mb": null,
  "rollout_seconds": null,
  "severity_text": "INFO",
  "raw_event": "{...original Logstash event...}"
}
```

The original event is still kept in `raw_event`.
This is useful because the next stages can still access the full OpenTelemetry payload, while the extracted fields give MLlib, Elasticsearch and Kibana a cleaner base to work with.

The `risk_hint` field is not the final ML result. It is only a simple Spark-side signal: failed events get a high value, warnings get a medium value, and normal CI stage events get a low value. The real prediction/anomaly logic belongs to the later MLlib stage.

## Running it

```bash
docker compose up -d --build
```

The same flow can also be started with the helper commands in the Makefile.
On the first run Spark may take a bit longer because it has to download the Kafka connector package declared in `docker-compose.yml`.

## Checking the result

After Jenkins has generated some telemetry, the processed topic can be checked with:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cicd.otel.processed \
  --from-beginning
```

The same topic can also be inspected from Kafka UI at http://localhost:8085. (easier to access)
