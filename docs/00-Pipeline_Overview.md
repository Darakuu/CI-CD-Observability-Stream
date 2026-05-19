# Pipeline Overview

Diagram in WIP.

## Data Ingestion

```mermaid
flowchart TD
    Jenkins["Jenkins demo pipeline"]
    OTel["OpenTelemetry Collector"]
    Files[("JSONL files\ntraces, metrics, logs")]
    Logstash["Logstash ingestion"]
    RawKafka[("Kafka topic\ncicd.otel.raw")]
    Spark["Spark Structured Streaming"]
    ProcessedKafka[("Kafka topic\ncicd.otel.processed")]
    MLlib["Spark MLlib\nrisk scoring"]
    ScoredKafka[("Kafka topic\ncicd.otel.scored")]
    Elasticsearch["Elasticsearch\ncicd-observability-events"]
    Kibana["Kibana\nCI/CD dashboard"]

    Jenkins -->|"OTLP telemetry"| OTel
    OTel -->|"writes"| Files
    Files -->|"tails"| Logstash
    Logstash -->|"enriched JSON events"| RawKafka
    RawKafka -->|"raw telemetry stream"| Spark
    Spark -->|"processed events"| ProcessedKafka
    ProcessedKafka -->|"clean CI/CD events"| MLlib
    MLlib -->|"scored observability events"| ScoredKafka
    ScoredKafka -->|"bulk indexed documents"| Elasticsearch
    Elasticsearch -->|"data view and dashboard"| Kibana
```
