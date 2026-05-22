# Pipeline Overview

This project demonstrates a small end-to-end CI/CD observability pipeline.


A **Jenkins demo job** simulates a CI/CD pipeline executing, which then emits telemetry signals from said pipeline. **OpenTelemetry** then collects these traces, metrics, and logs, which are then collected by **Logstash**. **Logstash** turns those signals (plus useful Jenkins build log lines) into structured events.

**Kafka** is the streaming handoff between most stages, with 3 topics:
- raw telemetry enter the `cicd.otel.raw` topic.
- **Spark Structured Streaming** cleans and enriches it into `cicd.otel.processed`
- **Spark MLlib** stage produces scored warning events in `cicd.otel.scored`.

The **Elasticsearch** indexer consumes those scored events, adds indexing metadata such as `@timestamp` and the Kafka source offset, writes them to the `cicd-observability-events` index, and finally we can use **Kibana** to visualize the indexed CI/CD warnings and failures in the final dashboard.
**Kibana** implements a Data view and a dashboard made in its UI, with no further work required by the user.

## Diagram overview

```mermaid
flowchart TD
    subgraph CI["CI/CD Source"]
        Jenkins["Jenkins Pipeline"]
    end

    subgraph Collect["Telemetry Collection"]
        OTel["OpenTelemetry Collector"]
        Files[("JSONL Files")]
        Logstash["Logstash"]
    end

    subgraph Stream["Apache Kafka"]
        RawKafka[("cicd.otel.raw")]
        ProcessedKafka[("cicd.otel.processed")]
        ScoredKafka[("cicd.otel.scored")]
    end

    subgraph Process["Spark Processing"]
        Spark["Spark Structured Streaming"]
        MLlib["Spark MLlib"]
    end

    subgraph Search["Search and Visualization"]
        Indexer["Elasticsearch Indexer"]
        Elasticsearch[("cicd-observability-events")]
        Kibana["Kibana Dashboard"]
    end

    Jenkins -->|"OTLP"| OTel
    OTel -->|"JSONL"| Files
    Files -->|"tail"| Logstash
    Logstash -->|"events"| RawKafka
    RawKafka -->|"raw"| Spark
    Spark -->|"processed"| ProcessedKafka
    ProcessedKafka -->|"features"| MLlib
    MLlib -->|"scored"| ScoredKafka
    ScoredKafka -->|"consume"| Indexer
    Indexer -->|"bulk"| Elasticsearch
    Elasticsearch -->|"query"| Kibana

    classDef jenkins fill:#FDE8E8,stroke:#D33833,stroke-width:2px,color:#1F2937;
    classDef otel fill:#EEF2FF,stroke:#4F46E5,stroke-width:2px,color:#1F2937;
    classDef file fill:#F8FAFC,stroke:#64748B,stroke-width:2px,color:#1F2937;
    classDef logstash fill:#ECFDF5,stroke:#54B399,stroke-width:2px,color:#1F2937;
    classDef kafka fill:#F4F4F5,stroke:#231F20,stroke-width:2px,color:#1F2937;
    classDef spark fill:#FFF1E8,stroke:#E25A1C,stroke-width:2px,color:#1F2937;
    classDef mllib fill:#FEF3C7,stroke:#B45309,stroke-width:2px,color:#1F2937;
    classDef indexer fill:#EFF6FF,stroke:#2563EB,stroke-width:2px,color:#1F2937;
    classDef elasticsearch fill:#E6FFFB,stroke:#00BFB3,stroke-width:2px,color:#1F2937;
    classDef kibana fill:#FDF2F8,stroke:#D36086,stroke-width:2px,color:#1F2937;

    class Jenkins jenkins;
    class OTel otel;
    class Files file;
    class Logstash logstash;
    class RawKafka,ProcessedKafka,ScoredKafka kafka;
    class Spark spark;
    class MLlib mllib;
    class Indexer indexer;
    class Elasticsearch elasticsearch;
    class Kibana kibana;
```
