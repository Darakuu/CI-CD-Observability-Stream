# CI-CD-Observability-Stream

Final project for Technologies for Advanced Programming (TAP).
A way to analyze events incoming from a CI/CD pipeline.

![Screenshot of the Kibana UI](assets/kibana_screenshot.png)

## Description

This project is a small observability pipeline for a Jenkins CI/CD job.

The idea is to simulate a normal build pipeline and follow its events while they move through a real-time data architecture:

- Jenkins produces CI/CD telemetry and build log events
- Logstash sends the raw events to Kafka
- Spark cleans the events and extracts useful fields
- Spark MLlib adds simple stage failure warnings
- Elasticsearch stores the final events
- Kibana shows the dashboard

The project is meant as an academic demo, so the main goal is to show the full path of the data from the pipeline to the dashboard.

---

# Requirements


For running the full stack locally, the requirements are:

- Docker
- Docker Compose V2
- At least 6-8 GB of available RAM (12 GB Recommended)
- A shell able to run Docker commands (Bash recommended to make use of the Makefile)

On Windows, Docker Desktop should be running before starting the stack.

For the optional local Python environment, install `uv`. This is useful for
local checks and for opening the presentation notebook.

# Installation


Clone the repository, create the local environment file, and start the
containers:

```bash
cp .env.example .env
docker compose up -d --build
```

The first start can take some time because Spark and the other services need to
download their images and dependencies.

To recreate the local Python environment declared by `pyproject.toml` and
`uv.lock`, run:

```bash
uv sync
```

The `dev` dependency group includes the Jupyter kernel support used by
`presentazione.ipynb`. The Docker services do not depend on that local
environment.

# Usage

Start the local demo stack:

```bash
# Windows, Powershell
docker compose up -d --build
```
or
```bash
# bash, WSL
make up
```

Main local entry points:

- Jenkins: http://localhost:8080
- Kafka UI: http://localhost:8085
- Elasticsearch: http://localhost:9200
- Kibana dashboard: http://localhost:5601/app/dashboards#/view/cicd-observability-command-center

Default demo credentials are listed in `.env.example`.

To generate data, open Jenkins, login with the demo user, and run the
`demo-ci-observability` job a few times.

Default local Jenkins login:

```text
admin / admin
```

Defaul Elastic login:
```text
elastic / admine
```

Some other useful commands:

```bash
# Show the Kafka topics
make topics

# Read raw events
make consume

# Read Spark processed events
make consume-processed

# Read ML scored events
make consume-scored

# Print a small Elasticsearch summary
make es-summary
```

All of these commands are basically macros that expand to windows-friendly commands.
See all definitions in the Makefile.

## Technologies and Infrastructure

- **Jenkins**: simulates the CI/CD pipeline used as the source of the data.
- **OpenTelemetry Collector**: receives Jenkins telemetry and writes it as JSON
  lines.
- **Logstash**: reads the telemetry files and Jenkins logs, then sends events to
  Kafka.
- **Kafka**: keeps the streaming topics used between the project components.
- **Spark Structured Streaming**: cleans raw telemetry and creates normalized CI/CD
  events.
- **Spark MLlib**: adds a simple Logistic Regression model for stage failure
  warnings.
- **Elasticsearch**: indexes the final events so they can be queried quickly.
- **Kibana**: shows the final dashboard over the Elasticsearch index.

## Repository Structure

```text
project/
|-- docker-compose.yml          # Local multi-container stack
|-- Makefile                    # Helper commands
|-- assets/                     # Images needed for this README or the presentation.
|-- docs/                       # Demo documentation per step
|-- jenkins/                    # Jenkins image, plugins, JCasC and demo job
|-- otel-collector/             # OpenTelemetry Collector configuration
|-- logstash/                   # Logstash ingestion pipeline
|-- spark/                      # Spark processing and MLlib scoring code
|-- elastic_indexer/            # Kafka to Elasticsearch indexer
|-- kibana/                     # Exported Kibana dashboard
|-- scripts/                    # Small topic consumer helpers (for debug purposes)
|-- README.md                   # This file!
```

## Mermaid Diagram of the Pipeline


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