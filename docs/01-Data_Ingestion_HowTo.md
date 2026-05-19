# Data Ingestion (and Streaming) how to:

First part of our pipeline (in detail):

```mermaid
flowchart LR
    subgraph CI["CI/CD Source"]
        A["🚀 Jenkins Pipeline Demo"]
    end

    subgraph OBS["Observability Layer"]
        B["📡 OpenTelemetry Collector"]
        C["📥 Logstash Ingestion"]
    end

    subgraph STREAM["Streaming Layer"]
        D[("🟣 Kafka Topic<br/>cicd.otel.raw")]
    end

    A -->|"OTLP/gRPC<br/>traces · metrics · logs"| B
    B -->|"OTLP JSON Lines<br/>shared volume"| C
    C -->|"JSON events"| D

    classDef source fill:#FFF7D6,stroke:#D97706,stroke-width:2px,color:#1F2937;
    classDef observability fill:#DBEAFE,stroke:#2563EB,stroke-width:2px,color:#1F2937;
    classDef ingestion fill:#DCFCE7,stroke:#16A34A,stroke-width:2px,color:#1F2937;
    classDef stream fill:#F3E8FF,stroke:#7C3AED,stroke-width:2px,color:#1F2937;

    class A source;
    class B observability;
    class C ingestion;
    class D stream;
```

## Pre-requisites

Intended for this part only:

- Docker + Docker Compose;
- At least 6-8 GB of available RAM.
- Git
- Docker (v 4.72.0)
- Docker Compose V2
- A Bash-compatible shell for helper scripts:
  - Linux/macOS terminal
  - WSL2 shell on Windows
- On Windows, ensure that Docker Desktop is running before executing any Docker commands.
  - Also: ensure that `✅ Use the WSL2 Based Engine` setting is turned on.


## Start

```bash
cp .env.example .env
docker compose up -d --build
```

Main services:

- Jenkins: http://localhost:8080
- Kafka UI: http://localhost:8085
- OpenTelemetry Collector OTLP/gRPC: localhost:4317
- OpenTelemetry Collector OTLP/HTTP: localhost:4318

Local Jenkins Credentials:

```text
admin / admin
```

## Demo (manual)

1. Open Jenkins on http://localhost:8080.
2. Login with `admin / admin`.
3. Open the job: `demo-ci-observability`.
4. Execute `Build Now` a few times. Some builds will fail, some will succeed.
5. Then, run the `consume_raw_topics` scripts (.sh for Linux, .ps1 for Windows Powershell).
    5b. Ensuring you have correct permissions...
6. Or simply open Kafka UI on http://localhost:8085.
7. And check the topic `cicd.otel.raw`.

Or, if you prefer, via CLI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cicd.otel.raw \
  --from-beginning
```
