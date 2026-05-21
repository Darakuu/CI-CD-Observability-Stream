# Data Ingestion (and Streaming) how to:

First part of our pipeline (in detail):

```mermaid
flowchart LR
    subgraph CI["CI/CD Source"]
        A["Jenkins Pipeline Demo"]
    end

    subgraph OBS["Observability Layer"]
        B["OpenTelemetry Collector"]
        C[("JSONL Files")]
        D["Logstash Ingestion"]
    end

    subgraph STREAM["Streaming Layer"]
        E[("Kafka Topic<br/>cicd.otel.raw")]
    end

    A -->|"OTLP"| B
    B -->|"JSONL"| C
    C -->|"tail"| D
    D -->|"events"| E

    classDef jenkins fill:#FDE8E8,stroke:#D33833,stroke-width:2px,color:#1F2937;
    classDef otel fill:#EEF2FF,stroke:#4F46E5,stroke-width:2px,color:#1F2937;
    classDef file fill:#F8FAFC,stroke:#64748B,stroke-width:2px,color:#1F2937;
    classDef logstash fill:#ECFDF5,stroke:#54B399,stroke-width:2px,color:#1F2937;
    classDef kafka fill:#F4F4F5,stroke:#231F20,stroke-width:2px,color:#1F2937;

    class A jenkins;
    class B otel;
    class C file;
    class D logstash;
    class E kafka;
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
