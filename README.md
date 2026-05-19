# CI-CD-Observability-Stream

Final project for Technologies for Advanced Programming (TAP).
A way to analyze events incoming from a CI/CD pipeline.

---

# Requirements

WIP

# Installation

WIP

# Usage

Start the local demo stack:

```bash
docker compose up -d --build
```

Main local entry points:

- Jenkins: http://localhost:8080
- Kafka UI: http://localhost:8085
- Elasticsearch: http://localhost:9200
- Kibana dashboard: http://localhost:5601/app/dashboards#/view/cicd-observability-command-center

Default demo credentials are listed in `.env.example`.
