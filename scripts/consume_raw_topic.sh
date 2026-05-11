#!/usr/bin/env bash
set -euo pipefail

docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cicd.otel.raw \
  --from-beginning
