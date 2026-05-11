#!/usr/bin/env bash
set -euo pipefail

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic cicd.otel.raw `
  --from-beginning