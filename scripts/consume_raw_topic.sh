#!/usr/bin/env bash
set -euo pipefail

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic cicd.otel.raw `
  --from-beginning

# this script is for debug purposes only, you don't need to run it for the project to function correctly.