# NOTE: These commands are for linux based systems only, with `make` installed.
# if on Windows, execute them through the included 'bash' environment in powershell.

# PHONY: ensure that these are commands, not files.
.PHONY: up down restart logs spark-logs ml-logs es-logs kibana-logs kibana-url es-summary topics consume consume-processed consume-scored clean clean-keep-kibana

# Convenience targets for running and inspecting the local demo stack.

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose down && docker compose up -d --build

clean:
	docker compose down -v --remove-orphans
	rm -rf jenkins_home logstash-data otel-output spark-checkpoints

clean-keep-kibana:
	docker compose down --remove-orphans
	project_name=$${COMPOSE_PROJECT_NAME:-$$(basename "$$(pwd)")}; docker volume rm "$${project_name}_kafka-data" "$${project_name}_spark-checkpoints" 2>/dev/null || true
	rm -rf jenkins_home logstash-data otel-output spark-checkpoints

logs:
	docker compose logs -f jenkins otel-collector logstash spark-processor spark-mllib elasticsearch elasticsearch-indexer kibana

spark-logs:
	docker compose logs -f spark-processor

ml-logs:
	docker compose logs -f spark-mllib

es-logs:
	docker compose logs -f elasticsearch elasticsearch-indexer

kibana-logs:
	docker compose logs -f kibana

kibana-url:
	@echo "Kibana dashboards: http://localhost:5601/app/dashboards"

es-summary:
	docker compose run --rm elasticsearch-indexer python /opt/tap/elastic_indexer/query_observability.py --summary

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

consume:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cicd.otel.raw --from-beginning

consume-processed:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cicd.otel.processed --from-beginning

consume-scored:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cicd.otel.scored --from-beginning
