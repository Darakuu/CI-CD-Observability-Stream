# NOTE: These commands are for linux based systems only, with `make` installed.
# if on Windows, execute them through the included 'bash' environment in powershell.

# PHONY: ensure that these are commands, not files.
.PHONY: up down restart logs spark-logs topics consume consume-processed clean

# Convenience targets for running and inspecting the local demo stack.

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose down && docker compose up -d --build

logs:
	docker compose logs -f jenkins otel-collector logstash spark-processor

spark-logs:
	docker compose logs -f spark-processor

topics:
	docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

consume:
	docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cicd.otel.raw --from-beginning

consume-processed:
	docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cicd.otel.processed --from-beginning

clean:
	docker compose down -v --remove-orphans
	rm -rf jenkins_home logstash-data otel-output spark-checkpoints
