.PHONY: up down restart logs topics consume clean

# Convenience targets for running and inspecting the local demo stack.

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose down && docker compose up -d --build

logs:
	docker compose logs -f jenkins otel-collector logstash

topics:
	docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

consume:
	docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cicd.otel.raw --from-beginning

clean:
	docker compose down -v --remove-orphans
	rm -rf jenkins_home logstash-data otel-output
