import os
from dataclasses import dataclass


DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_RAW_TOPIC = "cicd.otel.raw"
DEFAULT_PROCESSED_TOPIC = "cicd.otel.processed"
DEFAULT_CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/cicd-otel-processed"


@dataclass(frozen=True)
class StreamConfig:
    kafka_bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    raw_topic: str = DEFAULT_RAW_TOPIC
    processed_topic: str = DEFAULT_PROCESSED_TOPIC
    checkpoint_location: str = DEFAULT_CHECKPOINT_LOCATION

    @classmethod
    def from_env(cls) -> "StreamConfig":
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
            ),
            raw_topic=os.getenv("RAW_TOPIC", DEFAULT_RAW_TOPIC),
            processed_topic=os.getenv("PROCESSED_TOPIC", DEFAULT_PROCESSED_TOPIC),
            checkpoint_location=os.getenv(
                "CHECKPOINT_LOCATION",
                DEFAULT_CHECKPOINT_LOCATION,
            ),
        )
