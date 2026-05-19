"""Runtime settings for the MLlib scoring job."""

import os
from dataclasses import dataclass


DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_INPUT_TOPIC = "cicd.otel.processed"
DEFAULT_OUTPUT_TOPIC = "cicd.otel.scored"
DEFAULT_CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/cicd-otel-scored"


@dataclass(frozen=True)
class MllibConfig:
    """Configuration values used by the MLlib streaming job."""

    kafka_bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    input_topic: str = DEFAULT_INPUT_TOPIC
    output_topic: str = DEFAULT_OUTPUT_TOPIC
    checkpoint_location: str = DEFAULT_CHECKPOINT_LOCATION

    @classmethod
    def from_env(cls) -> "MllibConfig":
        """Build a config from environment variables with local defaults."""

        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
            ),
            input_topic=os.getenv("ML_INPUT_TOPIC", DEFAULT_INPUT_TOPIC),
            output_topic=os.getenv("ML_OUTPUT_TOPIC", DEFAULT_OUTPUT_TOPIC),
            checkpoint_location=os.getenv(
                "ML_CHECKPOINT_LOCATION",
                DEFAULT_CHECKPOINT_LOCATION,
            ),
        )
