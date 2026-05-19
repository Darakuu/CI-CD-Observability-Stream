"""Entrypoint for the Spark MLlib scoring component."""

from mllib_processor.config import MllibConfig
from mllib_processor.job import MllibScoringJob


CONFIG = MllibConfig.from_env()

KAFKA_BOOTSTRAP_SERVERS = CONFIG.kafka_bootstrap_servers
ML_INPUT_TOPIC = CONFIG.input_topic
ML_OUTPUT_TOPIC = CONFIG.output_topic
ML_CHECKPOINT_LOCATION = CONFIG.checkpoint_location


def main() -> None:
    MllibScoringJob(CONFIG).run()


if __name__ == "__main__":
    main()
