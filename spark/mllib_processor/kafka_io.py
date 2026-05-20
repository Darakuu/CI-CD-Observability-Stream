"""Kafka source and sink definitions for the MLlib stage."""

from pyspark.sql import SparkSession

from mllib_processor.config import MllibConfig


class KafkaScoredTelemetryStream:
    """Reads processed telemetry and writes ML warning telemetry."""

    def __init__(self, config: MllibConfig):
        self.config = config

    def read_processed_events(self, spark: SparkSession):
        """Create a streaming DataFrame over the processed CI/CD topic."""

        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
            .option("subscribe", self.config.input_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def write_scored_events(self, scored_events):
        """Write ML warning events to Kafka with checkpointing enabled."""

        return (
            scored_events.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
            .option("topic", self.config.output_topic)
            .option("checkpointLocation", self.config.checkpoint_location)
            .outputMode("append")
            .start()
        )
