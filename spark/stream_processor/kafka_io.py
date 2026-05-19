"""Kafka source and sink definitions for the Spark stream.

The processor reads raw OpenTelemetry messages from one topic and writes a
clean JSON event to the next topic. Checkpointing belongs to the sink because
that is where Spark tracks streaming progress.
"""

from pyspark.sql import SparkSession

from stream_processor.config import StreamConfig


class KafkaTelemetryStream:
    """Holds the Kafka read and write settings for the telemetry stream."""

    def __init__(self, config: StreamConfig):
        self.config = config

    def read_raw_events(self, spark: SparkSession):
        """Create a streaming DataFrame over the raw CI/CD telemetry topic."""

        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
            .option("subscribe", self.config.raw_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def write_processed_events(self, processed_events):
        """Write processed telemetry events to Kafka with Spark checkpointing enabled."""

        return (
            processed_events.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
            .option("topic", self.config.processed_topic)
            .option("checkpointLocation", self.config.checkpoint_location)
            .outputMode("append")
            .start()
        )
