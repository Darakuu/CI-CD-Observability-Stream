"""Top-level orchestration for the streaming processor.

This module keeps the job lifecycle in one place: create Spark, read raw Kafka
events, transform them, and start the processed Kafka sink.
"""

from stream_processor.config import StreamConfig
from stream_processor.kafka_io import KafkaTelemetryStream
from stream_processor.session import build_spark_session
from stream_processor.transforms import CiCdTelemetryTransformer


class ProcessingJob:
    """Coordinates the Spark session, Kafka stream, and telemetry transformations."""

    def __init__(self, config: StreamConfig):
        """Prepare the job with its runtime config and processing helpers."""

        self.config = config
        self.kafka = KafkaTelemetryStream(config)
        self.transformer = CiCdTelemetryTransformer()

    def start(self):
        """Start the Structured Streaming query and return Spark's query handle."""

        spark = build_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        # Spark builds the query lazily here. The stream starts only when the
        # Kafka sink calls writeStream.start().
        raw_events = self.kafka.read_raw_events(spark)
        processed_events = self.transformer.transform(raw_events)
        return self.kafka.write_processed_events(processed_events)

    def run(self) -> None:
        """Run the processor until Spark is stopped or the query fails."""

        self.start().awaitTermination()
