from stream_processor.config import StreamConfig
from stream_processor.kafka_io import KafkaTelemetryStream
from stream_processor.session import build_spark_session
from stream_processor.transforms import CiCdTelemetryTransformer


class ProcessingJob:
    def __init__(self, config: StreamConfig):
        self.config = config
        self.kafka = KafkaTelemetryStream(config)
        self.transformer = CiCdTelemetryTransformer()

    def start(self):
        spark = build_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        raw_events = self.kafka.read_raw_events(spark)
        processed_events = self.transformer.transform(raw_events)
        return self.kafka.write_processed_events(processed_events)

    def run(self) -> None:
        self.start().awaitTermination()
