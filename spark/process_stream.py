""" Refactored entrypoint 'main' file. """

from stream_processor.config import StreamConfig
from stream_processor.job import ProcessingJob
from stream_processor.kafka_io import KafkaTelemetryStream
from stream_processor.schemas import ATTRIBUTE_SCHEMA
from stream_processor.schemas import ATTRIBUTE_VALUE_SCHEMA
from stream_processor.schemas import OTEL_TRACE_SCHEMA
from stream_processor.schemas import SPAN_EVENT_SCHEMA
from stream_processor.schemas import SPAN_SCHEMA
from stream_processor.session import build_spark_session
from stream_processor.span_attributes import span_attr_int
from stream_processor.span_attributes import span_attr_string
from stream_processor.status import normalize_ci_status
from stream_processor.status import normalize_pipeline_status
from stream_processor.transforms import CiCdTelemetryTransformer
from stream_processor.transforms import JenkinsCiEventEnricher
from stream_processor.transforms import OpenTelemetrySpanExtractor
from stream_processor.transforms import ProcessedEventProjector


CONFIG = StreamConfig.from_env()

# Backwards-compatible names so that the project doesn't break randomly after merge on main.
KAFKA_BOOTSTRAP_SERVERS = CONFIG.kafka_bootstrap_servers
RAW_TOPIC = CONFIG.raw_topic
PROCESSED_TOPIC = CONFIG.processed_topic
CHECKPOINT_LOCATION = CONFIG.checkpoint_location


def read_raw_events(spark):
    return KafkaTelemetryStream(CONFIG).read_raw_events(spark)


def normalize_events(kafka_events):
    return CiCdTelemetryTransformer().transform(kafka_events)


def expand_otel_spans(normalized_events):
    return OpenTelemetrySpanExtractor().expand(normalized_events)


def normalize_ci_events(normalized_events, non_empty=None):
    return JenkinsCiEventEnricher(non_empty=non_empty).enrich(normalized_events)


def finalize_events(enriched_events):
    return ProcessedEventProjector().project(enriched_events)


def write_processed_events(processed_events):
    return KafkaTelemetryStream(CONFIG).write_processed_events(processed_events)


def main() -> None:
    ProcessingJob(CONFIG).run()


if __name__ == "__main__":
    main()
