"""Top-level orchestration for the MLlib scoring stage."""

from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.functions import lit
from pyspark.sql.functions import struct
from pyspark.sql.functions import to_json

from mllib_processor.config import MllibConfig
from mllib_processor.kafka_io import KafkaScoredTelemetryStream
from mllib_processor.risk_model import fit_risk_model
from mllib_processor.risk_model import prepare_features
from mllib_processor.risk_model import score_events
from mllib_processor.schemas import PROCESSED_EVENT_SCHEMA
from mllib_processor.schemas import SCORED_EVENT_FIELDS
from mllib_processor.session import build_spark_session


class MllibScoringJob:
    """Coordinates Spark, Kafka, the MLlib model, and the scored Kafka sink."""

    def __init__(self, config: MllibConfig):
        self.config = config
        self.kafka = KafkaScoredTelemetryStream(config)

    def start(self):
        """Start the Structured Streaming query and return Spark's query handle."""

        spark = build_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        model = fit_risk_model(spark)
        processed_events = self.kafka.read_processed_events(spark)
        parsed_events = self._parse_processed_events(processed_events)
        feature_events = prepare_features(parsed_events)
        scored_events = score_events(model, feature_events)
        return self.kafka.write_scored_events(self._project_for_kafka(scored_events))

    def run(self) -> None:
        """Run the scorer until Spark is stopped or the query fails."""

        self.start().awaitTermination()

    def _parse_processed_events(self, kafka_events):
        """Parse the cleaned JSON document emitted by the Spark processor."""

        parsed = (
            kafka_events.select(
                col("value").cast("string").alias("processed_event_json"),
            )
            .withColumn("event", from_json(col("processed_event_json"), PROCESSED_EVENT_SCHEMA))
            .select("event.*")
        )

        return (
            parsed.filter(col("raw_event_sha256").isNotNull())
            .withColumn("processing_component", lit("spark-mllib-risk-scoring"))
        )

    def _project_for_kafka(self, scored_events):
        """Create Kafka key/value columns for the scored topic."""

        return scored_events.select(
            col("raw_event_sha256").alias("key"),
            to_json(
                struct(*[col(field_name) for field_name in SCORED_EVENT_FIELDS]),
                {"ignoreNullFields": "true"},
            ).alias("value"),
        )
