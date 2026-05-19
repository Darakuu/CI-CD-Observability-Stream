"""Top-level orchestration for the MLlib scoring stage."""

from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import concat
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import from_json
from pyspark.sql.functions import greatest
from pyspark.sql.functions import least
from pyspark.sql.functions import lit
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import struct
from pyspark.sql.functions import to_json
from pyspark.sql.functions import when

from mllib_processor.baseline_data import BUILD_DURATION_LIMIT_MS
from mllib_processor.baseline_data import ROLLOUT_DURATION_LIMIT_SECONDS
from mllib_processor.baseline_data import TEST_DURATION_LIMIT_MS
from mllib_processor.config import MllibConfig
from mllib_processor.kafka_io import KafkaScoredTelemetryStream
from mllib_processor.model import MODEL_NAME
from mllib_processor.model import MODEL_TYPE
from mllib_processor.model import MODEL_VERSION
from mllib_processor.model import fit_risk_model
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
        feature_events = self._prepare_features(parsed_events)
        scored_events = self._score_events(model, feature_events)
        return self.kafka.write_scored_events(self._project_for_kafka(scored_events))

    def run(self) -> None:
        """Run the scorer until Spark is stopped or the query fails."""

        self.start().awaitTermination()

    def _parse_processed_events(self, kafka_events):
        """Parse the processed JSON document emitted by the Spark processor."""

        parsed = (
            kafka_events.select(
                col("topic").alias("ml_source_topic"),
                col("partition").alias("ml_source_partition"),
                col("offset").alias("ml_source_offset"),
                col("timestamp").alias("ml_source_kafka_timestamp"),
                col("value").cast("string").alias("processed_event_json"),
            )
            .withColumn("event", from_json(col("processed_event_json"), PROCESSED_EVENT_SCHEMA))
            .select(
                "ml_source_topic",
                "ml_source_partition",
                "ml_source_offset",
                "ml_source_kafka_timestamp",
                "event.*",
            )
        )

        return (
            parsed.filter(col("raw_event_sha256").isNotNull())
            .withColumnRenamed("processing_component", "spark_processing_component")
            .withColumn("processing_component", lit("spark-mllib-risk-scoring"))
        )

    def _prepare_features(self, events):
        """Create stable numeric and categorical features for the MLlib model."""

        test_failure_ratio = when(
            coalesce(col("test_total"), lit(0)) > 0,
            coalesce(col("failing_tests"), lit(0)).cast("double") / col("test_total").cast("double"),
        ).when(
            coalesce(col("failing_tests"), lit(0)) > 0,
            lit(1.0),
        ).otherwise(lit(0.0))

        low_disk_signal = when(col("failure_reason") == "disk_full", lit(1.0)).when(
            col("disk_free_pct").isNull(),
            lit(0.0),
        ).otherwise(
            least(
                greatest((lit(25.0) - col("disk_free_pct").cast("double")) / lit(25.0), lit(0.0)),
                lit(1.0),
            )
        )

        heat_signal = when(col("failure_reason") == "thermal_throttling", lit(1.0)).when(
            col("cpu_temp_c").isNull(),
            lit(0.0),
        ).otherwise(
            least(
                greatest((col("cpu_temp_c").cast("double") - lit(70.0)) / lit(25.0), lit(0.0)),
                lit(1.0),
            )
        )

        deploy_gap_signal = when(col("failure_reason") == "rollout_timeout", lit(1.0)).when(
            coalesce(col("replicas_expected"), lit(0)) > 0,
            greatest(
                (
                    col("replicas_expected").cast("double")
                    - coalesce(col("replicas_ready"), lit(0)).cast("double")
                )
                / col("replicas_expected").cast("double"),
                lit(0.0),
            ),
        ).otherwise(lit(0.0))

        duration_signal = (
            when(
                col("ci_stage") == "build",
                least(
                    coalesce(col("compile_time_ms").cast("double"), lit(0.0)) / lit(BUILD_DURATION_LIMIT_MS),
                    lit(1.0),
                ),
            )
            .when(
                col("ci_stage") == "test",
                least(
                    coalesce(col("test_duration_ms").cast("double"), lit(0.0)) / lit(TEST_DURATION_LIMIT_MS),
                    lit(1.0),
                ),
            )
            .when(
                col("ci_stage") == "deploy",
                least(
                    coalesce(col("rollout_seconds").cast("double"), lit(0.0))
                    / lit(ROLLOUT_DURATION_LIMIT_SECONDS),
                    lit(1.0),
                ),
            )
            .otherwise(lit(0.0))
        )

        http_error_signal = (
            when(col("http_status_code") >= 500, lit(1.0))
            .when(col("http_status_code") >= 400, lit(0.6))
            .otherwise(lit(0.0))
        )

        return (
            events.withColumn("ci_stage_for_model", coalesce(col("ci_stage"), lit("unknown")))
            .withColumn("ci_status_for_model", coalesce(col("ci_status"), col("pipeline_status"), lit("unknown")))
            .withColumn("failure_category_for_model", coalesce(col("failure_category"), lit("none")))
            .withColumn("risk_hint_value", coalesce(col("risk_hint"), lit(0.0)).cast("double"))
            .withColumn(
                "is_failure_signal",
                when(coalesce(col("is_failure"), lit(False)), lit(1.0)).otherwise(lit(0.0)),
            )
            .withColumn("duration_signal", duration_signal)
            .withColumn("test_failure_ratio", test_failure_ratio)
            .withColumn("low_disk_signal", low_disk_signal)
            .withColumn("heat_signal", heat_signal)
            .withColumn("deploy_gap_signal", deploy_gap_signal)
            .withColumn("http_error_signal", http_error_signal)
            .withColumn(
                "cache_miss_signal",
                when(col("failure_reason") == "dependency_resolution", lit(1.0))
                .when(col("dependency_cache") == "miss", lit(1.0))
                .otherwise(lit(0.0)),
            )
        )

    def _score_events(self, model, events):
        """Apply the MLlib model and add dashboard-friendly scoring fields."""

        scored = (
            model.transform(events)
            .withColumn(
                "ml_model_probability",
                spark_round(vector_to_array(col("probability")).getItem(1), 4),
            )
            .withColumn(
                "ml_risk_score",
                spark_round(greatest(col("ml_model_probability"), col("risk_hint_value")), 4),
            )
            .withColumn("ml_failure_prediction", col("ml_risk_score") >= lit(0.65))
            .withColumn("ml_risk_band", self._risk_band())
            .withColumn("ml_anomaly_class", self._anomaly_class())
            .withColumn("ml_scored_at", current_timestamp())
            .withColumn("ml_model_name", lit(MODEL_NAME))
            .withColumn("ml_model_version", lit(MODEL_VERSION))
            .withColumn("ml_model_type", lit(MODEL_TYPE))
            .withColumn("ml_feature_risk_hint", col("risk_hint_value"))
            .withColumn("ml_feature_duration_signal", spark_round(col("duration_signal"), 4))
            .withColumn("ml_feature_test_failure_ratio", spark_round(col("test_failure_ratio"), 4))
            .withColumn("ml_feature_low_disk_signal", spark_round(col("low_disk_signal"), 4))
            .withColumn("ml_feature_heat_signal", spark_round(col("heat_signal"), 4))
            .withColumn("ml_feature_deploy_gap_signal", spark_round(col("deploy_gap_signal"), 4))
            .withColumn("ml_feature_http_error_signal", spark_round(col("http_error_signal"), 4))
            .withColumn("ml_feature_cache_miss_signal", spark_round(col("cache_miss_signal"), 4))
        )

        return scored

    def _risk_band(self):
        """Convert a numeric model score into a compact band for dashboards."""

        return (
            when(col("ml_risk_score") >= 0.85, lit("critical"))
            .when(col("ml_risk_score") >= 0.65, lit("high"))
            .when(col("ml_risk_score") >= 0.35, lit("medium"))
            .otherwise(lit("low"))
        )

    def _anomaly_class(self):
        """Choose a readable anomaly class from model features and CI context."""

        return (
            when(col("ml_risk_score") < 0.35, lit("normal"))
            .when(col("failure_category").isNotNull(), concat(lit("known_"), col("failure_category")))
            .when(col("http_error_signal") >= 0.6, lit("jenkins_http_warning"))
            .when(col("test_failure_ratio") > 0.0, lit("test_instability"))
            .when((col("low_disk_signal") > 0.5) | (col("heat_signal") > 0.5), lit("agent_health_risk"))
            .when(col("deploy_gap_signal") > 0.0, lit("deployment_readiness_gap"))
            .when(col("duration_signal") > 0.75, lit("slow_stage"))
            .otherwise(lit("elevated_ci_risk"))
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
