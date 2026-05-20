"""Failure-prediction model and feature shaping for CI/CD stage warnings."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import concat
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


MODEL_NAME = "tap-ci-stage-failure-logistic-regression"
MODEL_VERSION = "cicd-stage-failure-logreg-v1"
MODEL_TYPE = "Spark MLlib LogisticRegression"
MODEL_TARGET = "stage_failure_warning"

BASELINE_BUILD_COUNT = 1500
TRAINING_SPLIT = 0.8
TESTING_SPLIT = 0.2
TRAIN_TEST_SEED = 20260520
FAILURE_WARNING_THRESHOLD = 0.55

STAGES = ("checkout", "preflight", "build", "test", "package", "deploy")
STAGE_ORDER = {stage: index + 1 for index, stage in enumerate(STAGES)}
STAGE_ORDER["pipeline"] = 7

BUILD_DURATION_LIMIT_MS = 12000.0
TEST_DURATION_LIMIT_MS = 12000.0
ROLLOUT_DURATION_LIMIT_SECONDS = 120.0

CATEGORICAL_FEATURES = (
    ("ci_stage_for_model", "ci_stage_index"),
    ("signal_domain_for_model", "signal_domain_index"),
    ("signal_name_for_model", "signal_name_index"),
)

NUMERIC_FEATURES = (
    "stage_order_value",
    "feature_overall_pressure",
    "feature_scm_pressure",
    "feature_agent_pressure",
    "feature_build_pressure",
    "feature_test_pressure",
    "feature_artifact_pressure",
    "feature_deploy_pressure",
)

FEATURE_COLUMNS = tuple(output for _, output in CATEGORICAL_FEATURES) + NUMERIC_FEATURES

TRAINING_SCHEMA = StructType(
    [
        *[StructField(input_col, StringType(), False) for input_col, _ in CATEGORICAL_FEATURES],
        *[StructField(feature_name, DoubleType(), False) for feature_name in NUMERIC_FEATURES],
        StructField("label", DoubleType(), False),
    ]
)

SIGNAL_DOMAINS = {
    "checkout": "source_control",
    "preflight": "agent_health",
    "build": "build",
    "test": "test_quality",
    "package": "artifact",
    "deploy": "deployment",
    "pipeline": "pipeline",
}


@dataclass(frozen=True)
class StageSample:
    """One generated baseline sample for a single Jenkins pipeline stage."""

    stage: str
    scenario: int
    variant: int


def fit_risk_model(spark):
    """Train the Logistic Regression classifier with a standard 80/20 split."""

    baseline = spark.createDataFrame(generate_training_rows(), TRAINING_SCHEMA)
    train_data, test_data = baseline.randomSplit(
        [TRAINING_SPLIT, TESTING_SPLIT],
        seed=TRAIN_TEST_SEED,
    )

    indexers = [
        StringIndexer(
            inputCol=input_col,
            outputCol=output_col,
            handleInvalid="keep",
        )
        for input_col, output_col in CATEGORICAL_FEATURES
    ]

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep",
    )
    classifier = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        probabilityCol="probability",
        predictionCol="ml_prediction",
        maxIter=40,
        regParam=0.02,
        elasticNetParam=0.0,
    )

    pipeline = Pipeline(stages=[*indexers, assembler, classifier])
    model = pipeline.fit(train_data)
    _log_test_metrics(baseline, train_data, test_data, model.transform(test_data))
    return model


def prepare_features(events):
    """Create non-leaky runtime features expected by the trained pipeline."""

    return (
        events.withColumn("ci_stage_for_model", coalesce(col("ci_stage"), lit("unknown")))
        .withColumn("signal_domain_for_model", coalesce(col("signal_domain"), lit("unknown")))
        .withColumn("signal_name_for_model", coalesce(col("signal_name"), lit("unknown")))
        .withColumn("stage_order_value", coalesce(col("stage_order").cast("double"), lit(0.0)) / lit(7.0))
        .withColumn("feature_overall_pressure", coalesce(col("feature_overall_pressure"), lit(0.0)).cast("double"))
        .withColumn("feature_scm_pressure", coalesce(col("feature_scm_pressure"), lit(0.0)).cast("double"))
        .withColumn("feature_agent_pressure", coalesce(col("feature_agent_pressure"), lit(0.0)).cast("double"))
        .withColumn("feature_build_pressure", coalesce(col("feature_build_pressure"), lit(0.0)).cast("double"))
        .withColumn("feature_test_pressure", coalesce(col("feature_test_pressure"), lit(0.0)).cast("double"))
        .withColumn("feature_artifact_pressure", coalesce(col("feature_artifact_pressure"), lit(0.0)).cast("double"))
        .withColumn("feature_deploy_pressure", coalesce(col("feature_deploy_pressure"), lit(0.0)).cast("double"))
    )


def score_events(model, events):
    """Apply MLlib prediction and publish only warning-oriented fields."""

    scored = (
        model.transform(events)
        .withColumn("_failure_probability", vector_to_array(col("probability")).getItem(1))
        .withColumn(
            "_stage_failure_prediction",
            col("_failure_probability") >= lit(FAILURE_WARNING_THRESHOLD),
        )
        .withColumn("ml_stage_failure_warning", _stage_failure_warning())
        .withColumn("predicted_failure_stage", _predicted_failure_stage())
        .withColumn("warning_level", _warning_level())
        .withColumn("warning_type", _warning_type())
        .withColumn("warning_reason", _warning_reason())
        .withColumn("recommended_action", _recommended_action())
        .withColumn("warning_title", _warning_title())
        .withColumn("warning_message", _warning_message())
        .withColumn("dashboard_category", _dashboard_category())
        .withColumn("ml_scored_at", current_timestamp())
        .withColumn("ml_model_name", lit(MODEL_NAME))
        .withColumn("ml_model_version", lit(MODEL_VERSION))
        .withColumn("ml_model_type", lit(MODEL_TYPE))
        .withColumn("ml_prediction_target", lit(MODEL_TARGET))
    )
    return scored


def generate_training_rows(build_count: int = BASELINE_BUILD_COUNT):
    """Generate synthetic stage samples from the Jenkins demo failure rules."""

    rows = []

    for build_number in range(1, build_count + 1):
        forced_success = build_number % 10 == 0
        for stage in STAGES:
            scenario = 0 if forced_success else _scenario_for_build(build_number, stage)
            rows.append(_stage_training_row(StageSample(stage, scenario, build_number % 5)))

    return tuple(rows)


def _stage_training_row(sample: StageSample):
    features = _stage_features(sample.stage, sample.scenario, sample.variant)
    label = 1.0 if _stage_failure_target(sample.stage, sample.scenario) else 0.0
    return _training_row(sample.stage, _signal_name(sample.stage, sample.scenario), label, **features)


def _training_row(stage: str, signal_name: str, label: float, **features):
    row = {
        "ci_stage_for_model": stage,
        "signal_domain_for_model": SIGNAL_DOMAINS[stage],
        "signal_name_for_model": signal_name,
        "stage_order_value": STAGE_ORDER[stage] / 7.0,
        "feature_overall_pressure": 0.0,
        "feature_scm_pressure": 0.0,
        "feature_agent_pressure": 0.0,
        "feature_build_pressure": 0.0,
        "feature_test_pressure": 0.0,
        "feature_artifact_pressure": 0.0,
        "feature_deploy_pressure": 0.0,
        "label": label,
    }
    row.update(features)
    return row


def _scenario_for_build(build_number: int, stage: str) -> int:
    # The spread covers normal, near-limit, and failing ranges for every stage.
    return (build_number * 7 + STAGE_ORDER[stage] * 3) % 12


def _stage_features(stage: str, scenario: int, variant: int):
    values = {
        "feature_scm_pressure": 0.0,
        "feature_agent_pressure": 0.0,
        "feature_build_pressure": 0.0,
        "feature_test_pressure": 0.0,
        "feature_artifact_pressure": 0.0,
        "feature_deploy_pressure": 0.0,
    }

    if stage == "checkout":
        scm_latency_ms = 900 + scenario * 450 + variant * 35
        retry_count = scenario // 4
        latency_pressure = _bounded((scm_latency_ms - 2500.0) / 2700.0)
        retry_pressure = _bounded(retry_count / 3.0)
        values["feature_scm_pressure"] = max(latency_pressure, retry_pressure)

    if stage == "preflight":
        disk_free_pct = 45 - scenario * 4 - variant
        cpu_temp_c = 56 + scenario * 4 + variant
        disk_pressure = _bounded((25.0 - disk_free_pct) / 25.0)
        heat_pressure = _bounded((cpu_temp_c - 70.0) / 25.0)
        values["feature_agent_pressure"] = max(disk_pressure, heat_pressure)

    if stage == "build":
        compile_time_ms = 4200 + scenario * 900 + variant * 70
        cache_pressure = 1.0 if scenario >= 8 else 0.0
        values["feature_build_pressure"] = max(_bounded(compile_time_ms / BUILD_DURATION_LIMIT_MS), cache_pressure)

    if stage == "test":
        test_duration_ms = 4800 + scenario * 180 + variant * 45
        failing_ratio = 1.0 if scenario >= 9 else 0.0
        near_flaky_pressure = 0.7 if scenario == 8 else 0.0
        duration_pressure = _bounded(test_duration_ms / TEST_DURATION_LIMIT_MS)
        values["feature_test_pressure"] = max(duration_pressure, failing_ratio, near_flaky_pressure)

    if stage == "package":
        artifact_size_mb = 16 + scenario
        values["feature_artifact_pressure"] = _bounded((artifact_size_mb - 18.0) / 6.0)

    if stage == "deploy":
        rollout_seconds = 35 + scenario * 10 + variant * 2
        replica_gap = 1.0 / 3.0 if scenario >= 10 else 0.0
        values["feature_deploy_pressure"] = max(_bounded(rollout_seconds / ROLLOUT_DURATION_LIMIT_SECONDS), replica_gap)

    values["feature_overall_pressure"] = max(values.values())
    return values


def _stage_failure_target(stage: str, scenario: int) -> bool:
    if stage == "checkout":
        return scenario >= 8
    if stage == "preflight":
        return scenario >= 7
    if stage == "build":
        return scenario >= 8
    if stage == "test":
        return scenario >= 8
    if stage == "package":
        return scenario >= 7
    if stage == "deploy":
        return scenario >= 7
    return False


def _signal_name(stage: str, scenario: int) -> str:
    if stage == "checkout":
        return "scm_latency"
    if stage == "preflight":
        return "cpu_temp_c" if scenario >= 7 else "disk_free_pct"
    if stage == "build":
        return "dependency_cache" if scenario >= 8 else "compile_time_ms"
    if stage == "test":
        return "test_failure_ratio" if scenario >= 9 else "test_duration_ms"
    if stage == "package":
        return "artifact_size_mb"
    if stage == "deploy":
        return "replica_readiness_gap" if scenario >= 10 else "rollout_seconds"
    return "unknown"


def _log_test_metrics(baseline, train_data, test_data, predictions) -> None:
    total_rows = baseline.count()
    training_rows = train_data.count()
    testing_rows = test_data.count()
    positive_rows = baseline.filter(col("label") == 1.0).count()
    negative_rows = total_rows - positive_rows

    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    metric_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="ml_prediction",
    )

    auc = binary_evaluator.evaluate(predictions)
    accuracy = metric_evaluator.setMetricName("accuracy").evaluate(predictions)
    precision = metric_evaluator.setMetricName("weightedPrecision").evaluate(predictions)
    recall = metric_evaluator.setMetricName("weightedRecall").evaluate(predictions)

    print(
        "MLlib stage-failure model trained "
        f"rows={total_rows} train={training_rows} test={testing_rows} "
        f"positive={positive_rows} negative={negative_rows} "
        f"auc={auc:.3f} accuracy={accuracy:.3f} precision={precision:.3f} recall={recall:.3f}",
        flush=True,
    )


def _stage_failure_warning():
    return (
        (~coalesce(col("is_failure"), lit(False)))
        & col("_stage_failure_prediction")
        & col("ci_stage").isin(*STAGES)
    )


def _predicted_failure_stage():
    return when(col("ml_stage_failure_warning") | coalesce(col("is_failure"), lit(False)), col("ci_stage"))


def _warning_level():
    return (
        when(coalesce(col("is_failure"), lit(False)), lit("critical"))
        .when(col("ml_stage_failure_warning"), lit("warning"))
        .otherwise(lit("info"))
    )


def _warning_type():
    return (
        when(coalesce(col("is_failure"), lit(False)), lit("observed_stage_failure"))
        .when(col("ml_stage_failure_warning"), lit("predicted_stage_failure"))
        .otherwise(lit("none"))
    )


def _warning_reason():
    return (
        when(coalesce(col("is_failure"), lit(False)), coalesce(col("failure_reason"), lit("pipeline_failure")))
        .when(col("ml_stage_failure_warning"), coalesce(col("alert_reason"), concat(col("signal_name"), lit("_near_failure_limit"))))
    )


def _recommended_action():
    return (
        when(col("signal_domain") == "source_control", lit("check_scm_latency_and_repository_availability"))
        .when(col("signal_domain") == "agent_health", lit("check_agent_disk_space_and_cpu_temperature"))
        .when(col("signal_domain") == "build", lit("check_dependency_cache_and_compile_time"))
        .when(col("signal_domain") == "test_quality", lit("inspect_failing_tests_and_suite_duration"))
        .when(col("signal_domain") == "artifact", lit("verify_artifact_size_and_checksum"))
        .when(col("signal_domain") == "deployment", lit("check_rollout_time_and_replica_readiness"))
        .when(col("signal_domain") == "pipeline", lit("inspect_pipeline_result_and_failed_stage"))
    )


def _warning_title():
    build_label = when(col("build_number").isNotNull(), concat(lit("#"), col("build_number").cast("string")))
    relevant_event = col("ml_stage_failure_warning") | coalesce(col("is_failure"), lit(False))

    return when(
        relevant_event,
        concat_ws(
            " ",
            when(col("ml_stage_failure_warning"), lit("Stage may fail"))
            .when(coalesce(col("is_failure"), lit(False)), lit("Stage failed")),
            coalesce(col("job_name"), lit("unknown-job")),
            build_label,
            coalesce(col("ci_stage"), lit("pipeline")),
        ),
    )


def _warning_message():
    relevant_event = col("ml_stage_failure_warning") | coalesce(col("is_failure"), lit(False))

    return when(
        relevant_event,
        concat_ws(
            " ",
            when(col("ml_stage_failure_warning"), lit("The model predicts a possible failure in"))
            .when(coalesce(col("is_failure"), lit(False)), lit("Observed failure in")),
            coalesce(col("ci_stage"), lit("pipeline")),
            lit("from"),
            coalesce(col("signal_name"), lit("ci_signal")),
            lit("value"),
            coalesce(col("signal_value").cast("string"), lit("unknown")),
            lit("action"),
            coalesce(col("recommended_action"), lit("inspect_pipeline")),
        ),
    )


def _dashboard_category():
    return (
        when(col("ml_stage_failure_warning"), lit("stage_failure_warning"))
        .when(coalesce(col("is_failure"), lit(False)), lit("observed_failure"))
        .otherwise(lit("observability_event"))
    )


def _bounded(value: float) -> float:
    return min(max(value, 0.0), 1.0)
