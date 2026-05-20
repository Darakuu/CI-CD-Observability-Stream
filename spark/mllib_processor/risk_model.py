"""Risk model and feature shaping for CI/CD predictive alerts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import concat
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import when
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


MODEL_NAME = "tap-ci-failure-risk-logistic-regression"
MODEL_VERSION = "cicd-risk-logreg-v4"
MODEL_TYPE = "Spark MLlib LogisticRegression"
MODEL_TARGET = "failure_risk_from_stage_pressure"
MODEL_SCORE_BASIS = "model_probability_no_status_leakage"

BASELINE_BUILD_COUNT = 72
PREDICTIVE_ALERT_THRESHOLD = 0.65

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
class BuildOutcome:
    """One deterministic training scenario for a synthetic Jenkins build."""

    name: str
    failure_stage: Optional[str] = None


OUTCOME_CYCLE = (
    BuildOutcome("success"),
    BuildOutcome("checkout_scm_timeout", "checkout"),
    BuildOutcome("success"),
    BuildOutcome("preflight_disk_full", "preflight"),
    BuildOutcome("preflight_thermal", "preflight"),
    BuildOutcome("build_dependency_failure", "build"),
    BuildOutcome("near_miss_success"),
    BuildOutcome("test_flaky_failure", "test"),
    BuildOutcome("success"),
    BuildOutcome("package_checksum_failure", "package"),
    BuildOutcome("deploy_rollout_timeout", "deploy"),
    BuildOutcome("near_miss_success"),
)


def fit_risk_model(spark):
    """Train the small MLlib classifier used by the streaming scoring job."""

    training = spark.createDataFrame(generate_training_rows(), TRAINING_SCHEMA)
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
        maxIter=30,
        regParam=0.03,
    )

    return Pipeline(stages=[*indexers, assembler, classifier]).fit(training)


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
    """Apply MLlib scoring and add fields that Kibana can filter directly."""

    return (
        model.transform(events)
        .withColumn(
            "ml_model_probability",
            spark_round(vector_to_array(col("probability")).getItem(1), 4),
        )
        .withColumn("ml_risk_score", col("ml_model_probability"))
        .withColumn("ml_failure_prediction", col("ml_risk_score") >= lit(PREDICTIVE_ALERT_THRESHOLD))
        .withColumn("ml_predictive_alert", _predictive_alert())
        .withColumn("ml_risk_band", _risk_band())
        .withColumn("ml_alert_type", _alert_type())
        .withColumn("ml_alert_reason", _alert_reason())
        .withColumn("ml_recommended_action", _recommended_action())
        .withColumn("ml_anomaly_class", _anomaly_class())
        .withColumn("dashboard_category", _dashboard_category())
        .withColumn("notification_level", _notification_level())
        .withColumn("notification_title", _notification_title())
        .withColumn("notification_message", _notification_message())
        .withColumn("ml_scored_at", current_timestamp())
        .withColumn("ml_model_name", lit(MODEL_NAME))
        .withColumn("ml_model_version", lit(MODEL_VERSION))
        .withColumn("ml_model_type", lit(MODEL_TYPE))
        .withColumn("ml_prediction_target", lit(MODEL_TARGET))
        .withColumn("ml_score_basis", lit(MODEL_SCORE_BASIS))
        .withColumn("ml_feature_overall_pressure", spark_round(col("feature_overall_pressure"), 4))
    )


def generate_training_rows(build_count: int = BASELINE_BUILD_COUNT):
    """Generate synthetic historical CI events that mirror the Jenkins demo."""

    rows = []

    for build_number in range(1, build_count + 1):
        forced_success = build_number % 10 == 0
        outcome = BuildOutcome("forced_success") if forced_success else _outcome_for_build(build_number)
        rows.extend(_build_rows(build_number, outcome, forced_success))

    return tuple(rows)


def _outcome_for_build(build_number: int) -> BuildOutcome:
    return OUTCOME_CYCLE[(build_number - 1) % len(OUTCOME_CYCLE)]


def _build_rows(build_number: int, outcome: BuildOutcome, forced_success: bool):
    rows = []
    failed = False

    for stage in STAGES:
        if outcome.failure_stage == stage:
            rows.append(_failure_row(stage, outcome))
            failed = True
            break

        rows.append(_stage_result_row(stage, build_number, outcome, forced_success))

    rows.extend(_pipeline_rows(failed))
    return rows


def _stage_result_row(stage: str, build_number: int, outcome: BuildOutcome, forced_success: bool):
    near_miss = outcome.name == "near_miss_success"
    scenario = 8 if near_miss else _normal_scenario(build_number, stage)

    if outcome.failure_stage and _stage_before_failure(stage, outcome.failure_stage):
        scenario = max(scenario, _pre_failure_scenario(build_number, stage, outcome.failure_stage))

    features = _stage_features(stage, scenario, forced_success)
    label = _risk_label(features)
    return _training_row(stage, _signal_name(stage, scenario, outcome), label, **features)


def _failure_row(stage: str, outcome: BuildOutcome):
    features = {
        "feature_overall_pressure": 1.0,
        "feature_scm_pressure": 1.0 if stage == "checkout" else 0.0,
        "feature_agent_pressure": 1.0 if stage == "preflight" else 0.0,
        "feature_build_pressure": 1.0 if stage == "build" else 0.0,
        "feature_test_pressure": 1.0 if stage == "test" else 0.0,
        "feature_artifact_pressure": 1.0 if stage == "package" else 0.0,
        "feature_deploy_pressure": 1.0 if stage == "deploy" else 0.0,
    }
    return _training_row(stage, _failure_signal_name(stage, outcome), 1.0, **features)


def _pipeline_rows(failed: bool):
    pressure = 0.85 if failed else 0.05
    return (
        _training_row(
            "pipeline",
            "pipeline_status",
            1.0 if failed else 0.0,
            feature_overall_pressure=pressure,
        ),
    )


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


def _stage_features(stage: str, scenario: int, forced_success: bool):
    if forced_success:
        scenario = 0

    values = {
        "feature_scm_pressure": 0.0,
        "feature_agent_pressure": 0.0,
        "feature_build_pressure": 0.0,
        "feature_test_pressure": 0.0,
        "feature_artifact_pressure": 0.0,
        "feature_deploy_pressure": 0.0,
    }

    if stage == "checkout":
        latency_pressure = _bounded((900 + scenario * 450 - 2500.0) / 2700.0)
        retry_pressure = _bounded((scenario // 4) / 3.0)
        values["feature_scm_pressure"] = max(latency_pressure, retry_pressure)

    if stage == "preflight":
        disk_free_pct = 45 - scenario * 4
        cpu_temp_c = 56 + scenario * 4
        values["feature_agent_pressure"] = max(
            _bounded((25.0 - disk_free_pct) / 25.0),
            _bounded((cpu_temp_c - 70.0) / 25.0),
        )

    if stage == "build":
        compile_time_ms = 4200 + scenario * 900
        cache_miss = 1.0 if scenario >= 8 else 0.0
        values["feature_build_pressure"] = max(_bounded(compile_time_ms / BUILD_DURATION_LIMIT_MS), cache_miss)

    if stage == "test":
        test_duration_ms = 4800 + scenario * 180
        failing_ratio = 1.0 if scenario >= 9 else 0.0
        values["feature_test_pressure"] = max(_bounded(test_duration_ms / TEST_DURATION_LIMIT_MS), failing_ratio)

    if stage == "package":
        artifact_size_mb = 16 + scenario
        values["feature_artifact_pressure"] = _bounded((artifact_size_mb - 18.0) / 6.0)

    if stage == "deploy":
        rollout_seconds = 35 + scenario * 10
        replica_gap = 1.0 / 3.0 if scenario >= 10 else 0.0
        values["feature_deploy_pressure"] = max(_bounded(rollout_seconds / ROLLOUT_DURATION_LIMIT_SECONDS), replica_gap)

    values["feature_overall_pressure"] = max(values.values())
    return values


def _normal_scenario(build_number: int, stage: str) -> int:
    return (build_number * 2 + STAGES.index(stage)) % 6


def _pre_failure_scenario(build_number: int, stage: str, failure_stage: str) -> int:
    distance = STAGES.index(failure_stage) - STAGES.index(stage)
    if distance <= 1:
        return 8
    return 5 + (build_number % 2)


def _stage_before_failure(stage: str, failure_stage: str) -> bool:
    return STAGES.index(stage) < STAGES.index(failure_stage)


def _risk_label(features) -> float:
    return 1.0 if features["feature_overall_pressure"] >= 0.65 else 0.0


def _signal_name(stage: str, scenario: int, outcome: BuildOutcome) -> str:
    if stage == "checkout":
        return "scm_latency"
    if stage == "preflight":
        if outcome.name == "preflight_disk_full":
            return "disk_free_pct"
        if outcome.name == "preflight_thermal":
            return "cpu_temp_c"
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


def _failure_signal_name(stage: str, outcome: BuildOutcome) -> str:
    if outcome.name == "preflight_disk_full":
        return "disk_free_pct"
    if outcome.name == "preflight_thermal":
        return "cpu_temp_c"
    if outcome.name == "build_dependency_failure":
        return "dependency_cache"
    if outcome.name == "test_flaky_failure":
        return "test_failure_ratio"
    if outcome.name == "deploy_rollout_timeout":
        return "replica_readiness_gap"
    return _signal_name(stage, 11, outcome)


def _predictive_alert():
    return (
        (~coalesce(col("is_failure"), lit(False)))
        & (col("ml_risk_score") >= lit(PREDICTIVE_ALERT_THRESHOLD))
        & col("ci_stage").isNotNull()
    )


def _risk_band():
    return (
        when(col("ml_risk_score") >= 0.85, lit("critical"))
        .when(col("ml_risk_score") >= 0.65, lit("high"))
        .when(col("ml_risk_score") >= 0.35, lit("medium"))
        .otherwise(lit("low"))
    )


def _alert_type():
    return (
        when(coalesce(col("is_failure"), lit(False)), concat(lit("known_"), coalesce(col("failure_category"), lit("failure"))))
        .when(col("ml_predictive_alert"), coalesce(col("alert_type"), concat(lit("predictive_"), col("signal_domain"))))
    )


def _alert_reason():
    return (
        when(coalesce(col("is_failure"), lit(False)), coalesce(col("failure_reason"), lit("pipeline_failure")))
        .when(col("ml_predictive_alert"), coalesce(col("alert_reason"), lit("model_probability_above_alert_threshold")))
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


def _anomaly_class():
    return (
        when(coalesce(col("is_failure"), lit(False)), concat(lit("known_"), coalesce(col("failure_category"), lit("failure"))))
        .when(col("ml_predictive_alert"), concat(lit("predictive_"), coalesce(col("signal_domain"), lit("ci_risk"))))
        .when(col("ml_risk_score") < 0.35, lit("normal"))
        .when(col("signal_domain").isNotNull(), concat(lit("elevated_"), col("signal_domain")))
        .otherwise(lit("elevated_ci_risk"))
    )


def _dashboard_category():
    return (
        when(col("ml_predictive_alert"), lit("predictive_alert"))
        .when(coalesce(col("is_failure"), lit(False)), lit("failure_event"))
        .otherwise(lit("observability_event"))
    )


def _notification_level():
    return (
        when(coalesce(col("is_failure"), lit(False)), lit("critical"))
        .when(col("ml_risk_band") == "critical", lit("critical"))
        .when((col("ml_predictive_alert")) | (col("ml_risk_band") == "high"), lit("warning"))
        .otherwise(lit("info"))
    )


def _notification_title():
    build_label = when(col("build_number").isNotNull(), concat(lit("#"), col("build_number").cast("string")))

    return concat_ws(
        " ",
        when(col("ml_predictive_alert"), lit("Predictive alert"))
        .when(coalesce(col("is_failure"), lit(False)), lit("CI/CD failure"))
        .otherwise(lit("CI/CD event")),
        coalesce(col("job_name"), lit("unknown-job")),
        build_label,
        coalesce(col("ci_stage"), lit("pipeline")),
    )


def _notification_message():
    return concat_ws(
        " ",
        lit("Model risk"),
        coalesce(col("ml_risk_band"), lit("unknown")),
        lit("for"),
        coalesce(col("signal_domain"), lit("ci")),
        coalesce(col("signal_name"), lit("signal")),
        lit("because"),
        coalesce(col("ml_alert_reason"), col("failure_reason"), col("alert_reason"), lit("no_specific_reason")),
        lit("action"),
        coalesce(col("ml_recommended_action"), lit("inspect_pipeline")),
    )


def _bounded(value: float) -> float:
    return min(max(value, 0.0), 1.0)
