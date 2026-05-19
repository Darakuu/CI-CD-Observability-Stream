"""Small MLlib model used to score CI/CD observability events."""

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler

from mllib_processor.baseline_data import TRAINING_SCHEMA
from mllib_processor.baseline_data import generate_baseline_training_rows


MODEL_NAME = "tap-ci-risk-logistic-regression"
MODEL_VERSION = "fakedatamodel_v1"
MODEL_TYPE = "Spark MLlib LogisticRegression"

CATEGORICAL_FEATURES = (
    ("ci_stage_for_model", "ci_stage_index"),
    ("ci_status_for_model", "ci_status_index"),
    ("failure_category_for_model", "failure_category_index"),
)

NUMERIC_FEATURES = (
    "risk_hint_value",
    "is_failure_signal",
    "duration_signal",
    "test_failure_ratio",
    "low_disk_signal",
    "heat_signal",
    "deploy_gap_signal",
    "http_error_signal",
    "cache_miss_signal",
)

FEATURE_COLUMNS = tuple(output for _, output in CATEGORICAL_FEATURES) + NUMERIC_FEATURES


def fit_risk_model(spark):
    """Fit the small baseline classifier used by the streaming scoring job."""

    training = spark.createDataFrame(generate_baseline_training_rows(), TRAINING_SCHEMA)

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
        maxIter=25,
        regParam=0.05,
    )

    return Pipeline(stages=[*indexers, assembler, classifier]).fit(training)
