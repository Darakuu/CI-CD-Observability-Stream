"""Generated training data for the demo MLlib model.

The project is a restartable demo, so there is no baseline dataset.
Instead, this file generates a small baseline that follows the same pipeline used by Jenkins.
"""

from dataclasses import dataclass
from typing import Optional

from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


BASELINE_BUILD_COUNT = 50

BUILD_DURATION_LIMIT_MS = 12000.0
TEST_DURATION_LIMIT_MS = 12000.0
ROLLOUT_DURATION_LIMIT_SECONDS = 120.0

STAGES = ("checkout", "preflight", "build", "test", "package", "deploy")

TRAINING_SCHEMA = StructType(
    [
        StructField("ci_stage_for_model", StringType(), False),
        StructField("ci_status_for_model", StringType(), False),
        StructField("failure_category_for_model", StringType(), False),
        StructField("risk_hint_value", DoubleType(), False),
        StructField("is_failure_signal", DoubleType(), False),
        StructField("duration_signal", DoubleType(), False),
        StructField("test_failure_ratio", DoubleType(), False),
        StructField("low_disk_signal", DoubleType(), False),
        StructField("heat_signal", DoubleType(), False),
        StructField("deploy_gap_signal", DoubleType(), False),
        StructField("http_error_signal", DoubleType(), False),
        StructField("cache_miss_signal", DoubleType(), False),
        StructField("label", DoubleType(), False),
    ]
)


@dataclass(frozen=True)
class BuildOutcome:
    """A deterministic build outcome used to generate baseline training rows."""

    name: str
    failure_stage: Optional[str] = None
    failure_category: Optional[str] = None


OUTCOME_CYCLE = (
    BuildOutcome("success"),
    BuildOutcome("checkout_scm_timeout", "checkout", "source_control"),
    BuildOutcome("success"),
    BuildOutcome("preflight_disk_full", "preflight", "infrastructure"),
    BuildOutcome("preflight_thermal", "preflight", "infrastructure"),
    BuildOutcome("build_dependency_failure", "build", "build"),
    BuildOutcome("success"),
    BuildOutcome("test_flaky_failure", "test", "test"),
    BuildOutcome("success"),
    BuildOutcome("package_checksum_failure", "package", "package"),
    BuildOutcome("deploy_rollout_timeout", "deploy", "deployment"),
    BuildOutcome("success"),
)


def generate_baseline_training_rows(build_count: int = BASELINE_BUILD_COUNT):
    """Generate feature rows from a deterministic Jenkins demo baseline."""

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
        rows.append(_simulation_row(stage))

        if outcome.failure_stage == stage:
            rows.append(_failure_row(stage, outcome))
            failed = True
            break

        rows.append(_success_row(stage, build_number, forced_success))

    rows.extend(_pipeline_rows(failed))
    return rows


def _training_row(
    stage: str,
    status: str,
    category: str = "none",
    risk_hint: Optional[float] = None,
    is_failure: bool = False,
    label: float = 0.0,
    **signals,
):
    row = {
        "ci_stage_for_model": stage,
        "ci_status_for_model": status,
        "failure_category_for_model": category,
        "risk_hint_value": _risk_hint(stage, is_failure) if risk_hint is None else risk_hint,
        "is_failure_signal": 1.0 if is_failure else 0.0,
        "duration_signal": 0.0,
        "test_failure_ratio": 0.0,
        "low_disk_signal": 0.0,
        "heat_signal": 0.0,
        "deploy_gap_signal": 0.0,
        "http_error_signal": 0.0,
        "cache_miss_signal": 0.0,
        "label": label,
    }
    row.update(signals)
    return row


def _simulation_row(stage: str):
    return _training_row(stage, "unknown")


def _success_row(stage: str, build_number: int, forced_success: bool):
    status = "passed" if stage == "test" else "success"
    scenario = _normal_scenario(build_number, stage)
    signals = {}

    if stage == "preflight":
        disk_free_pct = 42 if forced_success else 35 + scenario
        cpu_temp_c = 58 if forced_success else 55 + scenario
        signals["low_disk_signal"] = _low_disk_signal(disk_free_pct)
        signals["heat_signal"] = _heat_signal(cpu_temp_c)

    if stage == "build":
        compile_time_ms = 4200 if forced_success else 3800 + scenario * 250
        signals["duration_signal"] = _duration_signal(compile_time_ms, BUILD_DURATION_LIMIT_MS)

    if stage == "test":
        test_duration_ms = 5100 if forced_success else 4800 + scenario * 180
        signals["duration_signal"] = _duration_signal(test_duration_ms, TEST_DURATION_LIMIT_MS)

    if stage == "deploy":
        rollout_seconds = 35 if forced_success else 30 + scenario * 2
        signals["duration_signal"] = _duration_signal(rollout_seconds, ROLLOUT_DURATION_LIMIT_SECONDS)

    return _training_row(stage, status, **signals)


def _failure_row(stage: str, outcome: BuildOutcome):
    signals = {
        "low_disk_signal": 1.0 if outcome.name == "preflight_disk_full" else 0.0,
        "heat_signal": 1.0 if outcome.name == "preflight_thermal" else 0.0,
        "cache_miss_signal": 1.0 if outcome.name == "build_dependency_failure" else 0.0,
        "test_failure_ratio": 1.0 if outcome.name == "test_flaky_failure" else 0.0,
        "deploy_gap_signal": 1.0 if outcome.name == "deploy_rollout_timeout" else 0.0,
    }

    return _training_row(
        stage,
        "failed",
        category=outcome.failure_category or "other",
        is_failure=True,
        label=1.0,
        **signals,
    )


def _pipeline_rows(failed: bool):
    if failed:
        return (
            _training_row("pipeline", "failed", category="pipeline", is_failure=True, label=1.0),
            _training_row("pipeline", "failed", category="pipeline", is_failure=True, label=1.0),
        )

    return (
        _training_row("pipeline", "success", risk_hint=0.15),
        _training_row("pipeline", "success", risk_hint=0.15),
    )


def _normal_scenario(build_number: int, stage: str) -> int:
    scenario = (build_number * 3 + STAGES.index(stage) * 2) % 12
    blocked = _failure_scenarios(stage)

    while scenario in blocked:
        scenario = (scenario + 1) % 12

    return scenario


def _failure_scenarios(stage: str):
    return {
        "checkout": {1},
        "preflight": {2, 4},
        "build": {5},
        "test": {7},
        "package": {8},
        "deploy": {10},
    }.get(stage, set())


def _risk_hint(stage: str, failed: bool) -> float:
    if failed:
        return 1.0

    if stage in {"deploy", "package"}:
        return 0.3

    if stage:
        return 0.15

    return 0.05


def _duration_signal(value: float, limit: float) -> float:
    return min(value / limit, 1.0)


def _low_disk_signal(disk_free_pct: int) -> float:
    return min(max((25.0 - disk_free_pct) / 25.0, 0.0), 1.0)


def _heat_signal(cpu_temp_c: int) -> float:
    return min(max((cpu_temp_c - 70.0) / 25.0, 0.0), 1.0)
