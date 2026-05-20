"""Small Elasticsearch query helpers for the CI/CD observability index."""

from __future__ import annotations

import argparse
import json
from typing import Any

from index_scored_events import IndexerConfig
from index_scored_events import build_es_client


MAX_QUERY_SIZE = 100


def limited_size(size: int) -> int:
    return max(1, min(size, MAX_QUERY_SIZE))


def response_body(response) -> dict[str, Any]:
    return response.body if hasattr(response, "body") else response


def risk_summary(client, index_name: str, minutes: int):
    """Use aggregations for a compact live dashboard summary."""

    return client.search(
        index=index_name,
        size=0,
        query={
            "bool": {
                "filter": [
                    {"range": {"@timestamp": {"gte": f"now-{minutes}m"}}},
                ]
            }
        },
        aggs={
            "risk_bands": {"terms": {"field": "ml_risk_band", "size": 10}},
            "dashboard_categories": {"terms": {"field": "dashboard_category", "size": 10}},
            "notification_levels": {"terms": {"field": "notification_level", "size": 10}},
            "anomaly_classes": {"terms": {"field": "ml_anomaly_class", "size": 10}},
            "alert_types": {"terms": {"field": "ml_alert_type", "size": 10}},
            "signal_domains": {"terms": {"field": "signal_domain", "size": 10}},
            "severity_levels": {"terms": {"field": "severity_level", "size": 10}},
            "pipeline_statuses": {"terms": {"field": "pipeline_status", "size": 10}},
            "ci_stages": {"terms": {"field": "ci_stage", "size": 10}},
            "average_risk_score": {"avg": {"field": "ml_risk_score"}},
            "predictions": {
                "filters": {
                    "filters": {
                        "predictive_alert": {"term": {"ml_predictive_alert": True}},
                        "predicted_failure": {"term": {"ml_failure_prediction": True}},
                        "known_failure": {"term": {"is_failure": True}},
                    }
                }
            },
        },
    )


def recent_high_risk(client, index_name: str, minutes: int, size: int):
    """Fetch a bounded list of high-risk events using exact filters."""

    return client.search(
        index=index_name,
        size=limited_size(size),
        sort=[{"@timestamp": {"order": "desc"}}],
        query={
            "bool": {
                "filter": [
                    {"range": {"@timestamp": {"gte": f"now-{minutes}m"}}},
                    {"terms": {"ml_risk_band": ["high", "critical"]}},
                ]
            }
        },
        _source=[
            "@timestamp",
            "job_name",
            "build_number",
            "ci_stage",
            "ci_status",
            "pipeline_status",
            "ml_risk_score",
            "ml_risk_band",
            "ml_predictive_alert",
            "ml_alert_type",
            "ml_alert_reason",
            "ml_recommended_action",
            "ml_anomaly_class",
            "ml_prediction_target",
            "ml_score_basis",
            "dashboard_category",
            "notification_level",
            "notification_title",
            "notification_message",
            "signal_domain",
            "signal_name",
            "signal_value",
            "severity_level",
            "failure_reason",
        ],
    )


def events_for_job(client, index_name: str, job_name: str, build_number: int | None, size: int):
    filters: list[dict[str, Any]] = [{"term": {"job_name": job_name}}]
    if build_number is not None:
        filters.append({"term": {"build_number": build_number}})

    return client.search(
        index=index_name,
        size=limited_size(size),
        sort=[{"@timestamp": {"order": "desc"}}],
        query={"bool": {"filter": filters}},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Query the CI/CD observability index.")
    parser.add_argument("--minutes", type=int, default=60)
    parser.add_argument("--size", type=int, default=20)
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--high-risk", action="store_true")
    parser.add_argument("--job")
    parser.add_argument("--build", type=int)
    args = parser.parse_args()

    config = IndexerConfig.from_env()
    client = build_es_client(config)

    if args.high_risk:
        response = recent_high_risk(client, config.index_name, args.minutes, args.size)
    elif args.job:
        response = events_for_job(client, config.index_name, args.job, args.build, args.size)
    else:
        response = risk_summary(client, config.index_name, args.minutes)

    print(json.dumps(response_body(response), indent=2, default=str))


if __name__ == "__main__":
    main()
