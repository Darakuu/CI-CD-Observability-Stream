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


def warning_summary(client, index_name: str, minutes: int):
    """Use aggregations for a compact live warning summary."""

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
            "dashboard_categories": {"terms": {"field": "dashboard_category", "size": 10}},
            "warning_levels": {"terms": {"field": "warning_level", "size": 10}},
            "warning_types": {"terms": {"field": "warning_type", "size": 10}},
            "warning_reasons": {"terms": {"field": "warning_reason", "size": 10}},
            "signal_domains": {"terms": {"field": "signal_domain", "size": 10}},
            "severity_levels": {"terms": {"field": "severity_level", "size": 10}},
            "pipeline_statuses": {"terms": {"field": "pipeline_status", "size": 10}},
            "ci_stages": {"terms": {"field": "ci_stage", "size": 10}},
            "warnings": {
                "filters": {
                    "filters": {
                        "stage_failure_warning": {"term": {"ml_stage_failure_warning": True}},
                        "known_failure": {"term": {"is_failure": True}},
                    }
                }
            },
        },
    )


def recent_warnings(client, index_name: str, minutes: int, size: int):
    """Fetch a bounded list of warning and failure events using exact filters."""

    return client.search(
        index=index_name,
        size=limited_size(size),
        sort=[{"@timestamp": {"order": "desc"}}],
        query={
            "bool": {
                "filter": [
                    {"range": {"@timestamp": {"gte": f"now-{minutes}m"}}},
                    {"terms": {"dashboard_category": ["stage_failure_warning", "observed_failure"]}},
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
            "ml_stage_failure_warning",
            "predicted_failure_stage",
            "warning_level",
            "warning_type",
            "warning_title",
            "warning_message",
            "warning_reason",
            "recommended_action",
            "ml_prediction_target",
            "dashboard_category",
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
    parser.add_argument("--warnings", action="store_true")
    parser.add_argument("--job")
    parser.add_argument("--build", type=int)
    args = parser.parse_args()

    config = IndexerConfig.from_env()
    client = build_es_client(config)

    if args.warnings:
        response = recent_warnings(client, config.index_name, args.minutes, args.size)
    elif args.job:
        response = events_for_job(client, config.index_name, args.job, args.build, args.size)
    else:
        response = warning_summary(client, config.index_name, args.minutes)

    print(json.dumps(response_body(response), indent=2, default=str))


if __name__ == "__main__":
    main()
