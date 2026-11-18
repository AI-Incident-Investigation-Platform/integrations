from uuid import uuid7

import pytest

from src.api.topology import observation, parse_trace


def test_trace_edges_require_project_and_environment_tags() -> None:
    project, environment, source, target = uuid7(), uuid7(), uuid7(), uuid7()

    def batch(service_id: str, span_id: str, parent: str = "") -> dict:
        return {
            "resource": {
                "attributes": [
                    {"key": "project.id", "value": {"stringValue": str(project)}},
                    {"key": "environment.id", "value": {"stringValue": str(environment)}},
                    {"key": "service.id", "value": {"stringValue": service_id}},
                ]
            },
            "scopeSpans": [
                {
                    "spans": [
                        {
                            "spanId": span_id,
                            "parentSpanId": parent,
                            "startTimeUnixNano": "1000000000",
                            "endTimeUnixNano": "2000000000",
                        }
                    ]
                }
            ],
        }

    data = {"batches": [batch(str(source), "a"), batch(str(target), "b", "a")]}
    spans, edges = parse_trace(data, project, environment)
    assert len(spans) == 2
    assert [(edge["from_service_id"], edge["to_service_id"]) for edge in edges] == [
        (str(source), str(target))
    ]
    with pytest.raises(ValueError):
        parse_trace(data, uuid7(), environment)


def test_observation_uses_chronological_window_and_latest_edge_time() -> None:
    project, environment, source, target = uuid7(), uuid7(), uuid7(), uuid7()

    def batch(service: str, span_id: str, parent: str, started: int) -> dict:
        return {
            "resource": {
                "attributes": [
                    {"key": "project.id", "value": {"stringValue": str(project)}},
                    {"key": "environment.id", "value": {"stringValue": str(environment)}},
                    {"key": "service.id", "value": {"stringValue": service}},
                ]
            },
            "scopeSpans": [
                {
                    "spans": [
                        {
                            "spanId": span_id,
                            "parentSpanId": parent,
                            "startTimeUnixNano": str(started),
                            "endTimeUnixNano": str(started + 1),
                        }
                    ]
                }
            ],
        }

    data = {
        "batches": [
            batch(str(source), "root", "", 3_000_000_000),
            batch(str(target), "new", "root", 4_000_000_000),
            batch(str(target), "old", "root", 2_000_000_000),
        ]
    }
    spans, edges = parse_trace(data, project, environment)
    payload = observation(spans, edges)
    assert payload["window"] == {
        "from": "1970-01-01T00:00:02+00:00",
        "to": "1970-01-01T00:00:04+00:00",
    }
    assert payload["edges"][0]["last_observed_at"] == "1970-01-01T00:00:04+00:00"


def test_trace_rejects_oversized_batch_set_instead_of_ignoring_unchecked_resources() -> None:
    project, environment = uuid7(), uuid7()
    data: dict = {"batches": [{"resource": {"attributes": []}}] * 101}
    with pytest.raises(ValueError, match="limit"):
        parse_trace(data, project, environment)
