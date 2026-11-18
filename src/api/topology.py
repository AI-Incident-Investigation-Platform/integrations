"""Extract bounded service edges from project and environment scoped Tempo traces."""

from datetime import UTC, datetime
from uuid import UUID


class TraceScopeError(ValueError):
    pass


def observation(spans: list[dict], edges: list[dict]) -> dict:
    times = [span["started_at"] for span in spans]
    return {"edges": edges, "window": {"from": min(times), "to": max(times)}}


def attributes(items: list[dict]) -> dict[str, str]:
    return {
        str(item.get("key")): str(item.get("value", {}).get("stringValue", "")) for item in items
    }


def parse_trace(
    data: dict, project_id: UUID, environment_id: UUID
) -> tuple[list[dict], list[dict]]:
    spans: list[dict] = []
    parents: dict[str, str] = {}
    batches = data.get("batches", data.get("resourceSpans", []))
    if len(batches) > 100:
        raise ValueError("Trace resource limit exceeded")
    for batch in batches:
        tags = attributes(batch.get("resource", {}).get("attributes", []))
        if tags.get("project.id") != str(project_id) or tags.get("environment.id") != str(
            environment_id
        ):
            raise TraceScopeError("Trace does not belong to project environment")
        try:
            service_id = UUID(tags["service.id"])
        except (KeyError, ValueError) as exc:
            raise ValueError("Trace resource lacks service id") from exc
        for group in batch.get("scopeSpans", batch.get("instrumentationLibrarySpans", [])):
            for span in group.get("spans", []):
                if len(spans) >= 1000:
                    raise ValueError("Trace span limit exceeded")
                span_id = str(span.get("spanId", ""))
                parent = str(span.get("parentSpanId", "")) or None
                started = int(span.get("startTimeUnixNano", 0))
                ended = int(span.get("endTimeUnixNano", started))
                spans.append(
                    {
                        "span_id": span_id,
                        "parent_span_id": parent,
                        "service_id": str(service_id),
                        "operation": str(span.get("name", ""))[:300],
                        "started_at": datetime.fromtimestamp(started / 1e9, UTC).isoformat(),
                        "duration_ms": max(0, (ended - started) / 1e6),
                        "status": str(span.get("status", {}).get("code", "unknown")),
                    }
                )
                if parent:
                    parents[span_id] = parent
    if not spans:
        raise ValueError("Trace has no scoped spans")
    by_id = {span["span_id"]: span for span in spans}
    edges: dict[tuple[str, str], dict] = {}
    for span in spans:
        parent_span = by_id.get(parents.get(span["span_id"], ""))
        if parent_span is None or parent_span["service_id"] == span["service_id"]:
            continue
        key = (parent_span["service_id"], span["service_id"])
        if key not in edges or span["started_at"] > edges[key]["last_observed_at"]:
            edges[key] = {
                "from_service_id": key[0],
                "to_service_id": key[1],
                "last_observed_at": span["started_at"],
            }
        if len(edges) >= 500:
            break
    return spans, list(edges.values())
