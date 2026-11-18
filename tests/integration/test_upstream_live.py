import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import httpx
import pytest

from src.api.explorer import Explorer
from src.api.schemas import LogSearchRequest
from src.security import CredentialCipher


class Access:
    async def require(self, user: object, project_id: object, permission: str) -> object:
        return object()


def make_explorer(client: httpx.AsyncClient, endpoint_url: str) -> Explorer:
    explorer = Explorer(
        None,  # type: ignore[arg-type]
        Access(),  # type: ignore[arg-type]
        client,
        CredentialCipher({"v1": bytes(range(32))}, "v1"),
    )

    async def endpoint(project_id: object, environment_id: object, kind: str) -> tuple[str, dict]:
        return endpoint_url, {}

    explorer._endpoint = endpoint  # type: ignore[method-assign]
    return explorer


@pytest.mark.asyncio
async def test_loki_live_cursor_pages_real_query_range() -> None:
    endpoint = os.environ.get("TEST_LOKI_URL")
    if not endpoint:
        pytest.skip("Requires isolated Loki container")
    project_id, environment_id, service_id = uuid7(), uuid7(), uuid7()
    marker = str(uuid7())
    now = datetime.now(UTC)
    stamps = [int((now - timedelta(seconds=index + 5)).timestamp() * 1e9) for index in range(4)]
    entries = [[str(stamp), f"{marker} entry-{index}"] for index, stamp in enumerate(stamps)]
    labels = {
        "project_id": str(project_id),
        "environment_id": str(environment_id),
        "service_id": str(service_id),
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            endpoint + "/loki/api/v1/push",
            json={"streams": [{"stream": labels, "values": entries}]},
        )
        assert response.is_success, response.text
        explorer = make_explorer(client, endpoint)
        body = LogSearchRequest.model_validate(
            {
                "project_id": project_id,
                "environment_id": environment_id,
                "service_ids": [service_id],
                "from": now - timedelta(minutes=1),
                "to": now,
                "query": marker,
                "limit": 2,
            }
        )
        first = await explorer.logs(None, body)  # type: ignore[arg-type]
        assert len(first["items"]) == 2
        second = await explorer.logs(
            None,  # type: ignore[arg-type]
            body.model_copy(update={"cursor": first["next_cursor"]}),
        )
    assert [item["message"] for item in first["items"] + second["items"]] == [
        f"{marker} entry-{index}" for index in range(4)
    ]
    assert second["next_cursor"] is None


@pytest.mark.asyncio
async def test_tempo_live_search_validates_and_pages_real_traces() -> None:
    query_url = os.environ.get("TEST_TEMPO_URL")
    ingest_url = os.environ.get("TEST_TEMPO_OTLP_URL")
    if not query_url or not ingest_url:
        pytest.skip("Requires isolated Tempo container with OTLP HTTP")
    project_id, environment_id, service_id = uuid7(), uuid7(), uuid7()
    now = datetime.now(UTC).replace(microsecond=0)
    start = now - timedelta(minutes=3)
    resources = []
    trace_base = uuid7().int - 1
    for index in range(102):
        timestamp = int((now - timedelta(seconds=index + 5)).timestamp() * 1e9)
        trace_bytes = (trace_base + index + 1).to_bytes(16, "big")
        span_bytes = (index + 1).to_bytes(8, "big")
        resources.append(
            {
                "resource": {
                    "attributes": [
                        {"key": "project.id", "value": {"stringValue": str(project_id)}},
                        {"key": "environment.id", "value": {"stringValue": str(environment_id)}},
                        {"key": "service.id", "value": {"stringValue": str(service_id)}},
                        {"key": "service.name", "value": {"stringValue": "aiip-cursor-test"}},
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": trace_bytes.hex(),
                                "spanId": span_bytes.hex(),
                                "name": "cursor-test",
                                "startTimeUnixNano": str(timestamp),
                                "endTimeUnixNano": str(timestamp + 1_000_000),
                            }
                        ]
                    }
                ],
            }
        )
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(ingest_url + "/v1/traces", json={"resourceSpans": resources})
        assert response.is_success, response.text
        for _ in range(90):
            search = await client.get(
                query_url + "/api/search",
                params={
                    "tags": (
                        f"project.id={project_id} environment.id={environment_id} "
                        f"service.id={service_id}"
                    ),
                    "start": int(start.timestamp()),
                    "end": int(now.timestamp()),
                    "limit": 200,
                },
            )
            search.raise_for_status()
            if len(search.json().get("traces", [])) >= 102:
                break
            await asyncio.sleep(1)
        else:
            pytest.fail("Tempo did not index test traces within 90 seconds")
        explorer = make_explorer(client, query_url)
        ids: list[str] = []
        cursor = None
        for _ in range(4):
            page = await explorer.traces(
                None,  # type: ignore[arg-type]
                project_id,
                environment_id,
                service_id,
                start,
                now,
                cursor,
            )
            ids.extend(item["trace_id"] for item in page["items"])
            cursor = page["next_cursor"]
            if cursor is None:
                break
    assert len(ids) == 102
    assert len(set(ids)) == 102
    assert cursor is None
