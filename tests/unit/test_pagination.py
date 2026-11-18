from datetime import UTC, datetime, timedelta
from uuid import uuid7

import httpx
import pytest

from src.api.explorer import Explorer, normalize_trace_id
from src.api.schemas import LogSearchRequest
from src.security import CredentialCipher


class Access:
    async def require(self, user: object, project_id: object, permission: str) -> object:
        return object()


def explorer_with(client: httpx.AsyncClient) -> Explorer:
    explorer = Explorer(
        None,  # type: ignore[arg-type]
        Access(),  # type: ignore[arg-type]
        client,
        CredentialCipher({"v1": bytes(range(32))}, "v1"),
    )

    async def endpoint(project_id: object, environment_id: object, kind: str) -> tuple[str, dict]:
        return "https://example.com", {}

    explorer._endpoint = endpoint  # type: ignore[method-assign]
    return explorer


def test_tempo_short_trace_id_is_normalized_for_detail_lookup() -> None:
    assert normalize_trace_id("1") == "0" * 31 + "1"
    assert normalize_trace_id("not-a-trace") is None


@pytest.mark.asyncio
async def test_loki_cursor_keeps_same_timestamp_entries() -> None:
    project_id, environment_id, service_id = uuid7(), uuid7(), uuid7()
    end = datetime.now(UTC).replace(microsecond=0)
    tied = int((end - timedelta(seconds=2)).timestamp() * 1e9)
    older = tied - 1_000_000_000
    entries = [[str(tied), message] for message in ("a", "b", "c")]
    entries.append([str(older), "d"])

    def respond(request: httpx.Request) -> httpx.Response:
        boundary = int(request.url.params["end"])
        visible = [entry for entry in entries if int(entry[0]) < boundary]
        return httpx.Response(
            200,
            json={
                "data": {
                    "result": [
                        {
                            "stream": {
                                "project_id": str(project_id),
                                "environment_id": str(environment_id),
                                "service_id": str(service_id),
                            },
                            "values": visible[: int(request.url.params["limit"])],
                        }
                    ]
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        explorer = explorer_with(client)
        body = LogSearchRequest.model_validate(
            {
                "project_id": project_id,
                "environment_id": environment_id,
                "service_ids": [service_id],
                "from": end - timedelta(minutes=1),
                "to": end,
                "query": "error",
                "limit": 2,
            }
        )
        first = await explorer.logs(None, body)  # type: ignore[arg-type]
        second = await explorer.logs(
            None,  # type: ignore[arg-type]
            body.model_copy(update={"cursor": first["next_cursor"]}),
        )
    assert [item["message"] for item in first["items"]] == ["a", "b"]
    assert [item["message"] for item in second["items"]] == ["c", "d"]
    assert second["next_cursor"] is None
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        explorer = explorer_with(client)
        with pytest.raises(ValueError, match="Cursor does not match"):
            await explorer.logs(
                None,  # type: ignore[arg-type]
                body.model_copy(update={"query": "other", "cursor": first["next_cursor"]}),
            )


@pytest.mark.asyncio
async def test_tempo_cursor_validates_each_trace_and_pages_time_segments() -> None:
    project_id, environment_id, service_id = uuid7(), uuid7(), uuid7()
    end = datetime.now(UTC).replace(microsecond=0)
    start = end - timedelta(seconds=120)
    traces = [
        {
            "traceID": f"{index + 1:016x}",
            "startTimeUnixNano": str((int(end.timestamp()) - 1 - index) * 1_000_000_000),
        }
        for index in range(102)
    ]

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/search":
            low, high = int(request.url.params["start"]), int(request.url.params["end"])
            visible = [
                trace
                for trace in traces
                if low <= int(trace["startTimeUnixNano"]) // 1_000_000_000 < high
            ]
            return httpx.Response(200, json={"traces": visible[:101]})
        trace_id = request.url.path.rsplit("/", 1)[-1]
        foreign = trace_id == traces[4]["traceID"].zfill(32)
        attrs = {
            "project.id": str(uuid7() if foreign else project_id),
            "environment.id": str(environment_id),
            "service.id": str(service_id),
        }
        return httpx.Response(
            200,
            json={
                "batches": [
                    {
                        "resource": {
                            "attributes": [
                                {"key": key, "value": {"stringValue": value}}
                                for key, value in attrs.items()
                            ]
                        },
                        "scopeSpans": [
                            {"spans": [{"spanId": "root", "startTimeUnixNano": "1000000000"}]}
                        ],
                    }
                ]
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        explorer = explorer_with(client)
        seen: list[str] = []
        cursor = None
        for _ in range(4):
            page = await explorer.traces(
                None,  # type: ignore[arg-type]
                project_id,
                environment_id,
                service_id,
                start,
                end,
                cursor,
            )
            seen.extend(item["trace_id"] for item in page["items"])
            cursor = page["next_cursor"]
            if cursor is None:
                break
    assert len(seen) == 101
    assert len(set(seen)) == 101
    assert traces[4]["traceID"].zfill(32) not in seen
    assert cursor is None


@pytest.mark.asyncio
async def test_tempo_rejects_saturated_one_second_interval() -> None:
    project_id, environment_id, service_id = uuid7(), uuid7(), uuid7()
    end = datetime.now(UTC).replace(microsecond=0)

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/search"
        return httpx.Response(
            200,
            json={
                "traces": [
                    {
                        "traceID": f"{index:016x}",
                        "startTimeUnixNano": str((int(end.timestamp()) - 1) * 1_000_000_000),
                    }
                    for index in range(101)
                ]
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        explorer = explorer_with(client)
        with pytest.raises(ValueError, match="too dense"):
            await explorer.traces(
                None,  # type: ignore[arg-type]
                project_id,
                environment_id,
                service_id,
                end - timedelta(seconds=1),
                end,
            )
