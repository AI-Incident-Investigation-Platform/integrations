import os
from uuid import UUID, uuid7

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.explorer import Explorer
from src.core.db.models import Integration, outbox
from src.security import CredentialCipher


@pytest.mark.asyncio
async def test_topology_scan_emits_each_trace_once(monkeypatch: pytest.MonkeyPatch) -> None:
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not url or make_url(url).database != "integrations_test":
        pytest.skip("Requires isolated integrations_test PostgreSQL database")
    project_id, environment_id, source_id, target_id = uuid7(), uuid7(), uuid7(), uuid7()
    integration_id = uuid7()
    trace_id = "a" * 16

    def batch(service_id: str, span_id: str, parent_id: str) -> dict:
        return {
            "resource": {
                "attributes": [
                    {"key": "project.id", "value": {"stringValue": str(project_id)}},
                    {"key": "environment.id", "value": {"stringValue": str(environment_id)}},
                    {"key": "service.id", "value": {"stringValue": service_id}},
                ]
            },
            "scopeSpans": [
                {
                    "spans": [
                        {
                            "spanId": span_id,
                            "parentSpanId": parent_id,
                            "startTimeUnixNano": "1000000000",
                        }
                    ]
                }
            ],
        }

    async def public_url(value: str) -> str:
        return value

    monkeypatch.setattr("src.api.explorer.resolve_public_url", public_url)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/search":
            return httpx.Response(200, json={"traces": [{"traceID": trace_id}]})
        assert request.url.path == "/api/traces/" + trace_id
        return httpx.Response(
            200,
            json={
                "batches": [
                    batch(str(source_id), "root", ""),
                    batch(str(target_id), "child", "root"),
                ]
            },
        )

    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            session.add(
                Integration(
                    id=integration_id,
                    project_id=project_id,
                    environment_id=environment_id,
                    type="tempo",
                    name="tempo",
                    config_public={"url": "https://example.com"},
                    status="connected",
                )
            )
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            explorer = Explorer(
                sessions,
                None,  # type: ignore[arg-type]
                client,
                CredentialCipher({"v1": bytes(range(32))}, "v1"),
            )
            explorer.scan_after = UUID(int=integration_id.int - 1)
            await explorer.scan_topology_once()
            explorer.scan_after = UUID(int=integration_id.int - 1)
            await explorer.scan_topology_once()
        async with sessions() as session:
            rows = (
                (
                    await session.execute(
                        select(outbox.c.body).where(
                            outbox.c.body["project_id"].astext == str(project_id)
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert [row["type"] for row in rows] == ["topology.observed"]
            assert rows[0]["data"]["edges"][0]["from_service_id"] == str(source_id)
    finally:
        await engine.dispose()
