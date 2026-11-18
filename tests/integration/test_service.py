import os
from types import SimpleNamespace
from uuid import uuid7

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.schemas import IntegrationWrite
from src.api.service import IntegrationService
from src.core.db.models import HealthCheck, Integration, outbox
from src.security import CredentialCipher


@pytest.mark.asyncio
async def test_connector_health_check_persists_status_and_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not url or make_url(url).database != "integrations_test":
        pytest.skip("Requires isolated integrations_test PostgreSQL database")
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    project_id, environment_id = uuid7(), uuid7()
    cipher = CredentialCipher({"v1": bytes(range(32))}, "v1")

    class Access:
        async def require(self, user: object, project: object, permission: str) -> object:
            return SimpleNamespace(lifecycle_version=1)

    async def public_url(value: str) -> str:
        return value

    monkeypatch.setattr("src.api.service.resolve_public_url", public_url)

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.url.path == "/ready"
        return httpx.Response(200)

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            service = IntegrationService(sessions, Access(), cipher, client)  # type: ignore[arg-type]
            created = await service.create_integration(
                SimpleNamespace(user_id=uuid7()),  # type: ignore[arg-type]
                IntegrationWrite(
                    project_id=project_id,
                    environment_id=environment_id,
                    type="loki",
                    name="logs",
                    config={"url": "https://example.com"},
                    credential="test-token",
                ),
            )
            assert created.status == "untested"
            assert "test-token" not in created.model_dump_json()
            result = await service.test_integration(None, created.id)
            assert result.status == "connected"
            async with sessions() as session:
                connector = await session.get(Integration, created.id)
                assert connector is not None and connector.status == "connected"
                checks = (
                    await session.scalars(
                        select(HealthCheck).where(HealthCheck.integration_id == created.id)
                    )
                ).all()
                assert len(checks) == 1
                bodies = (
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
                assert [body["type"] for body in bodies] == [
                    "integration.changed",
                    "integration.health_changed",
                ]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_connector_list_pages_without_repeating_rows() -> None:
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not url or make_url(url).database != "integrations_test":
        pytest.skip("Requires isolated integrations_test PostgreSQL database")
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    project_id = uuid7()

    class Access:
        async def require(self, user: object, project: object, permission: str) -> object:
            return SimpleNamespace(lifecycle_version=1)

    try:
        async with sessions.begin() as session:
            session.add_all(
                [
                    Integration(
                        project_id=project_id,
                        type="loki",
                        name=str(index),
                        config_public={"url": "https://example.com"},
                    )
                    for index in range(101)
                ]
            )
        async with httpx.AsyncClient() as client:
            service = IntegrationService(
                sessions,
                Access(),  # type: ignore[arg-type]
                CredentialCipher({"v1": bytes(range(32))}, "v1"),
                client,
            )
            first = await service.list_integrations(None, project_id, None, None)  # type: ignore[arg-type]
            assert len(first["items"]) == 100
            assert first["next_cursor"] is not None
            second = await service.list_integrations(
                None,  # type: ignore[arg-type]
                project_id,
                None,
                first["next_cursor"],
            )
            assert len(second["items"]) == 1
            assert second["next_cursor"] is None
            assert {item.id for item in first["items"]}.isdisjoint(
                {item.id for item in second["items"]}
            )
    finally:
        await engine.dispose()
