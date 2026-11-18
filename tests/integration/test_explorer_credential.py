import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid7

import httpx
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.explorer import Explorer
from src.api.schemas import LogSearchRequest
from src.core.db.models import Credential, Integration
from src.security import CredentialCipher


@pytest.mark.asyncio
async def test_explorer_sends_encrypted_connector_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not url or make_url(url).database != "integrations_test":
        pytest.skip("Requires isolated integrations_test PostgreSQL database")
    project_id, environment_id, service_id, connector_id = uuid7(), uuid7(), uuid7(), uuid7()
    cipher = CredentialCipher({"v1": bytes(range(32))}, "v1")
    key_id, ciphertext = cipher.encrypt(connector_id, "test-token")
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    class Access:
        async def require(self, user: object, project: object, permission: str) -> object:
            return SimpleNamespace(lifecycle_version=1)

    async def public_url(value: str) -> str:
        return value

    monkeypatch.setattr("src.api.explorer.resolve_public_url", public_url)

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer test-token"
        return httpx.Response(200, json={"data": {"result": []}})

    try:
        async with sessions.begin() as session:
            session.add(
                Integration(
                    id=connector_id,
                    project_id=project_id,
                    environment_id=environment_id,
                    type="loki",
                    name="logs",
                    config_public={"url": "https://example.com"},
                    status="connected",
                )
            )
            session.add(Credential(owner_id=connector_id, key_id=key_id, ciphertext=ciphertext))
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            explorer = Explorer(sessions, Access(), client, cipher)  # type: ignore[arg-type]
            now = datetime.now(UTC)
            result = await explorer.logs(
                None,  # type: ignore[arg-type]
                LogSearchRequest.model_validate(
                    {  # type: ignore[arg-type]
                        "project_id": project_id,
                        "environment_id": environment_id,
                        "service_ids": [service_id],
                        "from": now - timedelta(minutes=1),
                        "to": now,
                        "query": "error",
                    }
                ),
            )
            assert result["items"] == []
    finally:
        await engine.dispose()
