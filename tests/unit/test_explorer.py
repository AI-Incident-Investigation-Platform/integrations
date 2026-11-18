from types import SimpleNamespace
from uuid import uuid7

import httpx
import pytest

from src.api.explorer import Explorer
from src.api.service import NotFound
from src.security import CredentialCipher


@pytest.mark.asyncio
async def test_foreign_trace_detail_is_not_found() -> None:
    project, environment = uuid7(), uuid7()

    class Access:
        async def require(self, user: object, project_id: object, permission: str) -> object:
            return SimpleNamespace(lifecycle_version=1)

    async with httpx.AsyncClient() as client:
        explorer = Explorer(
            None,  # type: ignore[arg-type]
            Access(),  # type: ignore[arg-type]
            client,
            CredentialCipher({"v1": bytes(range(32))}, "v1"),
        )

        async def endpoint(
            project_id: object, environment_id: object, kind: str
        ) -> tuple[str, dict[str, str]]:
            return "https://example.com", {}

        async def get(url: str, params: dict, headers: dict[str, str]) -> dict:
            return {"batches": [{"resource": {"attributes": []}}]}

        explorer._endpoint = endpoint  # type: ignore[method-assign]
        explorer._get = get  # type: ignore[method-assign]
        with pytest.raises(NotFound):
            await explorer.trace_detail(None, "a" * 16, project, environment)  # type: ignore[arg-type]
