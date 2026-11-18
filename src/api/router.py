from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Annotated
from uuid import UUID

from aiip_kit.access.model import Principal
from fastapi import APIRouter, Depends, Header, Query, status

from src.api.explorer import Explorer
from src.api.schemas import (
    IntegrationPatch,
    IntegrationView,
    IntegrationWrite,
    LogSearchRequest,
    MetricQueryRequest,
    RepositoryPatch,
    RepositoryView,
    RepositoryWrite,
    TestResult,
)
from src.api.service import IntegrationService

Authenticate = Callable[[str | None], Awaitable[Principal]]


def build_router(
    service: IntegrationService, explorer: Explorer, authenticate: Authenticate
) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["integrations"])

    async def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
        return await authenticate(authorization)

    User = Annotated[Principal, Depends(principal)]

    @router.get("/integration-types")
    async def integration_types(user: User) -> list[dict]:
        return [
            {
                "type": kind,
                "config_fields": [{"name": "url", "required": True, "kind": "url"}],
                "needs_credential": kind in {"github", "kubernetes"},
            }
            for kind in (
                "loki",
                "prometheus",
                "tempo",
                "github",
                "kubernetes",
                "healthcheck",
                "deployment",
            )
        ]

    @router.post("/integrations", status_code=status.HTTP_201_CREATED)
    async def create_integration(body: IntegrationWrite, user: User) -> IntegrationView:
        return await service.create_integration(user, body)

    @router.get("/integrations")
    async def list_integrations(
        project_id: UUID, user: User, environment_id: UUID | None = None, cursor: str | None = None
    ) -> dict:
        return await service.list_integrations(user, project_id, environment_id, cursor)

    @router.get("/integrations/{id}")
    async def get_integration(id: UUID, user: User) -> IntegrationView:
        return await service.get_integration(user, id)

    @router.patch("/integrations/{id}")
    async def patch_integration(id: UUID, body: IntegrationPatch, user: User) -> IntegrationView:
        return await service.patch_integration(user, id, body)

    @router.delete("/integrations/{id}", status_code=204)
    async def delete_integration(id: UUID, user: User) -> None:
        await service.delete_integration(user, id)

    @router.post("/integrations/{id}/test")
    async def test_integration(id: UUID, user: User) -> TestResult:
        return await service.test_integration(user, id)

    @router.get("/integrations/{id}/checks")
    async def checks(id: UUID, user: User, cursor: str | None = None) -> dict:
        return await service.checks(user, id, cursor)

    @router.post("/repositories", status_code=201)
    async def create_repository(body: RepositoryWrite, user: User) -> RepositoryView:
        return await service.create_repository(user, body)

    @router.get("/repositories")
    async def list_repositories(project_id: UUID, user: User, cursor: str | None = None) -> dict:
        return await service.list_repositories(user, project_id, cursor)

    @router.get("/repositories/{id}")
    async def get_repository(id: UUID, user: User) -> RepositoryView:
        return await service.get_repository(user, id)

    @router.patch("/repositories/{id}")
    async def patch_repository(id: UUID, body: RepositoryPatch, user: User) -> RepositoryView:
        return await service.patch_repository(user, id, body)

    @router.delete("/repositories/{id}", status_code=204)
    async def delete_repository(id: UUID, user: User) -> None:
        await service.delete_repository(user, id)

    @router.post("/telemetry/logs/search")
    async def logs(body: LogSearchRequest, user: User) -> dict:
        return await explorer.logs(user, body)

    @router.post("/telemetry/metrics/query")
    async def metrics(body: MetricQueryRequest, user: User) -> dict:
        return await explorer.metrics(user, body)

    @router.get("/telemetry/traces")
    async def traces(
        project_id: UUID,
        environment_id: UUID,
        service_id: UUID,
        user: User,
        start: Annotated[datetime, Query(alias="from")],
        end: Annotated[datetime, Query(alias="to")],
        cursor: str | None = None,
    ) -> dict:
        return await explorer.traces(
            user, project_id, environment_id, service_id, start, end, cursor
        )

    @router.get("/telemetry/traces/{trace_id}")
    async def trace_detail(
        trace_id: str, project_id: UUID, environment_id: UUID, user: User
    ) -> dict:
        return await explorer.trace_detail(user, trace_id, project_id, environment_id)

    return router
