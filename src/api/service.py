import asyncio
import contextlib
import random
import time
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
from aiip_kit.access.checker import AccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.schemas import (
    HealthCheckView,
    IntegrationPatch,
    IntegrationView,
    IntegrationWrite,
    RepositoryPatch,
    RepositoryView,
    RepositoryWrite,
    TestResult,
)
from src.core.db.models import (
    AuditEvent,
    Credential,
    HealthCheck,
    Integration,
    Repository,
    outbox,
)
from src.security import CredentialCipher, resolve_public_url, validate_url


class NotFound(Exception):
    pass


def public_config(kind: str, config: dict) -> dict:
    allowed: dict[str, set[str]] = {
        "loki": {"url"},
        "prometheus": {"url"},
        "tempo": {"url"},
        "github": {"url"},
        "kubernetes": {"url"},
        "healthcheck": {"url"},
        "deployment": {"url"},
    }
    if set(config) - allowed[kind] or "url" not in config:
        raise ValueError("Unsupported or missing config field")
    if not isinstance(config["url"], str):
        raise ValueError("url must be a string")
    return {"url": validate_url(config["url"])}


def integration_view(row: Integration) -> IntegrationView:
    return IntegrationView.model_validate(row, from_attributes=True)


def repository_view(row: Repository) -> RepositoryView:
    return RepositoryView.model_validate(
        {
            "id": row.id,
            "project_id": row.project_id,
            "provider": row.provider,
            "full_name": row.full_name,
            "default_branch": row.default_branch,
            "service_ids": row.service_ids,
            "status": row.status,
            "created_at": row.created_at,
        }
    )


class IntegrationService:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        access: AccessChecker,
        cipher: CredentialCipher,
        http: httpx.AsyncClient,
    ) -> None:
        self.sessions = sessions
        self.access = access
        self.cipher = cipher
        self.http = http
        self.writer = OutboxWriter(outbox)

    async def _event(
        self,
        session: AsyncSession,
        row: Integration,
        kind: str,
        data: dict,
        principal: Principal | None,
        lifecycle_version: int | None,
    ) -> None:
        event = MessageV1.event(
            kind,
            "integrations",
            data,
            project_id=row.project_id,
            aggregate_type="integration",
            aggregate_id=row.id,
            lifecycle_version=lifecycle_version,
            actor=Actor(kind="user", id=str(principal.user_id)) if principal else None,
        )
        await self.writer.add(session, event, Destination.event("integrations", kind))

    async def _get_project(self, model: type[Integration] | type[Repository], id: UUID) -> UUID:
        async with self.sessions() as session:
            project_id = await session.scalar(select(model.project_id).where(model.id == id))
        if project_id is None:
            raise NotFound
        return project_id

    async def create_integration(self, user: Principal, body: IntegrationWrite) -> IntegrationView:
        grant = await self.access.require(user, body.project_id, "integration.manage")
        config = public_config(body.type, body.config)
        async with self.sessions.begin() as session:
            row = Integration(
                project_id=body.project_id,
                environment_id=body.environment_id,
                type=body.type,
                name=body.name,
                config_public=config,
                status="untested",
                next_check_at=datetime.now(UTC),
            )
            session.add(row)
            await session.flush()
            if body.credential:
                key_id, ciphertext = self.cipher.encrypt(row.id, body.credential)
                session.add(Credential(owner_id=row.id, key_id=key_id, ciphertext=ciphertext))
            await self._event(
                session,
                row,
                "integration.changed",
                {"integration_id": str(row.id), "type": row.type, "status": row.status},
                user,
                grant.lifecycle_version,
            )
            session.add(
                AuditEvent(
                    project_id=row.project_id,
                    actor_id=user.user_id,
                    action="integration.created",
                    resource_type="integration",
                    resource_id=row.id,
                    data={},
                )
            )
            return integration_view(row)

    async def list_integrations(
        self, user: Principal, project_id: UUID, environment_id: UUID | None, cursor: str | None
    ) -> dict:
        await self.access.require(user, project_id, "project.read")
        async with self.sessions() as session:
            query = select(Integration).where(Integration.project_id == project_id)
            if environment_id:
                query = query.where(Integration.environment_id == environment_id)
            if cursor:
                query = query.where(Integration.id > UUID(cursor))
            rows = (await session.scalars(query.order_by(Integration.id).limit(101))).all()
            page = rows[:100]
            return {
                "items": [integration_view(row) for row in page],
                "next_cursor": str(page[-1].id) if len(rows) > 100 else None,
            }

    async def get_integration(self, user: Principal, id: UUID) -> IntegrationView:
        await self.access.require(user, await self._get_project(Integration, id), "project.read")
        async with self.sessions() as session:
            row = await session.get(Integration, id)
            if row is None:
                raise NotFound
            return integration_view(row)

    async def patch_integration(
        self, user: Principal, id: UUID, body: IntegrationPatch
    ) -> IntegrationView:
        project_id = await self._get_project(Integration, id)
        grant = await self.access.require(user, project_id, "integration.manage")
        changes = body.model_dump(exclude_unset=True)
        async with self.sessions.begin() as session:
            row = await session.get(Integration, id, with_for_update=True)
            if row is None or row.project_id != project_id:
                raise NotFound
            if "name" in changes:
                row.name = body.name or row.name
            if "config" in changes:
                row.config_public = public_config(row.type, body.config or {})
            if "credential" in changes:
                await session.execute(delete(Credential).where(Credential.owner_id == id))
                if body.credential:
                    key_id, ciphertext = self.cipher.encrypt(id, body.credential)
                    session.add(Credential(owner_id=id, key_id=key_id, ciphertext=ciphertext))
            await self._event(
                session,
                row,
                "integration.changed",
                {"integration_id": str(id), "type": row.type, "status": row.status},
                user,
                grant.lifecycle_version,
            )
            session.add(
                AuditEvent(
                    project_id=project_id,
                    actor_id=user.user_id,
                    action="integration.updated",
                    resource_type="integration",
                    resource_id=id,
                    data={"fields": sorted(changes)},
                )
            )
            await session.flush()
            return integration_view(row)

    async def delete_integration(self, user: Principal, id: UUID) -> None:
        project_id = await self._get_project(Integration, id)
        grant = await self.access.require(user, project_id, "integration.manage")
        async with self.sessions.begin() as session:
            row = await session.get(Integration, id, with_for_update=True)
            if row is None or row.project_id != project_id:
                raise NotFound
            await self._event(
                session,
                row,
                "integration.changed",
                {"integration_id": str(id), "type": row.type, "status": "disconnected"},
                user,
                grant.lifecycle_version,
            )
            await session.execute(delete(Credential).where(Credential.owner_id == id))
            await session.delete(row)

    async def test_integration(self, user: Principal | None, id: UUID) -> TestResult:
        project_id = await self._get_project(Integration, id)
        grant = await self.access.require(user, project_id, "integration.manage") if user else None
        async with self.sessions() as session:
            row = await session.get(Integration, id)
            credential = await session.get(Credential, id)
            if row is None:
                raise NotFound
            endpoint = row.config_public["url"]
            kind = row.type
        headers = {}
        if credential:
            headers["Authorization"] = "Bearer " + self.cipher.decrypt(
                id, credential.key_id, credential.ciphertext
            )
        started = time.monotonic()
        try:
            await resolve_public_url(endpoint)
            path = {"prometheus": "/-/healthy", "loki": "/ready", "tempo": "/ready"}.get(kind, "")
            response = await self.http.get(
                endpoint + path, headers=headers, follow_redirects=False, timeout=5.0
            )
            connected = 200 <= response.status_code < 300
            status = "connected" if connected else "disconnected"
            message = "Upstream responded" if connected else "Upstream unavailable"
        except httpx.HTTPError, ValueError, OSError:
            status, message = "disconnected", "Upstream unavailable"
        tested_at = datetime.now(UTC)
        result = TestResult(
            status=status,  # type: ignore[arg-type]  # values assigned above are closed literals
            latency_ms=int((time.monotonic() - started) * 1000),
            message=message,
            tested_at=tested_at,
        )
        next_check = tested_at + timedelta(seconds=300 + random.randint(-30, 30))
        async with self.sessions.begin() as session:
            row = await session.get(Integration, id, with_for_update=True)
            if row is None:
                raise NotFound
            previous = row.status
            row.status, row.last_tested_at, row.next_check_at = status, tested_at, next_check
            session.add(
                HealthCheck(
                    integration_id=id,
                    checked_at=tested_at,
                    status=status,
                    latency_ms=result.latency_ms,
                    message=message,
                    next_check_at=next_check,
                )
            )
            if previous != status:
                await self._event(
                    session,
                    row,
                    "integration.health_changed",
                    {
                        "integration_id": str(id),
                        "status": status,
                        "previous_status": previous,
                        "message": message,
                    },
                    user,
                    grant.lifecycle_version if grant else None,
                )
        return result

    async def checks(self, user: Principal, id: UUID, cursor: str | None) -> dict:
        await self.access.require(user, await self._get_project(Integration, id), "project.read")
        async with self.sessions() as session:
            query = select(HealthCheck).where(HealthCheck.integration_id == id)
            if cursor:
                checked_at = datetime.fromisoformat(cursor)
                if checked_at.tzinfo is None:
                    raise ValueError("Timezone-aware cursor required")
                query = query.where(HealthCheck.checked_at < checked_at)
            rows = (
                await session.scalars(query.order_by(HealthCheck.checked_at.desc()).limit(101))
            ).all()
            page = rows[:100]
            return {
                "items": [
                    HealthCheckView.model_validate(row, from_attributes=True) for row in page
                ],
                "next_cursor": page[-1].checked_at.isoformat() if len(rows) > 100 else None,
            }

    async def create_repository(self, user: Principal, body: RepositoryWrite) -> RepositoryView:
        await self.access.require(user, body.project_id, "repository.manage")
        async with self.sessions.begin() as session:
            row = Repository(
                project_id=body.project_id,
                provider=body.provider,
                full_name=body.full_name,
                default_branch=body.default_branch,
                service_ids=[str(value) for value in body.service_ids],
                status="untested",
            )
            session.add(row)
            await session.flush()
            if body.credential:
                key_id, ciphertext = self.cipher.encrypt(row.id, body.credential)
                session.add(Credential(owner_id=row.id, key_id=key_id, ciphertext=ciphertext))
            return repository_view(row)

    async def list_repositories(
        self, user: Principal, project_id: UUID, cursor: str | None
    ) -> dict:
        await self.access.require(user, project_id, "project.read")
        async with self.sessions() as session:
            query = select(Repository).where(Repository.project_id == project_id)
            if cursor:
                query = query.where(Repository.id > UUID(cursor))
            rows = (await session.scalars(query.order_by(Repository.id).limit(101))).all()
            page = rows[:100]
            return {
                "items": [repository_view(row) for row in page],
                "next_cursor": str(page[-1].id) if len(rows) > 100 else None,
            }

    async def get_repository(self, user: Principal, id: UUID) -> RepositoryView:
        await self.access.require(user, await self._get_project(Repository, id), "project.read")
        async with self.sessions() as session:
            row = await session.get(Repository, id)
            if row is None:
                raise NotFound
            return repository_view(row)

    async def patch_repository(
        self, user: Principal, id: UUID, body: RepositoryPatch
    ) -> RepositoryView:
        project_id = await self._get_project(Repository, id)
        await self.access.require(user, project_id, "repository.manage")
        changes = body.model_dump(exclude_unset=True)
        async with self.sessions.begin() as session:
            row = await session.get(Repository, id, with_for_update=True)
            if row is None or row.project_id != project_id:
                raise NotFound
            if "default_branch" in changes:
                row.default_branch = body.default_branch or row.default_branch
            if "service_ids" in changes:
                row.service_ids = [str(value) for value in (body.service_ids or [])]
            if "credential" in changes:
                await session.execute(delete(Credential).where(Credential.owner_id == id))
                if body.credential:
                    key_id, ciphertext = self.cipher.encrypt(id, body.credential)
                    session.add(Credential(owner_id=id, key_id=key_id, ciphertext=ciphertext))
            await session.flush()
            return repository_view(row)

    async def delete_repository(self, user: Principal, id: UUID) -> None:
        project_id = await self._get_project(Repository, id)
        await self.access.require(user, project_id, "repository.manage")
        async with self.sessions.begin() as session:
            await session.execute(delete(Credential).where(Credential.owner_id == id))
            row = await session.get(Repository, id)
            if row is None:
                raise NotFound
            await session.delete(row)

    async def run_checks(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            now = datetime.now(UTC)
            async with self.sessions.begin() as session:
                due_rows = (
                    await session.scalars(
                        select(Integration)
                        .where(Integration.next_check_at <= now)
                        .with_for_update(skip_locked=True)
                        .limit(20)
                    )
                ).all()
                due = [row.id for row in due_rows]
                for row in due_rows:
                    row.next_check_at = now + timedelta(minutes=1)
            for id in due:
                # A later pass retries; no credentials or response bodies are logged.
                with contextlib.suppress(Exception):
                    await self.test_integration(None, id)
            async with self.sessions.begin() as session:
                await session.execute(
                    delete(HealthCheck).where(HealthCheck.checked_at < now - timedelta(days=7))
                )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=30)
