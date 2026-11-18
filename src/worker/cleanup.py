from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.inbox import InboxGuard
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.db.models import AuditEvent, Credential, Integration, Repository, inbox, outbox


class CleanupWorker:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions
        self.guard = InboxGuard(inbox)
        self.writer = OutboxWriter(outbox)

    async def handle(self, message: MessageV1, attempt: int) -> None:
        if message.type != "project.deleting" or message.project_id is None:
            raise ValueError("Unknown project event")
        if message.lifecycle_version is None:
            raise ValueError("Project lifecycle version is required")
        project_id = message.project_id
        connector_ids = select(Integration.id).where(Integration.project_id == project_id)
        repository_ids = select(Repository.id).where(Repository.project_id == project_id)
        async with self.sessions.begin() as session:
            if not await self.guard.accept(session, "project.deleting", message):
                return
            await session.execute(
                delete(Credential).where(
                    or_(
                        Credential.owner_id.in_(connector_ids),
                        Credential.owner_id.in_(repository_ids),
                    )
                )
            )
            await session.execute(delete(Integration).where(Integration.project_id == project_id))
            await session.execute(delete(Repository).where(Repository.project_id == project_id))
            await session.execute(delete(AuditEvent).where(AuditEvent.project_id == project_id))
            await session.execute(
                delete(outbox).where(outbox.c.body["project_id"].astext == str(project_id))
            )
            done = message.caused(
                "cleanup.completed",
                "integrations",
                {
                    "project_id": str(project_id),
                    "lifecycle_version": message.lifecycle_version,
                    "participant": "integrations",
                },
                kind="command",
                actor=Actor(kind="service", id="integrations"),
            )
            await self.writer.add(
                session, done, Destination.command("projects", "cleanup.completed")
            )
