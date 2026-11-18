import asyncio
import os
from datetime import UTC, datetime
from uuid import uuid7

import aio_pika
import pytest
from aiip_kit.messaging.amqp import to_amqp
from aiip_kit.messaging.consumer import Binding, Consumer, QueueSpec
from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.topology import events_exchange
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.core.db.models import Credential, HealthCheck, Integration, Repository, outbox
from src.worker.cleanup import CleanupWorker


@pytest.mark.asyncio
async def test_project_deletion_removes_secrets_and_acknowledges_once() -> None:
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not url or make_url(url).database != "integrations_test":
        pytest.skip("Requires isolated integrations_test PostgreSQL database")
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    project_id, other_project = uuid7(), uuid7()
    connector_id, repository_id, other_id = uuid7(), uuid7(), uuid7()
    now = datetime.now(UTC)
    try:
        async with sessions.begin() as session:
            session.add_all(
                [
                    Integration(
                        id=connector_id,
                        project_id=project_id,
                        type="loki",
                        name="a",
                        config_public={"url": "https://example.com"},
                    ),
                    Integration(
                        id=other_id,
                        project_id=other_project,
                        type="loki",
                        name="b",
                        config_public={"url": "https://example.com"},
                    ),
                    Repository(
                        id=repository_id,
                        project_id=project_id,
                        provider="github",
                        full_name="owner/repo",
                        service_ids=[],
                    ),
                ]
            )
            await session.flush()
            session.add_all(
                [
                    Credential(owner_id=connector_id, key_id="v1", ciphertext=b"secret"),
                    Credential(owner_id=repository_id, key_id="v1", ciphertext=b"secret"),
                    HealthCheck(
                        integration_id=connector_id,
                        checked_at=now,
                        status="connected",
                        next_check_at=now,
                        message="ok",
                    ),
                ]
            )
        message = MessageV1.event(
            "project.deleting",
            "projects",
            {"project_id": str(project_id)},
            project_id=project_id,
            lifecycle_version=1,
        )
        worker = CleanupWorker(sessions)
        await worker.handle(message, 1)
        await worker.handle(message, 2)
        async with sessions() as session:
            assert await session.get(Integration, connector_id) is None
            assert await session.get(Repository, repository_id) is None
            assert await session.get(Credential, connector_id) is None
            assert await session.get(Credential, repository_id) is None
            assert await session.get(HealthCheck, (connector_id, now)) is None
            assert await session.get(Integration, other_id) is not None
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
            assert [row["type"] for row in rows] == ["cleanup.completed"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_project_deletion_arrives_from_rabbitmq() -> None:
    database_url = os.environ.get("TEST_DATABASE_URL", "")
    rabbit_url = os.environ.get("TEST_RABBITMQ_URL", "")
    if not database_url or make_url(database_url).database != "integrations_test" or not rabbit_url:
        pytest.skip("Requires isolated PostgreSQL and RabbitMQ")
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    connection = await aio_pika.connect_robust(rabbit_url)
    try:
        channel = await connection.channel(publisher_confirms=True)
        await Consumer(
            channel,
            QueueSpec(
                "integrations.projects-events",
                [Binding(events_exchange("projects"), "topic", "project.deleting")],
            ),
            CleanupWorker(sessions).handle,
        ).start()
        project_id = uuid7()
        event = MessageV1.event(
            "project.deleting",
            "projects",
            {"project_id": str(project_id)},
            project_id=project_id,
            lifecycle_version=1,
        )
        exchange = await channel.declare_exchange(
            events_exchange("projects"), aio_pika.ExchangeType.TOPIC, durable=True
        )
        await exchange.publish(to_amqp(event), routing_key="project.deleting")
        for _ in range(30):
            async with sessions() as session:
                rows = (
                    (
                        await session.execute(
                            select(outbox.c.body).where(
                                outbox.c.body["causation_id"].astext == str(event.message_id)
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            if rows:
                assert rows[0]["type"] == "cleanup.completed"
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("RabbitMQ event did not produce cleanup acknowledgement")
    finally:
        await connection.close()
        await engine.dispose()
