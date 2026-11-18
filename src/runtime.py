import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika
import httpx
import uvicorn
from aiip_kit.access.checker import DragonflyAccessChecker
from aiip_kit.access.source import HttpAclSource
from aiip_kit.messaging.consumer import Binding, Consumer, QueueSpec
from aiip_kit.messaging.publisher import AmqpPublisher
from aiip_kit.messaging.topology import events_exchange
from aiip_kit.outbox.relay import OutboxRelay
from aiip_kit.service_auth.jwks import JwksKeys
from aiip_kit.service_auth.provider import AuthServiceTokenProvider
from fastapi import FastAPI
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.explorer import Explorer
from src.api.service import IntegrationService
from src.core.db.models import outbox
from src.core.user_auth import JwtUserAuthenticator
from src.main import create_app
from src.security import CredentialCipher, PublicHTTPTransport
from src.worker.cleanup import CleanupWorker


class Config(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    database_url: str = Field(min_length=1)
    rabbitmq_url: str = Field(min_length=1)
    cache_url: str = Field(min_length=1)
    auth_url: str = "http://auth.apps:8001"
    projects_url: str = "http://projects.apps:8003"
    auth_client_id: str = "integrations"
    auth_client_secret: str = Field(min_length=1)
    port: int = 8007
    bind_host: str = "0.0.0.0"


cfg = Config()  # type: ignore[call-arg]
engine = create_async_engine(cfg.database_url, pool_size=3, max_overflow=0, pool_timeout=0.5)
sessions = async_sessionmaker(engine, expire_on_commit=False)
internal_http = httpx.AsyncClient(timeout=2.0)
external_http = httpx.AsyncClient(
    timeout=5.0,
    follow_redirects=False,
    trust_env=False,
    transport=PublicHTTPTransport(max_connections=8),
)
explorer_http = httpx.AsyncClient(
    timeout=5.0,
    follow_redirects=False,
    trust_env=False,
    transport=PublicHTTPTransport(max_connections=16),
)
redis = Redis.from_url(cfg.cache_url, decode_responses=True)
keys = JwksKeys(internal_http, f"{cfg.auth_url.rstrip('/')}/.well-known/jwks.json")
authenticator = JwtUserAuthenticator(keys)
service_tokens = AuthServiceTokenProvider(
    internal_http, cfg.auth_url, cfg.auth_client_id, cfg.auth_client_secret
)
acl_source = HttpAclSource(
    internal_http,
    cfg.projects_url,
    lambda: service_tokens.token("projects", ["projects.acl.read"]),
)
access = DragonflyAccessChecker(redis, acl_source)
cipher = CredentialCipher.from_environment()
publisher = AmqpPublisher(cfg.rabbitmq_url)
relay = OutboxRelay(sessions, outbox, publisher)
service = IntegrationService(sessions, access, cipher, external_http)
explorer = Explorer(sessions, access, explorer_http, cipher)
stop = asyncio.Event()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    connection = await aio_pika.connect_robust(cfg.rabbitmq_url)
    channel = await connection.channel(publisher_confirms=True)
    await Consumer(
        channel,
        QueueSpec(
            "integrations.projects-events",
            [Binding(events_exchange("projects"), "topic", "project.deleting")],
        ),
        CleanupWorker(sessions).handle,
    ).start()
    relay_task = asyncio.create_task(relay.run(stop))
    checks_task = asyncio.create_task(service.run_checks(stop))
    topology_task = asyncio.create_task(explorer.run_topology(stop))
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(relay_task, checks_task, topology_task)
        await publisher.close()
        await connection.close()
        await external_http.aclose()
        await explorer_http.aclose()
        await internal_http.aclose()
        await redis.aclose()
        await engine.dispose()


app = create_app(service, explorer, authenticator.authenticate)
app.router.lifespan_context = lifespan


@app.get("/ready")
async def ready() -> dict[str, str]:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
    await redis.ping()
    return {"status": "ok"}


def main() -> None:
    uvicorn.run("src.runtime:app", host=cfg.bind_host, port=cfg.port, workers=1)


if __name__ == "__main__":
    main()
