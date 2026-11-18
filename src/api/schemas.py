from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

IntegrationKind = Literal[
    "loki", "prometheus", "tempo", "github", "kubernetes", "healthcheck", "deployment"
]
Status = Literal["connected", "degraded", "disconnected", "untested"]


class IntegrationWrite(BaseModel):
    project_id: UUID
    environment_id: UUID | None = None
    type: IntegrationKind
    name: str = Field(min_length=1, max_length=160)
    config: dict[str, Any]
    credential: str | None = Field(default=None, repr=False)


class IntegrationPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    config: dict[str, Any] | None = None
    credential: str | None = Field(default=None, repr=False)


class IntegrationView(BaseModel):
    id: UUID
    project_id: UUID
    environment_id: UUID | None
    type: IntegrationKind
    name: str
    config_public: dict[str, Any]
    status: Status
    last_tested_at: datetime | None


class TestResult(BaseModel):
    status: Literal["connected", "degraded", "disconnected"]
    latency_ms: int | None
    message: str
    tested_at: datetime


class HealthCheckView(BaseModel):
    integration_id: UUID
    checked_at: datetime
    status: Literal["connected", "degraded", "disconnected"]
    latency_ms: int | None
    message: str


class RepositoryWrite(BaseModel):
    project_id: UUID
    provider: Literal["github", "gitlab"]
    full_name: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    default_branch: str = Field(default="main", min_length=1, max_length=160)
    service_ids: list[UUID] = Field(default_factory=list, max_length=100)
    credential: str | None = Field(default=None, repr=False)


class RepositoryPatch(BaseModel):
    default_branch: str | None = Field(default=None, min_length=1, max_length=160)
    service_ids: list[UUID] | None = Field(default=None, max_length=100)
    credential: str | None = Field(default=None, repr=False)


class RepositoryView(BaseModel):
    id: UUID
    project_id: UUID
    provider: Literal["github", "gitlab"]
    full_name: str
    default_branch: str
    service_ids: list[UUID]
    status: Status
    created_at: datetime


class LogSearchRequest(BaseModel):
    project_id: UUID
    environment_id: UUID
    service_ids: list[UUID] = Field(default_factory=list, max_length=20)
    start: datetime = Field(alias="from")
    end: datetime = Field(alias="to")
    query: str = Field(max_length=2000)
    limit: int = Field(default=100, ge=1, le=200)
    cursor: str | None = None


class MetricQueryRequest(BaseModel):
    project_id: UUID
    environment_id: UUID
    start: datetime = Field(alias="from")
    end: datetime = Field(alias="to")
    step_seconds: int = Field(ge=10, le=3600)
    query_id: Literal[
        "http_5xx_rate",
        "http_p95_latency",
        "http_request_rate",
        "db_pool_checked_out",
        "db_pool_timeouts",
        "pod_restarts",
        "service_up",
    ]
    service_ids: list[UUID] = Field(max_length=20)
