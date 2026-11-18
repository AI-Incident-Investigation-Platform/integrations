from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import ForeignKey, Index, LargeBinary, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class Integration(Base):
    __tablename__ = "integrations"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    project_id: Mapped[UUID] = mapped_column(index=True)
    environment_id: Mapped[UUID | None]
    type: Mapped[str] = mapped_column(String(24))
    name: Mapped[str] = mapped_column(String(160))
    config_public: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(24), server_default="untested")
    last_tested_at: Mapped[datetime | None]
    next_check_at: Mapped[datetime | None]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    __table_args__ = (Index("ix_integrations_due", "next_check_at"),)


class Credential(Base):
    __tablename__ = "credentials"
    owner_id: Mapped[UUID] = mapped_column(primary_key=True)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary)
    key_id: Mapped[str] = mapped_column(String(64))


class Repository(Base):
    __tablename__ = "repositories"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    project_id: Mapped[UUID] = mapped_column(index=True)
    provider: Mapped[str] = mapped_column(String(16))
    full_name: Mapped[str] = mapped_column(String(300))
    default_branch: Mapped[str] = mapped_column(String(160), server_default="main")
    service_ids: Mapped[list] = mapped_column(JSONB, default=list)
    status: Mapped[str] = mapped_column(String(24), server_default="untested")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    __table_args__ = (UniqueConstraint("project_id", "provider", "full_name"),)


class HealthCheck(Base):
    __tablename__ = "health_checks"
    integration_id: Mapped[UUID] = mapped_column(
        ForeignKey("integrations.id", ondelete="CASCADE"), primary_key=True
    )
    checked_at: Mapped[datetime] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column(String(24))
    latency_ms: Mapped[int | None]
    message: Mapped[str] = mapped_column(String(500))
    next_check_at: Mapped[datetime]


class MetricTemplate(Base):
    __tablename__ = "metric_templates"
    query_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    promql: Mapped[str] = mapped_column(String(1000))


class AuditEvent(Base):
    __tablename__ = "audit_events"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    project_id: Mapped[UUID] = mapped_column(index=True)
    actor_id: Mapped[UUID | None]
    action: Mapped[str] = mapped_column(String(100))
    resource_type: Mapped[str] = mapped_column(String(64))
    resource_id: Mapped[UUID]
    data: Mapped[dict] = mapped_column(JSONB)
    occurred_at: Mapped[datetime] = mapped_column(server_default=func.now())
