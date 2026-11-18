from aiip_kit.outbox.tables import inbox_table, outbox_table

from src.core.db.models.base import Base
from src.core.db.models.entities import (
    AuditEvent,
    Credential,
    HealthCheck,
    Integration,
    MetricTemplate,
    Repository,
)

inbox = inbox_table(Base.metadata)
outbox = outbox_table(Base.metadata)

__all__ = [
    "AuditEvent",
    "Base",
    "Credential",
    "HealthCheck",
    "Integration",
    "MetricTemplate",
    "Repository",
    "inbox",
    "outbox",
]
