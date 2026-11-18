"""Initial integrations schema.

Revision ID: 0001
Revises:
"""

from alembic import op

from src.core.db.models import Base

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    for table in Base.metadata.sorted_tables:
        table.create(bind, checkfirst=False)


def downgrade() -> None:
    # Dropping this schema would destroy customer credentials and health history.
    raise NotImplementedError("Initial schema downgrade would destroy customer data")
