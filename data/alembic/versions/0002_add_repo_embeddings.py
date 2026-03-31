"""Add embedding column to repos table

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-31
"""
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE repos ADD COLUMN IF NOT EXISTS embedding vector(768)")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_repos_embedding
        ON repos USING ivfflat (embedding vector_cosine_ops)
        WHERE embedding IS NOT NULL
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_repos_embedding")
    op.execute("ALTER TABLE repos DROP COLUMN IF EXISTS embedding")
