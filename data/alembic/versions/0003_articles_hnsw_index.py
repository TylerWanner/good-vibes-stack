"""Replace ivfflat with HNSW index on articles.embedding

HNSW gives better recall at all scales without probe tuning.
ivfflat recall degrades as data grows unless probes is continuously retuned.

Revision ID: 0003
Revises: 0002
Create Date: 2026-03-31
"""
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop ivfflat index
    op.execute("DROP INDEX IF EXISTS idx_articles_embedding")
    # Create HNSW index — better recall, scales well, no probe tuning needed
    op.execute("""
        CREATE INDEX idx_articles_embedding
        ON articles USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_articles_embedding")
    op.execute("""
        CREATE INDEX idx_articles_embedding
        ON articles USING ivfflat (embedding vector_cosine_ops)
        WHERE embedding IS NOT NULL
    """)
