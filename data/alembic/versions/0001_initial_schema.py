"""Initial schema (squashed)

Revision ID: 0001
Revises:
Create Date: 2026-03-26 00:00:00

This is a squashed migration combining all previous migrations into a single
initial schema for fresh deployments.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Extensions
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    # -------------------------------------------------------------------------
    # articles table
    # -------------------------------------------------------------------------
    op.create_table(
        "articles",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("url", sa.Text, nullable=False, unique=True),
        sa.Column("readwise_id", sa.Text, nullable=True),
        sa.Column("title", sa.Text, nullable=True),
        sa.Column("summary", sa.Text, nullable=True),
        sa.Column("tags", sa.ARRAY(sa.Text), server_default="{}"),
        sa.Column("source_type", sa.Text, nullable=True),
        sa.Column("raw_text", sa.Text, nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text, nullable=False, server_default="pending"),
        sa.Column("failure_log", JSONB, nullable=False, server_default="[]"),
        sa.Column("privacy", sa.Text, nullable=False, server_default="public"),
        sa.Column("file_path", sa.Text, nullable=True),
        sa.Column("contributor", sa.Text, nullable=True),
        sa.Column("curator_note", sa.Text, nullable=True),
        sa.Column("score_usefulness", sa.SmallInteger, nullable=True),
        sa.Column("score_interest", sa.SmallInteger, nullable=True),
        sa.Column("score_pov", sa.SmallInteger, nullable=True),
        sa.Column("score_uniqueness", sa.Integer, nullable=True),
        sa.Column("content_date", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("score_usefulness BETWEEN 1 AND 5", name="ck_articles_score_usefulness"),
        sa.CheckConstraint("score_interest BETWEEN 1 AND 5", name="ck_articles_score_interest"),
        sa.CheckConstraint("score_pov BETWEEN 1 AND 5", name="ck_articles_score_pov"),
    )
    # pgvector column (can't use sa.Column directly)
    op.execute("ALTER TABLE articles ADD COLUMN embedding vector(768);")

    # Indexes
    op.create_index("idx_articles_processed_at", "articles", ["processed_at"], postgresql_using="btree")
    op.create_index("idx_articles_status", "articles", ["status"])
    op.create_index("idx_articles_privacy", "articles", ["privacy"])
    op.create_index("idx_articles_contributor", "articles", ["contributor"])
    op.create_index("idx_articles_score_pov", "articles", ["score_pov"],
                    postgresql_where=sa.text("score_pov IS NOT NULL"))
    op.create_index("idx_articles_content_date", "articles", ["content_date"],
                    postgresql_where=sa.text("content_date IS NOT NULL"))
    op.execute("""
        CREATE INDEX idx_articles_embedding ON articles
        USING ivfflat (embedding vector_cosine_ops) WHERE embedding IS NOT NULL;
    """)
    op.execute("""
        CREATE INDEX idx_articles_fts ON articles
        USING gin(to_tsvector('english', coalesce(title, '') || ' ' || coalesce(summary, '')));
    """)

    # -------------------------------------------------------------------------
    # digests table
    # -------------------------------------------------------------------------
    op.create_table(
        "digests",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content", sa.Text, nullable=False),
        sa.Column("article_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    # -------------------------------------------------------------------------
    # ingest_jobs table (legacy, kept for compatibility)
    # -------------------------------------------------------------------------
    op.create_table(
        "ingest_jobs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default="processing"),
        sa.Column("result", JSONB, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_ingest_jobs_status", "ingest_jobs", ["status"])
    op.create_index("idx_ingest_jobs_created_at", "ingest_jobs", ["created_at"])

    # -------------------------------------------------------------------------
    # ingests table (new unified ingest tracking)
    # -------------------------------------------------------------------------
    op.create_table(
        "ingests",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default="pending"),
        sa.Column("destination", sa.Text, nullable=True),
        sa.Column("flow_run_id", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notify", JSONB, nullable=True),
    )
    op.create_index("idx_ingests_created_at", "ingests", ["created_at"])
    op.create_index("idx_ingests_status", "ingests", ["status"])
    op.create_index("idx_ingests_url", "ingests", ["url"])

    # -------------------------------------------------------------------------
    # settings table
    # -------------------------------------------------------------------------
    op.create_table(
        "settings",
        sa.Column("key", sa.Text, primary_key=True),
        sa.Column("value", sa.Text, nullable=False),
    )

    # -------------------------------------------------------------------------
    # watched_accounts table (third brain)
    # -------------------------------------------------------------------------
    op.create_table(
        "watched_accounts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("handle", sa.Text, nullable=False),
        sa.Column("platform", sa.Text, nullable=False, server_default="twitter"),
        sa.Column("display_name", sa.Text, nullable=True),
        sa.Column("added_reason", sa.Text, nullable=True),
        sa.Column("score_quality", sa.SmallInteger, nullable=True),
        sa.Column("active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint("handle", "platform", name="uq_watched_accounts_handle_platform"),
        sa.CheckConstraint("score_quality BETWEEN 1 AND 5", name="ck_watched_accounts_score_quality"),
    )
    op.create_index("idx_watched_accounts_active", "watched_accounts", ["active", "platform"])

    # -------------------------------------------------------------------------
    # repos table (GitHub repo tracking)
    # -------------------------------------------------------------------------
    op.create_table(
        "repos",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("url", sa.Text, nullable=False, unique=True),
        sa.Column("owner", sa.Text, nullable=False),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("purpose", sa.Text, nullable=True),
        sa.Column("architecture", sa.Text, nullable=True),
        sa.Column("key_features", sa.ARRAY(sa.Text), nullable=True),
        sa.Column("stack", sa.ARRAY(sa.Text), nullable=True),
        sa.Column("tradeoffs", sa.Text, nullable=True),
        sa.Column("our_notes", sa.Text, nullable=True),
        sa.Column("fit_for_us", sa.Text, nullable=True),
        sa.Column("watched", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("last_release", sa.Text, nullable=True),
        sa.Column("last_release_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("release_notes", sa.Text, nullable=True),
        sa.Column("stars", sa.Integer, nullable=True),
        sa.Column("readme_text", sa.Text, nullable=True),
        sa.Column("last_update_summary", sa.Text, nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_push_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("status", sa.Text, nullable=False, server_default="pending"),
        sa.Column("error_message", sa.Text, nullable=True),
    )
    op.create_index("idx_repos_owner_name", "repos", ["owner", "name"])
    op.create_index("idx_repos_watched", "repos", ["watched"],
                    postgresql_where=sa.text("watched = true"))
    op.create_index("idx_repos_last_push_at", "repos", ["last_push_at"],
                    postgresql_where=sa.text("last_push_at IS NOT NULL"))
    op.execute("""
        CREATE INDEX idx_repos_fts ON repos
        USING gin(to_tsvector('english',
            coalesce(name, '') || ' ' ||
            coalesce(purpose, '') || ' ' ||
            coalesce(architecture, '') || ' ' ||
            coalesce(tradeoffs, '')
        ));
    """)

    # -------------------------------------------------------------------------
    # skills table
    # -------------------------------------------------------------------------
    op.create_table(
        "skills",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("repo_id", UUID(as_uuid=True), sa.ForeignKey("repos.id", ondelete="SET NULL"), nullable=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("skill_path", sa.Text, nullable=True),
        sa.Column("source_url", sa.Text, nullable=True, unique=True),
        sa.Column("skill_md", sa.Text, nullable=True),
        sa.Column("install_cmd", sa.Text, nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.execute("CREATE INDEX idx_skills_repo_id ON skills (repo_id) WHERE repo_id IS NOT NULL")
    op.execute("""
        CREATE INDEX idx_skills_fts ON skills
        USING gin(to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, '')));
    """)


def downgrade() -> None:
    op.drop_table("skills")
    op.drop_table("repos")
    op.drop_table("watched_accounts")
    op.drop_table("settings")
    op.drop_table("ingests")
    op.drop_table("ingest_jobs")
    op.drop_table("digests")
    op.drop_table("articles")
