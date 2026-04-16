"""Declarative SQLAlchemy models — the single source of truth for all 6 tables.

Alembic autogenerates migrations from these models (0009+).
Existing migrations (0001–0008) are historical and untouched.
"""
from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    SmallInteger,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

try:
    from pgvector.sqlalchemy import Vector
except ImportError:  # pgvector not installed — models still importable for tests
    Vector = None  # type: ignore[assignment,misc]


class Base(DeclarativeBase):
    pass


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    url: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    readwise_id: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(Text)
    summary: Mapped[str | None] = mapped_column(Text)
    tags = Column(ARRAY(Text), server_default=text("'{}'"))
    source_type: Mapped[str | None] = mapped_column(Text)
    raw_text: Mapped[str | None] = mapped_column(Text)
    ingested_at = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    processed_at = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'"))
    failure_log = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    embedding = Column(Vector(768)) if Vector else Column(sa.LargeBinary)  # type: ignore[misc]
    privacy: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'public'"))
    file_path: Mapped[str | None] = mapped_column(Text)
    contributor: Mapped[str | None] = mapped_column(Text)
    score_usefulness = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_usefulness BETWEEN 1 AND 5", name="ck_articles_score_usefulness"),
        nullable=True,
    )
    score_interest = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_interest BETWEEN 1 AND 5", name="ck_articles_score_interest"),
        nullable=True,
    )
    score_pov = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_pov BETWEEN 1 AND 5", name="ck_articles_score_pov"),
        nullable=True,
    )
    score_uniqueness = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_uniqueness BETWEEN 1 AND 5", name="ck_articles_score_uniqueness"),
        nullable=True,
    )
    content_date = mapped_column(DateTime(timezone=True), nullable=True)
    curator_note: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        sa.Index("idx_articles_processed_at", "processed_at", postgresql_using="btree"),
        sa.Index("idx_articles_status", "status"),
        sa.Index("idx_articles_privacy", "privacy"),
        sa.Index("idx_articles_contributor", "contributor"),
        sa.Index("idx_articles_score_pov", "score_pov", postgresql_where=text("score_pov IS NOT NULL")),
        sa.Index("idx_articles_score_uniqueness", "score_uniqueness", postgresql_where=text("score_uniqueness IS NOT NULL")),
        sa.Index(
            "idx_articles_embedding",
            text("embedding vector_cosine_ops"),
            postgresql_using="ivfflat",
            postgresql_where=text("embedding IS NOT NULL"),
        ),
        sa.Index(
            "idx_articles_fts",
            text("to_tsvector('english', coalesce(title, '') || ' ' || coalesce(summary, ''))"),
            postgresql_using="gin",
        ),
    )


class Digest(Base):
    __tablename__ = "digests"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    period_start = mapped_column(DateTime(timezone=True), nullable=False)
    period_end = mapped_column(DateTime(timezone=True), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    article_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    created_at = mapped_column(DateTime(timezone=True), server_default=text("now()"))


class IngestJob(Base):
    __tablename__ = "ingest_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'processing'"))
    result = Column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text)
    created_at = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    completed_at = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        sa.Index("idx_ingest_jobs_status", "status"),
        sa.Index("idx_ingest_jobs_created_at", "created_at", postgresql_using="btree"),
    )


class Ingest(Base):
    """Durable intake queue. Created by the API before dispatching to Prefect.

    One record per URL submission. Status tracks the full lifecycle:
      pending → processing → completed | failed

    The `destination` field records where the data landed (articles, repos).
    The `flow_run_id` links back to the Prefect run for observability.
    """
    __tablename__ = "ingests"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'pending'")
    )
    destination: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # "articles" | "repos" | None
    flow_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    completed_at = mapped_column(DateTime(timezone=True), nullable=True)
    notify = Column(JSONB, nullable=True)  # stored so flows can re-notify on retry

    __table_args__ = (
        sa.Index("idx_ingests_created_at", "created_at", postgresql_using="btree"),
        sa.Index("idx_ingests_status", "status"),
        sa.Index("idx_ingests_url", "url"),
    )


class Setting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)


class WatchedAccount(Base):
    __tablename__ = "watched_accounts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    handle: Mapped[str] = mapped_column(Text, nullable=False)
    platform: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'twitter'"))
    display_name: Mapped[str | None] = mapped_column(Text)
    added_reason: Mapped[str | None] = mapped_column(Text)
    score_quality = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_quality BETWEEN 1 AND 5", name="ck_watched_accounts_score_quality"),
        nullable=True,
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    last_checked_at = mapped_column(DateTime(timezone=True), nullable=True)
    created_at = mapped_column(DateTime(timezone=True), server_default=text("now()"))

    __table_args__ = (
        sa.UniqueConstraint("handle", "platform", name="uq_watched_accounts_handle_platform"),
        sa.Index("idx_watched_accounts_active", "active", "platform"),
    )


class Skill(Base):
    __tablename__ = "skills"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    repo_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        sa.ForeignKey("repos.id", ondelete="SET NULL"),
        nullable=True,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    skill_path: Mapped[str | None] = mapped_column(Text)            # e.g. "skills/foo/SKILL.md" (relative, if from repo)
    source_url: Mapped[str | None] = mapped_column(Text, unique=True)  # canonical dedup key (raw URL or direct link)
    skill_md: Mapped[str | None] = mapped_column(Text)              # full SKILL.md content
    install_cmd: Mapped[str | None] = mapped_column(Text)           # extracted install instructions
    ingested_at = mapped_column(DateTime(timezone=True), server_default=text("now()"))

    __table_args__ = (
        sa.Index("idx_skills_repo_id", "repo_id", postgresql_where=text("repo_id IS NOT NULL")),
        sa.Index(
            "idx_skills_fts",
            text("to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, ''))"),
            postgresql_using="gin",
        ),
    )


class Repo(Base):
    __tablename__ = "repos"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    url: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    owner: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    purpose: Mapped[str | None] = mapped_column(Text)
    architecture: Mapped[str | None] = mapped_column(Text)
    key_features = Column(ARRAY(Text), nullable=True)
    stack = Column(ARRAY(Text), nullable=True)
    tradeoffs: Mapped[str | None] = mapped_column(Text)
    our_notes: Mapped[str | None] = mapped_column(Text)
    fit_for_us: Mapped[str | None] = mapped_column(Text)
    score_usefulness = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_usefulness BETWEEN 1 AND 5", name="ck_repos_score_usefulness"),
        nullable=True,
    )
    score_interest = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_interest BETWEEN 1 AND 5", name="ck_repos_score_interest"),
        nullable=True,
    )
    score_pov = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_pov BETWEEN 1 AND 5", name="ck_repos_score_pov"),
        nullable=True,
    )
    score_uniqueness = mapped_column(
        SmallInteger,
        sa.CheckConstraint("score_uniqueness BETWEEN 1 AND 5", name="ck_repos_score_uniqueness"),
        nullable=True,
    )
    watched: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    last_release: Mapped[str | None] = mapped_column(Text)
    last_release_at = mapped_column(DateTime(timezone=True), nullable=True)
    last_push_at = mapped_column(DateTime(timezone=True), nullable=True)
    release_notes: Mapped[str | None] = mapped_column(Text)
    stars: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ingested_at = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    updated_at = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'"))
    error_message: Mapped[str | None] = mapped_column(Text)
    readme_text: Mapped[str | None] = mapped_column(Text)
    last_update_summary: Mapped[str | None] = mapped_column(Text)
    last_checked_at = mapped_column(DateTime(timezone=True), nullable=True)
    embedding = Column(Vector(768)) if Vector else Column(sa.LargeBinary)  # type: ignore[misc]

    __table_args__ = (
        sa.Index("idx_repos_owner_name", "owner", "name"),
        sa.Index("idx_repos_watched", "watched", postgresql_where=text("watched = true")),
        sa.Index("idx_repos_score_pov", "score_pov", postgresql_where=text("score_pov IS NOT NULL")),
        sa.Index("idx_repos_score_uniqueness", "score_uniqueness", postgresql_where=text("score_uniqueness IS NOT NULL")),
        sa.Index(
            "idx_repos_embedding",
            text("embedding vector_cosine_ops"),
            postgresql_using="ivfflat",
            postgresql_where=text("embedding IS NOT NULL"),
        ),
        sa.Index(
            "idx_repos_fts",
            text(
                "to_tsvector('english', "
                "coalesce(name, '') || ' ' || "
                "coalesce(purpose, '') || ' ' || "
                "coalesce(architecture, '') || ' ' || "
                "coalesce(tradeoffs, ''))"
            ),
            postgresql_using="gin",
        ),
    )
