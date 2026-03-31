"""Async PostgresClient for FastAPI handlers.

This provides async versions of the most commonly used methods.
The sync PostgresClient remains for Prefect flows and scripts.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data.postgres.engine import get_async_session_factory
from data.postgres.models import Article, Ingest, Repo, Skill


def _serialize_row(row: dict) -> dict:
    """Coerce non-JSON-serializable types in a DB row dict.

    SQLAlchemy returns UUID columns as uuid.UUID objects. API response models
    type IDs as str | None, so we stringify UUIDs here at the boundary rather
    than adding field validators to every Pydantic model.
    """
    return {
        k: str(v) if isinstance(v, uuid.UUID) else v
        for k, v in row.items()
    }


class AsyncPostgresClient:
    """Async database client for FastAPI endpoints."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._session_factory = get_async_session_factory(dsn)

    # ------------------------------------------------------------------
    # Articles - Read
    # ------------------------------------------------------------------

    async def get_article_by_url(self, *, url: str) -> dict[str, Any] | None:
        """Get article by URL."""
        t = Article.__table__
        stmt = sa.select(t).where(t.c.url == url)
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            row = result.mappings().first()
            return _serialize_row(dict(row)) if row else None

    async def list_articles(
        self,
        *,
        q: str | None = None,
        url: str | None = None,
        status: str | None = None,
        since: datetime | None = None,
        tags: list[str] | None = None,
        score_min: int | None = None,
        source_type: str | None = None,
        include_private: bool = True,
        missing_embeddings: bool = False,
        limit: int = 20,
        offset: int = 0,
        embedding: list[float] | None = None,
    ) -> list[dict[str, Any]]:
        """Unified article query — all filters composable, all paths respect all filters.

        Modes (in priority order):
          url     → exact URL lookup (returns 0 or 1)
          q       → semantic search (if embedding provided) or FTS
          default → recency sort with filters applied
        """
        t = Article.__table__
        cols = [
            t.c.id, t.c.url, t.c.title, t.c.summary, t.c.tags,
            t.c.source_type, t.c.status, t.c.ingested_at, t.c.processed_at,
            t.c.score_usefulness, t.c.score_interest, t.c.score_pov, t.c.score_uniqueness,
            t.c.privacy,
        ]

        # Build shared filter list — applied to ALL query modes
        filters: list = []
        if url:
            filters.append(t.c.url == url)
        if status:
            filters.append(t.c.status == status)
        if since:
            filters.append(t.c.ingested_at >= since)
        if score_min is not None:
            filters.append(t.c.score_usefulness >= score_min)
        if source_type:
            filters.append(t.c.source_type == source_type)
        if not include_private:
            filters.append(sa.or_(t.c.privacy == "public", t.c.privacy.is_(None)))
        if tags:
            # All specified tags must be present (AND semantics)
            for tag in tags:
                filters.append(t.c.tags.contains(sa.cast([tag], sa.ARRAY(sa.Text))))
        if missing_embeddings:
            filters.append(t.c.embedding.is_(None))
            filters.append(t.c.status == "processed")

        where = sa.and_(*filters) if filters else sa.true()

        if url:
            # Exact lookup — skip search/sort entirely
            stmt = sa.select(*cols).where(where).limit(1)

        elif q and embedding:
            # Semantic search — order by cosine distance, filters still apply
            stmt = (
                sa.select(*cols, t.c.embedding.cosine_distance(embedding).label("distance"))
                .where(sa.and_(where, t.c.embedding.isnot(None)))
                .order_by("distance")
                .limit(limit)
                .offset(offset)
            )

        elif q:
            # FTS fallback — compute tsvector on the fly
            from data.postgres.queries import articles_fts
            ts_vec, ts_query = articles_fts(q, t)
            stmt = (
                sa.select(*cols)
                .where(sa.and_(where, ts_vec.op("@@")(ts_query)))
                .order_by(t.c.processed_at.desc().nulls_last())
                .limit(limit)
                .offset(offset)
            )

        else:
            # Default: recency sort
            # NULLS LAST keeps pending/failed (processed_at=NULL) at the bottom
            stmt = (
                sa.select(*cols)
                .where(where)
                .order_by(
                    t.c.processed_at.desc().nulls_last(),
                    t.c.ingested_at.desc(),
                )
                .limit(limit)
                .offset(offset)
            )

        async with self._session_factory() as session:
            result = await session.execute(stmt)
            return [_serialize_row(dict(r)) for r in result.mappings().all()]

    # Keep old names as thin aliases during transition
    async def get_recent_articles(self, *, limit: int = 50, since=None, status: str | None = None) -> list[dict[str, Any]]:
        return await self.list_articles(limit=limit, since=since, status=status)

    async def search_articles(self, *, query: str, limit: int = 20, embedding=None, include_private: bool = True) -> list[dict[str, Any]]:
        return await self.list_articles(q=query, limit=limit, embedding=embedding, include_private=include_private)

    async def get_articles_stats(self) -> dict[str, Any]:
        """Return count by status, embedding coverage, and recent failure info."""
        t = Article.__table__
        async with self._session_factory() as session:
            count_stmt = sa.select(t.c.status, func.count().label("count")).group_by(t.c.status)
            result = await session.execute(count_stmt)
            counts = {row.status: row.count for row in result}

            embed_stmt = sa.select(
                func.count().label("total"),
                func.count(t.c.embedding).label("with_embedding"),
            ).where(t.c.status == "processed")
            result = await session.execute(embed_stmt)
            row = result.one()
            embeddings = {
                "total_processed": row.total,
                "with_embedding": row.with_embedding,
                "without_embedding": row.total - row.with_embedding,
            }

            fail_stmt = (
                sa.select(t.c.url, t.c.processed_at, t.c.failure_log)
                .where(t.c.status == "failed")
                .order_by(t.c.processed_at.desc())
                .limit(5)
            )
            result = await session.execute(fail_stmt)
            recent_failures = [dict(r) for r in result.mappings().all()]
            return {"counts": counts, "embeddings": embeddings, "recent_failures": recent_failures}

    async def get_top_tags(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Return top tags across all processed articles, ordered by frequency."""
        stmt = sa.text("""
            SELECT tag, COUNT(*) as count
            FROM articles, unnest(tags) AS tag
            WHERE status = 'processed'
            GROUP BY tag
            ORDER BY count DESC
            LIMIT :limit
        """)
        async with self._session_factory() as session:
            result = await session.execute(stmt, {"limit": limit})
            return [{"tag": row[0], "count": row[1]} for row in result.fetchall()]



    async def delete_article_by_url(self, *, url: str) -> bool:
        """Delete an article by URL. Returns True if a row was deleted."""
        t = Article.__table__
        async with self._session_factory() as session:
            result = await session.execute(sa.delete(t).where(t.c.url == url))
            await session.commit()
            return result.rowcount > 0

    # ------------------------------------------------------------------
    # Articles - Write
    # ------------------------------------------------------------------

    async def upsert_article(
        self,
        *,
        url: str,
        readwise_id: str | None = None,
        title: str,
        summary: str,
        tags: list[str],
        source_type: str,
        raw_text: str | None = None,
        status: str = "processed",
        privacy: str = "public",
        contributor: str | None = None,
        file_path: str | None = None,
        processed_at: datetime | None = None,
    ) -> None:
        """Upsert an article."""
        from sqlalchemy.sql import func as sa_func
        t = Article.__table__
        # Set processed_at to now when status is 'processed' and not explicitly provided
        if processed_at is None and status == "processed":
            processed_at_value = sa_func.now()
        else:
            processed_at_value = processed_at
        values = {
            "url": url,
            "readwise_id": readwise_id,
            "title": title,
            "summary": summary,
            "tags": tags,
            "source_type": source_type,
            "raw_text": raw_text,
            "status": status,
            "privacy": privacy,
            "contributor": contributor,
            "file_path": file_path,
            "processed_at": processed_at_value,
        }
        stmt = pg_insert(t).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["url"],
            set_={k: v for k, v in values.items() if k != "url"},
        )
        async with self._session_factory() as session:
            await session.execute(stmt)
            await session.commit()



    # ------------------------------------------------------------------
    # Ingests
    # ------------------------------------------------------------------

    async def create_ingest(self, *, url: str, notify: dict | None = None) -> str:
        """Create an ingest record and return its ID."""
        t = Ingest.__table__
        stmt = (
            sa.insert(t)
            .values(url=url, status="pending", notify=notify or {})
            .returning(t.c.id)
        )
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            await session.commit()
            return str(result.scalar())

    async def get_ingest(self, ingest_id: str) -> dict[str, Any] | None:
        """Get ingest by ID."""
        t = Ingest.__table__
        stmt = sa.select(t).where(t.c.id == ingest_id)
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            row = result.mappings().first()
            return _serialize_row(dict(row)) if row else None

    async def get_recent_ingests(
        self, *, limit: int = 50, status: str | None = None
    ) -> list[dict[str, Any]]:
        """Get recent ingests ordered by created_at DESC, optionally filtered by status."""
        t = Ingest.__table__
        stmt = sa.select(
            t.c.id, t.c.url, t.c.status, t.c.destination,
            t.c.flow_run_id, t.c.error, t.c.created_at, t.c.completed_at
        )
        if status:
            stmt = stmt.where(t.c.status == status)
        stmt = stmt.order_by(t.c.created_at.desc()).limit(limit)
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            return [_serialize_row(dict(r)) for r in result.mappings().all()]

    async def update_ingest_flow_run(self, ingest_id: str, flow_run_id: str) -> None:
        """Update ingest with Prefect flow run ID."""
        stmt = (
            sa.update(Ingest)
            .where(Ingest.id == ingest_id)
            .values(flow_run_id=flow_run_id, status="processing")
        )
        async with self._session_factory() as session:
            await session.execute(stmt)
            await session.commit()

    async def complete_ingest(
        self, ingest_id: str, *, destination: str, error: str | None = None
    ) -> None:
        """Mark ingest as completed or failed."""
        status = "failed" if error else "completed"
        stmt = (
            sa.update(Ingest)
            .where(Ingest.id == ingest_id)
            .values(
                status=status,
                destination=destination,
                error=error,
                completed_at=func.now(),
            )
        )
        async with self._session_factory() as session:
            await session.execute(stmt)
            await session.commit()

    async def fail_ingest(self, ingest_id: str, *, error: str) -> None:
        """Mark ingest as failed with error message."""
        stmt = (
            sa.update(Ingest)
            .where(Ingest.id == ingest_id)
            .values(status="failed", error=error, completed_at=func.now())
        )
        async with self._session_factory() as session:
            await session.execute(stmt)
            await session.commit()

    # ------------------------------------------------------------------
    # Repos
    # ------------------------------------------------------------------

    async def get_repo_by_url(self, *, url: str) -> dict[str, Any] | None:
        """Get repo by URL."""
        t = Repo.__table__
        stmt = sa.select(t).where(t.c.url == url)
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            row = result.mappings().first()
            return _serialize_row(dict(row)) if row else None

    async def get_repo_by_id(self, repo_id: str) -> dict[str, Any] | None:
        """Get repo by ID."""
        t = Repo.__table__
        stmt = sa.select(t).where(t.c.id == repo_id)
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            row = result.mappings().first()
            return _serialize_row(dict(row)) if row else None

    async def list_repos(
        self,
        *,
        q: str | None = None,
        watched: bool | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """List or search repos with optional FTS and watched filter."""
        t = Repo.__table__
        cols = [
            t.c.id, t.c.url, t.c.owner, t.c.name, t.c.description,
            t.c.purpose, t.c.stars, t.c.watched, t.c.status, t.c.ingested_at,
            t.c.architecture, t.c.key_features, t.c.stack, t.c.tradeoffs,
            t.c.fit_for_us, t.c.our_notes, t.c.last_release,
        ]
        stmt = sa.select(*cols)
        if watched is not None:
            stmt = stmt.where(t.c.watched == watched)
        if q:
            stmt = stmt.where(
                sa.or_(
                    t.c.name.ilike(f"%{q}%"),
                    t.c.description.ilike(f"%{q}%"),
                    t.c.purpose.ilike(f"%{q}%"),
                )
            )
        stmt = stmt.order_by(t.c.ingested_at.desc()).limit(limit)
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            return [_serialize_row(dict(r)) for r in result.mappings().all()]

    async def update_repo(
        self,
        url: str,
        *,
        our_notes: str | None = None,
        watched: bool | None = None,
        last_release: str | None = None,
        last_checked_at: datetime | None = None,
    ) -> None:
        """Update repo fields by URL.
        
        Args:
            url: Repository URL (used as lookup key)
            our_notes: Human/agent annotations about the repo
            watched: Whether to track releases for this repo
            last_release: Last known release tag
            last_checked_at: Timestamp of last update check
        """
        updates: dict[str, Any] = {}
        if our_notes is not None:
            updates["our_notes"] = our_notes
        if watched is not None:
            updates["watched"] = watched
        if last_release is not None:
            updates["last_release"] = last_release
        if last_checked_at is not None:
            updates["last_checked_at"] = last_checked_at
        
        if not updates:
            return
        
        stmt = sa.update(Repo).where(Repo.url == url).values(**updates)
        async with self._session_factory() as session:
            await session.execute(stmt)
            await session.commit()

    # ------------------------------------------------------------------
    # Skills
    # ------------------------------------------------------------------

    async def list_skills(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """List all skills."""
        t = Skill.__table__
        stmt = (
            sa.select(
                t.c.id, t.c.repo_id, t.c.name, t.c.description,
                t.c.skill_path, t.c.source_url, t.c.install_cmd, t.c.ingested_at,
            )
            .order_by(t.c.name.asc())
            .limit(limit)
        )
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            return [_serialize_row(dict(r)) for r in result.mappings().all()]

    async def search_skills(self, query: str, *, limit: int = 20) -> list[dict[str, Any]]:
        """Search skills by name or description (case-insensitive substring match)."""
        t = Skill.__table__
        stmt = (
            sa.select(
                t.c.id, t.c.repo_id, t.c.name, t.c.description,
                t.c.skill_path, t.c.source_url, t.c.install_cmd, t.c.ingested_at,
            )
            .where(
                sa.or_(
                    t.c.name.ilike(f"%{query}%"),
                    t.c.description.ilike(f"%{query}%"),
                )
            )
            .order_by(t.c.name.asc())
            .limit(limit)
        )
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            return [_serialize_row(dict(r)) for r in result.mappings().all()]

    async def get_skills_for_repo(self, repo_id: str) -> list[dict[str, Any]]:
        """Get skills for a repo."""
        t = Skill.__table__
        stmt = (
            sa.select(
                t.c.id, t.c.repo_id, t.c.name, t.c.description,
                t.c.skill_path, t.c.source_url, t.c.install_cmd, t.c.ingested_at,
            )
            .where(t.c.repo_id == repo_id)
            .order_by(t.c.name.asc())
        )
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            return [_serialize_row(dict(r)) for r in result.mappings().all()]


