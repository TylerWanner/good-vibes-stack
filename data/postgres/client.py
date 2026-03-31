from __future__ import annotations

import json as _json
from datetime import datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert

from data.postgres.engine import get_engine, get_session_factory
from data.postgres.models import (
    Article,
    Digest,
    Ingest,

    Repo,
    Setting,
    Skill,
    WatchedAccount,
)


class PostgresClient:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._session_factory = get_session_factory(dsn)

    # ------------------------------------------------------------------
    # Articles
    # ------------------------------------------------------------------

    def upsert_article(
        self,
        *,
        url: str,
        readwise_id: str | None,
        title: str,
        summary: str,
        tags: list[str],
        source_type: str,
        raw_text: str,
        status: str = "processed",
        privacy: str = "public",
        contributor: str | None = None,
        file_path: str | None = None,
        score_usefulness: int | None = None,
        score_interest: int | None = None,
        score_pov: int | None = None,
        score_uniqueness: int | None = None,
        content_date: str | None = None,
    ) -> None:
        values: dict[str, Any] = dict(
            url=url,
            readwise_id=readwise_id,
            title=title,
            summary=summary,
            tags=tags,
            source_type=source_type,
            raw_text=raw_text,
            processed_at=func.now(),
            status=status,
            privacy=privacy,
            contributor=contributor,
            file_path=file_path,
        )
        update_values = {k: v for k, v in values.items() if k != "url"}

        # Only include optional fields when provided
        if score_usefulness is not None:
            values["score_usefulness"] = score_usefulness
        if score_interest is not None:
            values["score_interest"] = score_interest
        if score_pov is not None:
            values["score_pov"] = score_pov
        if score_uniqueness is not None:
            values["score_uniqueness"] = score_uniqueness
        if content_date is not None:
            try:
                # Try ISO8601 first (articles, schema.org)
                from datetime import datetime, timezone
                import re
                # Strip trailing Z, normalize to +00:00
                iso = re.sub(r'Z$', '+00:00', content_date)
                values["content_date"] = datetime.fromisoformat(iso)
            except Exception:
                try:
                    # Twitter format: "Mon Mar 23 12:27:38 +0000 2026"
                    from email.utils import parsedate_to_datetime
                    values["content_date"] = parsedate_to_datetime(content_date)
                except Exception:
                    pass

        # On conflict: COALESCE new score with existing for scores
        score_updates: dict[str, Any] = {}
        if score_usefulness is not None:
            score_updates["score_usefulness"] = func.coalesce(
                sa.literal(score_usefulness), Article.__table__.c.score_usefulness
            )
        if score_interest is not None:
            score_updates["score_interest"] = func.coalesce(
                sa.literal(score_interest), Article.__table__.c.score_interest
            )
        if score_pov is not None:
            score_updates["score_pov"] = func.coalesce(
                sa.literal(score_pov), Article.__table__.c.score_pov
            )
        if score_uniqueness is not None:
            score_updates["score_uniqueness"] = func.coalesce(
                sa.literal(score_uniqueness), Article.__table__.c.score_uniqueness
            )

        stmt = (
            pg_insert(Article)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["url"],
                set_={**update_values, **score_updates},
            )
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    def get_high_quality_articles(
        self,
        *,
        days: int = 7,
        limit: int = 20,
        min_pov: int = 4,
        min_usefulness: int = 1,
        min_interest: int = 1,
    ) -> list[dict[str, Any]]:
        """Return recent articles above score thresholds, ordered by score_pov DESC."""
        t = Article.__table__
        stmt = (
            sa.select(
                t.c.url, t.c.title, t.c.summary, t.c.tags, t.c.source_type,
                t.c.score_usefulness, t.c.score_interest, t.c.score_pov,
            )
            .where(
                t.c.status == "processed",
                t.c.processed_at >= func.now() - text(f"INTERVAL '{int(days)} days'"),
                t.c.score_pov >= min_pov,
                t.c.score_usefulness >= min_usefulness,
                t.c.score_interest >= min_interest,
            )
            .order_by(t.c.score_pov.desc(), t.c.score_interest.desc(), t.c.processed_at.desc())
            .limit(limit)
        )
        with self._session_factory() as session:
            rows = session.execute(stmt).mappings().all()
            return [dict(r) for r in rows]

    def get_article_by_url(self, *, url: str) -> dict[str, Any] | None:
        """Return a single article record by URL, or None if not found."""
        t = Article.__table__
        stmt = sa.select(
            t.c.url, t.c.title, t.c.status, t.c.processed_at, t.c.source_type,
            t.c.contributor, t.c.score_usefulness, t.c.score_interest, t.c.score_pov,
            t.c.score_uniqueness, t.c.content_date,
            t.c.failure_log, t.c.tags, t.c.summary,
        ).where(t.c.url == url)
        with self._session_factory() as session:
            row = session.execute(stmt).mappings().first()
            return dict(row) if row else None

    def get_recent_articles(
        self, *, limit: int = 20, since: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        """Return recent articles ordered by processed_at DESC."""
        t = Article.__table__
        stmt = sa.select(
            t.c.url, t.c.title, t.c.status, t.c.processed_at, t.c.source_type,
            t.c.contributor, t.c.score_usefulness, t.c.score_interest, t.c.score_pov,
        )
        if since:
            stmt = stmt.where(t.c.processed_at >= since)
        if status:
            stmt = stmt.where(t.c.status == status)
        stmt = stmt.order_by(t.c.processed_at.desc()).limit(limit)
        with self._session_factory() as session:
            rows = session.execute(stmt).mappings().all()
            return [dict(r) for r in rows]

    def get_articles_stats(self) -> dict[str, Any]:
        """Return count by status and recent failure info."""
        t = Article.__table__
        with self._session_factory() as session:
            count_stmt = sa.select(t.c.status, func.count().label("count")).group_by(t.c.status)
            counts = {row.status: row.count for row in session.execute(count_stmt)}

            fail_stmt = (
                sa.select(t.c.url, t.c.processed_at, t.c.failure_log)
                .where(t.c.status == "failed")
                .order_by(t.c.processed_at.desc())
                .limit(5)
            )
            recent_failures = [dict(r) for r in session.execute(fail_stmt).mappings().all()]
            return {"counts": counts, "recent_failures": recent_failures}

    def mark_article_pending(self, *, url: str) -> dict[str, Any] | None:
        """Atomic insert-if-absent for pending articles.

        Returns the existing article dict if URL already exists, or None if new.
        """
        stmt = (
            pg_insert(Article)
            .values(url=url, status="pending")
            .on_conflict_do_nothing(index_elements=["url"])
            .returning(sa.literal(None).label("existing"))
        )
        with self._session_factory() as session:
            result = session.execute(stmt).first()
            if result is not None:
                # New record created
                session.commit()
                return None
            # URL already exists — fetch
            t = Article.__table__
            existing = session.execute(
                sa.select(
                    t.c.url, t.c.readwise_id, t.c.title, t.c.summary,
                    t.c.tags, t.c.source_type, t.c.status,
                ).where(t.c.url == url)
            ).mappings().first()
            session.commit()
            return dict(existing) if existing else None

    def mark_article_failed(self, *, url: str, readwise_id: str | None, error_message: str) -> None:
        entry = {"ts": datetime.now(timezone.utc).isoformat(), "message": error_message}
        entry_array = [entry]
        t = Article.__table__
        stmt = (
            pg_insert(Article)
            .values(
                url=url,
                readwise_id=readwise_id,
                status="failed",
                failure_log=sa.type_coerce(entry_array, JSONB),
                processed_at=func.now(),
            )
            .on_conflict_do_update(
                index_elements=["url"],
                set_={
                    "readwise_id": readwise_id,
                    "status": "failed",
                    "failure_log": t.c.failure_log.concat(sa.type_coerce(entry_array, JSONB)),
                    "processed_at": func.now(),
                },
            )
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    def fetch_recent_processed(self, *, days: int = 7, limit: int = 100) -> list[dict[str, Any]]:
        since = datetime.now(timezone.utc) - timedelta(days=days)
        t = Article.__table__
        stmt = (
            sa.select(t.c.url, t.c.title, t.c.summary, t.c.tags, t.c.source_type, t.c.processed_at)
            .where(t.c.status == "processed", t.c.processed_at >= since)
            .order_by(t.c.processed_at.desc())
            .limit(limit)
        )
        with self._session_factory() as session:
            return [dict(r) for r in session.execute(stmt).mappings().all()]

    # ------------------------------------------------------------------
    # Digests
    # ------------------------------------------------------------------

    def store_digest(
        self,
        *,
        period_start: datetime,
        period_end: datetime,
        content: str,
        article_count: int,
    ) -> None:
        with self._session_factory() as session:
            session.execute(
                sa.insert(Digest).values(
                    period_start=period_start,
                    period_end=period_end,
                    content=content,
                    article_count=article_count,
                )
            )
            session.commit()

    def latest_digest(self) -> dict[str, Any] | None:
        t = Digest.__table__
        stmt = (
            sa.select(t.c.period_start, t.c.period_end, t.c.content, t.c.article_count, t.c.created_at)
            .order_by(t.c.created_at.desc())
            .limit(1)
        )
        with self._session_factory() as session:
            row = session.execute(stmt).mappings().first()
            return dict(row) if row else None

    # ------------------------------------------------------------------
    # Embeddings / search
    # ------------------------------------------------------------------

    def store_embedding(self, article_id: str, embedding: list[float]) -> None:
        """Store embedding vector for an article."""
        with self._session_factory() as session:
            session.execute(
                sa.update(Article)
                .where(Article.__table__.c.id == article_id)
                .values(embedding=embedding)
            )
            session.commit()

    def store_repo_embedding(self, repo_id: str, embedding: list[float]) -> None:
        """Store embedding vector for a repo."""
        from data.postgres.models import Repo
        with self._session_factory() as session:
            session.execute(
                sa.update(Repo)
                .where(Repo.__table__.c.id == repo_id)
                .values(embedding=embedding)
            )
            session.commit()

    def get_article_id_by_url(self, url: str) -> str | None:
        """Get article UUID by URL."""
        with self._session_factory() as session:
            row = session.execute(
                sa.select(Article.__table__.c.id).where(Article.__table__.c.url == url)
            ).first()
            return str(row[0]) if row else None

    def search_articles_semantic(
        self, embedding: list[float], *, limit: int = 10, include_private: bool = False
    ) -> list[dict[str, Any]]:
        """Semantic search using cosine distance."""
        t = Article.__table__
        stmt = sa.select(
            t.c.url, t.c.title, t.c.summary, t.c.tags, t.c.processed_at,
            t.c.privacy, t.c.contributor,
        ).where(t.c.embedding.isnot(None))
        if not include_private:
            stmt = stmt.where(t.c.privacy == "public")
        # Cosine distance via <=> operator (pgvector)
        embedding_param = sa.literal_column(":emb", type_=t.c.embedding.type)
        stmt = stmt.order_by(t.c.embedding.op("<=>")(embedding_param)).limit(limit)
        with self._session_factory() as session:
            return [dict(r) for r in session.execute(stmt, {"emb": str(embedding)}).mappings().all()]

    def search_articles(
        self,
        query: str,
        *,
        limit: int = 10,
        embedding: list[float] | None = None,
        include_private: bool = False,
    ) -> list[dict[str, Any]]:
        # Try semantic search first if embedding provided
        if embedding is not None:
            try:
                results = self.search_articles_semantic(embedding, limit=limit, include_private=include_private)
                if len(results) >= 3:
                    return results
            except Exception:
                pass  # Fall through to FTS

        # FTS fallback
        from data.postgres.queries import articles_fts
        t = Article.__table__
        ts_vec, ts_query = articles_fts(query, t)
        rank = func.ts_rank(ts_vec, ts_query).label("rank")

        stmt = sa.select(
            t.c.url, t.c.title, t.c.summary, t.c.tags, t.c.processed_at,
            t.c.privacy, t.c.contributor, rank,
        ).where(
            sa.or_(
                ts_vec.op("@@")(ts_query),
                func.coalesce(func.array_to_string(t.c.tags, " "), "").ilike(f"%{query}%"),
            )
        )
        if not include_private:
            stmt = stmt.where(t.c.privacy == "public")
        stmt = stmt.order_by(sa.desc("rank"), t.c.processed_at.desc()).limit(limit)
        with self._session_factory() as session:
            return [dict(r) for r in session.execute(stmt).mappings().all()]

    # ------------------------------------------------------------------
    # Settings
    # ------------------------------------------------------------------

    def get_setting(self, key: str) -> str | None:
        with self._session_factory() as session:
            row = session.execute(
                sa.select(Setting.__table__.c.value).where(Setting.__table__.c.key == key)
            ).first()
            return row[0] if row else None

    def set_setting(self, key: str, value: str) -> None:
        stmt = (
            pg_insert(Setting)
            .values(key=key, value=value)
            .on_conflict_do_update(index_elements=["key"], set_={"value": value})
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    # ------------------------------------------------------------------
    # ---------------------------------------------------------------------------
    # Ingests — durable intake queue
    # ---------------------------------------------------------------------------

    def create_ingest(self, *, url: str, notify: dict | None = None, flow_run_id: str | None = None) -> str:
        """Create a pending ingest record. Returns the UUID as a string."""
        t = Ingest.__table__
        stmt = (
            sa.insert(t)
            .values(url=url, status="pending", notify=notify, flow_run_id=flow_run_id)
            .returning(t.c.id)
        )
        with self._session_factory() as session:
            row = session.execute(stmt).first()
            session.commit()
            return str(row[0])

    def get_ingest(self, ingest_id: str) -> dict[str, Any] | None:
        t = Ingest.__table__
        stmt = sa.select(
            t.c.id, t.c.url, t.c.status, t.c.destination,
            t.c.flow_run_id, t.c.error, t.c.created_at, t.c.completed_at, t.c.notify,
        ).where(t.c.id == ingest_id)
        with self._session_factory() as session:
            row = session.execute(stmt).mappings().first()
            return dict(row) if row else None

    def complete_ingest(self, ingest_id: str, *, destination: str) -> None:
        """Mark an ingest record as completed with its destination table."""
        t = Ingest.__table__
        stmt = (
            sa.update(t)
            .where(t.c.id == ingest_id)
            .values(status="completed", destination=destination, completed_at=func.now())
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    def fail_ingest(self, ingest_id: str, *, error: str) -> None:
        """Mark an ingest record as failed."""
        t = Ingest.__table__
        stmt = (
            sa.update(t)
            .where(t.c.id == ingest_id)
            .values(status="failed", error=error[:500], completed_at=func.now())
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    def update_ingest_flow_run(self, ingest_id: str, *, flow_run_id: str) -> None:
        """Attach a Prefect flow run ID to an ingest record."""
        t = Ingest.__table__
        stmt = (
            sa.update(t)
            .where(t.c.id == ingest_id)
            .values(flow_run_id=flow_run_id)
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    def get_ingests(self, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """List ingest records, newest first."""
        t = Ingest.__table__
        stmt = sa.select(
            t.c.id, t.c.url, t.c.status, t.c.destination,
            t.c.flow_run_id, t.c.error, t.c.created_at, t.c.completed_at,
        ).order_by(t.c.created_at.desc()).limit(limit)
        if status:
            stmt = stmt.where(t.c.status == status)
        with self._session_factory() as session:
            return [dict(r) for r in session.execute(stmt).mappings().all()]

    def get_articles_missing_scores(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return processed articles missing scores or tags — candidates for rescoring."""
        t = Article.__table__
        stmt = (
            sa.select(t.c.id, t.c.url, t.c.title, t.c.raw_text, t.c.summary)
            .where(t.c.status == "processed")
            .where(
                sa.or_(
                    t.c.score_usefulness.is_(None),
                    t.c.score_interest.is_(None),
                    t.c.score_pov.is_(None),
                    t.c.score_uniqueness.is_(None),
                    t.c.tags == sa.cast(sa.literal("{}"), sa.ARRAY(sa.Text)),
                )
            )
            .order_by(t.c.processed_at.desc())
            .limit(limit)
        )
        with self._session_factory() as session:
            rows = session.execute(stmt).fetchall()
            return [{"id": str(r[0]), "url": r[1], "title": r[2], "raw_text": r[3] or "", "summary": r[4] or ""} for r in rows]

    def update_article_scores(self, *, url: str, score_usefulness: int | None, score_interest: int | None, score_pov: int | None, score_uniqueness: int | None = None, tags: list[str] | None) -> None:
        """Update scores and tags for an existing article (rescore path)."""
        t = Article.__table__
        updates: dict[str, Any] = {}
        if score_usefulness is not None:
            updates["score_usefulness"] = score_usefulness
        if score_interest is not None:
            updates["score_interest"] = score_interest
        if score_pov is not None:
            updates["score_pov"] = score_pov
        if score_uniqueness is not None:
            updates["score_uniqueness"] = score_uniqueness
        if tags is not None:
            updates["tags"] = tags
        if not updates:
            return
        with self._session_factory() as session:
            session.execute(sa.update(t).where(t.c.url == url).values(**updates))
            session.commit()

    def get_article_urls_by_status(self, status: str, limit: int = 50) -> list[str]:
        t = Article.__table__
        stmt = (
            sa.select(t.c.url)
            .where(t.c.status == status)
            .order_by(t.c.processed_at.desc())
            .limit(limit)
        )
        with self._session_factory() as session:
            return [row[0] for row in session.execute(stmt)]

    def get_stale_pending_article_urls(self, limit: int = 50, stale_after_minutes: int = 30) -> list[str]:
        """Return URLs of articles stuck in 'pending' beyond the stale threshold."""
        t = Article.__table__
        stmt = (
            sa.select(t.c.url)
            .where(
                t.c.status == "pending",
                t.c.ingested_at < func.now() - text(f"INTERVAL '{int(stale_after_minutes)} minutes'"),
            )
            .order_by(t.c.ingested_at.asc())
            .limit(limit)
        )
        with self._session_factory() as session:
            return [row[0] for row in session.execute(stmt)]

    # ------------------------------------------------------------------
    # Watched accounts
    # ------------------------------------------------------------------

    def get_watched_accounts(
        self, *, platform: str = "twitter", active_only: bool = True
    ) -> list[dict[str, Any]]:
        t = WatchedAccount.__table__
        stmt = sa.select(
            t.c.id, t.c.handle, t.c.platform, t.c.display_name, t.c.added_reason,
            t.c.score_quality, t.c.active, t.c.last_checked_at,
        ).where(t.c.platform == platform)
        if active_only:
            stmt = stmt.where(t.c.active == True)  # noqa: E712
        stmt = stmt.order_by(t.c.score_quality.desc().nullslast(), t.c.handle.asc())
        with self._session_factory() as session:
            return [dict(r) for r in session.execute(stmt).mappings().all()]

    def upsert_watched_account(
        self,
        *,
        handle: str,
        platform: str = "twitter",
        display_name: str | None = None,
        added_reason: str | None = None,
        score_quality: int | None = None,
        active: bool = True,
    ) -> None:
        stmt = (
            pg_insert(WatchedAccount)
            .values(
                handle=handle,
                platform=platform,
                display_name=display_name,
                added_reason=added_reason,
                score_quality=score_quality,
                active=active,
            )
            .on_conflict_do_update(
                constraint="uq_watched_accounts_handle_platform",
                set_={
                    "display_name": func.coalesce(
                        sa.literal(display_name),
                        WatchedAccount.__table__.c.display_name,
                    ),
                    "added_reason": func.coalesce(
                        sa.literal(added_reason),
                        WatchedAccount.__table__.c.added_reason,
                    ),
                    "score_quality": func.coalesce(
                        sa.literal(score_quality),
                        WatchedAccount.__table__.c.score_quality,
                    ),
                    "active": active,
                },
            )
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    def mark_account_checked(self, *, handle: str, platform: str = "twitter") -> None:
        t = WatchedAccount.__table__
        with self._session_factory() as session:
            session.execute(
                sa.update(WatchedAccount)
                .where(t.c.handle == handle, t.c.platform == platform)
                .values(last_checked_at=func.now())
            )
            session.commit()

    # ------------------------------------------------------------------
    # Repos
    # ------------------------------------------------------------------

    def mark_repo_pending(self, *, url: str, owner: str, name: str) -> dict[str, Any] | None:
        """Insert a PENDING repo if none exists. Returns existing record or None on new insert."""
        stmt = (
            pg_insert(Repo)
            .values(url=url, owner=owner, name=name, status="pending",
                    ingested_at=func.now(), updated_at=func.now())
            .on_conflict_do_nothing(index_elements=["url"])
            .returning(sa.literal(None).label("existing"))
        )
        with self._session_factory() as session:
            result = session.execute(stmt).first()
            if result is not None:
                session.commit()
                return None
            # Already exists
            t = Repo.__table__
            row = session.execute(
                sa.select(
                    t.c.id, t.c.url, t.c.owner, t.c.name, t.c.status, t.c.purpose,
                    t.c.architecture, t.c.key_features, t.c.stack, t.c.tradeoffs,
                    t.c.fit_for_us, t.c.our_notes, t.c.watched, t.c.last_release,
                    t.c.last_release_at, t.c.release_notes, t.c.stars,
                    t.c.ingested_at, t.c.updated_at, t.c.error_message,
                ).where(t.c.url == url)
            ).mappings().first()
            session.commit()
            return dict(row) if row else None

    def upsert_repo(self, *, url: str, owner: str, name: str, **fields: Any) -> None:
        """Upsert a repo record. Extra keyword args map to column names."""
        allowed = {
            "description", "purpose", "architecture", "key_features", "stack",
            "tradeoffs", "fit_for_us", "our_notes", "watched",
            "last_release", "last_release_at", "last_push_at", "release_notes",
            "stars", "status", "error_message",
            "readme_text", "last_update_summary", "last_checked_at",
        }
        values: dict[str, Any] = {
            "url": url, "owner": owner, "name": name,
            "ingested_at": func.now(), "updated_at": func.now(),
        }
        for k, v in fields.items():
            if k in allowed:
                values[k] = v

        conflict_set = {"owner": owner, "name": name, "updated_at": func.now()}
        for k, v in fields.items():
            if k in allowed:
                conflict_set[k] = v

        stmt = (
            pg_insert(Repo)
            .values(**values)
            .on_conflict_do_update(index_elements=["url"], set_=conflict_set)
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    def get_repo_by_url(self, url: str) -> dict[str, Any] | None:
        """Fetch a single repo record by canonical URL."""
        t = Repo.__table__
        stmt = sa.select(
            t.c.id, t.c.url, t.c.owner, t.c.name, t.c.description, t.c.purpose,
            t.c.architecture, t.c.key_features, t.c.stack, t.c.tradeoffs,
            t.c.fit_for_us, t.c.our_notes, t.c.watched, t.c.last_release,
            t.c.last_release_at, t.c.release_notes, t.c.stars,
            t.c.ingested_at, t.c.updated_at, t.c.status, t.c.error_message,
        ).where(t.c.url == url)
        with self._session_factory() as session:
            row = session.execute(stmt).mappings().first()
            return dict(row) if row else None

    def get_repo_by_id(self, repo_id: str) -> dict[str, Any] | None:
        """Fetch a single repo record by UUID."""
        t = Repo.__table__
        stmt = sa.select(
            t.c.id, t.c.url, t.c.owner, t.c.name, t.c.description, t.c.purpose,
            t.c.architecture, t.c.key_features, t.c.stack, t.c.tradeoffs,
            t.c.fit_for_us, t.c.our_notes, t.c.watched, t.c.last_release,
            t.c.last_release_at, t.c.release_notes, t.c.stars,
            t.c.ingested_at, t.c.updated_at, t.c.status, t.c.error_message,
        ).where(t.c.id == repo_id)
        with self._session_factory() as session:
            row = session.execute(stmt).mappings().first()
            return dict(row) if row else None

    def list_repos(
        self,
        *,
        q: str | None = None,
        watched: bool | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """List or search repos. Supports FTS via ?q= and filter by ?watched=."""
        t = Repo.__table__
        stmt = sa.select(
            t.c.id, t.c.url, t.c.owner, t.c.name, t.c.description, t.c.purpose,
            t.c.architecture, t.c.key_features, t.c.stack, t.c.tradeoffs,
            t.c.fit_for_us, t.c.our_notes, t.c.watched, t.c.last_release,
            t.c.last_release_at, t.c.release_notes, t.c.stars,
            t.c.ingested_at, t.c.updated_at, t.c.status,
        )
        if watched is not None:
            stmt = stmt.where(t.c.watched == watched)
        if q:
            from data.postgres.queries import repos_fts
            ts_vec, ts_query = repos_fts(q, t)
            stmt = stmt.where(ts_vec.op("@@")(ts_query))
        stmt = stmt.order_by(t.c.updated_at.desc()).limit(limit)
        with self._session_factory() as session:
            return [dict(r) for r in session.execute(stmt).mappings().all()]

    def update_repo(self, url: str, **fields: Any) -> None:
        """Partial update for a repo (our_notes, watched, release fields, etc.)."""
        allowed = {
            "description", "purpose", "architecture", "key_features", "stack",
            "tradeoffs", "fit_for_us", "our_notes", "watched",
            "last_release", "last_release_at", "last_push_at", "release_notes",
            "stars", "status", "error_message",
            "readme_text", "last_update_summary", "last_checked_at",
        }
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return
        updates["updated_at"] = func.now()
        with self._session_factory() as session:
            session.execute(
                sa.update(Repo).where(Repo.__table__.c.url == url).values(**updates)
            )
            session.commit()

    def mark_repo_failed(self, *, url: str, error_message: str) -> None:
        """Mark a repo as failed with an error message."""
        with self._session_factory() as session:
            session.execute(
                sa.update(Repo)
                .where(Repo.__table__.c.url == url)
                .values(status="failed", error_message=error_message, updated_at=func.now())
            )
            session.commit()

    # ------------------------------------------------------------------
    # Curator note (used by discover_content)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Skills
    # ------------------------------------------------------------------

    def upsert_skill(
        self,
        *,
        name: str,
        source_url: str,
        repo_id: str | None = None,
        skill_path: str | None = None,
        description: str | None = None,
        skill_md: str | None = None,
        install_cmd: str | None = None,
    ) -> None:
        """Upsert a skill record. source_url is the canonical dedup key.

        repo_id is optional — skills can exist without a parent repo
        (e.g. ingested via direct ClaWHub link or manual add).
        """
        import uuid as _uuid
        values: dict[str, Any] = {
            "source_url": source_url,
            "name": name,
            "description": description,
            "skill_path": skill_path,
            "skill_md": skill_md,
            "install_cmd": install_cmd,
            "ingested_at": func.now(),
        }
        if repo_id is not None:
            values["repo_id"] = _uuid.UUID(str(repo_id))

        stmt = (
            pg_insert(Skill)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["source_url"],
                set_={
                    "name": name,
                    "description": description,
                    "skill_path": skill_path,
                    "skill_md": skill_md,
                    "install_cmd": install_cmd,
                    "ingested_at": func.now(),
                },
            )
        )
        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

    def get_skills_for_repo(self, repo_id: str) -> list[dict[str, Any]]:
        """Return all skills linked to a repo."""
        import uuid as _uuid
        t = Skill.__table__
        stmt = sa.select(
            t.c.id, t.c.repo_id, t.c.name, t.c.description,
            t.c.skill_path, t.c.source_url, t.c.install_cmd, t.c.ingested_at,
        ).where(t.c.repo_id == _uuid.UUID(str(repo_id))).order_by(t.c.skill_path)
        with self._session_factory() as session:
            return [dict(r) for r in session.execute(stmt).mappings().all()]

    def search_skills(self, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        """FTS search across skill name and description."""
        from data.postgres.queries import skills_fts
        t = Skill.__table__
        ts_vec, ts_query = skills_fts(query, t)
        stmt = (
            sa.select(
                t.c.id, t.c.repo_id, t.c.name, t.c.description,
                t.c.skill_path, t.c.source_url, t.c.install_cmd, t.c.ingested_at,
            )
            .where(ts_vec.op("@@")(ts_query))
            .order_by(func.ts_rank(ts_vec, ts_query).desc())
            .limit(limit)
        )
        with self._session_factory() as session:
            return [dict(r) for r in session.execute(stmt).mappings().all()]

    # ------------------------------------------------------------------
    # Curator note (used by discover_content)
    # ------------------------------------------------------------------

    def set_curator_note(self, *, url: str, curator_note: str) -> None:
        """Set curator_note on an article by URL."""
        with self._session_factory() as session:
            session.execute(
                sa.update(Article)
                .where(Article.__table__.c.url == url)
                .values(curator_note=curator_note)
            )
            session.commit()
