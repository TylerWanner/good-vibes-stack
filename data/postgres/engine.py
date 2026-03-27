"""Engine factory — thread-safe, connection-pooled SQLAlchemy engine cache.

Provides both sync engines (for Prefect flows, migrations, scripts) and
async engines (for FastAPI handlers via AsyncPostgresClient).

Uses functools.lru_cache for thread-safe caching of engines and session factories.

WARNING: The lru_cache approach is NOT fork-safe. If you run uvicorn or gunicorn
with multiple workers (--workers N where N > 1), the cached engines/connections
will be inherited by child processes and cause connection errors. Use single-worker
mode or add --preload to initialize engines before forking.
"""
from __future__ import annotations

import functools

from sqlalchemy import create_engine, Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker


def _normalize_dsn(dsn: str) -> str:
    """Convert postgresql:// or postgres:// to postgresql+psycopg2:// for SQLAlchemy 2.x.

    Using psycopg2 (not psycopg3) because psycopg3's sync driver conflicts with
    uvicorn's running asyncio event loop, causing session.execute() to hang.
    """
    if dsn.startswith("postgresql+psycopg://"):
        return dsn.replace("postgresql+psycopg://", "postgresql+psycopg2://", 1)
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg2://", 1)
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql+psycopg2://", 1)
    return dsn


def _normalize_async_dsn(dsn: str) -> str:
    """Convert postgresql:// to postgresql+psycopg:// for async psycopg3."""
    if dsn.startswith("postgresql+psycopg://"):
        return dsn
    if dsn.startswith("postgresql+psycopg2://"):
        return dsn.replace("postgresql+psycopg2://", "postgresql+psycopg://", 1)
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql+psycopg://", 1)
    return dsn


# ---------------------------------------------------------------------------
# Sync engine support (for Prefect flows, migrations, scripts)
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=8)
def get_engine(dsn: str) -> Engine:
    """Return a pooled engine for the given DSN (cached, thread-safe)."""
    normalized = _normalize_dsn(dsn)
    return create_engine(
        normalized,
        pool_size=5,
        max_overflow=5,
        pool_pre_ping=True,
    )


@functools.lru_cache(maxsize=8)
def get_session_factory(dsn: str) -> sessionmaker[Session]:
    """Return a sessionmaker bound to the pooled engine for the given DSN."""
    return sessionmaker(bind=get_engine(dsn))


# ---------------------------------------------------------------------------
# Async engine support (for FastAPI handlers)
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=8)
def get_async_engine(dsn: str) -> AsyncEngine:
    """Return a pooled async engine for the given DSN (cached, thread-safe)."""
    normalized = _normalize_async_dsn(dsn)
    return create_async_engine(
        normalized,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )


@functools.lru_cache(maxsize=8)
def get_async_session_factory(dsn: str) -> async_sessionmaker[AsyncSession]:
    """Return an async sessionmaker bound to the pooled async engine."""
    return async_sessionmaker(
        bind=get_async_engine(dsn),
        class_=AsyncSession,
        expire_on_commit=False,
    )
