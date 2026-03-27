"""FastAPI dependency injection for database sessions."""
from __future__ import annotations

from collections.abc import Generator

from sqlalchemy.orm import Session

from shared.config import load_settings
from data.postgres.engine import get_session_factory


def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session, closing it on exit."""
    settings = load_settings()
    factory = get_session_factory(settings.database_url)
    session = factory()
    try:
        yield session
    finally:
        session.close()
