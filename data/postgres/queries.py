"""Shared query builders for PostgreSQL FTS and common patterns.

Keeps sync and async clients consistent without duplicating logic.
"""
from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.sql.expression import ColumnElement


def fts_match(
    query: str,
    *columns: ColumnElement,
    language: str = "english",
) -> tuple[ColumnElement, ColumnElement]:
    """Build FTS tsvector and tsquery for full-text search.
    
    Args:
        query: Search query string
        *columns: Columns to search (will be coalesced and concatenated)
        language: PostgreSQL text search language config
        
    Returns:
        Tuple of (tsvector, tsquery) for use in WHERE clause:
            .where(ts_vec.op("@@")(ts_query))
    
    Example:
        ts_vec, ts_query = fts_match(query, t.c.title, t.c.summary)
        stmt = select(...).where(ts_vec.op("@@")(ts_query))
    """
    # Build concatenated tsvector from columns
    if len(columns) == 1:
        combined = func.coalesce(columns[0], "")
    else:
        combined = func.coalesce(columns[0], "")
        for col in columns[1:]:
            combined = combined + " " + func.coalesce(col, "")
    
    ts_vec = func.to_tsvector(language, combined)
    ts_query = func.plainto_tsquery(language, query)
    
    return ts_vec, ts_query


def articles_fts(query: str, table) -> tuple[ColumnElement, ColumnElement]:
    """FTS match for articles table (title + summary)."""
    return fts_match(query, table.c.title, table.c.summary)


def repos_fts(query: str, table) -> tuple[ColumnElement, ColumnElement]:
    """FTS match for repos table (name + purpose + architecture + tradeoffs)."""
    return fts_match(query, table.c.name, table.c.purpose, table.c.architecture, table.c.tradeoffs)


def skills_fts(query: str, table) -> tuple[ColumnElement, ColumnElement]:
    """FTS match for skills table (name + description)."""
    return fts_match(query, table.c.name, table.c.description)
