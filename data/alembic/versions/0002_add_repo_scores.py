"""add repo scores

Revision ID: 0002_add_repo_scores
Revises: 0001_initial_schema
Create Date: 2026-04-16 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_add_repo_scores"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("repos", sa.Column("score_usefulness", sa.SmallInteger(), nullable=True))
    op.add_column("repos", sa.Column("score_interest", sa.SmallInteger(), nullable=True))
    op.add_column("repos", sa.Column("score_pov", sa.SmallInteger(), nullable=True))
    op.add_column("repos", sa.Column("score_uniqueness", sa.SmallInteger(), nullable=True))

    op.create_check_constraint(
        "ck_repos_score_usefulness", "repos", "score_usefulness BETWEEN 1 AND 5"
    )
    op.create_check_constraint(
        "ck_repos_score_interest", "repos", "score_interest BETWEEN 1 AND 5"
    )
    op.create_check_constraint(
        "ck_repos_score_pov", "repos", "score_pov BETWEEN 1 AND 5"
    )
    op.create_check_constraint(
        "ck_repos_score_uniqueness", "repos", "score_uniqueness BETWEEN 1 AND 5"
    )

    op.create_index(
        "idx_repos_score_pov",
        "repos",
        ["score_pov"],
        unique=False,
        postgresql_where=sa.text("score_pov IS NOT NULL"),
    )
    op.create_index(
        "idx_repos_score_uniqueness",
        "repos",
        ["score_uniqueness"],
        unique=False,
        postgresql_where=sa.text("score_uniqueness IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_repos_score_uniqueness", table_name="repos")
    op.drop_index("idx_repos_score_pov", table_name="repos")
    op.drop_constraint("ck_repos_score_uniqueness", "repos", type_="check")
    op.drop_constraint("ck_repos_score_pov", "repos", type_="check")
    op.drop_constraint("ck_repos_score_interest", "repos", type_="check")
    op.drop_constraint("ck_repos_score_usefulness", "repos", type_="check")
    op.drop_column("repos", "score_uniqueness")
    op.drop_column("repos", "score_pov")
    op.drop_column("repos", "score_interest")
    op.drop_column("repos", "score_usefulness")
