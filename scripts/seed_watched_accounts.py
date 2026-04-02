"""Seed initial watched accounts for the second brain.

These are accounts already appearing in the second brain that consistently
produce high-signal content on AI infra, agent systems, and builder topics.

Run once after migration 0006:
    python scripts/seed_watched_accounts.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, "apps/second_brain/src")

from second_brain.clients.postgres import PostgresClient

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}",
)

SEED_ACCOUNTS = [
    # AI infra / agent systems builders
    {"handle": "nyk_builderz",    "score_quality": 5, "added_reason": "Building xint + OpenClaw adjacent, high signal on agent infra"},
    {"handle": "dabit3",          "score_quality": 4, "added_reason": "Web3 + AI infra builder, consistently early on tooling"},
    {"handle": "omarsar0",        "score_quality": 4, "added_reason": "ML research quality signal, papers + practical insights"},
    {"handle": "teknium",         "score_quality": 4, "added_reason": "Open source model builder, Hermes series — genuine practitioner"},
    {"handle": "dillon_mulroy",   "score_quality": 4, "added_reason": "Infra/tooling builder, high technical depth"},
    {"handle": "leonprou",        "score_quality": 4, "added_reason": "On-chain + AI intersection, relevant to Reptilian stack"},
    {"handle": "theseamouse",     "score_quality": 3, "added_reason": "AI tooling takes, worth monitoring"},
    {"handle": "sharbel",         "score_quality": 3, "added_reason": "AI product builder, early on tooling trends"},
    {"handle": "kloss_xyz",       "score_quality": 3, "added_reason": "Agent systems builder"},
    {"handle": "jakevin7",        "score_quality": 3, "added_reason": "AI infra content, worth tracking"},
    {"handle": "chenchengpro",    "score_quality": 3, "added_reason": "Model evals + benchmarks practitioner"},
    {"handle": "bradmillscan",    "score_quality": 3, "added_reason": "Crypto + AI intersection, builder signal"},
    {"handle": "jasonrosenthal",  "score_quality": 3, "added_reason": "VC/operator perspective on AI infra"},
]


def main() -> None:
    db = PostgresClient(DATABASE_URL)
    for account in SEED_ACCOUNTS:
        db.upsert_watched_account(
            handle=account["handle"],
            platform="twitter",
            score_quality=account["score_quality"],
            added_reason=account["added_reason"],
        )
        print(f"  ✓ @{account['handle']} (quality={account['score_quality']})")
    print(f"\nSeeded {len(SEED_ACCOUNTS)} watched accounts.")


if __name__ == "__main__":
    main()
