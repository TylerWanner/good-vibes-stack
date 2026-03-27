from __future__ import annotations

"""
write_article.py — Iterative article writing flow

Architecture:
  1. Pull relevant context from second brain (optional)
  2. Qwen generates N drafts in parallel
  3. Claude scores each draft (clarity, voice, insight density, originality)
  4. Best draft selected; Claude edits for voice
  5. Claude critiques edited draft; Qwen revises
  6. Steps 4-5 repeat for `edit_rounds` iterations
  7. Final draft saved to output_path + Telegram notification

Parameters:
  topic         — What the article is about (required)
  angle         — Specific take or argument. If omitted, Claude derives from second brain context.
  format        — "thread" | "essay" | "short" (default: thread)
  drafts        — Number of parallel Qwen drafts to generate (default: 3)
  edit_rounds   — How many critique→revise cycles after draft selection (default: 2)
  output_path   — Where to save the final output (default: ARTICLE_DRAFTS_PATH env var, falls back to ./docs/drafts)
  use_second_brain — Pull relevant articles from second brain as context (default: True)
  notify        — Send Telegram notification when done (default: True)
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests as _requests
from prefect import flow, get_run_logger, task

from data.postgres.client import PostgresClient
from nervous_system.notifications.telegram import notify_telegram
from shared.config import load_settings


# ---------------------------------------------------------------------------
# Voice profile — loaded from config/voice_profile.txt
# ---------------------------------------------------------------------------

def _load_voice_profile() -> str:
    """Load voice profile from config file, with sensible fallback."""
    import os
    from pathlib import Path
    
    # Check env var first
    voice_path = os.getenv("VOICE_PROFILE_PATH")
    if voice_path:
        p = Path(voice_path)
        if p.exists():
            return p.read_text().strip()
    
    # Check config/ directory relative to repo root
    # Walk up from this file to find the repo root (contains pyproject.toml)
    current = Path(__file__).resolve().parent
    for _ in range(5):  # Max 5 levels up
        if (current / "pyproject.toml").exists():
            config_path = current / "config" / "voice_profile.txt"
            if config_path.exists():
                return config_path.read_text().strip()
        current = current.parent
    
    # Fallback — generic technical writer voice
    return """
You are writing as a technical author with a builder's perspective.

Voice characteristics:
- Direct and opinionated. No hedging.
- Technically credible. Specific tool names, real numbers.
- Honest about tradeoffs. Call out what's hard or broken.
- No fluff. Just what happened and what it means.

Format guidelines by type:
- thread: Hook tweet → 5-8 substantive tweets → landing tweet.
- essay: 600-1200 words. Clear thesis. H2 sections. Concrete takeaway.
- short: 150-300 words. One idea, fully landed.
""".strip()


VOICE_PROFILE = _load_voice_profile()


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

DRAFT_PROMPT = """You are writing a {format} for a technical audience.

Topic: {topic}
Angle: {angle}

{voice_profile}

{context_section}

Write {format_instructions}. 
Return only the draft text. No commentary, no preamble.
"""

SCORE_PROMPT = """You are evaluating article drafts. Score each on a 0-10 scale for:
- clarity: Is the argument easy to follow?
- voice: Does it sound like a builder with strong opinions, not a content farm?
- insight_density: How many concrete, specific insights per paragraph?
- originality: Does it say something worth saying, not just summarize?

Return strict JSON:
{{
  "scores": [
    {{"draft_index": 0, "clarity": 8, "voice": 7, "insight_density": 6, "originality": 8, "total": 29, "notes": "brief critique"}},
    ...
  ],
  "best_index": 0
}}

Drafts:
{drafts_json}
"""

EDIT_PROMPT = """You are editing a draft article to sharpen it.

{voice_profile}

Current draft:
{draft}

Critique from reviewer:
{critique}

Rewrite the draft incorporating the critique. Tighten the voice, increase specificity, cut anything that doesn't earn its place.
Return only the revised draft text. No commentary.
"""

CRITIQUE_PROMPT = """Critique this draft article in 3-5 bullet points. Be specific about what's weak:
- Which lines are vague or generic?
- Where does the voice slip into content-farm mode?
- What claims need more specificity or a real example?
- What should be cut?

Draft:
{draft}

Return only the bullet points. No preamble.
"""

FORMAT_INSTRUCTIONS = {
    "thread": "a Twitter/X thread. Start with a strong hook tweet (≤280 chars), follow with 5-8 substantive tweets (each ≤280 chars, each standalone), end with a landing tweet. Separate tweets with a blank line. No numbering like '1/' — just the tweets.",
    "essay": "a long-form essay (600-1200 words). Open with a clear thesis. Use ## headers for sections. End with concrete takeaways.",
    "short": "a short-form piece (150-300 words). One idea, fully landed. No headers needed.",
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch-context")
def fetch_context(topic: str, angle: str, settings: Any) -> str:
    """Pull relevant articles from second brain as writing context."""
    try:
        response = _requests.get(
            f"{settings.nervous_system_api_url}/articles",
            params={"q": f"{topic} {angle}".strip(), "limit": 8},
            timeout=15,
        )
        response.raise_for_status()
        articles = response.json()
        if not articles:
            return ""
        lines = ["Relevant context from your second brain:\n"]
        for a in articles:
            lines.append(f"- [{a.get('title', 'untitled')}]({a.get('url', '')}): {a.get('summary', '')[:200]}")
        return "\n".join(lines)
    except Exception:
        return ""


@task(name="generate-draft")
def generate_draft(
    topic: str,
    angle: str,
    format: str,
    context: str,
    draft_num: int,
    settings: Any,
) -> str:
    """Generate a single draft using Qwen (local Ollama)."""
    context_section = context if context else ""
    prompt = DRAFT_PROMPT.format(
        format=format,
        topic=topic,
        angle=angle or "derive a strong angle from the topic",
        voice_profile=VOICE_PROFILE,
        context_section=context_section,
        format_instructions=FORMAT_INSTRUCTIONS[format],
    )
    # Always use Ollama/Qwen for draft generation (saves Anthropic tokens)
    response = _requests.post(
        f"{settings.ollama_base_url}/api/chat",
        json={
            "model": settings.llm_model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {"temperature": 0.8, "num_predict": 2048},
        },
        timeout=300,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("message", {}).get("content", "").strip()


@task(name="score-drafts")
def score_drafts(drafts: list[str], settings: Any) -> dict[str, Any]:
    """Use Claude to score all drafts and pick the best one."""
    drafts_json = json.dumps(
        [{"draft_index": i, "text": d[:3000]} for i, d in enumerate(drafts)],
        indent=2,
    )
    prompt = SCORE_PROMPT.format(drafts_json=drafts_json)
    raw = _call_anthropic(prompt, max_tokens=1024)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: just return first draft
        return {"best_index": 0, "scores": []}
    return parsed


@task(name="critique-draft")
def critique_draft(draft: str, settings: Any) -> str:
    """Claude critiques the current draft."""
    prompt = CRITIQUE_PROMPT.format(draft=draft[:4000])
    return _call_anthropic(prompt, max_tokens=512)


@task(name="edit-draft")
def edit_draft(draft: str, critique: str, settings: Any) -> str:
    """Qwen revises the draft based on Claude's critique."""
    prompt = EDIT_PROMPT.format(
        voice_profile=VOICE_PROFILE,
        draft=draft[:4000],
        critique=critique,
    )
    response = _requests.post(
        f"{settings.ollama_base_url}/api/chat",
        json={
            "model": settings.llm_model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {"temperature": 0.6, "num_predict": 2048},
        },
        timeout=300,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("message", {}).get("content", "").strip()


@task(name="final-voice-pass")
def final_voice_pass(draft: str, settings: Any) -> str:
    """Claude does a final voice-sharpening pass."""
    prompt = f"""{VOICE_PROFILE}

Do a final edit on this draft. Make every sentence earn its place. Sharpen the voice — more direct, more specific, cut the filler.
Return only the revised text.

Draft:
{draft[:4000]}"""
    return _call_anthropic(prompt, max_tokens=2048)


@task(name="save-draft")
def save_draft(
    draft: str,
    topic: str,
    format: str,
    output_path: str,
    scores: dict[str, Any],
) -> str:
    """Save the final draft to disk."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    slug = topic.lower()[:40].replace(" ", "-").strip("-")
    filename = f"{timestamp}-{slug}.md"
    out_dir = Path(output_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / filename

    metadata = f"""---
topic: {topic}
format: {format}
generated_at: {timestamp}
---

"""
    full_path.write_text(metadata + draft)
    return str(full_path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _call_anthropic(prompt: str, max_tokens: int = 2048) -> str:
    from shared.secrets import load_anthropic_api_key
    api_key = load_anthropic_api_key()
    if not api_key:
        raise RuntimeError("Anthropic API key not configured")
    response = _requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-6",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=90,
    )
    response.raise_for_status()
    payload = response.json()
    content = payload.get("content", [])
    return "\n".join(p.get("text", "") for p in content if p.get("type") == "text").strip()


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="write-article")
def write_article(
    topic: str,
    angle: str = "",
    format: str = "thread",
    drafts: int = 3,
    edit_rounds: int = 2,
    output_path: str | None = None,  # Defaults to ARTICLE_DRAFTS_PATH env var if not provided
    use_second_brain: bool = True,
    notify: bool = True,
) -> dict[str, Any]:
    """
    Iterative article writing flow.

    Args:
        topic: What the article is about.
        angle: Specific take or argument (optional — Claude derives if omitted).
        format: 'thread', 'essay', or 'short'.
        drafts: Number of Qwen drafts to generate in parallel.
        edit_rounds: Critique→revise cycles after best draft selected.
        output_path: Directory to save the final draft.
        use_second_brain: Pull relevant articles as writing context.
        notify: Send Telegram notification on completion.
    """
    logger = get_run_logger()
    settings = load_settings()
    output_path = output_path or settings.article_drafts_path
    start = time.time()

    if format not in FORMAT_INSTRUCTIONS:
        raise ValueError(f"format must be one of {list(FORMAT_INSTRUCTIONS.keys())}")

    logger.info(f"Writing {format} on: {topic}")
    logger.info(f"Generating {drafts} drafts, {edit_rounds} edit rounds")

    # 1. Fetch second brain context
    context = ""
    if use_second_brain:
        context = fetch_context(topic=topic, angle=angle, settings=settings)
        logger.info(f"Context fetched: {len(context)} chars")

    # 2. Generate N drafts with Qwen
    logger.info(f"Generating {drafts} Qwen drafts...")
    draft_list = []
    for i in range(drafts):
        draft = generate_draft(
            topic=topic,
            angle=angle,
            format=format,
            context=context,
            draft_num=i,
            settings=settings,
        )
        draft_list.append(draft)
        logger.info(f"Draft {i+1}/{drafts} complete ({len(draft)} chars)")

    # 3. Claude scores all drafts, picks the best
    logger.info("Scoring drafts with Claude...")
    scoring = score_drafts(drafts=draft_list, settings=settings)
    best_idx = scoring.get("best_index", 0)
    logger.info(f"Best draft: #{best_idx + 1}")
    current_draft = draft_list[best_idx]

    # 4. Iterative critique → edit cycles
    for round_num in range(edit_rounds):
        logger.info(f"Edit round {round_num + 1}/{edit_rounds}")
        critique = critique_draft(draft=current_draft, settings=settings)
        logger.info(f"Critique: {critique[:200]}...")
        current_draft = edit_draft(draft=current_draft, critique=critique, settings=settings)
        logger.info(f"Draft revised ({len(current_draft)} chars)")

    # 5. Final voice pass by Claude
    logger.info("Final voice pass with Claude...")
    final = final_voice_pass(draft=current_draft, settings=settings)

    # 6. Save to disk
    saved_path = save_draft(
        draft=final,
        topic=topic,
        format=format,
        output_path=output_path,
        scores=scoring,
    )
    logger.info(f"Saved to: {saved_path}")

    elapsed = round(time.time() - start, 2)

    # 7. Notify
    if notify:
        notify_telegram(
            f"✍️ Article draft ready\n\n"
            f"*Topic:* {topic}\n"
            f"*Format:* {format}\n"
            f"*Drafts generated:* {drafts}\n"
            f"*Edit rounds:* {edit_rounds}\n"
            f"*Saved:* `{saved_path}`\n"
            f"*Time:* {elapsed}s"
        )

    return {
        "topic": topic,
        "format": format,
        "drafts_generated": drafts,
        "edit_rounds": edit_rounds,
        "best_draft_index": best_idx,
        "output_path": saved_path,
        "elapsed_seconds": elapsed,
        "scores": scoring,
    }


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Write an article with iterative AI editing")
    parser.add_argument("topic", help="What the article is about")
    parser.add_argument("--angle", default="", help="Specific take or argument")
    parser.add_argument("--format", default="thread", choices=["thread", "essay", "short"])
    parser.add_argument("--drafts", type=int, default=3, help="Number of Qwen drafts")
    parser.add_argument("--edit-rounds", type=int, default=2, help="Critique→revise cycles")
    parser.add_argument("--output-path", default=None, help="Output directory (default: ARTICLE_DRAFTS_PATH env var or ./docs/drafts)")
    parser.add_argument("--no-second-brain", action="store_true")
    parser.add_argument("--no-notify", action="store_true")
    args = parser.parse_args()

    result = write_article(
        topic=args.topic,
        angle=args.angle,
        format=args.format,
        drafts=args.drafts,
        edit_rounds=args.edit_rounds,
        output_path=args.output_path,
        use_second_brain=not args.no_second_brain,
        notify=not args.no_notify,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
