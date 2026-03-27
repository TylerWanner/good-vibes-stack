from __future__ import annotations

import subprocess
import tempfile
import os
import re
from typing import Any


def is_youtube_url(url: str) -> bool:
    return "youtube.com/watch" in url or "youtu.be/" in url


def _clean_youtube_url(url: str) -> str:
    """Strip tracking params (si=, feature=, etc.) and normalize youtu.be to youtube.com."""
    import urllib.parse as _urlparse
    parsed = _urlparse.urlparse(url)
    # Handle youtu.be short links
    if parsed.netloc in ("youtu.be", "www.youtu.be"):
        video_id = parsed.path.lstrip("/")
        return f"https://www.youtube.com/watch?v={video_id}"
    # Strip non-essential query params — keep only v=, list=, index=, t=
    keep = {"v", "list", "index", "t"}
    qs = _urlparse.parse_qs(parsed.query, keep_blank_values=False)
    clean_qs = {k: v for k, v in qs.items() if k in keep}
    clean_query = _urlparse.urlencode({k: v[0] for k, v in clean_qs.items()})
    return _urlparse.urlunparse(parsed._replace(query=clean_query))


def fetch_youtube_document(url: str) -> dict[str, Any]:
    """Extract transcript/subtitles from a YouTube video using yt-dlp.

    Downloads auto-generated or manual subtitles, strips VTT formatting,
    and returns the plain text transcript along with video metadata.
    """
    url = _clean_youtube_url(url)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_template = os.path.join(tmpdir, "%(id)s")

        SEP = "|||FIELD_SEP|||"

        # Pass 1: get metadata via --print (fast, no download)
        meta_result = subprocess.run(
            [
                "yt-dlp",
                "--skip-download",
                "--no-playlist",
                "--print", f"%(title)s{SEP}%(channel)s{SEP}%(description)s",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if meta_result.returncode != 0:
            raise RuntimeError(f"yt-dlp metadata failed: {meta_result.stderr[:300]}")

        parts = meta_result.stdout.strip().split(SEP)
        title = parts[0].strip() if parts else ""
        channel = parts[1].strip() if len(parts) > 1 else ""
        description = parts[2].strip() if len(parts) > 2 else ""

        # Pass 2: download subtitles separately (--print suppresses subtitle writing in some versions)
        result = subprocess.run(
            [
                "yt-dlp",
                "--write-auto-sub",
                "--write-sub",
                "--sub-lang", "en",
                "--sub-format", "vtt",
                "--skip-download",
                "--no-playlist",
                "--output", output_template,
                url,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        # Non-zero is ok if subtitles just unavailable — check for real errors
        if result.returncode != 0 and "subtitles" not in result.stderr.lower():
            raise RuntimeError(f"yt-dlp subtitle pass failed: {result.stderr[:300]}")

        # Find the subtitle file
        subtitle_text = ""
        all_files = os.listdir(tmpdir)
        import logging as _logging
        _logging.getLogger(__name__).info("yt-dlp tmpdir files: %s", all_files)
        for fname in all_files:
            if ".vtt" in fname:  # catches .en.vtt, .en-US.vtt, .en-orig.vtt, etc.
                with open(os.path.join(tmpdir, fname)) as f:
                    vtt_content = f.read()
                subtitle_text = _parse_vtt(vtt_content)
                if subtitle_text:
                    break

        if not subtitle_text:
            raise RuntimeError(f"No transcript found for {url} — subtitles unavailable or yt-dlp could not extract them")

        # Combine channel + description + transcript
        parts = [f"Channel: {channel}"]
        if description:
            parts.append(f"Description:\n{description}")
        # Cap transcript at ~24000 chars (~6000 tokens, safe for qwen2.5:14b 128k context)
        transcript_excerpt = subtitle_text[:24000]
        if len(subtitle_text) > 24000:
            transcript_excerpt += "\n[transcript truncated]"
        parts.append(f"Transcript:\n{transcript_excerpt}")

        return {
            "title": title,
            "text": "\n\n".join(parts).strip(),
            "source_type": "video",
            "url": url,
            "has_transcript": bool(subtitle_text),
        }


def _parse_vtt(vtt: str) -> str:
    """Strip VTT timing/formatting markers and return plain transcript text."""
    # Remove WEBVTT header
    vtt = re.sub(r"WEBVTT.*?\n\n", "", vtt, flags=re.DOTALL)
    # Remove timestamp lines (00:00:00.000 --> 00:00:00.000)
    vtt = re.sub(r"\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}.*\n", "", vtt)
    # Remove VTT tags like <00:00:00.000>, <c>, </c>
    vtt = re.sub(r"<[^>]+>", "", vtt)
    # Remove cue identifiers (lines that are just numbers)
    vtt = re.sub(r"^\d+$", "", vtt, flags=re.MULTILINE)
    # Collapse repeated lines (VTT often duplicates lines)
    lines = vtt.split("\n")
    seen = []
    for line in lines:
        line = line.strip()
        if line and (not seen or seen[-1] != line):
            seen.append(line)
    return " ".join(seen)
