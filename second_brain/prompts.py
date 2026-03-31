"""LLM prompts for intelligence layer.

Extracted from llm.py for easier customization and review.
"""

SUMMARY_PROMPT = """You are processing content for a personal knowledge base.
Return strict JSON with keys:
- summary: write a knowledge-dense extraction optimized for future retrieval, NOT a high-level summary. Preserve specific names (tools, products, people, companies), concrete techniques, workflows, examples, and numbered lists as-is. If the content lists 21 use cases, capture all 21. If it names specific tools, name them. Avoid meta-commentary like "the video explores..." or "the article discusses..." — just extract the actual knowledge. Scale length to content: a tweet gets 1-2 sentences, a long video or article gets as many sentences as needed to capture the specifics.
- tags: array of 3-8 concise topic tags
- source_type: one of tweet, article, blog, paper, docs, video, github, reddit, other
- relevant_links: array of URLs from the content that are contextually relevant and worth reading further (e.g. referenced papers, tools, repos, articles). For tweets, always include the URL of any quoted tweet. Exclude social profiles, nav links, affiliate/tracking links, and generic homepages. Empty array if none.
- score_usefulness: integer 1-5. How actionable is this for a technical builder or AI infrastructure practitioner? Use the FULL range — most content is average (2-3). Reserve 4 for genuinely useful, specific, applicable content. Reserve 5 for rare, immediately deployable insight or technique. 1=generic/vague/obvious, 2=mildly useful but surface-level, 3=solid but not exceptional, 4=concrete and directly applicable, 5=immediately deployable, rare insight.
- score_interest: integer 1-5. How likely is this to make a technically-minded builder stop scrolling? Use the FULL range. 1=routine news or announcement, 2=mildly interesting, 3=worth reading, 4=genuinely surprising or highly specific, 5=counter-intuitive, paradigm-shifting, or stop-everything insight.
- score_pov: integer 1-5. Authority signal: does the source have genuine skin in the game (built it, shipped it, lived it — not just commenting)? 1=anonymous or no demonstrated expertise. 2=credible background but nothing beyond their lane. 3=clear practitioner voice. 4=domain expert making a non-obvious claim. 5=rare — someone who has built it at scale saying something almost nobody else could say.
- score_uniqueness: integer 1-5. How distinct is this take from what else exists on the topic? 1=commodity (could be generated from reading the headline). 2=slight angle but mostly familiar. 3=some original framing or perspective. 4=a claim or connection you wouldn't find in 10 other articles on the same topic. 5=genuinely rare — a perspective almost no one else would produce.
Scoring calibration: scores should follow a rough normal distribution. The majority of content scores 2-3. A score of 4 means "notably above average." A score of 5 is rare — reserve it for content you would remember a week later. Do not default to 4.
Output JSON only. No prose outside JSON. No markdown fences.

URL: {url}
Title: {title}
Content:
{content}
"""


CHUNK_EXTRACT_PROMPT = """You are building a cumulative knowledge extraction from a long document, section by section.

What has been captured so far:
{running_summary}

Now process the next section. Extract NEW information that adds to or deepens what's already captured above. Avoid repeating what's already covered. If this section introduces new tools, names, techniques, steps, metrics, or insights not yet captured — add them. If it deepens or contradicts something already captured — note that.

IMPORTANT: Return ONLY plain prose text. NO JSON. NO bullet points. NO structured formatting. Write in dense, flowing sentences. Be specific. No meta-commentary. Just the extracted knowledge, building on what came before.

Section {chunk_num} of {chunk_total}:
{content}
"""

COMPACT_SUMMARY_PROMPT = """You are compacting a running knowledge extraction to make room for more content.

Compress the following into the most essential points — preserve all specific names (tools, people, companies), key techniques, concrete metrics, and critical claims. Drop repetition, filler, and anything that can be inferred from context. Target: ~2000 chars max.

Running extraction so far:
{running_summary}
"""

MERGE_SUMMARY_PROMPT = """You are synthesizing extracted knowledge from multiple sections of a document into a unified knowledge base entry.
Return strict JSON with keys:
- summary: a unified knowledge-dense extraction. Merge the section extracts into one coherent summary. Preserve specific names (tools, products, people, companies), concrete techniques, workflows, examples. If multiple sections cover the same topic, synthesize rather than repeat. Avoid meta-commentary. Scale length to content richness.
- tags: array of 3-8 concise topic tags covering the full document
- source_type: one of tweet, article, blog, paper, docs, video, github, reddit, other
- relevant_links: array of URLs mentioned across sections that are contextually relevant. Empty array if none.
- score_usefulness: integer 1-5. How actionable is this for a technical builder? 1=generic, 3=solid, 5=immediately deployable rare insight.
- score_interest: integer 1-5. How likely to make a builder stop scrolling? 1=routine, 3=worth reading, 5=paradigm-shifting.
- score_pov: integer 1-5. Authority signal — built it, shipped it, lived it? 1=no expertise demonstrated, 5=rare practitioner voice.
- score_uniqueness: integer 1-5. How distinct from other takes on this topic? 1=commodity, 5=perspective almost no one else would produce.
Scoring: follow a normal distribution. Most content is 2-3. Score 4 means notably above average. Score 5 is rare.
Output JSON only. No prose outside JSON. No markdown fences.

URL: {url}
Title: {title}

Extracted sections:
{extracts}
"""

TWEET_DRAFT_PROMPT = """You are drafting tweets for a technical AI practitioner account. The voice is direct, opinionated, and technically credible — no hype, no fluff. Share real observations, honest takes, and useful signals from the space.

Given a set of recent knowledge base articles, draft {count} tweet(s) worth posting. Each tweet must:
- Be under 280 characters
- Share a concrete insight, observation, or take — not a summary headline
- Sound like a practitioner, not a content farm
- Stand alone (no thread notation like 1/n unless it's a genuine multi-tweet thread)

Return strict JSON with a single key "tweets" containing an array of objects, each with:
- "text": the tweet text (under 280 chars)
- "source_url": the URL of the article it draws from

Output JSON only. No prose outside JSON. No markdown fences.

Example format:
{{"tweets": [{{"text": "example tweet", "source_url": "https://example.com"}}]}}

Recent articles:
{articles}
"""


RESEARCH_PROMPT = """You are a research assistant synthesizing multiple web sources into a clear, dense report.

Query: {query}

Sources:
{sources}

Write a research report that:
- Opens with a direct 2-3 sentence answer to the query
- Follows with key findings organized by theme (use ## headers)
- Ends with a "Key Takeaways" section (bullet list, 4-6 items)
- Cites sources inline as [1], [2], etc. matching the source list order

Be concrete and specific — names, numbers, tools, techniques. Skip filler.
Output plain text with markdown formatting. No JSON.
"""


WEEKLY_DIGEST_PROMPT = """You are preparing a weekly digest from saved reading.
Return strict JSON with keys:
- digest: markdown string with sections: Themes, Notable Ideas, Actionable Next Steps
- themes: array of short theme labels
Output JSON only. No prose outside JSON. No markdown fences.

Items:
{items}
"""

GITHUB_REPO_UPDATE_PROMPT = """You are checking whether a GitHub repository has changed in a meaningful way since it was last ingested.

You will be given:
- What we knew about the repo (stored knowledge: purpose, architecture, key features)
- New releases since the last known release (may be empty)
- The current README (to compare against the stored snapshot)
- The stored README snapshot (may be empty if this is first check)

Assess whether anything meaningful changed. "Meaningful" means: new features, breaking changes, architectural shifts, important fixes, or significant additions to the README that change understanding of what the project is or does. Version bumps, typo fixes, and minor dependency updates are NOT meaningful.

Return strict JSON with keys:
- changed: boolean — true if something meaningful changed
- update_summary: if changed=true, 2-4 sentences describing what changed and why it matters. If changed=false, empty string.
- should_reanalyze: boolean — true if the changes are significant enough to warrant a full re-analysis of purpose/architecture/key_features (e.g. major new features, architectural changes). False for minor additions.

Output JSON only. No prose outside JSON. No markdown fences.

Stored knowledge:
Purpose: {purpose}
Architecture: {architecture}
Key features: {key_features}
Last known release: {last_release}

New releases since {last_release}:
{new_releases}

Stored README snapshot:
{stored_readme}

Current README:
{current_readme}
"""

GITHUB_REPO_ANALYSIS_PROMPT = """You are analyzing a GitHub repository for a technical knowledge base.

Extract structured knowledge from the provided README, release notes, and directory structure.

Return strict JSON with keys:
- purpose: 2-3 sentences. What problem does this solve? Who is it for?
- architecture: 2-3 sentences. How is it designed/structured? Key patterns or decisions.
- key_features: array of 5-10 specific, concrete features (not marketing bullets)
- stack: array of languages, frameworks, infrastructure components
- tradeoffs: 2-3 sentences. What are the real strengths and limitations?
- fit_for_us: 1-2 sentences. How does this fit a stack running agent harnesses + workflow orchestration + FastAPI + Postgres?
- release_summary: 1-2 sentences summarizing the latest release (if provided). Empty string if none.

Be specific. Name actual features, not categories. Skip hype.
Output JSON only. No prose outside JSON. No markdown fences.

Repo: {full_name}
Description: {description}
Language: {language}
Topics: {topics}
Stars: {stars}

README:
{readme}

Latest Releases:
{releases}

CHANGELOG:
{changelog}

Directory Tree:
{tree}
"""
