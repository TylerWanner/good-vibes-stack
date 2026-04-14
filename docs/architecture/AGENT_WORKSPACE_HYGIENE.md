# Agent Workspace Hygiene

Agents get expensive and error-prone when the workspace lies to them.

This document defines a few boring rules that keep agent sessions cheaper, more reliable, and less likely to touch the wrong copy of a repo.

---

## Goals

- Minimize fixed context/token burn per session
- Reduce ambiguity about which repo/path is authoritative
- Keep deep docs available without loading them by default
- Avoid broad search output that floods context with duplicates

---

## Rules

### 1. Keep startup context lean

Preserve the framework's expected file conventions (for example: `AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `TOOLS.md`, `HEARTBEAT.md`, `USER.md`) when the runtime expects them.

The optimization target is **content**, not file shape.

Startup files should be short and role-defining, not full runbooks.

Good startup context:
- who the agent is
- what job it owns
- safety boundaries
- the names of control planes
- where deeper docs live

Bad startup context:
- long examples
- repeated persona text across multiple files
- daily notes loaded by default
- service inventories that are only needed for debugging

**Pattern:** keep the conventional file layout, but make each file do one job and avoid repeating the same instructions across all of them.

---

### 2. Do not auto-load memory/history unless the task needs it

Prior state is useful, but it should be pulled on demand.

Use prior memory/history when the task depends on:
- earlier decisions
- dates
- people
- TODOs
- previous work products

Do not pay that token cost on every new session just in case.

---

### 3. Declare a canonical repo/path

If multiple copies, mirrors, or worktrees exist, agents should know which path is authoritative by default.

Example policy:
- canonical repo path: `/workspace/repos/good-vibes-stack`
- alternate worktrees: only when the branch/path is the point of the task
- mirrored/shared copies: non-canonical unless explicitly targeted

This avoids patching the wrong tree and reduces duplicate search results.

---

### 4. Search narrowly first

Default search behavior should be:
- one repo
- one subdirectory
- one pattern
- capped output

Broad recursive search across every mounted repo is a token furnace and often returns the same answer multiple times.

---

### 5. Keep heavy runbooks on demand

Large architecture docs, service maps, and operational examples should exist — just not in the startup path.

Use a short index doc up front and keep deep runbooks for when real debugging starts.

---

### 6. Treat mirrors and shared-workspace copies carefully

Do not delete or move duplicate-looking repos blindly.

`shared-workspace/` is intentional. It is the explicit handoff mount for artifacts that should be visible to both the host and the agent without pretending they are part of the curated startup workspace.

In containerized/dev-volume setups, two paths may refer to the same underlying storage in ways that are not obvious at first glance.

Safer approach:
- mark canonical vs non-canonical paths first
- avoid searching the mirrors by default
- verify mount/path relationships before cleanup or moves

---

## Failure smells

You likely have poor workspace hygiene if:
- new sessions always start by loading large memory files
- the same repo appears under multiple active search roots
- agents regularly grep the whole workspace
- operators are unsure which copy of a file is real
- docs needed at startup are mixed with deep runbooks

---

## Desired property

A new agent session should be able to answer:
- who am I
- what do I own
- what path is canonical
- where do I go for deeper detail

without loading half the workspace to find out.
